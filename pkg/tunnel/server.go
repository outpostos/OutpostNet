package tunnel

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

// Server is a QUIC tunnel server that accepts connections and routes
// incoming streams to local TCP services based on the ServiceHeader.
type Server struct {
	cfg      ServerConfig
	listener *quic.Listener
	logger   *slog.Logger

	mu     sync.RWMutex
	routes map[string]string // serviceName → localAddr
}

// NewServer creates a new tunnel server with the given configuration.
func NewServer(cfg ServerConfig, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:    cfg,
		logger: logger,
		routes: make(map[string]string),
	}
}

// AddRoute registers a service route. When a client opens a stream for
// the given service name, it will be forwarded to localAddr.
func (s *Server) AddRoute(serviceName, localAddr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routes[serviceName] = localAddr
	s.logger.Info("added route", "service", serviceName, "addr", localAddr)
}

// RemoveRoute removes a service route.
func (s *Server) RemoveRoute(serviceName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.routes, serviceName)
	s.logger.Info("removed route", "service", serviceName)
}

// getRoute returns the local address for a service, or empty string if not found.
func (s *Server) getRoute(serviceName string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	addr, ok := s.routes[serviceName]
	return addr, ok
}

// ListenAndServe starts listening for QUIC connections and serving them.
// It blocks until the context is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := Listen(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.listener = listener
	s.logger.Info("tunnel server listening", "addr", listener.Addr())

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.logger.Error("accept connection", "error", err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

// Addr returns the listener's address. Only valid after ListenAndServe starts.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close stops the server.
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConnection(ctx context.Context, conn *quic.Conn) {
	s.logger.Info("accepted connection", "remote", conn.RemoteAddr())
	defer conn.CloseWithError(0, "server closing")

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Debug("accept stream ended", "remote", conn.RemoteAddr(), "error", err)
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleStream(ctx, stream)
		}()
	}
}

func (s *Server) handleStream(ctx context.Context, stream *quic.Stream) {
	defer stream.Close()

	// Read service header.
	serviceName, err := ReadServiceHeader(stream)
	if err != nil {
		s.logger.Error("read service header", "error", err)
		return
	}

	localAddr, ok := s.getRoute(serviceName)
	if !ok {
		s.logger.Warn("no route for service", "service", serviceName)
		return
	}

	s.logger.Debug("routing stream", "service", serviceName, "addr", localAddr)

	// Connect to local service.
	localConn, err := net.Dial("tcp", localAddr)
	if err != nil {
		s.logger.Error("dial local service", "service", serviceName, "addr", localAddr, "error", err)
		return
	}
	defer localConn.Close()

	// Bidirectional forwarding.
	relay(stream, localConn)
}

// relay copies data bidirectionally between two ReadWriteClosers.
func relay(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(b, a)
		if tc, ok := b.(interface{ CloseWrite() error }); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		io.Copy(a, b)
		if tc, ok := a.(interface{ CloseWrite() error }); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()

	// Wait for at least one direction to finish.
	<-done
}
