package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// StreamFallback is called when a stream arrives for a service with no local
// route. The handler receives the stream header (including hop count for
// relay streams) and the raw QUIC stream for bidirectional forwarding.
type StreamFallback func(ctx context.Context, stream *quic.Stream, hdr StreamHeader)

// Server is a QUIC tunnel server that accepts connections and routes
// incoming streams to local TCP services based on the ServiceHeader.
type Server struct {
	cfg    ServerConfig
	logger *slog.Logger

	// lnMu guards listener: ListenAndServe sets it on its own goroutine while
	// callers poll Addr() or call Close() concurrently.
	lnMu     sync.Mutex
	listener *quic.Listener

	mu       sync.RWMutex
	routes   map[string]string // serviceName → localAddr
	fallback StreamFallback

	// peers maps a dialer's declared node_id to its latest accepted
	// connection (ADR-0003 D5), so this side can open reverse streams.
	peersMu sync.Mutex
	peers   map[string]*quic.Conn
}

// ErrNoPeerConn means no accepted connection is registered for the node_id:
// the peer has not dialed this server, did not declare a node_id (older
// version or no auth token configured), or its connection closed.
var ErrNoPeerConn = errors.New("no accepted connection from peer")

// OpenPeerStream opens a service stream toward a peer over the connection
// that peer dialed into this server. The peer routes it with its own route
// table, exactly as this server routes streams the peer opens.
func (s *Server) OpenPeerStream(ctx context.Context, nodeID, serviceName string) (*quic.Stream, error) {
	s.peersMu.Lock()
	conn := s.peers[nodeID]
	s.peersMu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("peer %q: %w", nodeID, ErrNoPeerConn)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream to accepted peer %q: %w", nodeID, err)
	}
	if err := WriteServiceHeader(stream, serviceName); err != nil {
		stream.CancelRead(0)
		stream.Close()
		return nil, err
	}
	return stream, nil
}

// AcceptedPeers returns the node_ids with a registered accepted connection.
func (s *Server) AcceptedPeers() []string {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	out := make([]string, 0, len(s.peers))
	for id := range s.peers {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// registerPeer makes conn the reverse-stream channel for nodeID. A newer
// connection from the same node_id replaces the older registration; the
// older connection itself is left to close on its own.
func (s *Server) registerPeer(nodeID string, conn *quic.Conn) {
	s.peersMu.Lock()
	old := s.peers[nodeID]
	s.peers[nodeID] = conn
	s.peersMu.Unlock()
	if old != nil && old != conn {
		s.logger.Info("accepted peer connection replaced", "node_id", nodeID, "remote", conn.RemoteAddr())
	} else {
		s.logger.Info("accepted peer connection registered", "node_id", nodeID, "remote", conn.RemoteAddr())
	}
}

// unregisterPeer drops nodeID's registration if it still points at conn.
func (s *Server) unregisterPeer(nodeID string, conn *quic.Conn) {
	s.peersMu.Lock()
	if s.peers[nodeID] == conn {
		delete(s.peers, nodeID)
	}
	s.peersMu.Unlock()
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
		peers:  make(map[string]*quic.Conn),
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

// SetStreamFallback registers a handler that is called when a stream arrives
// for a service with no local route. This is used by the relay manager.
func (s *Server) SetStreamFallback(fn StreamFallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fallback = fn
}

func (s *Server) getFallback() StreamFallback {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fallback
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
	s.lnMu.Lock()
	s.listener = listener
	s.lnMu.Unlock()
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
	s.lnMu.Lock()
	defer s.lnMu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close stops the server.
func (s *Server) Close() error {
	s.lnMu.Lock()
	listener := s.listener
	s.lnMu.Unlock()
	if listener != nil {
		return listener.Close()
	}
	return nil
}

func (s *Server) handleConnection(ctx context.Context, conn *quic.Conn) {
	s.logger.Info("accepted connection", "remote", conn.RemoteAddr())

	// If token auth is configured, authenticate before processing service streams.
	if s.cfg.Token != "" {
		peerID, err := s.authenticateConn(ctx, conn)
		if err != nil {
			s.logger.Warn("authentication failed", "remote", conn.RemoteAddr(), "error", err)
			conn.CloseWithError(1, "authentication failed")
			return
		}
		s.logger.Debug("authentication succeeded", "remote", conn.RemoteAddr(), "declared_node_id", peerID)
		if peerID != "" {
			s.registerPeer(peerID, conn)
			defer s.unregisterPeer(peerID, conn)
		}
	}

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

// ServeStream routes one incoming service stream through this server's route
// table and fallback, exactly like a stream accepted by the server itself.
// A dialer uses it for reverse streams opened by the peer it dialed. It
// returns when forwarding ends and closes the stream.
func (s *Server) ServeStream(ctx context.Context, stream *quic.Stream) {
	s.handleStream(ctx, stream)
}

func (s *Server) handleStream(ctx context.Context, stream *quic.Stream) {
	defer stream.Close()
	// Close only ends the write side. Release the receive side as well,
	// even when a rejected stream returns before reading the peer's FIN.
	defer stream.CancelRead(0)

	// Read service header (handles both standard and relay headers).
	hdr, err := ReadStreamHeader(stream)
	if err != nil {
		s.logger.Error("read service header", "error", err)
		return
	}

	localAddr, ok := s.getRoute(hdr.ServiceName)
	if !ok {
		if hdr.WantsConfirm {
			// Confirmed streams are never relayed: the opener must learn
			// that this provider cannot serve it, before any data moves.
			s.logger.Debug("no route for confirmed stream", "service", hdr.ServiceName)
			_ = writeStreamAck(stream, StreamAckNoRoute)
			return
		}
		// No local route — try the fallback (relay handler).
		if fb := s.getFallback(); fb != nil {
			fb(ctx, stream, hdr)
			return
		}
		s.logger.Warn("no route for service", "service", hdr.ServiceName)
		return
	}

	s.logger.Debug("routing stream", "service", hdr.ServiceName, "addr", localAddr)

	// Connect to local service.
	localConn, err := net.Dial("tcp", localAddr)
	if err != nil {
		s.logger.Error("dial local service", "service", hdr.ServiceName, "addr", localAddr, "error", err)
		if hdr.WantsConfirm {
			_ = writeStreamAck(stream, StreamAckDialFailed)
		}
		return
	}
	defer localConn.Close()
	if hdr.WantsConfirm {
		if err := writeStreamAck(stream, StreamAckOK); err != nil {
			return
		}
	}

	// Bidirectional forwarding.
	relay(stream, localConn)
}

// authenticateConn performs the auth handshake on the first stream of a new connection.
// The client opens a stream and sends its token; the server responds with
// authOK or authFail. After authOK it reads the optional node_id declaration
// (see writePeerHello) and returns it ("" when the dialer sent none).
func (s *Server) authenticateConn(ctx context.Context, conn *quic.Conn) (string, error) {
	authCtx, cancel := context.WithTimeout(ctx, authHandshakeTimeout*time.Second)
	defer cancel()

	stream, err := conn.AcceptStream(authCtx)
	if err != nil {
		return "", fmt.Errorf("accept auth stream: %w", err)
	}
	defer stream.Close()

	deadline := time.Now().Add(authHandshakeTimeout * time.Second)
	stream.SetReadDeadline(deadline)
	stream.SetWriteDeadline(deadline)

	token, err := readAuthToken(stream)
	if err != nil {
		return "", fmt.Errorf("read auth token: %w", err)
	}

	if token != s.cfg.Token {
		stream.Write([]byte{authFail}) //nolint:errcheck — best-effort, connection closes anyway
		return "", fmt.Errorf("token mismatch")
	}

	if _, err := stream.Write([]byte{authOK}); err != nil {
		return "", fmt.Errorf("write auth response: %w", err)
	}
	// An older dialer closes its side after reading authOK (clean EOF); a
	// newer one already sent the declaration with the token.
	peerID, err := readPeerHello(stream)
	if err != nil {
		return "", err
	}
	return peerID, nil
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
