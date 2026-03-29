package tunnel

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Client is a QUIC tunnel client that dials a tunnel server and opens
// streams for imported services.
type Client struct {
	cfg    ClientConfig
	conn   *quic.Conn
	logger *slog.Logger
	mu     sync.Mutex
}

// NewClient creates a new tunnel client.
func NewClient(cfg ClientConfig, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		cfg:    cfg,
		logger: logger,
	}
}

// Connect establishes the QUIC connection to the tunnel server.
// If a token is configured, it performs the auth handshake before returning.
func (c *Client) Connect(ctx context.Context) error {
	conn, err := dialAndAuth(ctx, c.cfg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.logger.Info("connected to tunnel server", "addr", c.cfg.ServerAddr)
	return nil
}

// dialAndAuth dials the tunnel server and, if a token is configured,
// performs the client-side auth handshake before returning the connection.
// The connection is closed and an error returned on any failure.
func dialAndAuth(ctx context.Context, cfg ClientConfig) (*quic.Conn, error) {
	conn, err := Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Token == "" {
		return conn, nil
	}

	authCtx, cancel := context.WithTimeout(ctx, authHandshakeTimeout*time.Second)
	defer cancel()

	stream, err := conn.OpenStreamSync(authCtx)
	if err != nil {
		conn.CloseWithError(1, "auth failed")
		return nil, fmt.Errorf("open auth stream: %w", err)
	}
	defer stream.Close()

	deadline := time.Now().Add(authHandshakeTimeout * time.Second)
	stream.SetReadDeadline(deadline)
	stream.SetWriteDeadline(deadline)

	if err := writeAuthToken(stream, cfg.Token); err != nil {
		conn.CloseWithError(1, "auth failed")
		return nil, fmt.Errorf("write auth token: %w", err)
	}

	resp := make([]byte, 1)
	if _, err := io.ReadFull(stream, resp); err != nil {
		conn.CloseWithError(1, "auth failed")
		return nil, fmt.Errorf("read auth response: %w", err)
	}

	if resp[0] != authOK {
		conn.CloseWithError(1, "auth failed")
		return nil, fmt.Errorf("tunnel auth: server rejected authentication")
	}

	return conn, nil
}

// OpenServiceStream opens a new QUIC stream and sends the ServiceHeader.
// The returned stream is ready for bidirectional data transfer.
func (c *Client) OpenServiceStream(ctx context.Context, serviceName string) (*quic.Stream, error) {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return nil, net.ErrClosed
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	if err := WriteServiceHeader(stream, serviceName); err != nil {
		stream.Close()
		return nil, err
	}

	return stream, nil
}

// ServeImport listens on listenAddr and for each incoming TCP connection,
// opens a QUIC stream to the tunnel server for the given service.
// It blocks until the context is cancelled.
func (c *Client) ServeImport(ctx context.Context, serviceName, listenAddr string) error {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	c.logger.Info("serving import", "service", serviceName, "listen", ln.Addr())

	// Close listener when context is done.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("accept", "error", err)
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			c.handleImportConn(ctx, serviceName, conn)
		}()
	}
}

func (c *Client) handleImportConn(ctx context.Context, serviceName string, conn net.Conn) {
	defer conn.Close()

	stream, err := c.OpenServiceStream(ctx, serviceName)
	if err != nil {
		c.logger.Error("open service stream", "service", serviceName, "error", err)
		return
	}
	defer stream.Close()

	c.logger.Debug("relaying import", "service", serviceName, "remote", conn.RemoteAddr())
	relay(stream, conn)
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn != nil {
		return conn.CloseWithError(0, "client closing")
	}
	return nil
}
