package tunnel

import (
	"context"
	"log/slog"
	"math"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// ConnState represents the connection state of a ManagedClient.
type ConnState int

const (
	StateDisconnected ConnState = iota
	StateConnecting
	StateConnected
	StateReconnecting
)

func (s ConnState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	default:
		return "unknown"
	}
}

// StateCallback is called when the connection state changes.
type StateCallback func(state ConnState)

// ManagedClientConfig extends ClientConfig with lifecycle management options.
type ManagedClientConfig struct {
	ClientConfig

	// InitialReconnectDelay is the initial delay before reconnecting. Default: 1s.
	InitialReconnectDelay time.Duration

	// MaxReconnectDelay is the maximum reconnect delay (exponential backoff cap). Default: 30s.
	MaxReconnectDelay time.Duration

	// OnStateChange is called when the connection state changes.
	OnStateChange StateCallback
}

// ManagedClient wraps Client with automatic reconnection and lifecycle management.
type ManagedClient struct {
	cfg    ManagedClientConfig
	logger *slog.Logger

	mu    sync.RWMutex
	conn  *quic.Conn
	state ConnState

	// activeStreams tracks in-flight streams for graceful shutdown.
	streamWg sync.WaitGroup

	// cancel for the managed lifecycle goroutine.
	cancel context.CancelFunc
}

// NewManagedClient creates a new managed client.
func NewManagedClient(cfg ManagedClientConfig, logger *slog.Logger) *ManagedClient {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.InitialReconnectDelay == 0 {
		cfg.InitialReconnectDelay = 1 * time.Second
	}
	if cfg.MaxReconnectDelay == 0 {
		cfg.MaxReconnectDelay = 30 * time.Second
	}
	return &ManagedClient{
		cfg:    cfg,
		logger: logger,
		state:  StateDisconnected,
	}
}

func (mc *ManagedClient) setState(s ConnState) {
	mc.mu.Lock()
	mc.state = s
	mc.mu.Unlock()
	if mc.cfg.OnStateChange != nil {
		mc.cfg.OnStateChange(s)
	}
}

// State returns the current connection state.
func (mc *ManagedClient) State() ConnState {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.state
}

// Connect establishes the initial connection and starts monitoring.
// It returns after the first connection is established or the context is cancelled.
func (mc *ManagedClient) Connect(ctx context.Context) error {
	mc.setState(StateConnecting)

	conn, err := Dial(ctx, mc.cfg.ClientConfig)
	if err != nil {
		mc.setState(StateDisconnected)
		return err
	}

	mc.mu.Lock()
	mc.conn = conn
	mc.mu.Unlock()
	mc.setState(StateConnected)

	mc.logger.Info("managed client connected", "addr", mc.cfg.ServerAddr)

	// Start monitoring the connection for failures.
	monitorCtx, cancel := context.WithCancel(ctx)
	mc.cancel = cancel
	go mc.monitor(monitorCtx)

	return nil
}

// monitor watches for connection failure and triggers reconnection.
func (mc *ManagedClient) monitor(ctx context.Context) {
	for {
		mc.mu.RLock()
		conn := mc.conn
		mc.mu.RUnlock()

		if conn == nil {
			return
		}

		// Wait for connection to close (context done on the connection).
		select {
		case <-ctx.Done():
			return
		case <-conn.Context().Done():
			// Connection lost.
			mc.logger.Warn("connection lost, reconnecting...", "error", conn.Context().Err())
			mc.reconnect(ctx)
		}
	}
}

func (mc *ManagedClient) reconnect(ctx context.Context) {
	mc.setState(StateReconnecting)

	delay := mc.cfg.InitialReconnectDelay
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			mc.setState(StateDisconnected)
			return
		case <-time.After(delay):
		}

		attempt++
		mc.logger.Info("reconnect attempt", "attempt", attempt, "delay", delay)

		conn, err := Dial(ctx, mc.cfg.ClientConfig)
		if err != nil {
			mc.logger.Error("reconnect failed", "attempt", attempt, "error", err)
			// Exponential backoff.
			delay = time.Duration(math.Min(
				float64(delay)*2,
				float64(mc.cfg.MaxReconnectDelay),
			))
			continue
		}

		mc.mu.Lock()
		mc.conn = conn
		mc.mu.Unlock()
		mc.setState(StateConnected)
		mc.logger.Info("reconnected", "attempt", attempt)
		return
	}
}

// OpenServiceStream opens a stream on the current connection.
// Tracks the stream for graceful shutdown.
func (mc *ManagedClient) OpenServiceStream(ctx context.Context, serviceName string) (*quic.Stream, error) {
	mc.mu.RLock()
	conn := mc.conn
	mc.mu.RUnlock()

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

	mc.streamWg.Add(1)
	return stream, nil
}

// StreamDone signals that a stream has completed. Call this after closing
// a stream obtained from OpenServiceStream.
func (mc *ManagedClient) StreamDone() {
	mc.streamWg.Done()
}

// GracefulShutdown waits for all in-flight streams to complete, then closes.
func (mc *ManagedClient) GracefulShutdown(timeout time.Duration) error {
	mc.logger.Info("graceful shutdown initiated", "timeout", timeout)

	done := make(chan struct{})
	go func() {
		mc.streamWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mc.logger.Info("all streams completed, closing connection")
	case <-time.After(timeout):
		mc.logger.Warn("shutdown timeout, forcing close")
	}

	return mc.Close()
}

// Close immediately closes the client connection.
func (mc *ManagedClient) Close() error {
	if mc.cancel != nil {
		mc.cancel()
	}

	mc.mu.Lock()
	conn := mc.conn
	mc.conn = nil
	mc.mu.Unlock()

	mc.setState(StateDisconnected)

	if conn != nil {
		return conn.CloseWithError(0, "managed client closing")
	}
	return nil
}
