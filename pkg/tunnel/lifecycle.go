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

// ServerAddr returns the current remote tunnel server address.
func (mc *ManagedClient) ServerAddr() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.cfg.ServerAddr
}

// Connect establishes the initial connection and starts monitoring.
// It returns after the first connection is established or the context is cancelled.
func (mc *ManagedClient) Connect(ctx context.Context) error {
	mc.setState(StateConnecting)

	mc.mu.RLock()
	cfg := mc.cfg.ClientConfig
	mc.mu.RUnlock()
	conn, err := dialAndAuth(ctx, cfg)
	if err != nil {
		mc.setState(StateDisconnected)
		return err
	}

	mc.mu.Lock()
	mc.conn = conn
	mc.mu.Unlock()
	mc.setState(StateConnected)

	mc.logger.Info("managed client connected", "addr", cfg.ServerAddr)

	// Start monitoring the connection for failures.
	monitorCtx, cancel := context.WithCancel(ctx)
	mc.cancel = cancel
	go mc.monitor(monitorCtx)

	return nil
}

// ConnectUntilReady retries the initial connection with the managed client's
// normal exponential backoff until it succeeds or ctx is cancelled.
func (mc *ManagedClient) ConnectUntilReady(ctx context.Context) {
	delay := mc.cfg.InitialReconnectDelay
	for {
		if err := mc.Connect(ctx); err == nil {
			return
		} else {
			mc.logger.Warn("initial connect failed, retrying", "addr", mc.ServerAddr(), "delay", delay, "error", err)
		}

		select {
		case <-ctx.Done():
			mc.setState(StateDisconnected)
			return
		case <-time.After(delay):
		}

		delay = time.Duration(math.Min(
			float64(delay)*2,
			float64(mc.cfg.MaxReconnectDelay),
		))
	}
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
		mc.mu.RLock()
		cfg := mc.cfg.ClientConfig
		mc.mu.RUnlock()
		mc.logger.Info("reconnect attempt", "attempt", attempt, "delay", delay, "addr", cfg.ServerAddr)

		conn, err := dialAndAuth(ctx, cfg)
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

// UpdateServerAddr closes the active connection so the existing managed
// reconnect loop redials the new address with its normal backoff settings.
func (mc *ManagedClient) UpdateServerAddr(ctx context.Context, addr string) error {
	mc.mu.Lock()
	if mc.cfg.ServerAddr == addr {
		mc.mu.Unlock()
		return nil
	}
	oldAddr := mc.cfg.ServerAddr
	mc.cfg.ServerAddr = addr
	conn := mc.conn
	mc.conn = nil
	mc.mu.Unlock()

	mc.logger.Info("managed client remote address changed", "old_addr", oldAddr, "new_addr", addr)
	if conn != nil {
		_ = conn.CloseWithError(0, "managed client remote address changed")
	}
	return nil
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

// OpenRelayStream opens a stream with a relay header (includes hop count).
// Tracks the stream for graceful shutdown, so call StreamDone when finished.
func (mc *ManagedClient) OpenRelayStream(ctx context.Context, serviceName string, hopCount uint8) (*quic.Stream, error) {
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

	if err := WriteRelayHeader(stream, serviceName, hopCount); err != nil {
		stream.Close()
		return nil, err
	}

	mc.streamWg.Add(1)
	return stream, nil
}

// StreamDone signals that a stream has completed. Call this after closing
// a stream obtained from OpenServiceStream or OpenRelayStream.
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
