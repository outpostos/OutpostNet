package service

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// DefaultRestartDelay is the pause before an entry re-listens after its
// listener failed (matches the pre-refactor import behavior).
const DefaultRestartDelay = 1 * time.Second

// DefaultShutdownGrace is how long in-flight connections may keep running
// after the entry is cancelled before the entry closes them (matches the
// tunnel GracefulShutdown timeout).
const DefaultShutdownGrace = 5 * time.Second

// EntryConfig configures a stable local entry.
type EntryConfig struct {
	// Target is the service the entry reaches.
	Target Target
	// ListenAddr is the stable local TCP address applications dial.
	ListenAddr string
	// RestartDelay overrides DefaultRestartDelay.
	RestartDelay time.Duration
	// ShutdownGrace overrides DefaultShutdownGrace.
	ShutdownGrace time.Duration
}

// Entry is a stable local TCP entry for one service target. Each accepted
// connection opens a fresh stream through the StreamOpener, so the entry
// follows endpoint changes for new connections. Existing connections stay on
// the stream they were given and end when that stream ends.
type Entry struct {
	cfg    EntryConfig
	opener StreamOpener
	logger *slog.Logger

	mu   sync.Mutex
	addr net.Addr
}

// NewEntry creates an entry.
func NewEntry(cfg EntryConfig, opener StreamOpener, logger *slog.Logger) *Entry {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.RestartDelay == 0 {
		cfg.RestartDelay = DefaultRestartDelay
	}
	if cfg.ShutdownGrace == 0 {
		cfg.ShutdownGrace = DefaultShutdownGrace
	}
	return &Entry{
		cfg:    cfg,
		opener: opener,
		logger: logger.With("service", cfg.Target.String(), "listen", cfg.ListenAddr),
	}
}

// Addr returns the bound listener address, or nil when not listening.
func (e *Entry) Addr() net.Addr {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.addr
}

func (e *Entry) setAddr(a net.Addr) {
	e.mu.Lock()
	e.addr = a
	e.mu.Unlock()
}

// Run serves the entry until ctx is cancelled, re-listening after listener
// failures.
func (e *Entry) Run(ctx context.Context) {
	e.logger.Info("serving service entry")
	for {
		err := e.Serve(ctx)
		if ctx.Err() != nil {
			return
		}
		e.logger.Error("service entry stopped, restarting...", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.cfg.RestartDelay):
		}
	}
}

// Serve runs one listener lifetime. It returns nil when ctx is cancelled.
// Before returning it waits for its in-flight connections; once ctx is
// cancelled they get ShutdownGrace to finish and are then closed, so Serve
// (and Run) always return after cancellation.
func (e *Entry) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", e.cfg.ListenAddr)
	if err != nil {
		return err
	}
	e.setAddr(ln.Addr())
	defer e.setAddr(nil)
	defer ln.Close()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			ln.Close()
		case <-stop:
		}
	}()

	var (
		wg      sync.WaitGroup
		connsMu sync.Mutex
		conns   = make(map[net.Conn]struct{})
	)
	defer e.drain(ctx, &wg, &connsMu, conns)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		connsMu.Lock()
		conns[conn] = struct{}{}
		connsMu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				connsMu.Lock()
				delete(conns, conn)
				connsMu.Unlock()
			}()
			e.handle(ctx, conn)
		}()
	}
}

// drain waits for in-flight connections. While ctx is live (listener
// failure) it waits for them to end on their own, as before; once ctx is
// cancelled it allows ShutdownGrace and then closes what is left, which ends
// each pipe and releases its stream.
func (e *Entry) drain(ctx context.Context, wg *sync.WaitGroup, mu *sync.Mutex, conns map[net.Conn]struct{}) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-ctx.Done():
	}
	select {
	case <-done:
		return
	case <-time.After(e.cfg.ShutdownGrace):
	}
	mu.Lock()
	n := len(conns)
	for c := range conns {
		c.Close()
	}
	mu.Unlock()
	e.logger.Info("closed in-flight connections after shutdown grace", "count", n, "grace", e.cfg.ShutdownGrace)
	<-done
}

func (e *Entry) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	stream, err := e.opener.OpenStream(ctx, e.cfg.Target)
	if err != nil {
		e.logger.Warn("service target pending or unavailable", "error", err)
		return
	}
	defer stream.Close()
	pipe(stream, conn)
}

// pipe copies bidirectionally and returns when either direction finishes;
// callers close both ends afterwards (pre-refactor import semantics).
func pipe(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(b, a)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(a, b)
		done <- struct{}{}
	}()
	<-done
}
