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

// EntryConfig configures a stable local entry.
type EntryConfig struct {
	// Target is the service the entry reaches.
	Target Target
	// ListenAddr is the stable local TCP address applications dial.
	ListenAddr string
	// RestartDelay overrides DefaultRestartDelay.
	RestartDelay time.Duration
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

// Serve runs one listener lifetime. It returns nil when ctx is cancelled and
// waits for its in-flight connections to finish before returning.
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

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.handle(ctx, conn)
		}()
	}
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
