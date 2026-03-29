// Package agent implements the OutpostNet Agent, which manages QUIC tunnels
// between edge regions and provides cross-region service access.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// Agent manages QUIC tunnels and cross-region service bridging.
type Agent struct {
	cfg    *agentconfig.Config
	logger *slog.Logger

	tunnels       *TunnelManager
	persistence   *Persistence
	metrics       *Metrics
	healthServer  *http.Server
	metricsServer *http.Server

	mu     sync.Mutex
	ready  bool
	cancel context.CancelFunc
}

// New creates a new Agent.
func New(cfg *agentconfig.Config, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{
		cfg:    cfg,
		logger: logger,
	}
}

// Run starts the Agent and blocks until the context is cancelled or Stop is called.
func (a *Agent) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	a.cancel = cancel
	a.mu.Unlock()
	defer cancel()

	a.logger.Info("agent starting",
		"tunnels", len(a.cfg.Tunnels),
		"exports", len(a.cfg.Exports),
		"imports", len(a.cfg.Imports),
	)

	// Start health check server.
	if err := a.startHealthServer(); err != nil {
		return fmt.Errorf("start health server: %w", err)
	}
	defer a.stopHealthServer()

	// Start metrics server.
	a.metrics = NewMetrics()
	if err := a.startMetricsServer(); err != nil {
		return fmt.Errorf("start metrics server: %w", err)
	}
	defer a.stopMetricsServer()

	// Initialize persistence if enabled.
	if a.cfg.Persistence.IsEnabled() {
		a.persistence = NewPersistence(a.cfg.Persistence.Dir, a.logger)
		// Persist initial config.
		if err := a.persistence.Save(a.cfg, 1); err != nil {
			a.logger.Warn("failed to persist initial config", "error", err)
		}
	}

	// Start tunnel manager if tunnels are configured.
	if len(a.cfg.Tunnels) > 0 {
		a.tunnels = NewTunnelManager(a.cfg, a.logger, a.metrics)
		if err := a.tunnels.Start(ctx); err != nil {
			return fmt.Errorf("start tunnels: %w", err)
		}
		defer a.tunnels.Stop()
	}

	// Mark ready after all components are up.
	a.mu.Lock()
	a.ready = true
	a.mu.Unlock()

	a.logger.Info("agent ready", "health_addr", a.cfg.HealthAddr)

	// Block until context is cancelled.
	<-ctx.Done()

	a.logger.Info("agent shutting down")
	return nil
}

// Stop initiates graceful shutdown.
func (a *Agent) Stop() {
	a.mu.Lock()
	cancel := a.cancel
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (a *Agent) startHealthServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)

	a.healthServer = &http.Server{
		Addr:              a.cfg.HealthAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := a.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Give it a moment to fail on bind errors.
	select {
	case err := <-errCh:
		return err
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func (a *Agent) startMetricsServer() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", a.metrics.Handler())

	a.metricsServer = &http.Server{
		Addr:              a.cfg.Metrics.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := a.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-time.After(100 * time.Millisecond):
		a.logger.Info("metrics server started", "addr", a.cfg.Metrics.Addr)
		return nil
	}
}

func (a *Agent) stopMetricsServer() {
	if a.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.metricsServer.Shutdown(ctx)
	}
}

func (a *Agent) stopHealthServer() {
	if a.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.healthServer.Shutdown(ctx)
	}
}

func (a *Agent) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (a *Agent) handleReadyz(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	ready := a.ready
	a.mu.Unlock()

	if ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("not ready"))
	}
}
