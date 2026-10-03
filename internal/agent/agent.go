// Package agent implements the OutpostNet Agent, which manages QUIC tunnels
// between edge regions and provides cross-region service access.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/internal/observation"
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
	mgmtServer    *ManagementServer

	mu          sync.Mutex
	ready       bool
	cancel      context.CancelFunc
	healthAddr  net.Addr
	metricsAddr net.Addr

	observationAddr string
	observation     *Observation
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

	// The tunnel manager exists when static tunnels or dynamic imports are
	// configured, or when node_id enables the peer directory.
	startTunnels := len(a.cfg.Tunnels) > 0 || hasServiceImports(a.cfg.Imports)
	if startTunnels || a.cfg.NodeID != "" {
		tm := NewTunnelManager(a.cfg, a.logger, a.metrics)
		a.mu.Lock()
		a.tunnels = tm
		a.mu.Unlock()
	}

	// Observation starts before the tunnels so their first connection
	// attempts are reported; it never fails the agent.
	a.mu.Lock()
	observationAddr := a.observationAddr
	a.mu.Unlock()
	obs := StartObservation(ctx, ObservationConfig{
		Addr:      observationAddr,
		NodeID:    a.cfg.NodeID,
		Component: observation.ComponentNet,
	}, a.tunnels, a.logger)
	defer obs.Wait()
	defer cancel()
	a.mu.Lock()
	a.observation = obs
	a.mu.Unlock()

	if startTunnels {
		if err := a.tunnels.Start(ctx); err != nil {
			return fmt.Errorf("start tunnels: %w", err)
		}
		a.tunnels.StartDynamicImports(ctx)
		defer a.tunnels.Stop()
	}

	if a.cfg.NodeID != "" {
		directory := NewPeerDirectory(a.cfg.NodeID, a.tunnels, a.logger)
		a.mgmtServer = NewManagementServer(a.cfg.Management.SocketPath, directory, a.logger)
		if err := a.mgmtServer.Start(ctx); err != nil {
			return fmt.Errorf("start management server: %w", err)
		}
		defer a.mgmtServer.Stop()
	} else if hasServiceImports(a.cfg.Imports) {
		a.logger.Warn("service imports configured without node_id; peer-directory imports will remain pending")
	}

	// Mark ready after all components are up.
	a.mu.Lock()
	a.ready = true
	a.mu.Unlock()

	a.logger.Info("agent ready", "health_addr", a.BoundAddrs().Health)

	// Block until context is cancelled.
	<-ctx.Done()

	a.logger.Info("agent shutting down")
	return nil
}

func hasServiceImports(imports []agentconfig.ImportConfig) bool {
	for _, imp := range imports {
		if imp.Service != "" {
			return true
		}
	}
	return false
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

// BoundAddrs is what a running agent actually listens on. A configured
// port 0 is resolved here, which is how a caller discovers the port the
// agent picked.
type BoundAddrs struct {
	// Health and Metrics are nil until their servers are listening.
	Health  net.Addr
	Metrics net.Addr
	// TunnelServers are the role=server tunnel listeners, in config order.
	TunnelServers []net.Addr
	// Imports are the import entries keyed by service (service imports) or
	// name (static imports). An entry whose listen failed is nil while it
	// retries.
	Imports map[string]net.Addr
}

// BoundAddrs reports the agent's bound listeners. Everything is bound
// before the agent reports ready, except imports whose listen failed.
func (a *Agent) BoundAddrs() BoundAddrs {
	a.mu.Lock()
	b := BoundAddrs{Health: a.healthAddr, Metrics: a.metricsAddr}
	tm := a.tunnels
	a.mu.Unlock()
	if tm != nil {
		for _, srv := range tm.Servers() {
			b.TunnelServers = append(b.TunnelServers, srv.Addr())
		}
		b.Imports = tm.ImportAddrs()
	}
	return b
}

func (a *Agent) startHealthServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)

	a.healthServer = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	addr, err := a.serveHTTP(a.healthServer, a.cfg.HealthAddr, "health")
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.healthAddr = addr
	a.mu.Unlock()
	return nil
}

func (a *Agent) startMetricsServer() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", a.metrics.Handler())

	a.metricsServer = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	addr, err := a.serveHTTP(a.metricsServer, a.cfg.Metrics.Addr, "metrics")
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.metricsAddr = addr
	a.mu.Unlock()
	a.logger.Info("metrics server started", "addr", addr)
	return nil
}

// serveHTTP binds addr now, so a bind error fails startup and a port 0 is
// resolved before anyone asks for it, then serves srv on that listener.
// An empty addr means ":http", as for http.Server.ListenAndServe.
func (a *Agent) serveHTTP(srv *http.Server, addr, name string) (net.Addr, error) {
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			a.logger.Error(name+" server stopped", "error", err)
		}
	}()
	return ln.Addr(), nil
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

// EnableObservation makes Run serve the OutpostRange observation API on the
// loopback addr (stream component OUTPOSTNET, requires node_id). Call before
// Run. Any observation failure is logged and never stops the agent.
func (a *Agent) EnableObservation(addr string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.observationAddr = addr
}

// ObservationAddr is the bound observation endpoint, or nil.
func (a *Agent) ObservationAddr() net.Addr {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.observation.Addr()
}
