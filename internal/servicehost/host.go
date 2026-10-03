// Package servicehost runs the OutpostService command: it composes the
// existing Net host pieces from internal/agent (TunnelManager for QUIC
// tunnels and reconnects, PeerDirectory and its Unix-socket management API)
// with the Service domain in pkg/service (stable entries and exports).
//
// Nothing here replicates directories: peers come from the config seed and
// the existing PUT /v1/peers input; exports change through ApplyExports
// (wired to the existing ConfigWatcher by the command).
package servicehost

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/internal/agent"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// Host is one outpostnet-service node.
type Host struct {
	cfg    *service.Config
	logger *slog.Logger

	mu       sync.Mutex
	ready    bool
	exports  []agentconfig.ExportConfig
	tunnels  *agent.TunnelManager
	entries  []*service.Entry
	healthLn net.Listener
}

// New creates a host for a validated Service config.
func New(cfg *service.Config, logger *slog.Logger) *Host {
	if logger == nil {
		logger = slog.Default()
	}
	return &Host{cfg: cfg, logger: logger.With("component", "outpostnet-service", "node_id", cfg.NodeID)}
}

// Run starts the host and blocks until ctx is cancelled.
//
// Startup order is tunnels -> exports -> peers -> management -> health ->
// entries; teardown (deferred, so it also runs on every startup error) is the
// reverse: entries are cancelled and waited for first, then health,
// management and finally the tunnels the entries were using. Nothing started
// here outlives Run.
func (h *Host) Run(ctx context.Context) error {
	// Background Net work started below (client connect loops, peer
	// reconnects) is bound to this context; it is cancelled last.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Net: tunnels only. Exports and entries are owned by the Service side,
	// so the TunnelManager gets a config without them.
	netCfg := h.cfg.Config
	netCfg.Exports = nil
	netCfg.Imports = nil
	tm := agent.NewTunnelManager(&netCfg, h.logger, nil)
	defer tm.Stop()
	if err := tm.Start(ctx); err != nil {
		return fmt.Errorf("start tunnels: %w", err)
	}

	// Service: exports on every tunnel server.
	exports := append([]agentconfig.ExportConfig(nil), h.cfg.Exports...)
	for _, srv := range tm.Servers() {
		service.RegisterExports(srv, exports)
	}

	// Net management input: existing peer directory + Unix-socket API.
	dir := agent.NewPeerDirectory(h.cfg.NodeID, tm, h.logger)
	if len(h.cfg.Peers) > 0 {
		peers := make([]agent.Peer, 0, len(h.cfg.Peers))
		for _, p := range h.cfg.Peers {
			peers = append(peers, agent.Peer{NodeID: p.NodeID, Addr: p.Addr})
		}
		if err := dir.Replace(ctx, peers); err != nil {
			return fmt.Errorf("seed peers: %w", err)
		}
	}
	mgmt := agent.NewManagementServer(h.cfg.Management.SocketPath, dir, h.logger)
	if err := mgmt.Start(ctx); err != nil {
		return fmt.Errorf("start management server: %w", err)
	}
	defer mgmt.Stop()

	stopHealth, err := h.startHealth()
	if err != nil {
		return fmt.Errorf("start health server: %w", err)
	}
	defer stopHealth()

	// Service: stable entries, resolved through the Net host per connection.
	// Bind every entry before serving any, so a taken address fails startup
	// with the real bind error.
	var entries []*service.Entry
	defer func() {
		for _, e := range entries {
			e.Close() // no-op once Run has taken the listener over
		}
	}()
	for i, tgt := range h.cfg.EntryTargets() {
		e := service.NewEntry(service.EntryConfig{
			Target:     tgt,
			ListenAddr: h.cfg.Imports[i].ListenAddr,
		}, service.NetOpener(tm), h.logger)
		if err := e.Listen(); err != nil {
			return fmt.Errorf("service entry %s: listen %s: %w", tgt, h.cfg.Imports[i].ListenAddr, err)
		}
		entries = append(entries, e)
	}
	// Entries run under their own context so teardown can stop and wait for
	// them before health, management and the tunnels go away.
	entryCtx, cancelEntries := context.WithCancel(ctx)
	var entryWG sync.WaitGroup
	defer func() {
		cancelEntries()
		entryWG.Wait()
	}()
	for _, e := range entries {
		entryWG.Add(1)
		go func(e *service.Entry) {
			defer entryWG.Done()
			e.Run(entryCtx)
		}(e)
	}

	h.mu.Lock()
	h.tunnels = tm
	h.exports = exports
	h.entries = entries
	h.ready = true
	h.mu.Unlock()
	h.logger.Info("service host ready", "exports", len(exports), "entries", len(entries), "peers", len(h.cfg.Peers))

	<-ctx.Done()

	h.mu.Lock()
	h.ready = false
	h.mu.Unlock()
	h.logger.Info("service host shutting down")
	return nil
}

func (h *Host) startHealth() (func(), error) {
	if h.cfg.HealthAddr == "" {
		return func() {}, nil
	}
	ln, err := net.Listen("tcp", h.cfg.HealthAddr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.Ready() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("not ready"))
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			h.logger.Error("health server stopped", "error", err)
		}
	}()
	h.mu.Lock()
	h.healthLn = ln
	h.mu.Unlock()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// Ready reports whether tunnels, management API and entries are up.
func (h *Host) Ready() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready
}

// ApplyExports moves this node's exports to next on every tunnel server
// (add / re-point / remove). Established streams keep their backend.
func (h *Host) ApplyExports(next []agentconfig.ExportConfig) service.ExportChanges {
	h.mu.Lock()
	defer h.mu.Unlock()
	next = append([]agentconfig.ExportConfig(nil), next...)
	var ch service.ExportChanges
	if h.tunnels != nil {
		for _, srv := range h.tunnels.Servers() {
			ch = service.ApplyExports(srv, h.exports, next)
		}
	}
	h.exports = next
	if len(ch.Added)+len(ch.Updated)+len(ch.Removed) > 0 {
		h.logger.Info("exports applied", "added", ch.Added, "updated", ch.Updated, "removed", ch.Removed)
	}
	return ch
}

// WatchConfig polls path with the existing agent ConfigWatcher (using the
// stricter Service loader) until ctx is cancelled. Export changes apply in
// place; tunnels, entries, peers, auth and TLS require a restart, and peer
// address changes at runtime go through PUT /v1/peers.
func (h *Host) WatchConfig(ctx context.Context, path string, interval time.Duration) error {
	w := agent.NewConfigWatcher(path, interval, h.logger)
	w.SetLoader(func(p string) (*agentconfig.Config, error) {
		cfg, err := service.LoadConfig(p)
		if err != nil {
			return nil, err
		}
		return &cfg.Config, nil
	})
	w.OnChange(h.reload)
	return w.Watch(ctx)
}

func (h *Host) reload(next *agentconfig.Config) {
	h.ApplyExports(next.Exports)
	if !sameRestartScope(&h.cfg.Config, next) {
		h.logger.Warn("config change outside exports ignored until restart (use PUT /v1/peers for peer address changes)")
	}
}

func sameRestartScope(a, b *agentconfig.Config) bool {
	return fmt.Sprint(a.NodeID, a.Tunnels, a.Imports, a.TLS, a.Auth, a.Management, a.HealthAddr) ==
		fmt.Sprint(b.NodeID, b.Tunnels, b.Imports, b.TLS, b.Auth, b.Management, b.HealthAddr)
}

// TunnelAddrs returns the bound addresses of this node's tunnel listeners.
func (h *Host) TunnelAddrs() []net.Addr {
	h.mu.Lock()
	tm := h.tunnels
	h.mu.Unlock()
	if tm == nil {
		return nil
	}
	var out []net.Addr
	for _, srv := range tm.Servers() {
		if a := srv.Addr(); a != nil {
			out = append(out, a)
		}
	}
	return out
}

// EntryAddrs returns the bound addresses of the stable entries, in config order.
func (h *Host) EntryAddrs() []net.Addr {
	h.mu.Lock()
	entries := h.entries
	h.mu.Unlock()
	out := make([]net.Addr, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Addr())
	}
	return out
}

// HealthAddr returns the bound health address, or nil.
func (h *Host) HealthAddr() net.Addr {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.healthLn == nil {
		return nil
	}
	return h.healthLn.Addr()
}
