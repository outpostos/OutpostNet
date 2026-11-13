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
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/internal/agent"
	"github.com/outpostos/OutpostNet/internal/meshdirectory"
	"github.com/outpostos/OutpostNet/internal/observation"
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

	observationAddr string
	observation     *agent.Observation

	directory *meshdirectory.Directory
	logical   map[string]*service.LogicalOpener // by logical service name

	// Export routes: static exports (h.exports) merged with directory
	// exports (dirRoutes), static winning; routes is what the tunnel
	// servers currently hold. See direxports.go.
	routes         map[string]string
	dirRoutes      map[string]string
	conflictWarned map[string]time.Time
}

// New creates a host for a validated Service config.
func New(cfg *service.Config, logger *slog.Logger) *Host {
	if logger == nil {
		logger = slog.Default()
	}
	return &Host{
		cfg:            cfg,
		logger:         logger.With("component", "outpostnet-service", "node_id", cfg.NodeID),
		conflictWarned: map[string]time.Time{},
	}
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

	// Observation is started before the tunnels so their first connection
	// attempts are reported; it never fails the host (see StartObservation).
	h.mu.Lock()
	observationAddr := h.observationAddr
	h.mu.Unlock()
	obs := agent.StartObservation(ctx, agent.ObservationConfig{
		Addr:      observationAddr,
		NodeID:    h.cfg.NodeID,
		Component: observation.ComponentService,
	}, tm, h.logger)
	// Stops after the deferred tunnel/entry teardown above it has run.
	defer obs.Wait()
	defer cancel()
	h.mu.Lock()
	h.observation = obs
	h.mu.Unlock()

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

	// Service directory from the local Mesh gateway (ADR-0003 D2). It stops
	// after the entries that read it (deferred later, so run earlier).
	var svcDir *meshdirectory.Directory
	if h.cfg.Mesh.GatewayURL != "" {
		cachePath := ""
		if h.cfg.Mesh.DirectoryCacheDir != "" {
			cachePath = filepath.Join(h.cfg.Mesh.DirectoryCacheDir, meshdirectory.CacheFileName)
		}
		svcDir, err = meshdirectory.New(meshdirectory.Config{GatewayURL: h.cfg.Mesh.GatewayURL, CachePath: cachePath, Logger: h.logger})
		if err != nil {
			return fmt.Errorf("service directory: %w", err)
		}
		dirCtx, cancelDir := context.WithCancel(ctx)
		dirDone := make(chan struct{})
		go func() { defer close(dirDone); svcDir.Run(dirCtx) }()
		defer func() { cancelDir(); <-dirDone }()
	}
	logical := map[string]*service.LogicalOpener{}

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
		var base service.StreamOpener = service.NetOpener(tm)
		if tgt.Logical {
			lo, err := service.NewLogicalOpener(service.LogicalConfig{
				Self:      h.cfg.NodeID,
				Directory: svcDir,
				Peers:     tm,
				Reachable: tm.PeerReachable,
				Logger:    h.logger,
			})
			if err != nil {
				return fmt.Errorf("service entry %s: %w", tgt, err)
			}
			logical[tgt.Service] = lo
			base = lo
		}
		opener, observer := observedOpener(obs, tm, tgt, base)
		e := service.NewEntry(service.EntryConfig{
			Target:     tgt,
			ListenAddr: h.cfg.Imports[i].ListenAddr,
		}, opener, h.logger)
		if err := e.Listen(); err != nil {
			return fmt.Errorf("service entry %s: listen %s: %w", tgt, h.cfg.Imports[i].ListenAddr, err)
		}
		if observer != nil {
			observer.SetEntryAddress(e.Addr().String())
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
	h.routes = map[string]string{}
	for _, e := range exports {
		h.routes[e.Name] = e.LocalAddr
	}
	h.entries = entries
	h.directory = svcDir
	h.logical = logical
	h.ready = true
	h.mu.Unlock()

	// Directory-driven exports (ADR-0003 D4); stopped before the directory.
	if svcDir != nil {
		expCtx, cancelExp := context.WithCancel(ctx)
		expDone := make(chan struct{})
		go func() { defer close(expDone); h.runDirectoryExports(expCtx, svcDir) }()
		defer func() { cancelExp(); <-expDone }()
	}
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

// ApplyExports moves this node's static exports to next (add / re-point /
// remove) on every tunnel server and reports the routes that changed. A
// static export also overrides a directory export of the same name.
// Established streams keep their backend.
func (h *Host) ApplyExports(next []agentconfig.ExportConfig) service.ExportChanges {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.exports = append([]agentconfig.ExportConfig(nil), next...)
	if h.routes == nil {
		return service.ExportChanges{}
	}
	return h.syncRoutesLocked()
}

// WatchConfig polls path with the existing agent ConfigWatcher (using the
// stricter Service loader) until ctx is cancelled. Export changes apply in
// place; tunnels, entries, peers, auth, TLS, management and health require a
// restart and are only reported, and peer address changes at runtime go
// through PUT /v1/peers.
//
// override must be the same adjustment the command applied to the file
// config before New (for example --node-id / --health-addr); it is applied
// to every reloaded file so the restart-scope comparison is against what is
// actually running, not against the raw file. nil means no overrides.
func (h *Host) WatchConfig(ctx context.Context, path string, interval time.Duration, override func(*service.Config)) error {
	// The watcher hands OnChange only the agentconfig part; keep the full
	// Service config (peers) from the loader for the restart-scope check.
	// Loader and OnChange run sequentially on the watcher goroutine.
	var loaded *service.Config
	w := agent.NewConfigWatcher(path, interval, h.logger)
	w.SetLoader(func(p string) (*agentconfig.Config, error) {
		cfg, err := service.LoadConfig(p)
		if err != nil {
			return nil, err
		}
		if override != nil {
			override(cfg)
			if err := service.ValidateConfig(cfg); err != nil {
				return nil, fmt.Errorf("validate config with overrides: %w", err)
			}
		}
		loaded = cfg
		return &cfg.Config, nil
	})
	w.OnChange(func(*agentconfig.Config) { h.reload(loaded) })
	return w.Watch(ctx)
}

func (h *Host) reload(next *service.Config) {
	h.ApplyExports(next.Exports)
	if !sameRestartScope(h.cfg, next) {
		h.logger.Warn("config change outside exports ignored until restart (use PUT /v1/peers for peer address changes)")
	}
}

// sameRestartScope compares everything Run reads at startup except exports.
func sameRestartScope(a, b *service.Config) bool {
	return a.NodeID == b.NodeID &&
		a.Mesh == b.Mesh &&
		reflect.DeepEqual(a.Tunnels, b.Tunnels) &&
		reflect.DeepEqual(a.Imports, b.Imports) &&
		reflect.DeepEqual(a.TLS, b.TLS) &&
		reflect.DeepEqual(a.Auth, b.Auth) &&
		reflect.DeepEqual(a.Management, b.Management) &&
		a.HealthAddr == b.HealthAddr &&
		reflect.DeepEqual(a.Peers, b.Peers)
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

// LogicalStats returns the resolution counters of the logical entry for the
// named service, if one is configured.
func (h *Host) LogicalStats(name string) (service.LogicalStats, bool) {
	h.mu.Lock()
	lo := h.logical[name]
	h.mu.Unlock()
	if lo == nil {
		return service.LogicalStats{}, false
	}
	return lo.Stats(), true
}

// Directory returns the service directory, or nil when mesh.gateway_url is
// not configured or the host is not running.
func (h *Host) Directory() *meshdirectory.Directory {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.directory
}
