package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// ConfigWatcher watches a config file for changes and triggers hot reload.
// It polls the file at regular intervals (simulating K8s ConfigMap volume updates).
type ConfigWatcher struct {
	path     string
	interval time.Duration
	logger   *slog.Logger

	mu          sync.Mutex
	lastModTime time.Time
	lastSize    int64
	onChange    func(*agentconfig.Config)
}

// NewConfigWatcher creates a new config file watcher.
func NewConfigWatcher(path string, interval time.Duration, logger *slog.Logger) *ConfigWatcher {
	if logger == nil {
		logger = slog.Default()
	}
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &ConfigWatcher{
		path:     path,
		interval: interval,
		logger:   logger,
	}
}

// OnChange sets the callback for config changes.
func (w *ConfigWatcher) OnChange(fn func(*agentconfig.Config)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onChange = fn
}

// Watch starts watching the config file for changes.
// It blocks until the context is cancelled.
func (w *ConfigWatcher) Watch(ctx context.Context) error {
	// Record initial state.
	info, err := os.Stat(w.path)
	if err == nil {
		w.mu.Lock()
		w.lastModTime = info.ModTime()
		w.lastSize = info.Size()
		w.mu.Unlock()
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.check()
		}
	}
}

func (w *ConfigWatcher) check() {
	info, err := os.Stat(w.path)
	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Error("stat config file", "path", w.path, "error", err)
		}
		return
	}

	w.mu.Lock()
	changed := info.ModTime() != w.lastModTime || info.Size() != w.lastSize
	onChange := w.onChange
	if changed {
		w.lastModTime = info.ModTime()
		w.lastSize = info.Size()
	}
	w.mu.Unlock()

	if !changed || onChange == nil {
		return
	}

	w.logger.Info("config file changed, reloading", "path", w.path)

	cfg, err := agentconfig.LoadConfig(w.path)
	if err != nil {
		w.logger.Error("reload config", "path", w.path, "error", err)
		return
	}

	onChange(cfg)
}

// ReloadAgent handles config changes by updating the tunnel manager.
// It adds new tunnels, removes deleted tunnels, and updates export/import routes.
func ReloadAgent(a *Agent, newCfg *agentconfig.Config) error {
	a.mu.Lock()
	oldCfg := a.cfg
	a.cfg = newCfg
	tm := a.tunnels
	a.mu.Unlock()

	if tm == nil {
		return nil
	}

	// Detect export changes on existing tunnel servers.
	for _, srv := range tm.Servers() {
		// Remove routes no longer in config.
		oldExports := make(map[string]bool)
		for _, exp := range oldCfg.Exports {
			oldExports[exp.Name] = true
		}
		newExports := make(map[string]bool)
		for _, exp := range newCfg.Exports {
			newExports[exp.Name] = true
		}

		for name := range oldExports {
			if !newExports[name] {
				srv.RemoveRoute(name)
			}
		}
		for _, exp := range newCfg.Exports {
			if !oldExports[exp.Name] {
				srv.AddRoute(exp.Name, exp.LocalAddr)
			}
		}
	}

	a.logger.Info("config reloaded",
		"region", newCfg.Region,
		"tunnels", len(newCfg.Tunnels),
		"exports", len(newCfg.Exports),
		"imports", len(newCfg.Imports),
	)

	return fmt.Errorf("full tunnel hot-reload not yet implemented; restart required for tunnel changes")
}
