package agent

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// Persistence manages local config persistence for offline recovery.
// When a new config is received (e.g., from ConfigMap), it is persisted
// to a local file. On restart, if the primary config source is unavailable,
// the agent can load from the persisted copy.
type Persistence struct {
	dir    string
	logger *slog.Logger

	mu      sync.Mutex
	version int64
}

const configFileName = "agent-config.yaml"

// NewPersistence creates a new Persistence manager.
func NewPersistence(dir string, logger *slog.Logger) *Persistence {
	if logger == nil {
		logger = slog.Default()
	}
	return &Persistence{
		dir:    dir,
		logger: logger,
	}
}

// Save persists the config to the local file with version tracking.
// Only saves if the version is newer than the current persisted version.
func (p *Persistence) Save(cfg *agentconfig.Config, version int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if version <= p.version && p.version > 0 {
		p.logger.Debug("skipping config save, version not newer",
			"current", p.version, "incoming", version)
		return nil
	}

	data, err := agentconfig.MarshalConfig(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.MkdirAll(p.dir, 0755); err != nil {
		return fmt.Errorf("create persistence dir: %w", err)
	}

	path := filepath.Join(p.dir, configFileName)

	// Write to temp file and rename for atomicity.
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename config: %w", err)
	}

	p.version = version
	p.logger.Info("config persisted", "version", version, "path", path)
	return nil
}

// Load attempts to load a persisted config from the local file.
// Returns nil, nil if no persisted config exists.
func (p *Persistence) Load() (*agentconfig.Config, error) {
	path := filepath.Join(p.dir, configFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read persisted config: %w", err)
	}

	cfg, err := agentconfig.ParseConfig(data)
	if err != nil {
		return nil, fmt.Errorf("parse persisted config: %w", err)
	}

	p.logger.Info("loaded persisted config", "path", path, "region", cfg.Region)
	return cfg, nil
}

// Version returns the current persisted config version.
func (p *Persistence) Version() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.version
}

// Exists checks whether a persisted config file exists.
func (p *Persistence) Exists() bool {
	path := filepath.Join(p.dir, configFileName)
	_, err := os.Stat(path)
	return err == nil
}
