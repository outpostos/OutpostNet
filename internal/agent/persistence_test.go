package agent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

func testConfig() *agentconfig.Config {
	return &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "cloud", RemoteAddr: "1.2.3.4:4242", Role: "client"},
		},
		Exports: []agentconfig.ExportConfig{
			{Name: "web", LocalAddr: "127.0.0.1:8080", Protocol: "tcp"},
		},
		HealthAddr:  ":8082",
		Metrics:     agentconfig.MetricsConfig{Addr: ":9090"},
		Persistence: agentconfig.PersistenceConfig{Dir: "/tmp/test"},
	}
}

func TestPersistenceSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	p := NewPersistence(dir, nil)

	cfg := testConfig()

	// Save config.
	if err := p.Save(cfg, 1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if p.Version() != 1 {
		t.Errorf("version = %d, want 1", p.Version())
	}

	// Verify file exists.
	path := filepath.Join(dir, configFileName)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file not found: %v", err)
	}

	// Load config.
	loaded, err := p.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("loaded config is nil")
	}

	if loaded.Region != cfg.Region {
		t.Errorf("loaded region = %q, want %q", loaded.Region, cfg.Region)
	}
	if len(loaded.Tunnels) != len(cfg.Tunnels) {
		t.Errorf("loaded tunnels = %d, want %d", len(loaded.Tunnels), len(cfg.Tunnels))
	}
	if len(loaded.Exports) != len(cfg.Exports) {
		t.Errorf("loaded exports = %d, want %d", len(loaded.Exports), len(cfg.Exports))
	}
}

func TestPersistenceVersionTracking(t *testing.T) {
	dir := t.TempDir()
	p := NewPersistence(dir, nil)

	cfg := testConfig()

	// Save v1.
	if err := p.Save(cfg, 1); err != nil {
		t.Fatalf("Save v1: %v", err)
	}

	// Save v2 (should succeed).
	cfg2 := testConfig()
	cfg2.Region = "edge-b"
	if err := p.Save(cfg2, 2); err != nil {
		t.Fatalf("Save v2: %v", err)
	}

	if p.Version() != 2 {
		t.Errorf("version = %d, want 2", p.Version())
	}

	// Verify v2 was saved.
	loaded, _ := p.Load()
	if loaded.Region != "edge-b" {
		t.Errorf("loaded region = %q, want edge-b", loaded.Region)
	}

	// Save v1 again (should be skipped — older version).
	if err := p.Save(cfg, 1); err != nil {
		t.Fatalf("Save v1 again: %v", err)
	}

	if p.Version() != 2 {
		t.Errorf("version should still be 2, got %d", p.Version())
	}

	// Verify the loaded config is still v2.
	loaded2, _ := p.Load()
	if loaded2.Region != "edge-b" {
		t.Errorf("region should still be edge-b, got %q", loaded2.Region)
	}
}

func TestPersistenceLoadNonExistent(t *testing.T) {
	dir := t.TempDir()
	p := NewPersistence(dir, nil)

	cfg, err := p.Load()
	if err != nil {
		t.Fatalf("Load non-existent: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil config for non-existent file")
	}
}

func TestPersistenceExists(t *testing.T) {
	dir := t.TempDir()
	p := NewPersistence(dir, nil)

	if p.Exists() {
		t.Fatal("Exists() should be false before save")
	}

	p.Save(testConfig(), 1)

	if !p.Exists() {
		t.Fatal("Exists() should be true after save")
	}
}

func TestPersistenceAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	p := NewPersistence(dir, nil)

	cfg := testConfig()
	p.Save(cfg, 1)

	// Verify no temp file remains.
	tmpPath := filepath.Join(dir, configFileName+".tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatal("temp file should not exist after successful save")
	}
}
