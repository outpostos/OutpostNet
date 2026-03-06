package agent

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

func TestConfigWatcherDetectsChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")

	// Write initial config.
	initialYAML := `
region: edge-a
tunnels:
  - remote_region: cloud
    remote_addr: "1.2.3.4:4242"
    role: client
`
	os.WriteFile(path, []byte(initialYAML), 0644)

	var mu sync.Mutex
	var receivedRegion string

	watcher := NewConfigWatcher(path, 100*time.Millisecond, nil)
	watcher.OnChange(func(cfg *agentconfig.Config) {
		mu.Lock()
		receivedRegion = cfg.Region
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watcher.Watch(ctx)

	// Wait for initial stat to be recorded.
	time.Sleep(200 * time.Millisecond)

	// Update config file.
	updatedYAML := `
region: edge-b
tunnels:
  - remote_region: cloud
    remote_addr: "5.6.7.8:4242"
    role: client
`
	os.WriteFile(path, []byte(updatedYAML), 0644)

	// Wait for watcher to detect change.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		r := receivedRegion
		mu.Unlock()
		if r == "edge-b" {
			t.Log("config change detected successfully")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	t.Fatalf("config change not detected, receivedRegion = %q", receivedRegion)
	mu.Unlock()
}

func TestConfigWatcherIgnoresUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")

	yaml := `
region: static
tunnels:
  - remote_region: cloud
    remote_addr: "1.2.3.4:4242"
    role: client
`
	os.WriteFile(path, []byte(yaml), 0644)

	callCount := 0
	var mu sync.Mutex

	watcher := NewConfigWatcher(path, 100*time.Millisecond, nil)
	watcher.OnChange(func(cfg *agentconfig.Config) {
		mu.Lock()
		callCount++
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watcher.Watch(ctx)

	// Wait for several polling cycles.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	if callCount != 0 {
		t.Errorf("onChange called %d times for unchanged file", callCount)
	}
	mu.Unlock()
}

func TestConfigWatcherHandlesMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.yaml")

	watcher := NewConfigWatcher(path, 100*time.Millisecond, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Should not panic or error when file doesn't exist.
	err := watcher.Watch(ctx)
	if err != nil {
		t.Errorf("Watch returned error: %v", err)
	}
}
