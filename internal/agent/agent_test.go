package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestAgentHealthEndpoints(t *testing.T) {
	port := freePort(t)
	metricsPort := freePort(t)
	cfg := &agentconfig.Config{
		HealthAddr: fmt.Sprintf("127.0.0.1:%d", port),
		Metrics:    agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", metricsPort)},
		Persistence: agentconfig.PersistenceConfig{
			Dir: t.TempDir(),
		},
	}

	a := New(cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(ctx)
	}()

	// Wait for health server to be ready.
	deadline := time.Now().Add(3 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		resp, _ = http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
		if resp != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if resp == nil {
		t.Fatal("health server did not start")
	}

	// Test /healthz.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Errorf("healthz body = %q, want ok", body)
	}

	// Test /readyz (may need a moment for ready=true to be set after health server starts).
	var err error
	for time.Now().Before(deadline) {
		resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", port))
		if err != nil {
			t.Fatalf("readyz: %v", err)
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("readyz status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Errorf("readyz body = %q, want ok", body)
	}

	// Stop agent.
	a.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop in time")
	}
}

func TestAgentStopBeforeRun(t *testing.T) {
	cfg := &agentconfig.Config{
		HealthAddr:  "127.0.0.1:0",
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
	}
	a := New(cfg, nil)
	// Stop should not panic when called before Run.
	a.Stop()
}
