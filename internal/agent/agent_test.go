package agent

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// loopbackAny lets the agent pick its own port. Tests read the bound address
// back through Agent.BoundAddrs instead of pre-picking a port: a port released
// by the test can be taken by another process (and a TCP-free port can be a
// busy UDP port) before the agent binds it.
const loopbackAny = "127.0.0.1:0"

// runAgent runs a and returns once /readyz on its bound health address
// answers 200. Run returning first fails the test. The agent is stopped and
// awaited at cleanup.
func runAgent(t *testing.T, a *Agent) *Agent {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = a.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("agent did not stop")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-done:
			t.Fatalf("agent exited before ready: %v", runErr)
		default:
		}
		if h := a.BoundAddrs().Health; h != nil {
			resp, err := http.Get("http://" + h.String() + "/readyz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return a
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("agent not ready within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// tunnelServerAddr is the bound address of a's i-th role=server tunnel.
func tunnelServerAddr(t *testing.T, a *Agent, i int) string {
	t.Helper()
	addrs := a.BoundAddrs().TunnelServers
	if i >= len(addrs) || addrs[i] == nil {
		t.Fatalf("tunnel server %d not bound: %v", i, addrs)
	}
	return addrs[i].String()
}

// importAddr is the bound address of a's import with the given key.
func importAddr(t *testing.T, a *Agent, key string) string {
	t.Helper()
	addr := a.BoundAddrs().Imports[key]
	if addr == nil {
		t.Fatalf("import %q not bound: %v", key, a.BoundAddrs().Imports)
	}
	return addr.String()
}

// TestAgentBoundAddrsResolvePortZero: every listener configured on port 0
// is bound by the agent itself and reported by BoundAddrs once the agent is
// ready, and the import entry already accepts connections at that point.
func TestAgentBoundAddrsResolvePortZero(t *testing.T) {
	echoLn := startTCPEchoServer(t)
	server := runAgent(t, New(&agentconfig.Config{
		Region:      "cloud",
		Tunnels:     []agentconfig.TunnelConfig{{RemoteRegion: "edge", Role: "server", ListenAddr: loopbackAny}},
		Exports:     []agentconfig.ExportConfig{{Name: "echo-svc", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"}},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}, nil))
	addrs := server.BoundAddrs()
	for name, addr := range map[string]net.Addr{"health": addrs.Health, "metrics": addrs.Metrics} {
		if addr == nil || addr.(*net.TCPAddr).Port == 0 {
			t.Fatalf("%s addr = %v, want a bound port", name, addr)
		}
	}
	if len(addrs.TunnelServers) != 1 || addrs.TunnelServers[0].(*net.UDPAddr).Port == 0 {
		t.Fatalf("tunnel servers = %v, want one bound UDP port", addrs.TunnelServers)
	}
	resp, err := http.Get("http://" + addrs.Metrics.String() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.StatusCode)
	}

	client := runAgent(t, New(&agentconfig.Config{
		Region:      "edge",
		Tunnels:     []agentconfig.TunnelConfig{{RemoteRegion: "cloud", Role: "client", RemoteAddr: tunnelServerAddr(t, server, 0)}},
		Imports:     []agentconfig.ImportConfig{{Name: "echo-svc", ListenAddr: loopbackAny, RemoteRegion: "cloud", Protocol: "tcp"}},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}, nil))
	// No settling sleep: the import is bound before the agent reports ready.
	conn, err := net.DialTimeout("tcp", importAddr(t, client, "echo-svc"), 5*time.Second)
	if err != nil {
		t.Fatalf("dial import: %v", err)
	}
	defer conn.Close()
	msg := []byte("bound-addrs")
	conn.Write(msg)
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("echo = %q, want %q", buf, msg)
	}
}

func TestAgentHealthEndpoints(t *testing.T) {
	cfg := &agentconfig.Config{
		HealthAddr: loopbackAny,
		Metrics:    agentconfig.MetricsConfig{Addr: loopbackAny},
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
	var base string
	for time.Now().Before(deadline) {
		if health := a.BoundAddrs().Health; health != nil {
			base = "http://" + health.String()
			resp, _ = http.Get(base + "/healthz")
		}
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
		resp, err = http.Get(base + "/readyz")
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
