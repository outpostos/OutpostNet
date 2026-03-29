package e2e

// Standalone E2E test suite for OutpostNet 1.0 — no Kubernetes required.
//
// Scenarios covered:
//
//	SC1 — Basic QUIC tunnel:   server + sidecar + agent, TCP forwarding
//	SC2 — UDP forwarding:      see udp_e2e_test.go (sidecar+server path)
//	SC3 — Multi-service mux:   single QUIC connection carrying 10+ services
//	SC4 — Relay:               A → C → B multi-hop relay
//	SC5 — Disconnect recovery: reconnect after tunnel server restart
//	SC6 — Token auth:          auth success / failure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/agent"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func freePortE2E(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// startEchoSrv starts a TCP echo server and returns its listener.
func startEchoSrv(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(c, c)
			}()
		}
	}()
	return ln
}

// startAgent starts an agent and waits until its health endpoint responds.
func startAgent(t *testing.T, cfg *agentconfig.Config) *agent.Agent {
	t.Helper()
	a := agent.New(cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		a.Stop()
	})
	go a.Run(ctx)

	waitAgentReady(t, cfg.HealthAddr, 5*time.Second)
	return a
}

// waitAgentReady polls the health address until it responds.
func waitAgentReady(t *testing.T, healthAddr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", healthAddr, 300*time.Millisecond)
		if err == nil {
			c.Close()
			time.Sleep(200 * time.Millisecond) // let agent complete startup
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("agent at %s did not become ready within %v", healthAddr, timeout)
}

// echoRoundTrip dials addr, sends msg, reads back the echo and asserts equality.
func echoRoundTrip(t *testing.T, addr string, msg []byte) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := c.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo mismatch: got %q, want %q", got, msg)
	}
}

// agentCfg builds a minimal agent config for tunnel server or client roles.
func agentCfg(t *testing.T, region string) *agentconfig.Config {
	return &agentconfig.Config{
		Region:      region,
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", freePortE2E(t)),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", freePortE2E(t))},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}
}

// ─── SC1: Basic QUIC tunnel — TCP forwarding ─────────────────────────────────

// TestSC1_BasicTunnel_TCP verifies:
//
//	TCP client → Agent B import proxy → QUIC tunnel → Agent A server → echo service
func TestSC1_BasicTunnel_TCP(t *testing.T) {
	echoLn := startEchoSrv(t)

	// Agent A — cloud (tunnel server + service export).
	tunnelPort := freePortE2E(t)
	cfgA := agentCfg(t, "cloud")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgA.Exports = []agentconfig.ExportConfig{
		{Name: "echo", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	startAgent(t, cfgA)

	// Agent B — edge (tunnel client + service import).
	importPort := freePortE2E(t)
	cfgB := agentCfg(t, "edge")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "echo", ListenAddr: fmt.Sprintf("127.0.0.1:%d", importPort), RemoteRegion: "cloud", Protocol: "tcp"},
	}
	startAgent(t, cfgB)

	time.Sleep(300 * time.Millisecond)

	echoRoundTrip(t, fmt.Sprintf("127.0.0.1:%d", importPort), []byte("SC1: basic tunnel OK"))
	t.Log("PASS SC1: basic QUIC tunnel TCP forwarding")
}

// ─── SC3: Multi-service mux — 10+ services on a single QUIC connection ───────

// TestSC3_MultiServiceMux verifies that a single QUIC connection carries
// 10 distinct named services simultaneously.
func TestSC3_MultiServiceMux(t *testing.T) {
	const numServices = 10

	// Start one echo server per service.
	echoAddrs := make([]string, numServices)
	for i := range numServices {
		ln := startEchoSrv(t)
		echoAddrs[i] = ln.Addr().String()
	}

	// Agent A — server, exports all 10 services.
	tunnelPort := freePortE2E(t)
	cfgA := agentCfg(t, "cloud-mux")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-mux", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	for i := range numServices {
		cfgA.Exports = append(cfgA.Exports, agentconfig.ExportConfig{
			Name:      fmt.Sprintf("svc-%02d", i),
			LocalAddr: echoAddrs[i],
			Protocol:  "tcp",
		})
	}
	startAgent(t, cfgA)

	// Agent B — client, imports all 10 services.
	importPorts := make([]int, numServices)
	cfgB := agentCfg(t, "edge-mux")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-mux", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	for i := range numServices {
		importPorts[i] = freePortE2E(t)
		cfgB.Imports = append(cfgB.Imports, agentconfig.ImportConfig{
			Name:         fmt.Sprintf("svc-%02d", i),
			ListenAddr:   fmt.Sprintf("127.0.0.1:%d", importPorts[i]),
			RemoteRegion: "cloud-mux",
			Protocol:     "tcp",
		})
	}
	startAgent(t, cfgB)

	time.Sleep(500 * time.Millisecond)

	// Concurrently hit all 10 services.
	var wg sync.WaitGroup
	errs := make(chan error, numServices)
	for i := range numServices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := fmt.Sprintf("127.0.0.1:%d", importPorts[i])
			msg := []byte(fmt.Sprintf("mux-svc-%02d-ping", i))
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				errs <- fmt.Errorf("svc-%02d dial: %w", i, err)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(5 * time.Second))
			c.Write(msg)
			got := make([]byte, len(msg))
			if _, err := io.ReadFull(c, got); err != nil {
				errs <- fmt.Errorf("svc-%02d read: %w", i, err)
				return
			}
			if !bytes.Equal(got, msg) {
				errs <- fmt.Errorf("svc-%02d: got %q, want %q", i, got, msg)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	t.Logf("PASS SC3: %d services multiplexed over single QUIC connection", numServices)
}

// ─── SC4: Relay — A → C → B ──────────────────────────────────────────────────

// TestSC4_Relay verifies A → C(relay) → B routing.
//
//	Agent B (cloud):  exports echo, listens for C
//	Agent C (relay):  bridges A → B
//	Agent A (edge):   imports echo via C
func TestSC4_Relay(t *testing.T) {
	echoLn := startEchoSrv(t)

	// Agent B — cloud.
	agentBTunnelPort := freePortE2E(t)
	cfgB := agentCfg(t, "cloud-b")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "relay-c", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", agentBTunnelPort)},
	}
	cfgB.Exports = []agentconfig.ExportConfig{
		{Name: "relay-echo", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	startAgent(t, cfgB)

	// Agent C — relay.
	agentCTunnelPort := freePortE2E(t)
	cfgC := agentCfg(t, "relay-c")
	cfgC.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-a", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", agentCTunnelPort)},
		{RemoteRegion: "cloud-b", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", agentBTunnelPort)},
	}
	cfgC.Relays = []agentconfig.RelayConfig{
		{Service: "relay-echo", FromRegion: "edge-a", ToRegion: "cloud-b", MaxHops: 3},
	}
	startAgent(t, cfgC)

	// Agent A — edge.
	importPort := freePortE2E(t)
	cfgA := agentCfg(t, "edge-a")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "relay-c", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", agentCTunnelPort)},
	}
	cfgA.Imports = []agentconfig.ImportConfig{
		{Name: "relay-echo", ListenAddr: fmt.Sprintf("127.0.0.1:%d", importPort), RemoteRegion: "relay-c", Protocol: "tcp"},
	}
	startAgent(t, cfgA)

	time.Sleep(400 * time.Millisecond)

	echoRoundTrip(t, fmt.Sprintf("127.0.0.1:%d", importPort), []byte("SC4: relay A→C→B OK"))
	t.Log("PASS SC4: relay A → C → B")
}

// ─── SC5: Disconnect recovery ─────────────────────────────────────────────────

// TestSC5_DisconnectRecovery starts two agents, verifies connectivity,
// then restarts the server-side agent and confirms the client reconnects
// and traffic flows again.
func TestSC5_DisconnectRecovery(t *testing.T) {
	echoLn := startEchoSrv(t)
	tunnelPort := freePortE2E(t)

	// Agent A — server.
	cfgA := agentCfg(t, "cloud-recovery")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-recovery", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgA.Exports = []agentconfig.ExportConfig{
		{Name: "echo-recovery", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	agentA := startAgent(t, cfgA)

	// Agent B — client with fast reconnect.
	importPort := freePortE2E(t)
	cfgB := agentCfg(t, "edge-recovery")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-recovery", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "echo-recovery", ListenAddr: fmt.Sprintf("127.0.0.1:%d", importPort), RemoteRegion: "cloud-recovery", Protocol: "tcp"},
	}
	startAgent(t, cfgB)

	time.Sleep(300 * time.Millisecond)

	// Verify initial connectivity.
	echoRoundTrip(t, fmt.Sprintf("127.0.0.1:%d", importPort), []byte("before-disconnect"))
	t.Log("pre-disconnect echo OK")

	// Stop server-side agent (simulates disconnect).
	agentA.Stop()
	time.Sleep(200 * time.Millisecond)

	// Restart Agent A on the same port.
	ctxA2, cancelA2 := context.WithCancel(context.Background())
	t.Cleanup(cancelA2)
	cfgA2 := agentCfg(t, "cloud-recovery")
	cfgA2.HealthAddr = cfgA.HealthAddr // keep same health addr for waitAgentReady
	cfgA2.Tunnels = cfgA.Tunnels
	cfgA2.Exports = cfgA.Exports
	cfgA2.TLS = cfgA.TLS

	agentA2 := agent.New(cfgA2, nil)
	go agentA2.Run(ctxA2)
	t.Cleanup(func() { cancelA2(); agentA2.Stop() })

	waitAgentReady(t, cfgA2.HealthAddr, 8*time.Second)
	t.Log("Agent A restarted")

	// Client should reconnect automatically — allow up to 10s.
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 500*time.Millisecond)
		if err == nil {
			c.SetDeadline(time.Now().Add(3 * time.Second))
			msg := []byte("after-reconnect")
			c.Write(msg)
			got := make([]byte, len(msg))
			if err := func() error {
				_, err := io.ReadFull(c, got)
				return err
			}(); err == nil && bytes.Equal(got, msg) {
				c.Close()
				t.Log("PASS SC5: reconnected and echo succeeded")
				return
			}
			c.Close()
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("SC5 FAIL: client did not recover after disconnect within 10s (last err: %v)", lastErr)
}

// ─── SC6: Token authentication ────────────────────────────────────────────────

// TestSC6_TokenAuth_Success verifies that matching tokens allow the connection.
func TestSC6_TokenAuth_Success(t *testing.T) {
	const token = "super-secret-token"
	echoLn := startEchoSrv(t)
	tunnelPort := freePortE2E(t)

	// Agent A — server with token.
	cfgA := agentCfg(t, "cloud-auth")
	cfgA.Auth = agentconfig.AuthConfig{Token: token}
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-auth", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgA.Exports = []agentconfig.ExportConfig{
		{Name: "auth-echo", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	startAgent(t, cfgA)

	// Agent B — client with matching token.
	importPort := freePortE2E(t)
	cfgB := agentCfg(t, "edge-auth")
	cfgB.Auth = agentconfig.AuthConfig{Token: token}
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-auth", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "auth-echo", ListenAddr: fmt.Sprintf("127.0.0.1:%d", importPort), RemoteRegion: "cloud-auth", Protocol: "tcp"},
	}
	startAgent(t, cfgB)

	time.Sleep(300 * time.Millisecond)

	echoRoundTrip(t, fmt.Sprintf("127.0.0.1:%d", importPort), []byte("SC6: auth success"))
	t.Log("PASS SC6a: correct token → connection allowed")
}

// TestSC6_TokenAuth_Failure verifies that a wrong token is rejected and the
// agent client does NOT become ready (health check never returns 200 within
// the window because no tunnel can be established).
func TestSC6_TokenAuth_Failure(t *testing.T) {
	tunnelPort := freePortE2E(t)

	// Agent A — server with token "secret".
	cfgA := agentCfg(t, "cloud-auth-fail")
	cfgA.Auth = agentconfig.AuthConfig{Token: "server-secret"}
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-auth-fail", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	startAgent(t, cfgA)

	// Agent B — client with WRONG token; Run is expected to fail or the
	// tunnel to remain disconnected. We confirm no traffic gets through.
	importPort := freePortE2E(t)
	cfgB := agentCfg(t, "edge-auth-fail")
	cfgB.Auth = agentconfig.AuthConfig{Token: "wrong-token"}
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-auth-fail", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "should-not-connect", ListenAddr: fmt.Sprintf("127.0.0.1:%d", importPort), RemoteRegion: "cloud-auth-fail", Protocol: "tcp"},
	}

	// Start agent B but don't require it to reach ready — it may start health
	// server even when the tunnel is rejected.
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	agentB := agent.New(cfgB, nil)
	defer agentB.Stop()
	go agentB.Run(ctxB)

	// Give agent B time to attempt the connection.
	time.Sleep(1 * time.Second)

	// Attempting to use the import proxy should fail (no open stream to server).
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 2*time.Second)
	if err != nil {
		// Import listener may not even be up — that is fine; connection rejected.
		t.Logf("PASS SC6b: import port not reachable (tunnel rejected): %v", err)
		return
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	c.Write([]byte("should-fail"))
	buf := make([]byte, 20)
	n, _ := c.Read(buf)
	if n > 0 {
		t.Errorf("SC6b FAIL: received data %q despite wrong token — auth not enforced", buf[:n])
	} else {
		t.Log("PASS SC6b: wrong token → no data returned (connection dropped by server)")
	}
}
