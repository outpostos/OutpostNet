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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/agent"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// loopbackAny lets the agent pick its own port. Tests read the bound address
// back through Agent.BoundAddrs instead of pre-picking a port, which another
// process could take (or hold as UDP) before the agent binds it.
const loopbackAny = "127.0.0.1:0"

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

// launchAgent runs an agent until test cleanup. done is closed when Run
// returns; *runErr is its result.
func launchAgent(t *testing.T, cfg *agentconfig.Config) (a *agent.Agent, done <-chan struct{}, runErr *error) {
	t.Helper()
	a = agent.New(cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	runErr = new(error)
	go func() {
		defer close(ch)
		*runErr = a.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-ch:
		case <-time.After(15 * time.Second):
			t.Error("agent did not stop")
		}
	})
	return a, ch, runErr
}

// agentReady reports whether /readyz on a's bound health address answers 200.
func agentReady(a *agent.Agent) bool {
	health := a.BoundAddrs().Health
	if health == nil {
		return false
	}
	resp, err := http.Get("http://" + health.String() + "/readyz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// startAgent starts an agent and waits until it reports ready. Run returning
// first fails the test.
func startAgent(t *testing.T, cfg *agentconfig.Config) *agent.Agent {
	t.Helper()
	a, done, runErr := launchAgent(t, cfg)
	deadline := time.Now().Add(10 * time.Second)
	for !agentReady(a) {
		select {
		case <-done:
			t.Fatalf("agent exited before ready: %v", *runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("agent not ready within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return a
}

// tunnelServerAddr is the bound address of a's i-th role=server tunnel.
func tunnelServerAddr(t *testing.T, a *agent.Agent, i int) string {
	t.Helper()
	addrs := a.BoundAddrs().TunnelServers
	if i >= len(addrs) || addrs[i] == nil {
		t.Fatalf("tunnel server %d not bound: %v", i, addrs)
	}
	return addrs[i].String()
}

// importAddr is the bound address of a's import with the given key.
func importAddr(t *testing.T, a *agent.Agent, key string) string {
	t.Helper()
	addr := a.BoundAddrs().Imports[key]
	if addr == nil {
		t.Fatalf("import %q not bound: %v", key, a.BoundAddrs().Imports)
	}
	return addr.String()
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
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}
}

// agentCfgNoRegion builds a minimal agent config with no region set (FRP-style).
func agentCfgNoRegion(t *testing.T) *agentconfig.Config {
	return &agentconfig.Config{
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
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
	cfgA := agentCfg(t, "cloud")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge", Role: "server", ListenAddr: loopbackAny},
	}
	cfgA.Exports = []agentconfig.ExportConfig{
		{Name: "echo", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	agentA := startAgent(t, cfgA)

	// Agent B — edge (tunnel client + service import).
	cfgB := agentCfg(t, "edge")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud", Role: "client", RemoteAddr: tunnelServerAddr(t, agentA, 0)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "echo", ListenAddr: loopbackAny, RemoteRegion: "cloud", Protocol: "tcp"},
	}
	agentB := startAgent(t, cfgB)

	echoRoundTrip(t, importAddr(t, agentB, "echo"), []byte("SC1: basic tunnel OK"))
	t.Log("PASS SC1: basic QUIC tunnel TCP forwarding")
}

// ─── SC2b: FRP-style — no region on either side ──────────────────────────────

// TestSC2b_FRPStyle_NoRegion verifies the 1.0 target config where neither
// server nor client specifies a region.
func TestSC2b_FRPStyle_NoRegion(t *testing.T) {
	echoLn := startEchoSrv(t)

	// Server side (frps equivalent) — no region.
	cfgS := agentCfgNoRegion(t)
	cfgS.Auth = agentconfig.AuthConfig{Token: "changeme"}
	cfgS.Tunnels = []agentconfig.TunnelConfig{
		{Role: "server", ListenAddr: loopbackAny},
	}
	cfgS.Exports = []agentconfig.ExportConfig{
		{Name: "my-http", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	agentS := startAgent(t, cfgS)

	// Client side (frpc equivalent) — no region.
	cfgC := agentCfgNoRegion(t)
	cfgC.Auth = agentconfig.AuthConfig{Token: "changeme"}
	cfgC.Tunnels = []agentconfig.TunnelConfig{
		{Role: "client", RemoteAddr: tunnelServerAddr(t, agentS, 0)},
	}
	cfgC.Imports = []agentconfig.ImportConfig{
		{Name: "my-http", ListenAddr: loopbackAny, Protocol: "tcp"},
	}
	agentC := startAgent(t, cfgC)

	echoRoundTrip(t, importAddr(t, agentC, "my-http"), []byte("SC2b: FRP-style no-region OK"))
	t.Log("PASS SC2b: FRP-style no-region tunnel")
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
	cfgA := agentCfg(t, "cloud-mux")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-mux", Role: "server", ListenAddr: loopbackAny},
	}
	for i := range numServices {
		cfgA.Exports = append(cfgA.Exports, agentconfig.ExportConfig{
			Name:      fmt.Sprintf("svc-%02d", i),
			LocalAddr: echoAddrs[i],
			Protocol:  "tcp",
		})
	}
	agentA := startAgent(t, cfgA)

	// Agent B — client, imports all 10 services.
	cfgB := agentCfg(t, "edge-mux")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-mux", Role: "client", RemoteAddr: tunnelServerAddr(t, agentA, 0)},
	}
	for i := range numServices {
		cfgB.Imports = append(cfgB.Imports, agentconfig.ImportConfig{
			Name:         fmt.Sprintf("svc-%02d", i),
			ListenAddr:   loopbackAny,
			RemoteRegion: "cloud-mux",
			Protocol:     "tcp",
		})
	}
	agentB := startAgent(t, cfgB)
	importAddrs := make([]string, numServices)
	for i := range numServices {
		importAddrs[i] = importAddr(t, agentB, fmt.Sprintf("svc-%02d", i))
	}

	// Concurrently hit all 10 services.
	var wg sync.WaitGroup
	errs := make(chan error, numServices)
	for i := range numServices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := importAddrs[i]
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
	cfgB := agentCfg(t, "cloud-b")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "relay-c", Role: "server", ListenAddr: loopbackAny},
	}
	cfgB.Exports = []agentconfig.ExportConfig{
		{Name: "relay-echo", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	agentB := startAgent(t, cfgB)

	// Agent C — relay.
	cfgC := agentCfg(t, "relay-c")
	cfgC.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-a", Role: "server", ListenAddr: loopbackAny},
		{RemoteRegion: "cloud-b", Role: "client", RemoteAddr: tunnelServerAddr(t, agentB, 0)},
	}
	cfgC.Relays = []agentconfig.RelayConfig{
		{Service: "relay-echo", FromRegion: "edge-a", ToRegion: "cloud-b", MaxHops: 3},
	}
	agentC := startAgent(t, cfgC)

	// Agent A — edge.
	cfgA := agentCfg(t, "edge-a")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "relay-c", Role: "client", RemoteAddr: tunnelServerAddr(t, agentC, 0)},
	}
	cfgA.Imports = []agentconfig.ImportConfig{
		{Name: "relay-echo", ListenAddr: loopbackAny, RemoteRegion: "relay-c", Protocol: "tcp"},
	}
	agentA := startAgent(t, cfgA)

	echoRoundTrip(t, importAddr(t, agentA, "relay-echo"), []byte("SC4: relay A→C→B OK"))
	t.Log("PASS SC4: relay A → C → B")
}

// ─── SC5: Disconnect recovery ─────────────────────────────────────────────────

// TestSC5_DisconnectRecovery starts two agents, verifies connectivity,
// then restarts the server-side agent and confirms the client reconnects
// and traffic flows again.
func TestSC5_DisconnectRecovery(t *testing.T) {
	echoLn := startEchoSrv(t)

	// Agent A — server.
	cfgA := agentCfg(t, "cloud-recovery")
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-recovery", Role: "server", ListenAddr: loopbackAny},
	}
	cfgA.Exports = []agentconfig.ExportConfig{
		{Name: "echo-recovery", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	agentA := startAgent(t, cfgA)
	tunnelAddr := tunnelServerAddr(t, agentA, 0)

	// Agent B — client with fast reconnect.
	cfgB := agentCfg(t, "edge-recovery")
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-recovery", Role: "client", RemoteAddr: tunnelAddr},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "echo-recovery", ListenAddr: loopbackAny, RemoteRegion: "cloud-recovery", Protocol: "tcp"},
	}
	agentB := startAgent(t, cfgB)
	importTarget := importAddr(t, agentB, "echo-recovery")

	// Verify initial connectivity.
	echoRoundTrip(t, importTarget, []byte("before-disconnect"))
	t.Log("pre-disconnect echo OK")

	// Stop server-side agent (simulates disconnect).
	agentA.Stop()
	time.Sleep(200 * time.Millisecond)

	// Restart Agent A on the tunnel address the client keeps dialing. This
	// one port is necessarily re-bound by number after its release; every
	// other listener still picks its own port.
	cfgA2 := agentCfg(t, "cloud-recovery")
	cfgA2.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-recovery", Role: "server", ListenAddr: tunnelAddr},
	}
	cfgA2.Exports = cfgA.Exports
	cfgA2.TLS = cfgA.TLS
	startAgent(t, cfgA2)
	t.Log("Agent A restarted")

	// Client should reconnect automatically — allow up to 10s.
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", importTarget, 500*time.Millisecond)
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

	// Agent A — server with token.
	cfgA := agentCfg(t, "cloud-auth")
	cfgA.Auth = agentconfig.AuthConfig{Token: token}
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-auth", Role: "server", ListenAddr: loopbackAny},
	}
	cfgA.Exports = []agentconfig.ExportConfig{
		{Name: "auth-echo", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"},
	}
	agentA := startAgent(t, cfgA)

	// Agent B — client with matching token.
	cfgB := agentCfg(t, "edge-auth")
	cfgB.Auth = agentconfig.AuthConfig{Token: token}
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-auth", Role: "client", RemoteAddr: tunnelServerAddr(t, agentA, 0)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "auth-echo", ListenAddr: loopbackAny, RemoteRegion: "cloud-auth", Protocol: "tcp"},
	}
	agentB := startAgent(t, cfgB)

	echoRoundTrip(t, importAddr(t, agentB, "auth-echo"), []byte("SC6: auth success"))
	t.Log("PASS SC6a: correct token → connection allowed")
}

// TestSC6_TokenAuth_Failure verifies that a wrong token is rejected: the
// client agent fails startup with the server's auth rejection and never
// reports ready, so its import never serves traffic.
func TestSC6_TokenAuth_Failure(t *testing.T) {
	// Agent A — server with token "server-secret".
	cfgA := agentCfg(t, "cloud-auth-fail")
	cfgA.Auth = agentconfig.AuthConfig{Token: "server-secret"}
	cfgA.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "edge-auth-fail", Role: "server", ListenAddr: loopbackAny},
	}
	agentA := startAgent(t, cfgA)

	// Agent B — client with WRONG token.
	cfgB := agentCfg(t, "edge-auth-fail")
	cfgB.Auth = agentconfig.AuthConfig{Token: "wrong-token"}
	cfgB.Tunnels = []agentconfig.TunnelConfig{
		{RemoteRegion: "cloud-auth-fail", Role: "client", RemoteAddr: tunnelServerAddr(t, agentA, 0)},
	}
	cfgB.Imports = []agentconfig.ImportConfig{
		{Name: "should-not-connect", ListenAddr: loopbackAny, RemoteRegion: "cloud-auth-fail", Protocol: "tcp"},
	}
	agentB, done, runErr := launchAgent(t, cfgB)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-done:
			if *runErr == nil || !strings.Contains((*runErr).Error(), "authentication") {
				t.Fatalf("SC6b FAIL: Run = %v, want the server's auth rejection", *runErr)
			}
			t.Logf("PASS SC6b: wrong token → agent startup rejected: %v", *runErr)
			return
		case <-deadline:
			t.Fatal("SC6b FAIL: agent with wrong token neither failed nor stopped within 15s")
		default:
		}
		if agentReady(agentB) {
			t.Fatal("SC6b FAIL: agent with wrong token reported ready — auth not enforced")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
