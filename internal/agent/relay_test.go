package agent

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// TestRelayThreeRegion simulates a 3-region relay scenario:
//
//	Region B (cloud): runs echo server, Agent B acts as tunnel server
//	Region C (relay): Agent C acts as tunnel server (for A) + tunnel client (to B)
//	Region A (edge):  Agent A acts as tunnel client (to C), imports echo service
//
// Data flow: TCP client → Agent A import → QUIC → Agent C relay → QUIC → Agent B → echo
func TestRelayThreeRegion(t *testing.T) {
	// 1. Start echo server (simulates the service in region B).
	echoLn := startTCPEchoServer(t)
	t.Logf("echo server on %s", echoLn.Addr())

	// 2. Start Agent B (cloud) as tunnel server.
	agentBCfg := &agentconfig.Config{
		Region: "cloud-b",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "relay-c",
				Role:         "server",
				ListenAddr:   loopbackAny,
			},
		},
		Exports: []agentconfig.ExportConfig{
			{
				Name:      "echo-svc",
				LocalAddr: echoLn.Addr().String(),
				Protocol:  "tcp",
			},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := runAgent(t, New(agentBCfg, nil))
	t.Log("Agent B (cloud) ready")

	// 3. Start Agent C (relay) as tunnel server (for A) + tunnel client (to B).
	agentCCfg := &agentconfig.Config{
		Region: "relay-c",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "edge-a",
				Role:         "server",
				ListenAddr:   loopbackAny,
			},
			{
				RemoteRegion: "cloud-b",
				Role:         "client",
				RemoteAddr:   tunnelServerAddr(t, agentB, 0),
			},
		},
		Relays: []agentconfig.RelayConfig{
			{
				Service:    "echo-svc",
				FromRegion: "edge-a",
				ToRegion:   "cloud-b",
				MaxHops:    3,
			},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := runAgent(t, New(agentCCfg, nil))
	t.Log("Agent C (relay) ready")

	// 4. Start Agent A (edge) as tunnel client (to C), importing echo-svc.
	agentACfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "relay-c",
				Role:         "client",
				RemoteAddr:   tunnelServerAddr(t, agentC, 0),
			},
		},
		Imports: []agentconfig.ImportConfig{
			{
				Name:         "echo-svc",
				ListenAddr:   loopbackAny,
				RemoteRegion: "relay-c",
				Protocol:     "tcp",
			},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := runAgent(t, New(agentACfg, nil))
	importTarget := importAddr(t, agentA, "echo-svc")
	t.Log("Agent A (edge) ready")

	// 5. SC-1: Verify relay connectivity.
	t.Run("SC-1_RelayConnectivity", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial import proxy: %v", err)
		}
		defer conn.Close()

		msg := []byte("hello via relay!")
		conn.Write(msg)

		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(buf, msg) {
			t.Fatalf("expected %q, got %q", msg, buf)
		}
		t.Log("relay echo successful: A → C → B → echo → B → C → A")
	})

	// 6. SC-2: Multiple concurrent streams through relay.
	t.Run("SC-2_RelayMultiStream", func(t *testing.T) {
		const concurrentStreams = 10
		var wg sync.WaitGroup
		errCh := make(chan error, concurrentStreams)

		for i := range concurrentStreams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
				if err != nil {
					errCh <- fmt.Errorf("stream %d dial: %w", i, err)
					return
				}
				defer conn.Close()

				msg := []byte(fmt.Sprintf("relay-stream-%d", i))
				conn.Write(msg)

				buf := make([]byte, len(msg))
				if _, err := io.ReadFull(conn, buf); err != nil {
					errCh <- fmt.Errorf("stream %d read: %w", i, err)
					return
				}
				if !bytes.Equal(buf, msg) {
					errCh <- fmt.Errorf("stream %d: expected %q, got %q", i, msg, buf)
				}
			}()
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Error(err)
		}
		t.Logf("%d concurrent streams through relay successful", concurrentStreams)
	})

	// 7. SC-3: Large data transfer through relay.
	t.Run("SC-3_RelayLargeData", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		const dataSize = 256 * 1024 // 256KB
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 251) // deterministic pattern
		}

		go conn.Write(data)

		received := make([]byte, dataSize)
		if _, err := io.ReadFull(conn, received); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(received, data) {
			t.Fatal("data integrity check failed through relay")
		}
		t.Logf("transferred %d bytes through relay", dataSize)
	})

	// Cleanup.
	agentA.Stop()
	agentC.Stop()
	agentB.Stop()
}

// TestRelayHopLimit verifies that the relay rejects streams exceeding max_hops.
func TestRelayHopLimit(t *testing.T) {
	echoLn := startTCPEchoServer(t)

	// Agent B (destination).
	agentBCfg := &agentconfig.Config{
		Region: "cloud-b",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "relay-c", Role: "server", ListenAddr: loopbackAny},
		},
		Exports:     []agentconfig.ExportConfig{{Name: "echo-svc", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"}},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := runAgent(t, New(agentBCfg, nil))

	// Agent C (relay) with max_hops=1.
	agentCCfg := &agentconfig.Config{
		Region: "relay-c",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "edge-a", Role: "server", ListenAddr: loopbackAny},
			{RemoteRegion: "cloud-b", Role: "client", RemoteAddr: tunnelServerAddr(t, agentB, 0)},
		},
		Relays: []agentconfig.RelayConfig{
			{Service: "echo-svc", FromRegion: "edge-a", ToRegion: "cloud-b", MaxHops: 1},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := runAgent(t, New(agentCCfg, nil))

	// Agent A (edge).
	agentACfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "relay-c", Role: "client", RemoteAddr: tunnelServerAddr(t, agentC, 0)},
		},
		Imports: []agentconfig.ImportConfig{
			{Name: "echo-svc", ListenAddr: loopbackAny, RemoteRegion: "relay-c", Protocol: "tcp"},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := runAgent(t, New(agentACfg, nil))
	importTarget := importAddr(t, agentA, "echo-svc")

	// max_hops=1 means hop count 0→1 is allowed (first relay).
	// A standard stream (hop count 0) gets incremented to 1, which equals max_hops.
	// So single-hop relay should work.
	t.Run("SingleHopAllowed", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		msg := []byte("hop-test")
		conn.Write(msg)

		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(buf, msg) {
			t.Fatalf("expected %q, got %q", msg, buf)
		}
		t.Log("single hop relay works with max_hops=1")
	})

	agentA.Stop()
	agentC.Stop()
	agentB.Stop()
}

// TestRelayHopLimitExceeded verifies that a 4-region chain with max_hops=1
// rejects the second hop. Chain: A → C → D → B where D has max_hops=1.
// C relays with hop_count=1 (allowed), D would need hop_count=2 (rejected).
func TestRelayHopLimitExceeded(t *testing.T) {
	echoLn := startTCPEchoServer(t)

	// Agent B (destination) — tunnel server for D.
	agentBCfg := &agentconfig.Config{
		Region: "region-b",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-d", Role: "server", ListenAddr: loopbackAny},
		},
		Exports:     []agentconfig.ExportConfig{{Name: "echo-svc", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"}},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := runAgent(t, New(agentBCfg, nil))

	// Agent D (relay) — server for C, client to B. max_hops=1.
	agentDCfg := &agentconfig.Config{
		Region: "region-d",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-c", Role: "server", ListenAddr: loopbackAny},
			{RemoteRegion: "region-b", Role: "client", RemoteAddr: tunnelServerAddr(t, agentB, 0)},
		},
		Relays: []agentconfig.RelayConfig{
			{Service: "echo-svc", FromRegion: "region-c", ToRegion: "region-b", MaxHops: 1},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentD := runAgent(t, New(agentDCfg, nil))

	// Agent C (relay) — server for A, client to D. max_hops=3.
	agentCCfg := &agentconfig.Config{
		Region: "region-c",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-a", Role: "server", ListenAddr: loopbackAny},
			{RemoteRegion: "region-d", Role: "client", RemoteAddr: tunnelServerAddr(t, agentD, 0)},
		},
		Relays: []agentconfig.RelayConfig{
			{Service: "echo-svc", FromRegion: "region-a", ToRegion: "region-d", MaxHops: 3},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := runAgent(t, New(agentCCfg, nil))

	// Agent A (edge) — client to C, imports echo-svc.
	agentACfg := &agentconfig.Config{
		Region: "region-a",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-c", Role: "client", RemoteAddr: tunnelServerAddr(t, agentC, 0)},
		},
		Imports: []agentconfig.ImportConfig{
			{Name: "echo-svc", ListenAddr: loopbackAny, RemoteRegion: "region-c", Protocol: "tcp"},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := runAgent(t, New(agentACfg, nil))
	importTarget := importAddr(t, agentA, "echo-svc")

	// A→C: hop_count=0 → C relays with hop_count=1 → D receives hop_count=1,
	// increments to 2, which exceeds max_hops=1. D rejects the stream.
	// The TCP connection should fail or timeout.
	t.Run("TwoHopExceedsLimit", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", importTarget, 2*time.Second)
		if err != nil {
			t.Logf("dial failed as expected: %v", err)
			return
		}
		defer conn.Close()

		msg := []byte("should-not-arrive")
		conn.Write(msg)

		// Expect read to fail (connection closed by relay rejection).
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, len(msg))
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			t.Logf("read failed as expected (hop limit exceeded): %v", err)
		} else {
			t.Errorf("expected read failure due to hop limit, but got data: %q", buf)
		}
	})

	agentA.Stop()
	agentC.Stop()
	agentD.Stop()
	agentB.Stop()
}

// TestRelayDirectAndRelay verifies that direct and relay paths work simultaneously.
// Setup: A ↔ B (direct), A → C → D (relay), all independent.
func TestRelayDirectAndRelay(t *testing.T) {
	echoLn1 := startTCPEchoServer(t) // service in B (direct)
	echoLn2 := startTCPEchoServer(t) // service in D (relay)

	// Agent B — tunnel server for A, exports direct-svc.
	agentBCfg := &agentconfig.Config{
		Region: "region-b",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-a", Role: "server", ListenAddr: loopbackAny},
		},
		Exports: []agentconfig.ExportConfig{
			{Name: "direct-svc", LocalAddr: echoLn1.Addr().String(), Protocol: "tcp"},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := runAgent(t, New(agentBCfg, nil))

	// Agent D — tunnel server for C, exports relay-svc.
	agentDCfg := &agentconfig.Config{
		Region: "region-d",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-c", Role: "server", ListenAddr: loopbackAny},
		},
		Exports: []agentconfig.ExportConfig{
			{Name: "relay-svc", LocalAddr: echoLn2.Addr().String(), Protocol: "tcp"},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentD := runAgent(t, New(agentDCfg, nil))

	// Agent C (relay) — server for A, client to D, relays relay-svc.
	agentCCfg := &agentconfig.Config{
		Region: "region-c",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-a", Role: "server", ListenAddr: loopbackAny},
			{RemoteRegion: "region-d", Role: "client", RemoteAddr: tunnelServerAddr(t, agentD, 0)},
		},
		Relays: []agentconfig.RelayConfig{
			{Service: "relay-svc", FromRegion: "region-a", ToRegion: "region-d", MaxHops: 3},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := runAgent(t, New(agentCCfg, nil))

	// Agent A — client to B (direct) and client to C (relay).
	agentACfg := &agentconfig.Config{
		Region: "region-a",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "region-b", Role: "client", RemoteAddr: tunnelServerAddr(t, agentB, 0)},
			{RemoteRegion: "region-c", Role: "client", RemoteAddr: tunnelServerAddr(t, agentC, 0)},
		},
		Imports: []agentconfig.ImportConfig{
			{Name: "direct-svc", ListenAddr: loopbackAny, RemoteRegion: "region-b", Protocol: "tcp"},
			{Name: "relay-svc", ListenAddr: loopbackAny, RemoteRegion: "region-c", Protocol: "tcp"},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := runAgent(t, New(agentACfg, nil))
	directTarget := importAddr(t, agentA, "direct-svc")
	relayTarget := importAddr(t, agentA, "relay-svc")

	// SC-5a: Direct path works.
	t.Run("DirectPath", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", directTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial direct: %v", err)
		}
		defer conn.Close()

		msg := []byte("direct-path-msg")
		conn.Write(msg)

		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(buf, msg) {
			t.Fatalf("expected %q, got %q", msg, buf)
		}
		t.Log("direct path A→B works")
	})

	// SC-5b: Relay path works simultaneously.
	t.Run("RelayPath", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", relayTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial relay: %v", err)
		}
		defer conn.Close()

		msg := []byte("relay-path-msg")
		conn.Write(msg)

		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(buf, msg) {
			t.Fatalf("expected %q, got %q", msg, buf)
		}
		t.Log("relay path A→C→D works")
	})

	// SC-5c: Both paths concurrently.
	t.Run("BothConcurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		errCh := make(chan error, 2)

		for _, tc := range []struct {
			name string
			addr string
		}{
			{"direct", directTarget},
			{"relay", relayTarget},
		} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := net.DialTimeout("tcp", tc.addr, 5*time.Second)
				if err != nil {
					errCh <- fmt.Errorf("%s dial: %w", tc.name, err)
					return
				}
				defer conn.Close()

				msg := []byte(fmt.Sprintf("concurrent-%s", tc.name))
				conn.Write(msg)

				buf := make([]byte, len(msg))
				if _, err := io.ReadFull(conn, buf); err != nil {
					errCh <- fmt.Errorf("%s read: %w", tc.name, err)
					return
				}
				if !bytes.Equal(buf, msg) {
					errCh <- fmt.Errorf("%s: expected %q, got %q", tc.name, msg, buf)
				}
			}()
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Error(err)
		}
		t.Log("direct and relay paths work concurrently")
	})

	agentA.Stop()
	agentC.Stop()
	agentD.Stop()
	agentB.Stop()
}

// TestRelayManagerHasRules verifies HasRules returns false when empty and true after adding a rule.
func TestRelayManagerHasRules(t *testing.T) {
	rm := NewRelayManager(nil, nil)

	if rm.HasRules() {
		t.Fatal("expected HasRules() == false for empty RelayManager")
	}

	rm.AddRule(agentconfig.RelayConfig{
		Service:    "test-svc",
		FromRegion: "region-a",
		ToRegion:   "region-b",
		MaxHops:    3,
	}, nil)

	if !rm.HasRules() {
		t.Fatal("expected HasRules() == true after adding a rule")
	}
}

// TestRelayMetricsE2E verifies that relay metrics are populated after traffic.
func TestRelayMetricsE2E(t *testing.T) {
	echoLn := startTCPEchoServer(t)

	// Agent B (cloud) as tunnel server.
	agentBCfg := &agentconfig.Config{
		Region: "cloud-b",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "relay-c", Role: "server", ListenAddr: loopbackAny},
		},
		Exports:     []agentconfig.ExportConfig{{Name: "echo-svc", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"}},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := runAgent(t, New(agentBCfg, nil))

	// Agent C (relay).
	agentCCfg := &agentconfig.Config{
		Region: "relay-c",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "edge-a", Role: "server", ListenAddr: loopbackAny},
			{RemoteRegion: "cloud-b", Role: "client", RemoteAddr: tunnelServerAddr(t, agentB, 0)},
		},
		Relays: []agentconfig.RelayConfig{
			{Service: "echo-svc", FromRegion: "edge-a", ToRegion: "cloud-b", MaxHops: 3},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := runAgent(t, New(agentCCfg, nil))

	// Agent A (edge).
	agentACfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "relay-c", Role: "client", RemoteAddr: tunnelServerAddr(t, agentC, 0)},
		},
		Imports: []agentconfig.ImportConfig{
			{Name: "echo-svc", ListenAddr: loopbackAny, RemoteRegion: "relay-c", Protocol: "tcp"},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := runAgent(t, New(agentACfg, nil))
	importTarget := importAddr(t, agentA, "echo-svc")

	// Send some traffic through the relay.
	for i := range 3 {
		conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("stream %d dial: %v", i, err)
		}
		msg := []byte(fmt.Sprintf("metrics-test-%d", i))
		conn.Write(msg)
		buf := make([]byte, len(msg))
		io.ReadFull(conn, buf)
		conn.Close()
	}

	// Give metrics time to update.
	time.Sleep(100 * time.Millisecond)

	// Scrape metrics from Agent C (the relay node).
	resp, err := http.Get("http://" + agentC.BoundAddrs().Metrics.String() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	// Verify relay metrics are present.
	checks := []struct {
		metric string
		desc   string
	}{
		{"outpostnet_relay_streams_total", "relay streams counter"},
		{"outpostnet_relay_active_streams", "relay active streams gauge"},
		{"outpostnet_relay_latency_seconds", "relay latency histogram"},
		{"outpostnet_relay_bytes_total", "relay bytes counter"},
	}

	for _, check := range checks {
		if !strings.Contains(output, check.metric) {
			t.Errorf("%s (%s) not found in metrics output", check.metric, check.desc)
		}
	}

	// Verify relay_streams_total has at least 3 streams.
	if !strings.Contains(output, `outpostnet_relay_streams_total{from_region="edge-a",service="echo-svc",to_region="cloud-b"} 3`) {
		t.Logf("metrics output:\n%s", output)
		t.Error("relay_streams_total should be 3 for echo-svc")
	}

	t.Log("relay metrics verified on relay node")

	agentA.Stop()
	agentC.Stop()
	agentB.Stop()
}
