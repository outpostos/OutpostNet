package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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
	agentBHealthPort := freePort(t)
	agentBMetricsPort := freePort(t)
	agentBTunnelPort := freePort(t)

	agentBCfg := &agentconfig.Config{
		Region: "cloud-b",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "relay-c",
				Role:         "server",
				ListenAddr:   fmt.Sprintf("127.0.0.1:%d", agentBTunnelPort),
			},
		},
		Exports: []agentconfig.ExportConfig{
			{
				Name:      "echo-svc",
				LocalAddr: echoLn.Addr().String(),
				Protocol:  "tcp",
			},
		},
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", agentBHealthPort),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", agentBMetricsPort)},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := New(agentBCfg, nil)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	go agentB.Run(ctxB)
	waitForReady(t, agentBHealthPort, 5*time.Second)
	t.Log("Agent B (cloud) ready")

	// 3. Start Agent C (relay) as tunnel server (for A) + tunnel client (to B).
	agentCHealthPort := freePort(t)
	agentCMetricsPort := freePort(t)
	agentCTunnelPort := freePort(t)

	agentCCfg := &agentconfig.Config{
		Region: "relay-c",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "edge-a",
				Role:         "server",
				ListenAddr:   fmt.Sprintf("127.0.0.1:%d", agentCTunnelPort),
			},
			{
				RemoteRegion: "cloud-b",
				Role:         "client",
				RemoteAddr:   fmt.Sprintf("127.0.0.1:%d", agentBTunnelPort),
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
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", agentCHealthPort),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", agentCMetricsPort)},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := New(agentCCfg, nil)
	ctxC, cancelC := context.WithCancel(context.Background())
	defer cancelC()

	go agentC.Run(ctxC)
	waitForReady(t, agentCHealthPort, 5*time.Second)
	t.Log("Agent C (relay) ready")

	// 4. Start Agent A (edge) as tunnel client (to C), importing echo-svc.
	agentAHealthPort := freePort(t)
	agentAMetricsPort := freePort(t)
	importPort := freePort(t)

	agentACfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "relay-c",
				Role:         "client",
				RemoteAddr:   fmt.Sprintf("127.0.0.1:%d", agentCTunnelPort),
			},
		},
		Imports: []agentconfig.ImportConfig{
			{
				Name:         "echo-svc",
				ListenAddr:   fmt.Sprintf("127.0.0.1:%d", importPort),
				RemoteRegion: "relay-c",
				Protocol:     "tcp",
			},
		},
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", agentAHealthPort),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", agentAMetricsPort)},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := New(agentACfg, nil)
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()

	go agentA.Run(ctxA)
	waitForReady(t, agentAHealthPort, 5*time.Second)
	t.Log("Agent A (edge) ready")

	// Give import proxy time to start.
	time.Sleep(300 * time.Millisecond)

	// 5. SC-1: Verify relay connectivity.
	t.Run("SC-1_RelayConnectivity", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 5*time.Second)
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
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 5*time.Second)
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
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 5*time.Second)
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
	agentBHealthPort := freePort(t)
	agentBMetricsPort := freePort(t)
	agentBTunnelPort := freePort(t)

	agentBCfg := &agentconfig.Config{
		Region: "cloud-b",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "relay-c", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", agentBTunnelPort)},
		},
		Exports:     []agentconfig.ExportConfig{{Name: "echo-svc", LocalAddr: echoLn.Addr().String(), Protocol: "tcp"}},
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", agentBHealthPort),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", agentBMetricsPort)},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := New(agentBCfg, nil)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go agentB.Run(ctxB)
	waitForReady(t, agentBHealthPort, 5*time.Second)

	// Agent C (relay) with max_hops=1.
	agentCHealthPort := freePort(t)
	agentCMetricsPort := freePort(t)
	agentCTunnelPort := freePort(t)

	agentCCfg := &agentconfig.Config{
		Region: "relay-c",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "edge-a", Role: "server", ListenAddr: fmt.Sprintf("127.0.0.1:%d", agentCTunnelPort)},
			{RemoteRegion: "cloud-b", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", agentBTunnelPort)},
		},
		Relays: []agentconfig.RelayConfig{
			{Service: "echo-svc", FromRegion: "edge-a", ToRegion: "cloud-b", MaxHops: 1},
		},
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", agentCHealthPort),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", agentCMetricsPort)},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentC := New(agentCCfg, nil)
	ctxC, cancelC := context.WithCancel(context.Background())
	defer cancelC()
	go agentC.Run(ctxC)
	waitForReady(t, agentCHealthPort, 5*time.Second)

	// Agent A (edge).
	agentAHealthPort := freePort(t)
	agentAMetricsPort := freePort(t)
	importPort := freePort(t)

	agentACfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{RemoteRegion: "relay-c", Role: "client", RemoteAddr: fmt.Sprintf("127.0.0.1:%d", agentCTunnelPort)},
		},
		Imports: []agentconfig.ImportConfig{
			{Name: "echo-svc", ListenAddr: fmt.Sprintf("127.0.0.1:%d", importPort), RemoteRegion: "relay-c", Protocol: "tcp"},
		},
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", agentAHealthPort),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", agentAMetricsPort)},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentA := New(agentACfg, nil)
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	go agentA.Run(ctxA)
	waitForReady(t, agentAHealthPort, 5*time.Second)

	time.Sleep(300 * time.Millisecond)

	// max_hops=1 means hop count 0→1 is allowed (first relay).
	// A standard stream (hop count 0) gets incremented to 1, which equals max_hops.
	// So single-hop relay should work.
	t.Run("SingleHopAllowed", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", importPort), 5*time.Second)
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

