package agent

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// TestCrossRegionEndToEnd simulates two agents in different regions
// communicating through QUIC tunnels:
//
//	Region A (cloud): runs echo server, Agent acts as tunnel server
//	Region B (edge):  Agent acts as tunnel client, imports echo service
//
// Data flow: TCP client → Agent B import proxy → QUIC tunnel → Agent A server → echo service
func TestCrossRegionEndToEnd(t *testing.T) {
	// 1. Start an echo server (simulates the service in region A).
	echoLn := startTCPEchoServer(t)
	t.Logf("echo server on %s", echoLn.Addr())

	// 2. Start Agent A (cloud) as tunnel server.
	agentACfg := &agentconfig.Config{
		Region: "cloud",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "edge-a",
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

	agentA := runAgent(t, New(agentACfg, nil))
	t.Log("Agent A (cloud) ready")

	// 3. Start Agent B (edge) as tunnel client.
	agentBCfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "cloud",
				Role:         "client",
				RemoteAddr:   tunnelServerAddr(t, agentA, 0),
			},
		},
		Imports: []agentconfig.ImportConfig{
			{
				Name:         "echo-svc",
				ListenAddr:   loopbackAny,
				RemoteRegion: "cloud",
				Protocol:     "tcp",
			},
		},
		HealthAddr:  loopbackAny,
		Metrics:     agentconfig.MetricsConfig{Addr: loopbackAny},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	agentB := runAgent(t, New(agentBCfg, nil))
	importTarget := importAddr(t, agentB, "echo-svc")
	t.Log("Agent B (edge) ready")

	// 4. SC-1: Verify cross-region TCP connectivity.
	t.Run("SC-1_CrossRegionConnectivity", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial import proxy: %v", err)
		}
		defer conn.Close()

		msg := []byte("hello cross-region!")
		conn.Write(msg)

		buf := make([]byte, len(msg))
		io.ReadFull(conn, buf)

		if !bytes.Equal(buf, msg) {
			t.Fatalf("expected %q, got %q", msg, buf)
		}
		t.Log("cross-region echo successful")
	})

	// 5. SC-2: Verify single tunnel multi-stream multiplexing.
	t.Run("SC-2_SingleTunnelMultiStream", func(t *testing.T) {
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

				msg := []byte(fmt.Sprintf("stream-%d-data", i))
				conn.Write(msg)

				buf := make([]byte, len(msg))
				_, err = io.ReadFull(conn, buf)
				if err != nil {
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
		t.Logf("%d concurrent streams through single tunnel successful", concurrentStreams)
	})

	// 6. Large data transfer through cross-region tunnel.
	t.Run("LargeDataTransfer", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", importTarget, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		const dataSize = 256 * 1024 // 256KB
		data := make([]byte, dataSize)
		rand.Read(data)

		// Write concurrently to avoid deadlock with echo server.
		go conn.Write(data)

		received := make([]byte, dataSize)
		_, err = io.ReadFull(conn, received)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(received, data) {
			t.Fatal("data integrity check failed")
		}
		t.Logf("transferred %d bytes through cross-region tunnel", dataSize)
	})

	// Cleanup.
	agentA.Stop()
	agentB.Stop()
}
