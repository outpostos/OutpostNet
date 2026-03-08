package mesh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Wire protocol tests ---

func TestForwardHeader_RoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
	}{
		{"simple", "web-service"},
		{"with-dots", "my.namespace.service"},
		{"short", "a"},
		{"long", strings.Repeat("x", 4096)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			if err := writeForwardHeader(&buf, tt.serviceName); err != nil {
				t.Fatalf("write header: %v", err)
			}

			got, err := readForwardHeader(&buf)
			if err != nil {
				t.Fatalf("read header: %v", err)
			}

			if got != tt.serviceName {
				t.Errorf("expected %q, got %q", tt.serviceName, got)
			}
		})
	}
}

func TestForwardHeader_TooLong(t *testing.T) {
	var buf bytes.Buffer
	err := writeForwardHeader(&buf, strings.Repeat("x", 0x10000))
	if err == nil {
		t.Fatal("expected error for oversized service name")
	}
}

func TestForwardHeader_InvalidLength(t *testing.T) {
	// Zero-length name.
	var buf bytes.Buffer
	buf.Write([]byte{0, 0})
	_, err := readForwardHeader(&buf)
	if err == nil {
		t.Fatal("expected error for zero-length name")
	}
}

func TestForwardResponse_RoundTrip(t *testing.T) {
	tests := []struct {
		status uint8
		msg    string
	}{
		{forwardStatusOK, ""},
		{forwardStatusNotFound, "service not found"},
		{forwardStatusError, "internal error occurred"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.status), func(t *testing.T) {
			var buf bytes.Buffer

			if err := writeForwardResponse(&buf, tt.status, tt.msg); err != nil {
				t.Fatalf("write response: %v", err)
			}

			status, msg, err := readForwardResponse(&buf)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}

			if status != tt.status {
				t.Errorf("expected status %d, got %d", tt.status, status)
			}
			if msg != tt.msg {
				t.Errorf("expected msg %q, got %q", tt.msg, msg)
			}
		})
	}
}

// --- Echo service helper ---

// echoService creates a TCP listener that echoes data back with a prefix.
func echoService(t *testing.T, prefix string) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
				default:
				}
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				buf := make([]byte, 4096)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					reply := prefix + string(buf[:n])
					conn.Write([]byte(reply))
				}
			}()
		}
	}()

	return ln.Addr().String(), func() {
		close(done)
		ln.Close()
		wg.Wait()
	}
}

// --- Forwarder end-to-end tests ---

func TestForwarder_EndToEnd(t *testing.T) {
	// Setup: echo service on "remote" node.
	echoAddr, stopEcho := echoService(t, "echo:")
	defer stopEcho()

	cluster := NewClusterState("node-local")

	// Add peer node with forwarding listener.
	peerHandler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		if serviceName == "echo-svc" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("unknown service: %s", serviceName)
	})

	peerForwarder := NewForwarder(ForwarderConfig{
		ListenAddr:  "127.0.0.1:0",
		DialTimeout: 2 * time.Second,
	}, cluster, peerHandler, nil)

	go peerForwarder.ListenAndServe()
	// Wait for listener to start.
	deadline := time.Now().Add(2 * time.Second)
	for peerForwarder.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("peer forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer peerForwarder.Close()

	peerAddr := peerForwarder.Addr().String()

	// Register peer node and service in cluster state.
	cluster.AddNode(NodeInfo{
		NodeID:  "node-peer",
		Address: peerAddr,
		Status:  NodeStatusHealthy,
	})
	cluster.AddService(ServiceEndpoint{
		NodeID:      "node-peer",
		ServiceName: "echo-svc",
		Protocol:    0,
		Healthy:     true,
	})

	// Create local forwarder (outbound only).
	localForwarder := NewForwarder(ForwarderConfig{
		DialTimeout: 2 * time.Second,
		MaxRetries:  1,
	}, cluster, nil, nil)
	defer localForwarder.Close()

	// Forward a request.
	ctx := context.Background()
	conn, err := localForwarder.Forward(ctx, "echo-svc")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	defer conn.Close()

	// Send data and verify echo.
	_, err = conn.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	got := string(buf[:n])
	if got != "echo:hello" {
		t.Errorf("expected 'echo:hello', got %q", got)
	}
}

func TestForwarder_ServiceNotFound(t *testing.T) {
	cluster := NewClusterState("node-local")

	fwd := NewForwarder(DefaultForwarderConfig(), cluster, nil, nil)
	defer fwd.Close()

	_, err := fwd.Forward(context.Background(), "nonexistent")
	if !errors.Is(err, ErrServiceNotFound) {
		t.Errorf("expected ErrServiceNotFound, got %v", err)
	}
}

func TestForwarder_ForwarderClosed(t *testing.T) {
	cluster := NewClusterState("node-local")
	cluster.AddNode(NodeInfo{NodeID: "peer", Address: "127.0.0.1:1", Status: NodeStatusHealthy})
	cluster.AddService(ServiceEndpoint{NodeID: "peer", ServiceName: "svc", Healthy: true})

	fwd := NewForwarder(DefaultForwarderConfig(), cluster, nil, nil)
	fwd.Close()

	_, err := fwd.Forward(context.Background(), "svc")
	if !errors.Is(err, ErrForwarderClosed) {
		t.Errorf("expected ErrForwarderClosed, got %v", err)
	}
}

func TestForwarder_RoundRobin(t *testing.T) {
	// Create 3 echo services.
	var addrs [3]string
	var stops [3]func()
	for i := range 3 {
		addrs[i], stops[i] = echoService(t, fmt.Sprintf("node%d:", i))
		defer stops[i]()
	}

	cluster := NewClusterState("local")

	// Create 3 peer forwarders, one for each echo service.
	var peerForwarders [3]*Forwarder
	for i := range 3 {
		addr := addrs[i]
		handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
			return net.Dial("tcp", addr)
		})
		peerForwarders[i] = NewForwarder(ForwarderConfig{
			ListenAddr:  "127.0.0.1:0",
			DialTimeout: 2 * time.Second,
		}, cluster, handler, nil)
		go peerForwarders[i].ListenAndServe()
		defer peerForwarders[i].Close()
	}

	// Wait for all listeners.
	for i := range 3 {
		deadline := time.Now().Add(2 * time.Second)
		for peerForwarders[i].Addr() == nil {
			if time.Now().After(deadline) {
				t.Fatalf("peer forwarder %d did not start", i)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Register all peers.
	for i := range 3 {
		nodeID := fmt.Sprintf("peer-%d", i)
		cluster.AddNode(NodeInfo{
			NodeID:  nodeID,
			Address: peerForwarders[i].Addr().String(),
			Status:  NodeStatusHealthy,
		})
		cluster.AddService(ServiceEndpoint{
			NodeID:      nodeID,
			ServiceName: "echo-svc",
			Healthy:     true,
		})
	}

	// Forward multiple requests and track distribution.
	localFwd := NewForwarder(ForwarderConfig{
		DialTimeout: 2 * time.Second,
		MaxRetries:  0, // No failover to verify round-robin.
	}, cluster, nil, nil)
	defer localFwd.Close()

	counts := make(map[string]int)
	for range 9 {
		conn, err := localFwd.Forward(context.Background(), "echo-svc")
		if err != nil {
			t.Fatalf("forward: %v", err)
		}

		conn.Write([]byte("ping"))
		buf := make([]byte, 256)
		n, _ := conn.Read(buf)
		resp := string(buf[:n])
		conn.Close()

		// Extract prefix (e.g., "node0:").
		prefix := resp[:strings.Index(resp, ":")+1]
		counts[prefix]++
	}

	// Each node should get exactly 3 requests.
	for _, prefix := range []string{"node0:", "node1:", "node2:"} {
		if counts[prefix] != 3 {
			t.Errorf("expected 3 requests to %s, got %d (distribution: %v)", prefix, counts[prefix], counts)
		}
	}
}

func TestForwarder_Failover(t *testing.T) {
	// Only node-1 has a working echo service.
	echoAddr, stopEcho := echoService(t, "ok:")
	defer stopEcho()

	cluster := NewClusterState("local")

	// Node 0: dead (unreachable).
	cluster.AddNode(NodeInfo{
		NodeID:  "peer-0",
		Address: "127.0.0.1:1", // likely refused
		Status:  NodeStatusHealthy,
	})
	cluster.AddService(ServiceEndpoint{
		NodeID: "peer-0", ServiceName: "svc", Healthy: true,
	})

	// Node 1: alive with forwarding.
	handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", echoAddr)
	})
	peerFwd := NewForwarder(ForwarderConfig{
		ListenAddr:  "127.0.0.1:0",
		DialTimeout: 2 * time.Second,
	}, cluster, handler, nil)
	go peerFwd.ListenAndServe()
	deadline := time.Now().Add(2 * time.Second)
	for peerFwd.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("peer forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer peerFwd.Close()

	cluster.AddNode(NodeInfo{
		NodeID:  "peer-1",
		Address: peerFwd.Addr().String(),
		Status:  NodeStatusHealthy,
	})
	cluster.AddService(ServiceEndpoint{
		NodeID: "peer-1", ServiceName: "svc", Healthy: true,
	})

	// Forward with failover.
	localFwd := NewForwarder(ForwarderConfig{
		DialTimeout: 500 * time.Millisecond,
		MaxRetries:  2,
	}, cluster, nil, nil)
	defer localFwd.Close()

	conn, err := localFwd.Forward(context.Background(), "svc")
	if err != nil {
		t.Fatalf("expected failover to succeed, got: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("test"))
	buf := make([]byte, 256)
	n, _ := conn.Read(buf)
	got := string(buf[:n])
	if got != "ok:test" {
		t.Errorf("expected 'ok:test', got %q", got)
	}
}

func TestForwarder_IncomingServiceNotFound(t *testing.T) {
	cluster := NewClusterState("node-peer")
	handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("service %s not found", serviceName)
	})

	fwd := NewForwarder(ForwarderConfig{
		ListenAddr:  "127.0.0.1:0",
		DialTimeout: 2 * time.Second,
	}, cluster, handler, nil)

	go fwd.ListenAndServe()
	deadline := time.Now().Add(2 * time.Second)
	for fwd.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer fwd.Close()

	// Connect and send forward request for unknown service.
	conn, err := net.Dial("tcp", fwd.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	writeForwardHeader(conn, "unknown-svc")

	status, _, err := readForwardResponse(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if status != forwardStatusNotFound {
		t.Errorf("expected status NotFound (%d), got %d", forwardStatusNotFound, status)
	}
}

func TestForwarder_Metrics(t *testing.T) {
	echoAddr, stopEcho := echoService(t, "m:")
	defer stopEcho()

	cluster := NewClusterState("local")
	handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", echoAddr)
	})

	peerFwd := NewForwarder(ForwarderConfig{
		ListenAddr:  "127.0.0.1:0",
		DialTimeout: 2 * time.Second,
	}, cluster, handler, nil)
	go peerFwd.ListenAndServe()
	deadline := time.Now().Add(2 * time.Second)
	for peerFwd.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("peer forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer peerFwd.Close()

	cluster.AddNode(NodeInfo{NodeID: "peer", Address: peerFwd.Addr().String(), Status: NodeStatusHealthy})
	cluster.AddService(ServiceEndpoint{NodeID: "peer", ServiceName: "svc", Healthy: true})

	localFwd := NewForwarder(ForwarderConfig{
		DialTimeout: 2 * time.Second,
		MaxRetries:  1,
	}, cluster, nil, nil)
	defer localFwd.Close()

	// Make one successful forward.
	conn, err := localFwd.Forward(context.Background(), "svc")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	conn.Write([]byte("x"))
	buf := make([]byte, 256)
	conn.Read(buf)
	conn.Close()

	// Check outbound metrics.
	if localFwd.Metrics.ForwardTotal.Load() != 1 {
		t.Errorf("expected ForwardTotal=1, got %d", localFwd.Metrics.ForwardTotal.Load())
	}
	if localFwd.Metrics.ForwardErrors.Load() != 0 {
		t.Errorf("expected ForwardErrors=0, got %d", localFwd.Metrics.ForwardErrors.Load())
	}
	if localFwd.Metrics.ForwardLatencyNs.Load() <= 0 {
		t.Error("expected positive ForwardLatencyNs")
	}

	// Wait briefly for active count to settle after conn.Close().
	time.Sleep(50 * time.Millisecond)
	if localFwd.Metrics.ForwardActive.Load() != 0 {
		t.Errorf("expected ForwardActive=0 after close, got %d", localFwd.Metrics.ForwardActive.Load())
	}

	// Check incoming metrics on peer.
	if peerFwd.Metrics.IncomingTotal.Load() != 1 {
		t.Errorf("expected IncomingTotal=1, got %d", peerFwd.Metrics.IncomingTotal.Load())
	}

	// Make a failed forward to trigger error metric.
	_, err = localFwd.Forward(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if localFwd.Metrics.ForwardErrors.Load() != 1 {
		t.Errorf("expected ForwardErrors=1, got %d", localFwd.Metrics.ForwardErrors.Load())
	}
}

func TestForwarder_ConcurrentForwards(t *testing.T) {
	echoAddr, stopEcho := echoService(t, "c:")
	defer stopEcho()

	cluster := NewClusterState("local")
	handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", echoAddr)
	})

	peerFwd := NewForwarder(ForwarderConfig{
		ListenAddr:  "127.0.0.1:0",
		DialTimeout: 2 * time.Second,
	}, cluster, handler, nil)
	go peerFwd.ListenAndServe()
	deadline := time.Now().Add(2 * time.Second)
	for peerFwd.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("peer forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer peerFwd.Close()

	cluster.AddNode(NodeInfo{NodeID: "peer", Address: peerFwd.Addr().String(), Status: NodeStatusHealthy})
	cluster.AddService(ServiceEndpoint{NodeID: "peer", ServiceName: "svc", Healthy: true})

	localFwd := NewForwarder(ForwarderConfig{
		DialTimeout: 5 * time.Second,
		MaxRetries:  1,
	}, cluster, nil, nil)
	defer localFwd.Close()

	const concurrency = 20
	var wg sync.WaitGroup
	var errCount atomic.Int32

	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			conn, err := localFwd.Forward(context.Background(), "svc")
			if err != nil {
				errCount.Add(1)
				t.Logf("forward %d error: %v", idx, err)
				return
			}
			defer conn.Close()

			msg := fmt.Sprintf("msg-%d", idx)
			conn.Write([]byte(msg))
			buf := make([]byte, 256)
			n, err := conn.Read(buf)
			if err != nil {
				errCount.Add(1)
				return
			}
			expected := "c:" + msg
			if string(buf[:n]) != expected {
				errCount.Add(1)
				t.Logf("forward %d: expected %q, got %q", idx, expected, string(buf[:n]))
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("%d out of %d concurrent forwards failed", errCount.Load(), concurrency)
	}
}

func TestForwarder_ContextCancelled(t *testing.T) {
	cluster := NewClusterState("local")
	// Use a slow/unreachable address so the dial takes time.
	cluster.AddNode(NodeInfo{NodeID: "peer", Address: "192.0.2.1:1234", Status: NodeStatusHealthy})
	cluster.AddService(ServiceEndpoint{NodeID: "peer", ServiceName: "svc", Healthy: true})

	fwd := NewForwarder(ForwarderConfig{
		DialTimeout: 30 * time.Second,
		MaxRetries:  0,
	}, cluster, nil, nil)
	defer fwd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := fwd.Forward(ctx, "svc")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestForwarder_LargeDataTransfer(t *testing.T) {
	// Echo service that relays everything.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn) // pure echo
	}()

	cluster := NewClusterState("local")
	handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", ln.Addr().String())
	})

	peerFwd := NewForwarder(ForwarderConfig{
		ListenAddr:  "127.0.0.1:0",
		DialTimeout: 2 * time.Second,
	}, cluster, handler, nil)
	go peerFwd.ListenAndServe()
	deadline := time.Now().Add(2 * time.Second)
	for peerFwd.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("peer forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer peerFwd.Close()

	cluster.AddNode(NodeInfo{NodeID: "peer", Address: peerFwd.Addr().String(), Status: NodeStatusHealthy})
	cluster.AddService(ServiceEndpoint{NodeID: "peer", ServiceName: "svc", Healthy: true})

	localFwd := NewForwarder(ForwarderConfig{
		DialTimeout: 2 * time.Second,
	}, cluster, nil, nil)
	defer localFwd.Close()

	conn, err := localFwd.Forward(context.Background(), "svc")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	defer conn.Close()

	// Send 256KB of data.
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i % 251) // prime to avoid patterns
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var readErr error
	received := make([]byte, len(data))
	go func() {
		defer wg.Done()
		_, readErr = io.ReadFull(conn, received)
	}()

	_, err = conn.Write(data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	wg.Wait()
	if readErr != nil {
		t.Fatalf("read: %v", readErr)
	}

	if !bytes.Equal(data, received) {
		t.Error("data mismatch after forwarding")
	}
}
