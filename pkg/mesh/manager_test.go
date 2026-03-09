package mesh

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestManager_NewManager(t *testing.T) {
	cfg := DefaultClusterConfig("node-1", "127.0.0.1:0")
	cfg.Peers = []PeerConfig{
		{NodeID: "node-2", Address: "10.0.0.2:7630"},
	}

	handler := LocalServiceHandlerFunc(func(name string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not implemented")
	})

	mgr := NewManager(cfg, handler, nil)
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}

	// Check that peer was registered.
	if mgr.State().NodeCount() != 1 {
		t.Errorf("expected 1 peer node, got %d", mgr.State().NodeCount())
	}

	n, ok := mgr.State().GetNode("node-2")
	if !ok {
		t.Fatal("expected node-2 to exist")
	}
	if n.Address != "10.0.0.2:7630" {
		t.Errorf("expected address '10.0.0.2:7630', got %q", n.Address)
	}
}

func TestManager_RegisterUnregisterService(t *testing.T) {
	cfg := DefaultClusterConfig("node-1", "")
	mgr := NewManager(cfg, nil, nil)

	mgr.RegisterLocalService("web", 0, 8080)

	if !mgr.IsLocal("web") {
		t.Error("expected web to be local")
	}

	all := mgr.State().AllServices()
	if len(all["web"]) != 1 {
		t.Fatalf("expected 1 web endpoint, got %d", len(all["web"]))
	}
	if all["web"][0].LocalPort != 8080 {
		t.Errorf("expected port 8080, got %d", all["web"][0].LocalPort)
	}

	mgr.UnregisterLocalService("web")
	if mgr.IsLocal("web") {
		t.Error("expected web to be removed")
	}
}

func TestManager_StartStop(t *testing.T) {
	echoAddr, stopEcho := echoService(t, "mgr:")
	defer stopEcho()

	handler := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		if serviceName == "echo" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("unknown: %s", serviceName)
	})

	cfg := DefaultClusterConfig("node-1", "127.0.0.1:0")
	cfg.SyncInterval = 100 * time.Millisecond
	mgr := NewManager(cfg, handler, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	mgr.RegisterLocalService("echo", 0, 8080)

	// Stop should be clean.
	if err := mgr.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestManager_ForwardIntegration(t *testing.T) {
	// Node B: has echo service.
	echoAddr, stopEcho := echoService(t, "B:")
	defer stopEcho()

	handlerB := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		if serviceName == "echo" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("unknown: %s", serviceName)
	})

	cfgB := DefaultClusterConfig("node-B", "127.0.0.1:0")
	cfgB.SyncInterval = 1 * time.Hour // disable auto-sync for this test
	mgrB := NewManager(cfgB, handlerB, nil)

	ctx := context.Background()
	if err := mgrB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer mgrB.Stop()

	mgrB.RegisterLocalService("echo", 0, 8080)

	// Node A: wants to forward to B.
	cfgA := DefaultClusterConfig("node-A", "")
	cfgA.Peers = []PeerConfig{
		{NodeID: "node-B", Address: mgrB.forwarder.Addr().String()},
	}
	cfgA.SyncInterval = 1 * time.Hour
	mgrA := NewManager(cfgA, nil, nil)

	if err := mgrA.Start(ctx); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer mgrA.Stop()

	// Register B's echo service in A's cluster state (simulating sync).
	mgrA.State().AddService(ServiceEndpoint{
		NodeID:      "node-B",
		ServiceName: "echo",
		Protocol:    0,
		Healthy:     true,
	})

	// Forward from A to B.
	conn, err := mgrA.Forward(ctx, "echo")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("hello"))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	got := string(buf[:n])
	if got != "B:hello" {
		t.Errorf("expected 'B:hello', got %q", got)
	}
}

func TestManager_SyncBetweenPeers(t *testing.T) {
	// Node A and Node B both have services.
	echoA, stopA := echoService(t, "A:")
	defer stopA()
	echoB, stopB := echoService(t, "B:")
	defer stopB()

	handlerA := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", echoA)
	})
	handlerB := LocalServiceHandlerFunc(func(serviceName string) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", echoB)
	})

	// Start Node B first.
	cfgB := DefaultClusterConfig("node-B", "127.0.0.1:0")
	cfgB.SyncInterval = 1 * time.Hour
	mgrB := NewManager(cfgB, handlerB, nil)

	ctx := context.Background()
	if err := mgrB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer mgrB.Stop()

	mgrB.RegisterLocalService("svc-B", 0, 3306)

	// Start Node A with B as peer.
	cfgA := DefaultClusterConfig("node-A", "127.0.0.1:0")
	cfgA.Peers = []PeerConfig{
		{NodeID: "node-B", Address: mgrB.forwarder.Addr().String()},
	}
	cfgA.SyncInterval = 100 * time.Millisecond
	mgrA := NewManager(cfgA, handlerA, nil)

	if err := mgrA.Start(ctx); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer mgrA.Stop()

	mgrA.RegisterLocalService("svc-A", 0, 8080)

	// Wait for sync to propagate.
	deadline := time.Now().Add(3 * time.Second)
	for {
		remote := mgrA.State().LookupRemote("svc-B")
		if len(remote) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sync did not propagate svc-B to node-A within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify A knows about B's service.
	remote := mgrA.State().LookupRemote("svc-B")
	if len(remote) != 1 {
		t.Fatalf("expected 1 remote endpoint for svc-B, got %d", len(remote))
	}
	if remote[0].NodeID != "node-B" {
		t.Errorf("expected node-B, got %s", remote[0].NodeID)
	}
}

func TestManager_ForwardNotFound(t *testing.T) {
	cfg := DefaultClusterConfig("node-1", "")
	mgr := NewManager(cfg, nil, nil)
	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer mgr.Stop()

	_, err := mgr.Forward(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent service")
	}
}

func TestSyncPayload_RoundTrip(t *testing.T) {
	services := []ServiceEndpoint{
		{ServiceName: "web", Protocol: 0, LocalPort: 8080},
		{ServiceName: "db", Protocol: 0, LocalPort: 3306},
		{ServiceName: "cache", Protocol: 1, LocalPort: 6379},
	}

	// Use a pipe to test write/read.
	r, w := io.Pipe()

	go func() {
		if err := writeSyncPayload(w, services); err != nil {
			t.Errorf("write: %v", err)
		}
		w.Close()
	}()

	got, err := readSyncPayload(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(got) != len(services) {
		t.Fatalf("expected %d services, got %d", len(services), len(got))
	}

	for i, ep := range got {
		if ep.ServiceName != services[i].ServiceName {
			t.Errorf("[%d] name: expected %q, got %q", i, services[i].ServiceName, ep.ServiceName)
		}
		if ep.Protocol != services[i].Protocol {
			t.Errorf("[%d] protocol: expected %d, got %d", i, services[i].Protocol, ep.Protocol)
		}
		if ep.LocalPort != services[i].LocalPort {
			t.Errorf("[%d] port: expected %d, got %d", i, services[i].LocalPort, ep.LocalPort)
		}
	}
}

func TestSyncPayload_Empty(t *testing.T) {
	r, w := io.Pipe()

	go func() {
		writeSyncPayload(w, nil)
		w.Close()
	}()

	got, err := readSyncPayload(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 services, got %d", len(got))
	}
}
