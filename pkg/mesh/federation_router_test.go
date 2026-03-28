package mesh

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func TestFederationRouter_LocalHit(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Create a local echo service.
	echoAddr, cleanup := echoFederationService(t, "local:")
	defer cleanup()

	// Manager with local service.
	config := DefaultClusterConfig("node-1", "127.0.0.1:0")
	handler := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		if svc == "my-svc" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("not found")
	})
	mgr := NewManager(config, handler, logger)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()
	mgr.RegisterLocalService("my-svc", 0, 8080)

	router := NewFederationRouter(mgr, nil, nil, nil, logger)

	rwc, err := router.Route(ctx, "my-svc")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	defer rwc.Close()

	rwc.Write([]byte("test"))
	buf := make([]byte, 256)
	n, err := rwc.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf[:n]) != "local:test" {
		t.Fatalf("expected local:test, got %s", string(buf[:n]))
	}

	if router.Metrics.LocalHits.Load() != 1 {
		t.Fatalf("expected 1 local hit, got %d", router.Metrics.LocalHits.Load())
	}
	if router.Metrics.TotalRequests.Load() != 1 {
		t.Fatalf("expected 1 total request, got %d", router.Metrics.TotalRequests.Load())
	}
}

func TestFederationRouter_ClusterHit(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Echo service on node-2.
	echoAddr, cleanup := echoFederationService(t, "cluster:")
	defer cleanup()

	// Node 2 with the service.
	config2 := DefaultClusterConfig("node-2", "127.0.0.1:0")
	handler2 := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		if svc == "remote-svc" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("not found")
	})
	mgr2 := NewManager(config2, handler2, logger)
	if err := mgr2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgr2.Stop()
	mgr2.RegisterLocalService("remote-svc", 0, 9090)

	// Wait for listener.
	deadline := time.Now().Add(5 * time.Second)
	for mgr2.forwarder.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("forwarder did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Node 1: knows about node-2 as a peer.
	config1 := DefaultClusterConfig("node-1", "127.0.0.1:0")
	config1.Peers = []PeerConfig{{NodeID: "node-2", Address: mgr2.forwarder.Addr().String()}}
	handler1 := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgr1 := NewManager(config1, handler1, logger)
	if err := mgr1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgr1.Stop()

	// Sync so node-1 knows about remote-svc on node-2.
	time.Sleep(100 * time.Millisecond)

	router := NewFederationRouter(mgr1, nil, nil, nil, logger)

	rwc, err := router.Route(ctx, "remote-svc")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	defer rwc.Close()

	rwc.Write([]byte("hello"))
	buf := make([]byte, 256)
	n, err := rwc.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf[:n]) != "cluster:hello" {
		t.Fatalf("expected cluster:hello, got %s", string(buf[:n]))
	}

	if router.Metrics.ClusterHits.Load() != 1 {
		t.Fatalf("expected 1 cluster hit, got %d", router.Metrics.ClusterHits.Load())
	}
}

func TestFederationRouter_FederationHit(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Echo service on remote cluster.
	echoAddr, cleanup := echoFederationService(t, "fed:")
	defer cleanup()

	// Remote cluster A with service.
	configA := DefaultClusterConfig("node-a1", "127.0.0.1:0")
	handlerA := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		if svc == "cloud-db" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("not found")
	})
	mgrA := NewManager(configA, handlerA, logger)
	if err := mgrA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()
	mgrA.RegisterLocalService("cloud-db", 0, 3306)

	fedAConfig := DefaultFederationConfig("cluster-a", "127.0.0.1:0")
	fedA := NewFederationManager(fedAConfig, mgrA, logger)
	fedLnA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fedA.listener = fedLnA
	go fedA.acceptLoop()
	defer fedA.Stop()
	fedAAddr := fedLnA.Addr().String()

	// Local cluster B: no such service.
	configB := DefaultClusterConfig("node-b1", "127.0.0.1:0")
	handlerB := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgrB := NewManager(configB, handlerB, logger)
	if err := mgrB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrB.Stop()

	fedBConfig := DefaultFederationConfig("cluster-b", "")
	fedB := NewFederationManager(fedBConfig, mgrB, logger)
	fedB.state.AddCluster(ClusterInfo{ClusterID: "cluster-a", Address: fedAAddr, Status: NodeStatusHealthy})
	fedB.state.SetRemoteServices("cluster-a", []ServiceEndpoint{
		{ServiceName: "cloud-db", Protocol: 0, LocalPort: 3306, Healthy: true},
	})

	router := NewFederationRouter(mgrB, fedB, nil, nil, logger)

	rwc, err := router.Route(ctx, "cloud-db")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	defer rwc.Close()

	rwc.Write([]byte("query"))
	buf := make([]byte, 256)
	n, err := rwc.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf[:n]) != "fed:query" {
		t.Fatalf("expected fed:query, got %s", string(buf[:n]))
	}

	if router.Metrics.FederationHits.Load() != 1 {
		t.Fatalf("expected 1 federation hit, got %d", router.Metrics.FederationHits.Load())
	}
}

func TestFederationRouter_NotFound(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	config := DefaultClusterConfig("node-1", "127.0.0.1:0")
	handler := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgr := NewManager(config, handler, logger)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()

	router := NewFederationRouter(mgr, nil, nil, nil, logger)

	_, err := router.Route(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent service")
	}

	if router.Metrics.RoutingErrors.Load() != 1 {
		t.Fatalf("expected 1 routing error, got %d", router.Metrics.RoutingErrors.Load())
	}
}

func TestFederationRouter_LeaderOnly(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Create a federation manager with a remote service.
	fedConfig := DefaultFederationConfig("cluster-b", "")
	fed := NewFederationManager(fedConfig, nil, logger)
	fed.state.AddCluster(ClusterInfo{ClusterID: "cluster-a", Status: NodeStatusHealthy})
	fed.state.SetRemoteServices("cluster-a", []ServiceEndpoint{
		{ServiceName: "cloud-svc", Healthy: true},
	})

	// Create a non-leader election state.
	electionConfig := DefaultElectionConfig("node-1")
	election := NewLeaderElection(electionConfig, logger)
	// Don't make it leader - it's a follower by default.

	router := NewFederationRouter(nil, fed, nil, election, logger)

	// Non-leader should not forward to federation.
	_, err := router.Route(ctx, "cloud-svc")
	if err == nil {
		t.Fatal("expected error when non-leader")
	}

	if router.Metrics.RoutingErrors.Load() != 1 {
		t.Fatalf("expected 1 routing error, got %d", router.Metrics.RoutingErrors.Load())
	}
}

func TestFederationRouter_Lookup(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Manager with local and cluster services.
	config := DefaultClusterConfig("node-1", "127.0.0.1:0")
	handler := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgr := NewManager(config, handler, logger)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()
	mgr.RegisterLocalService("local-svc", 0, 8080)

	// Federation with a remote service.
	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, mgr, logger)
	fed.state.AddCluster(ClusterInfo{ClusterID: "cluster-remote", Status: NodeStatusHealthy})
	fed.state.SetRemoteServices("cluster-remote", []ServiceEndpoint{
		{ServiceName: "remote-svc", Healthy: true},
	})

	router := NewFederationRouter(mgr, fed, nil, nil, logger)

	// Lookup local service.
	routes := router.Lookup("local-svc")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route for local-svc, got %d", len(routes))
	}
	if routes[0].Tier != "local" {
		t.Fatalf("expected local tier, got %s", routes[0].Tier)
	}

	// Lookup remote service.
	routes = router.Lookup("remote-svc")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route for remote-svc, got %d", len(routes))
	}
	if routes[0].Tier != "federation" {
		t.Fatalf("expected federation tier, got %s", routes[0].Tier)
	}
	if routes[0].ClusterID != "cluster-remote" {
		t.Fatalf("expected cluster-remote, got %s", routes[0].ClusterID)
	}

	// Lookup nonexistent service.
	routes = router.Lookup("unknown")
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes for unknown, got %d", len(routes))
	}
}

func TestFederationRouter_NilComponents(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Router with all nil components should not panic.
	router := NewFederationRouter(nil, nil, nil, nil, logger)

	_, err := router.Route(ctx, "any-service")
	if err == nil {
		t.Fatal("expected error with nil components")
	}

	routes := router.Lookup("any-service")
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes, got %d", len(routes))
	}
}
