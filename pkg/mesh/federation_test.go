package mesh

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFederationState_ClusterManagement(t *testing.T) {
	fs := NewFederationState("cluster-a")

	// Add clusters.
	fs.AddCluster(ClusterInfo{ClusterID: "cluster-b", Address: "10.0.1.1:7640", Status: NodeStatusHealthy})
	fs.AddCluster(ClusterInfo{ClusterID: "cluster-c", Address: "10.0.2.1:7640", Status: NodeStatusUnknown})

	if fs.ClusterCount() != 2 {
		t.Fatalf("expected 2 clusters, got %d", fs.ClusterCount())
	}

	// Get cluster.
	c, ok := fs.GetCluster("cluster-b")
	if !ok {
		t.Fatal("cluster-b not found")
	}
	if c.Address != "10.0.1.1:7640" {
		t.Fatalf("expected address 10.0.1.1:7640, got %s", c.Address)
	}

	// Healthy clusters.
	healthy := fs.HealthyClusters()
	if len(healthy) != 1 {
		t.Fatalf("expected 1 healthy cluster, got %d", len(healthy))
	}
	if healthy[0].ClusterID != "cluster-b" {
		t.Fatalf("expected cluster-b, got %s", healthy[0].ClusterID)
	}

	// Update status.
	fs.UpdateClusterStatus("cluster-c", NodeStatusHealthy)
	healthy = fs.HealthyClusters()
	if len(healthy) != 2 {
		t.Fatalf("expected 2 healthy clusters, got %d", len(healthy))
	}

	// Remove cluster.
	fs.RemoveCluster("cluster-b")
	if fs.ClusterCount() != 1 {
		t.Fatalf("expected 1 cluster after removal, got %d", fs.ClusterCount())
	}
}

func TestFederationState_ServiceLookup(t *testing.T) {
	fs := NewFederationState("cluster-a")

	fs.AddCluster(ClusterInfo{ClusterID: "cluster-b", Address: "10.0.1.1:7640", Status: NodeStatusHealthy})
	fs.AddCluster(ClusterInfo{ClusterID: "cluster-c", Address: "10.0.2.1:7640", Status: NodeStatusUnhealthy})

	// Set services for both clusters.
	fs.SetRemoteServices("cluster-b", []ServiceEndpoint{
		{NodeID: "node-b1", ServiceName: "web-svc", Protocol: 0, LocalPort: 8080, Healthy: true},
		{NodeID: "node-b2", ServiceName: "db-svc", Protocol: 0, LocalPort: 3306, Healthy: true},
	})
	fs.SetRemoteServices("cluster-c", []ServiceEndpoint{
		{NodeID: "node-c1", ServiceName: "web-svc", Protocol: 0, LocalPort: 8080, Healthy: true},
	})

	// Lookup web-svc - should only return from healthy cluster-b.
	eps := fs.LookupRemoteService("web-svc")
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint (from healthy cluster only), got %d", len(eps))
	}
	if eps[0].ClusterID != "cluster-b" {
		t.Fatalf("expected cluster-b, got %s", eps[0].ClusterID)
	}

	// Mark cluster-c healthy and re-check.
	fs.UpdateClusterStatus("cluster-c", NodeStatusHealthy)
	eps = fs.LookupRemoteService("web-svc")
	if len(eps) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(eps))
	}

	// Unknown service returns empty.
	eps = fs.LookupRemoteService("unknown-svc")
	if len(eps) != 0 {
		t.Fatalf("expected 0 endpoints for unknown service, got %d", len(eps))
	}
}

func TestFederationState_ConcurrentAccess(t *testing.T) {
	fs := NewFederationState("cluster-a")

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clusterID := fmt.Sprintf("cluster-%d", id)
			fs.AddCluster(ClusterInfo{ClusterID: clusterID, Status: NodeStatusHealthy})
			fs.SetRemoteServices(clusterID, []ServiceEndpoint{
				{ServiceName: "svc", Healthy: true},
			})
			fs.LookupRemoteService("svc")
			fs.HealthyClusters()
			fs.GetCluster(clusterID)
			fs.UpdateClusterStatus(clusterID, NodeStatusHealthy)
			fs.AllRemoteServices()
		}(i)
	}
	wg.Wait()
}

func TestFederationState_RemoveClusterClearsServices(t *testing.T) {
	fs := NewFederationState("cluster-a")

	fs.AddCluster(ClusterInfo{ClusterID: "cluster-b", Status: NodeStatusHealthy})
	fs.SetRemoteServices("cluster-b", []ServiceEndpoint{
		{ServiceName: "svc-1", Healthy: true},
		{ServiceName: "svc-2", Healthy: true},
	})

	if len(fs.LookupRemoteService("svc-1")) != 1 {
		t.Fatal("expected svc-1 before removal")
	}

	fs.RemoveCluster("cluster-b")

	if len(fs.LookupRemoteService("svc-1")) != 0 {
		t.Fatal("expected no svc-1 after cluster removal")
	}
}

// --- Integration tests with real TCP connections ---

// echoFederationService creates a simple echo service for testing.
func echoFederationService(t *testing.T, prefix string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 1024)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					conn.Write([]byte(prefix + string(buf[:n])))
				}
			}()
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		ln.Close()
	}
}

func TestFederationManager_PingPong(t *testing.T) {
	logger := slog.Default()

	// Create cluster A manager.
	clusterAConfig := DefaultClusterConfig("node-a1", "127.0.0.1:0")
	handlerA := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("no service")
	})
	mgrA := NewManager(clusterAConfig, handlerA, logger)
	ctx := context.Background()
	if err := mgrA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()

	// Create federation A (listener).
	fedAConfig := DefaultFederationConfig("cluster-a", "127.0.0.1:0")
	fedAConfig.HealthCheckInterval = 100 * time.Millisecond
	fedA := NewFederationManager(fedAConfig, mgrA, logger)

	// Start a dedicated federation listener for cluster A.
	fedLnA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fedA.listener = fedLnA
	go fedA.acceptLoop()
	defer fedA.Stop()

	fedAAddr := fedLnA.Addr().String()

	// Create cluster B with federation pointing to A.
	clusterBConfig := DefaultClusterConfig("node-b1", "127.0.0.1:0")
	handlerB := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("no service")
	})
	mgrB := NewManager(clusterBConfig, handlerB, logger)
	if err := mgrB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrB.Stop()

	fedBConfig := DefaultFederationConfig("cluster-b", "")
	fedBConfig.RemoteClusters = []RemoteClusterConfig{
		{ClusterID: "cluster-a", Address: fedAAddr},
	}
	fedBConfig.DialTimeout = 2 * time.Second
	fedB := NewFederationManager(fedBConfig, mgrB, logger)

	// Register cluster-a in federation B state.
	fedB.state.AddCluster(ClusterInfo{
		ClusterID: "cluster-a",
		Address:   fedAAddr,
		Status:    NodeStatusUnknown,
	})

	// Ping cluster A from B.
	cluster, _ := fedB.state.GetCluster("cluster-a")
	err = fedB.pingCluster(cluster)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	// After successful ping, cluster A should be marked healthy.
	fedB.state.UpdateClusterStatus("cluster-a", NodeStatusHealthy)
	c, ok := fedB.state.GetCluster("cluster-a")
	if !ok || c.Status != NodeStatusHealthy {
		t.Fatal("cluster-a should be healthy after ping")
	}
}

func TestFederationManager_ServiceSync(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Cluster A: has "web-svc" registered.
	clusterAConfig := DefaultClusterConfig("node-a1", "127.0.0.1:0")
	handlerA := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgrA := NewManager(clusterAConfig, handlerA, logger)
	if err := mgrA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()
	mgrA.RegisterLocalService("web-svc", 0, 8080)

	// Federation A listener.
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

	// Cluster B: no local services, connects to A.
	clusterBConfig := DefaultClusterConfig("node-b1", "127.0.0.1:0")
	handlerB := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgrB := NewManager(clusterBConfig, handlerB, logger)
	if err := mgrB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrB.Stop()

	fedBConfig := DefaultFederationConfig("cluster-b", "")
	fedBConfig.RemoteClusters = []RemoteClusterConfig{
		{ClusterID: "cluster-a", Address: fedAAddr},
	}
	fedB := NewFederationManager(fedBConfig, mgrB, logger)

	// Mark cluster-a as healthy so sync works.
	fedB.state.UpdateClusterStatus("cluster-a", NodeStatusHealthy)

	// Sync with cluster A.
	clusterInfo, _ := fedB.state.GetCluster("cluster-a")
	localSvcs := fedB.collectLocalServices()
	remote, err := fedB.syncWithCluster(clusterInfo, localSvcs)
	if err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if len(remote) != 1 {
		t.Fatalf("expected 1 remote service, got %d", len(remote))
	}
	if remote[0].ServiceName != "web-svc" {
		t.Fatalf("expected web-svc, got %s", remote[0].ServiceName)
	}

	// Store in state and verify lookup.
	fedB.state.SetRemoteServices("cluster-a", remote)
	eps := fedB.state.LookupRemoteService("web-svc")
	if len(eps) != 1 {
		t.Fatalf("expected 1 federated endpoint, got %d", len(eps))
	}
	if eps[0].ClusterID != "cluster-a" {
		t.Fatalf("expected cluster-a, got %s", eps[0].ClusterID)
	}
}

func TestFederationManager_CrossClusterForward(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Start an echo service.
	echoAddr, cleanup := echoFederationService(t, "echo:")
	defer cleanup()

	// Cluster A: has the echo service registered.
	clusterAConfig := DefaultClusterConfig("node-a1", "127.0.0.1:0")
	handlerA := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		if svc == "echo-svc" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("not found: %s", svc)
	})
	mgrA := NewManager(clusterAConfig, handlerA, logger)
	if err := mgrA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()
	mgrA.RegisterLocalService("echo-svc", 0, 8080)

	// Federation A listener.
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

	// Cluster B: wants to access echo-svc on cluster A.
	clusterBConfig := DefaultClusterConfig("node-b1", "127.0.0.1:0")
	handlerB := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgrB := NewManager(clusterBConfig, handlerB, logger)
	if err := mgrB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrB.Stop()

	fedBConfig := DefaultFederationConfig("cluster-b", "")
	fedBConfig.RemoteClusters = []RemoteClusterConfig{
		{ClusterID: "cluster-a", Address: fedAAddr},
	}
	fedB := NewFederationManager(fedBConfig, mgrB, logger)
	fedB.state.UpdateClusterStatus("cluster-a", NodeStatusHealthy)

	// Forward to cluster A.
	conn, err := fedB.ForwardToCluster(ctx, "cluster-a", "echo-svc")
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	defer conn.Close()

	// Send data and verify echo.
	testMsg := "hello federation"
	_, err = conn.Write([]byte(testMsg))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	expected := "echo:" + testMsg
	if string(buf[:n]) != expected {
		t.Fatalf("expected %q, got %q", expected, string(buf[:n]))
	}
}

func TestFederationManager_ForwardFederated(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Start echo service.
	echoAddr, cleanup := echoFederationService(t, "fed:")
	defer cleanup()

	// Cluster A with echo service.
	clusterAConfig := DefaultClusterConfig("node-a1", "127.0.0.1:0")
	handlerA := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		if svc == "my-svc" {
			return net.Dial("tcp", echoAddr)
		}
		return nil, fmt.Errorf("not found")
	})
	mgrA := NewManager(clusterAConfig, handlerA, logger)
	if err := mgrA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()
	mgrA.RegisterLocalService("my-svc", 0, 8080)

	// Federation A.
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

	// Cluster B.
	clusterBConfig := DefaultClusterConfig("node-b1", "127.0.0.1:0")
	handlerB := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgrB := NewManager(clusterBConfig, handlerB, logger)
	if err := mgrB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrB.Stop()

	fedBConfig := DefaultFederationConfig("cluster-b", "")
	fedB := NewFederationManager(fedBConfig, mgrB, logger)
	fedB.state.AddCluster(ClusterInfo{ClusterID: "cluster-a", Address: fedAAddr, Status: NodeStatusHealthy})
	fedB.state.SetRemoteServices("cluster-a", []ServiceEndpoint{
		{ServiceName: "my-svc", Healthy: true},
	})

	// ForwardFederated should find the service on cluster-a.
	conn, err := fedB.ForwardFederated(ctx, "my-svc")
	if err != nil {
		t.Fatalf("forward federated failed: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("test"))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "fed:") {
		t.Fatalf("expected fed: prefix, got %q", string(buf[:n]))
	}
}

func TestFederationManager_ForwardNotFound(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	fedConfig := DefaultFederationConfig("cluster-b", "")
	fedMgr := NewFederationManager(fedConfig, nil, logger)

	_, err := fedMgr.ForwardFederated(ctx, "nonexistent-svc")
	if err == nil {
		t.Fatal("expected error for nonexistent service")
	}
}

func TestFederationManager_HealthCheckTimeout(t *testing.T) {
	logger := slog.Default()

	fs := NewFederationState("cluster-a")
	fs.AddCluster(ClusterInfo{
		ClusterID: "cluster-b",
		Address:   "127.0.0.1:1", // unreachable
		Status:    NodeStatusHealthy,
		LastSeen:  time.Now().Add(-time.Hour), // old
	})

	fedConfig := DefaultFederationConfig("cluster-a", "")
	fedConfig.DialTimeout = 100 * time.Millisecond
	fedConfig.HealthTimeout = 30 * time.Second
	fedMgr := NewFederationManager(fedConfig, nil, logger)
	fedMgr.state = fs

	// Health check should fail and mark cluster unhealthy.
	fedMgr.healthCheckAll()

	c, _ := fs.GetCluster("cluster-b")
	if c.Status != NodeStatusUnhealthy {
		t.Fatalf("expected unhealthy after timeout, got %s", c.Status)
	}
}

func TestFederationConfig_Defaults(t *testing.T) {
	config := DefaultFederationConfig("test-cluster", ":7640")

	if config.LocalClusterID != "test-cluster" {
		t.Fatalf("expected test-cluster, got %s", config.LocalClusterID)
	}
	if config.SyncInterval != 60*time.Second {
		t.Fatalf("expected 60s sync interval, got %s", config.SyncInterval)
	}
	if config.HealthCheckInterval != 15*time.Second {
		t.Fatalf("expected 15s health check interval, got %s", config.HealthCheckInterval)
	}
	if config.DialTimeout != 5*time.Second {
		t.Fatalf("expected 5s dial timeout, got %s", config.DialTimeout)
	}
	if config.HealthTimeout != 45*time.Second {
		t.Fatalf("expected 45s health timeout, got %s", config.HealthTimeout)
	}
}

func TestFederationState_AllRemoteServices(t *testing.T) {
	fs := NewFederationState("cluster-a")

	fs.AddCluster(ClusterInfo{ClusterID: "cluster-b", Status: NodeStatusHealthy})
	fs.AddCluster(ClusterInfo{ClusterID: "cluster-c", Status: NodeStatusHealthy})

	fs.SetRemoteServices("cluster-b", []ServiceEndpoint{
		{ServiceName: "svc-1", Healthy: true},
		{ServiceName: "svc-2", Healthy: true},
	})
	fs.SetRemoteServices("cluster-c", []ServiceEndpoint{
		{ServiceName: "svc-3", Healthy: true},
	})

	all := fs.AllRemoteServices()
	if len(all["cluster-b"]) != 2 {
		t.Fatalf("expected 2 services from cluster-b, got %d", len(all["cluster-b"]))
	}
	if len(all["cluster-c"]) != 1 {
		t.Fatalf("expected 1 service from cluster-c, got %d", len(all["cluster-c"]))
	}
}
