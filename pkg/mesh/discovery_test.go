package mesh

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

func TestServiceCacheEntry_Expiry(t *testing.T) {
	entry := &ServiceCacheEntry{
		ClusterID: "cluster-a",
		Endpoints: []ServiceEndpoint{{ServiceName: "svc"}},
		CachedAt:  time.Now(),
		TTL:       100 * time.Millisecond,
	}

	if entry.IsExpired() {
		t.Fatal("entry should not be expired yet")
	}

	time.Sleep(150 * time.Millisecond)
	if !entry.IsExpired() {
		t.Fatal("entry should be expired")
	}
}

func TestDiscoveryConfig_Defaults(t *testing.T) {
	config := DefaultDiscoveryConfig()

	if config.CacheTTL != 30*time.Second {
		t.Fatalf("expected 30s cache TTL, got %s", config.CacheTTL)
	}
	if config.QueryTimeout != 5*time.Second {
		t.Fatalf("expected 5s query timeout, got %s", config.QueryTimeout)
	}
	if config.DialTimeout != 5*time.Second {
		t.Fatalf("expected 5s dial timeout, got %s", config.DialTimeout)
	}
}

func TestServiceDiscovery_DiscoverFromFederationState(t *testing.T) {
	logger := slog.Default()

	// Set up a federation manager with pre-synced state.
	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, nil, logger)
	fed.state.AddCluster(ClusterInfo{ClusterID: "cluster-a", Status: NodeStatusHealthy})
	fed.state.SetRemoteServices("cluster-a", []ServiceEndpoint{
		{ServiceName: "web-svc", Protocol: 0, LocalPort: 8080, Healthy: true},
	})

	sd := NewServiceDiscovery(DefaultDiscoveryConfig(), fed, logger)
	ctx := context.Background()

	eps, err := sd.Discover(ctx, "web-svc")
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(eps))
	}
	if eps[0].ClusterID != "cluster-a" {
		t.Fatalf("expected cluster-a, got %s", eps[0].ClusterID)
	}
	if sd.Metrics.CacheHits.Load() != 1 {
		t.Fatalf("expected 1 cache hit, got %d", sd.Metrics.CacheHits.Load())
	}
}

func TestServiceDiscovery_DiscoverNotFound(t *testing.T) {
	logger := slog.Default()

	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, nil, logger)

	sd := NewServiceDiscovery(DefaultDiscoveryConfig(), fed, logger)
	ctx := context.Background()

	_, err := sd.Discover(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent service")
	}
}

func TestServiceDiscovery_OnDemandQuery(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()

	// Cluster A: has a service we'll query for.
	clusterAConfig := DefaultClusterConfig("node-a1", "127.0.0.1:0")
	handlerA := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})
	mgrA := NewManager(clusterAConfig, handlerA, logger)
	if err := mgrA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()
	mgrA.RegisterLocalService("db-svc", 0, 3306)

	// Federation A with discovery handler.
	fedAConfig := DefaultFederationConfig("cluster-a", "127.0.0.1:0")
	fedA := NewFederationManager(fedAConfig, mgrA, logger)
	fedLnA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fedA.listener = fedLnA

	sdA := NewServiceDiscovery(DefaultDiscoveryConfig(), fedA, logger)
	fedA.QueryHandler = sdA.HandleServiceQuery

	go fedA.acceptLoop()
	defer fedA.Stop()

	fedAAddr := fedLnA.Addr().String()

	// Cluster B: queries cluster A for db-svc.
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

	sdB := NewServiceDiscovery(DefaultDiscoveryConfig(), fedB, logger)

	// Query for db-svc.
	eps, err := sdB.Discover(ctx, "db-svc")
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(eps))
	}
	if eps[0].Endpoint.ServiceName != "db-svc" {
		t.Fatalf("expected db-svc, got %s", eps[0].Endpoint.ServiceName)
	}
	if eps[0].ClusterID != "cluster-a" {
		t.Fatalf("expected cluster-a, got %s", eps[0].ClusterID)
	}

	// Verify caching.
	if sdB.CacheSize() != 1 {
		t.Fatalf("expected 1 cache entry, got %d", sdB.CacheSize())
	}
	if sdB.Metrics.CacheMisses.Load() != 1 {
		t.Fatalf("expected 1 cache miss, got %d", sdB.Metrics.CacheMisses.Load())
	}
	if sdB.Metrics.QueriesSent.Load() != 1 {
		t.Fatalf("expected 1 query sent, got %d", sdB.Metrics.QueriesSent.Load())
	}
}

func TestServiceDiscovery_CacheEviction(t *testing.T) {
	logger := slog.Default()

	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, nil, logger)

	config := DefaultDiscoveryConfig()
	config.CacheTTL = 100 * time.Millisecond
	sd := NewServiceDiscovery(config, fed, logger)

	// Manually populate cache.
	sd.setCache("cluster-a", "svc-1", []ServiceEndpoint{
		{ServiceName: "svc-1", Healthy: true},
	})

	if sd.CacheSize() != 1 {
		t.Fatalf("expected 1 cache entry, got %d", sd.CacheSize())
	}

	// Wait for TTL to expire.
	time.Sleep(150 * time.Millisecond)

	// Evict.
	sd.evictExpired()

	if sd.CacheSize() != 0 {
		t.Fatalf("expected 0 cache entries after eviction, got %d", sd.CacheSize())
	}
}

func TestServiceDiscovery_Watch(t *testing.T) {
	logger := slog.Default()

	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, nil, logger)

	sd := NewServiceDiscovery(DefaultDiscoveryConfig(), fed, logger)

	var mu sync.Mutex
	var notified []FederatedEndpoint
	sd.Watch("web-svc", func(svcName string, eps []FederatedEndpoint) {
		mu.Lock()
		notified = eps
		mu.Unlock()
	})

	// Manually trigger notification.
	testEps := []FederatedEndpoint{
		{ClusterID: "cluster-a", Endpoint: ServiceEndpoint{ServiceName: "web-svc"}},
	}
	sd.notifyWatchers("web-svc", testEps)

	mu.Lock()
	if len(notified) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(notified))
	}
	if notified[0].ClusterID != "cluster-a" {
		t.Fatalf("expected cluster-a, got %s", notified[0].ClusterID)
	}
	mu.Unlock()
}

func TestServiceDiscovery_QueryWireProtocol(t *testing.T) {
	// Test the wire protocol encoding/decoding directly.
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		writeDiscoveryQuery(c1, "test-service")
	}()

	name, err := readDiscoveryQuery(c2)
	if err != nil {
		t.Fatalf("read query: %v", err)
	}
	if name != "test-service" {
		t.Fatalf("expected test-service, got %s", name)
	}
}

func TestServiceDiscovery_ResponseWireProtocol(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	eps := []ServiceEndpoint{
		{ServiceName: "svc-1", Protocol: 0, LocalPort: 8080, Healthy: true},
		{ServiceName: "svc-2", Protocol: 1, LocalPort: 9090, Healthy: true},
	}

	go func() {
		writeDiscoveryResponse(c1, eps)
	}()

	result, err := readDiscoveryResponse(c2)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(result))
	}
	if result[0].ServiceName != "svc-1" {
		t.Fatalf("expected svc-1, got %s", result[0].ServiceName)
	}
	if result[1].ServiceName != "svc-2" {
		t.Fatalf("expected svc-2, got %s", result[1].ServiceName)
	}
}

func TestServiceDiscovery_ConcurrentDiscover(t *testing.T) {
	logger := slog.Default()

	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, nil, logger)
	fed.state.AddCluster(ClusterInfo{ClusterID: "cluster-a", Status: NodeStatusHealthy})
	fed.state.SetRemoteServices("cluster-a", []ServiceEndpoint{
		{ServiceName: "svc-1", Healthy: true},
	})

	sd := NewServiceDiscovery(DefaultDiscoveryConfig(), fed, logger)
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sd.Discover(ctx, "svc-1")
		}()
	}
	wg.Wait()
}

func TestServiceDiscovery_StartStop(t *testing.T) {
	logger := slog.Default()

	fedConfig := DefaultFederationConfig("cluster-local", "")
	fed := NewFederationManager(fedConfig, nil, logger)

	config := DefaultDiscoveryConfig()
	config.CacheTTL = 50 * time.Millisecond
	sd := NewServiceDiscovery(config, fed, logger)

	ctx, cancel := context.WithCancel(context.Background())
	sd.Start(ctx)

	// Populate cache.
	sd.setCache("cluster-a", "svc-1", []ServiceEndpoint{
		{ServiceName: "svc-1", Healthy: true},
	})

	// Wait for at least one maintenance cycle.
	time.Sleep(150 * time.Millisecond)

	cancel()
	sd.Stop()

	// Should have evicted expired entries.
	if sd.CacheSize() != 0 {
		t.Fatalf("expected 0 cache entries after maintenance, got %d", sd.CacheSize())
	}
}
