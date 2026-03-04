package mesh

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func createTestService(name, namespace string) *types.Service {
	return &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      name,
			Namespace: namespace,
			Labels:    make(map[string]string),
		},
		Spec: types.ServiceSpec{
			Selector:     make(map[string]string),
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
			Ports: []types.ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: 8080,
					Protocol:   "tcp",
				},
			},
		},
	}
}

func createTestEndpointsWithZones(count int, zones []string) []*types.Endpoint {
	endpoints := make([]*types.Endpoint, count)
	for i := 0; i < count; i++ {
		zone := zones[i%len(zones)]
		endpoints[i] = &types.Endpoint{
			ID:       generateNodeID(),
			IP:       "127.0.0.1",
			Port:     8080 + i,
			Protocol: "tcp",
			Labels: map[string]string{
				"topology.kubernetes.io/zone": zone,
				"node.servicekeel.io/id":       generateNodeID(),
			},
			Status: types.EndpointStatus{
				Ready: true,
			},
			LastSeen: time.Now(),
		}
	}
	return endpoints
}

func TestNewServiceRouter(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	router := NewServiceRouter(NewDistributedServiceRegistry(registry, cluster), cluster)

	if router.registry == nil {
		t.Error("Router should have registry")
	}

	if router.meshCluster == nil {
		t.Error("Router should have mesh cluster")
	}

	if router.routingPolicy == nil {
		t.Error("Router should have routing policy")
	}

	if router.loadBalancers == nil || len(router.loadBalancers) == 0 {
		t.Error("Router should have load balancers")
	}

	if router.metrics == nil {
		t.Error("Router should have metrics")
	}
}

func TestDefaultRoutingPolicy(t *testing.T) {
	policy := DefaultRoutingPolicy()

	if !policy.PreferLocal {
		t.Error("Default policy should prefer local")
	}

	if !policy.PreferSameZone {
		t.Error("Default policy should prefer same zone")
	}

	if !policy.EnableFailover {
		t.Error("Default policy should enable failover")
	}

	if policy.DefaultLBAlgorithm != "round_robin" {
		t.Errorf("Expected default LB algorithm 'round_robin', got '%s'", policy.DefaultLBAlgorithm)
	}

	if policy.MaxRetries <= 0 {
		t.Error("Default policy should have positive max retries")
	}
}

func TestServiceRouterLoadBalancerInitialization(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	router := NewServiceRouter(NewDistributedServiceRegistry(registry, cluster), cluster)

	expectedLBs := []string{
		"round_robin",
		"weighted_round_robin",
		"least_connections",
		"random",
		"consistent_hash",
	}

	for _, lbName := range expectedLBs {
		if _, exists := router.loadBalancers[lbName]; !exists {
			t.Errorf("Load balancer '%s' should be initialized", lbName)
		}
	}
}

func TestCalculateEndpointScore(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	router := NewServiceRouter(NewDistributedServiceRegistry(registry, cluster), cluster)

	endpoint := &types.Endpoint{
		ID:       "test-endpoint",
		IP:       "127.0.0.1",
		Port:     8080,
		Protocol: "tcp",
		Labels:   make(map[string]string),
		Status: types.EndpointStatus{
			Ready: true,
		},
		LastSeen: time.Now(),
	}

	scoredEndpoint := &ScoredEndpoint{
		Endpoint:  endpoint,
		IsLocal:   true,
		Zone:      "us-west-1",
		RTT:       10 * time.Millisecond,
		LastCheck: time.Now(),
	}

	score := router.calculateEndpointScore(scoredEndpoint)

	// 健康端点应该有基础分数
	if score < 50 {
		t.Errorf("Healthy endpoint should have score >= 50, got %.1f", score)
	}

	// 本地端点应该有额外分数
	if score < 80 {
		t.Errorf("Local healthy endpoint should have score >= 80, got %.1f", score)
	}

	// 测试不健康端点
	endpoint.Status.Ready = false
	scoredEndpoint.IsLocal = false
	score2 := router.calculateEndpointScore(scoredEndpoint)

	if score2 >= score {
		t.Errorf("Unhealthy remote endpoint should have lower score than healthy local endpoint")
	}
}

func TestFilterEndpoints(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "router-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	router := NewServiceRouter(NewDistributedServiceRegistry(registry, cluster), cluster)
	
	// 设置路由策略
	router.routingPolicy.LocalZone = "us-west-1"

	zones := []string{"us-west-1", "us-east-1", "eu-west-1"}
	endpoints := createTestEndpointsWithZones(6, zones)

	// 设置一些端点为本地节点
	endpoints[0].Labels["node.servicekeel.io/id"] = localNode.ID
	endpoints[3].Labels["node.servicekeel.io/id"] = localNode.ID

	// 创建评分端点
	scoredEndpoints := make([]*ScoredEndpoint, len(endpoints))
	for i, ep := range endpoints {
		scoredEndpoints[i] = &ScoredEndpoint{
			Endpoint: ep,
			NodeID:   ep.Labels["node.servicekeel.io/id"],
			Zone:     ep.Labels["topology.kubernetes.io/zone"],
			IsLocal:  ep.Labels["node.servicekeel.io/id"] == localNode.ID,
		}
	}

	// 测试本地端点过滤
	localEndpoints := router.filterLocalEndpoints(scoredEndpoints)
	if len(localEndpoints) != 2 {
		t.Errorf("Expected 2 local endpoints, got %d", len(localEndpoints))
	}

	// 测试同区域端点过滤
	sameZoneEndpoints := router.filterSameZoneEndpoints(scoredEndpoints)
	expectedSameZone := 2 // us-west-1 区域的端点数量
	if len(sameZoneEndpoints) != expectedSameZone {
		t.Errorf("Expected %d same zone endpoints, got %d", expectedSameZone, len(sameZoneEndpoints))
	}

	// 测试健康端点过滤
	endpoints[1].Status.Ready = false
	endpoints[2].Status.Ready = false
	
	healthyEndpoints := router.filterHealthyEndpoints(scoredEndpoints)
	expectedHealthy := len(endpoints) - 2 // 减去2个不健康的端点
	if len(healthyEndpoints) != expectedHealthy {
		t.Errorf("Expected %d healthy endpoints, got %d", expectedHealthy, len(healthyEndpoints))
	}

	// 测试低延迟端点过滤
	for i, se := range scoredEndpoints {
		se.RTT = time.Duration(i*10) * time.Millisecond
	}
	
	lowLatencyEndpoints := router.filterLowLatencyEndpoints(scoredEndpoints)
	// 应该返回延迟最低的50%端点
	expectedLowLatency := len(scoredEndpoints) / 2
	if expectedLowLatency == 0 {
		expectedLowLatency = 1
	}
	if len(lowLatencyEndpoints) != expectedLowLatency {
		t.Errorf("Expected %d low latency endpoints, got %d", expectedLowLatency, len(lowLatencyEndpoints))
	}
}

func TestApplyRoutingPolicy(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "policy-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	router := NewServiceRouter(NewDistributedServiceRegistry(registry, cluster), cluster)

	zones := []string{"us-west-1", "us-east-1"}
	endpoints := createTestEndpointsWithZones(4, zones)

	// 设置本地端点
	endpoints[0].Labels["node.servicekeel.io/id"] = localNode.ID
	endpoints[1].Labels["node.servicekeel.io/id"] = localNode.ID

	scoredEndpoints := make([]*ScoredEndpoint, len(endpoints))
	for i, ep := range endpoints {
		scoredEndpoints[i] = &ScoredEndpoint{
			Endpoint: ep,
			NodeID:   ep.Labels["node.servicekeel.io/id"],
			Zone:     ep.Labels["topology.kubernetes.io/zone"],
			IsLocal:  ep.Labels["node.servicekeel.io/id"] == localNode.ID,
		}
	}

	// 测试优先本地策略
	router.routingPolicy.PreferLocal = true
	router.routingPolicy.PreferSameZone = false
	router.routingPolicy.PreferLowLatency = false

	filtered := router.applyRoutingPolicy(scoredEndpoints)
	if len(filtered) != 2 {
		t.Errorf("With PreferLocal, expected 2 endpoints, got %d", len(filtered))
	}

	// 测试无本地端点时的回退
	noLocalEndpoints := []*ScoredEndpoint{scoredEndpoints[2], scoredEndpoints[3]}
	filtered2 := router.applyRoutingPolicy(noLocalEndpoints)
	if len(filtered2) != 2 {
		t.Errorf("Without local endpoints, should return all healthy endpoints, got %d", len(filtered2))
	}
}

func TestRouteService(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "route-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 创建测试服务
	service := createTestService("test-service", "default")
	err = registry.RegisterService(service)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}

	// 创建测试端点
	endpoints := createTestEndpointsWithZones(3, []string{"us-west-1"})
	serviceKey := "test-service.default"
	registry.endpoints[serviceKey] = endpoints

	// 测试路由服务
	selectedEndpoint, err := router.RouteService("test-service", "default", nil)
	if err != nil {
		t.Fatalf("Failed to route service: %v", err)
	}

	if selectedEndpoint == nil {
		t.Error("Selected endpoint should not be nil")
	}

	// 检查指标
	metrics := router.GetMetrics()
	if metrics.TotalRequests == 0 {
		t.Error("Total requests should be incremented")
	}

	if metrics.SuccessfulRoutes == 0 {
		t.Error("Successful routes should be incremented")
	}
}

func TestRouteServiceNotFound(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "notfound-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 尝试路由不存在的服务
	_, err = router.RouteService("nonexistent-service", "default", nil)
	if err == nil {
		t.Error("Should return error for nonexistent service")
	}

	// 检查失败指标
	metrics := router.GetMetrics()
	if metrics.FailedRoutes == 0 {
		t.Error("Failed routes should be incremented")
	}
}

func TestRouteWithFailover(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "failover-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 配置故障转移
	router.routingPolicy.EnableFailover = true
	router.routingPolicy.MaxRetries = 2
	router.routingPolicy.FailoverTimeout = 10 * time.Millisecond

	// 测试不存在的服务（会触发故障转移）
	_, err = router.RouteWithFailover("nonexistent-service", "default", nil)
	if err == nil {
		t.Error("Should return error even with failover for nonexistent service")
	}

	// 检查故障转移指标
	metrics := router.GetMetrics()
	if metrics.FailoversTriggered == 0 {
		t.Error("Failovers should be triggered")
	}
}

func TestDiscoverServices(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "discover-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 注册测试服务
	services := []*types.Service{
		createTestService("service-1", "default"),
		createTestService("service-2", "default"),
		createTestService("service-3", "kube-system"),
	}

	for _, service := range services {
		err = registry.RegisterService(service)
		if err != nil {
			t.Fatalf("Failed to register service: %v", err)
		}

		// 为每个服务创建端点
		serviceKey := service.Metadata.Name + "." + service.Metadata.Namespace
		registry.endpoints[serviceKey] = createTestEndpointsWithZones(1, []string{"us-west-1"})
	}

	// 发现所有服务
	discoveredServices, err := router.DiscoverServices("")
	if err != nil {
		t.Fatalf("Failed to discover services: %v", err)
	}

	if len(discoveredServices) != 3 {
		t.Errorf("Expected 3 services, got %d", len(discoveredServices))
	}

	// 发现特定命名空间的服务
	defaultServices, err := router.DiscoverServices("default")
	if err != nil {
		t.Fatalf("Failed to discover default services: %v", err)
	}

	if len(defaultServices) != 2 {
		t.Errorf("Expected 2 default services, got %d", len(defaultServices))
	}
}

func TestUpdateRoutingPolicy(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "policy-update-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 初始策略
	if !router.routingPolicy.PreferLocal {
		t.Error("Initial policy should prefer local")
	}

	// 更新策略
	newPolicy := &RoutingPolicy{
		PreferLocal:        false,
		PreferSameZone:     false,
		PreferLowLatency:   true,
		EnableFailover:     false,
		DefaultLBAlgorithm: "random",
		LocalZone:          "eu-west-1",
	}

	router.UpdateRoutingPolicy(newPolicy)

	if router.routingPolicy.PreferLocal {
		t.Error("Updated policy should not prefer local")
	}

	if !router.routingPolicy.PreferLowLatency {
		t.Error("Updated policy should prefer low latency")
	}

	if router.routingPolicy.DefaultLBAlgorithm != "random" {
		t.Errorf("Expected default LB algorithm 'random', got '%s'", router.routingPolicy.DefaultLBAlgorithm)
	}
}

func TestCacheOperations(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "cache-test-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 设置短缓存超时用于测试
	router.cacheTimeout = 100 * time.Millisecond

	// 创建路由条目
	routeEntry := &RouteEntry{
		ServiceName: "test-service",
		Namespace:   "default",
		Endpoints:   []*ScoredEndpoint{},
		LastUpdated: time.Now(),
		Version:     1,
		TTL:         router.cacheTimeout,
	}

	// 缓存路由条目
	cacheKey := "test-service.default"
	router.cacheRouteEntry(cacheKey, routeEntry)

	// 检查缓存
	router.cacheMu.RLock()
	cachedEntry, exists := router.routeCache[cacheKey]
	router.cacheMu.RUnlock()

	if !exists {
		t.Error("Route entry should be cached")
	}

	if cachedEntry.ServiceName != "test-service" {
		t.Error("Cached entry should match original")
	}

	// 测试缓存过期
	time.Sleep(150 * time.Millisecond)
	if !router.isCacheExpired(routeEntry) {
		t.Error("Cache entry should be expired")
	}

	// 刷新缓存
	router.RefreshCache()

	router.cacheMu.RLock()
	_, exists = router.routeCache[cacheKey]
	router.cacheMu.RUnlock()

	if exists {
		t.Error("Cache should be empty after refresh")
	}
}

// 性能测试
func BenchmarkRouteService(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "bench-route-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		b.Fatal(err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 设置测试数据
	service := createTestService("bench-service", "default")
	registry.RegisterService(service)
	
	endpoints := createTestEndpointsWithZones(10, []string{"us-west-1", "us-east-1"})
	serviceKey := "bench-service.default"
	registry.endpoints[serviceKey] = endpoints

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := router.RouteService("bench-service", "default", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCalculateEndpointScore(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "bench-score-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		b.Fatal(err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	endpoint := &types.Endpoint{
		ID:       "bench-endpoint",
		IP:       "127.0.0.1",
		Port:     8080,
		Protocol: "tcp",
		Labels:   make(map[string]string),
		Status:   types.EndpointStatus{Ready: true},
		LastSeen: time.Now(),
	}

	scoredEndpoint := &ScoredEndpoint{
		Endpoint:  endpoint,
		IsLocal:   true,
		Zone:      "us-west-1",
		RTT:       10 * time.Millisecond,
		LastCheck: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = router.calculateEndpointScore(scoredEndpoint)
	}
}

// 并发测试
func TestRouterConcurrency(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "concurrent-router-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	// 设置测试数据
	service := createTestService("concurrent-service", "default")
	registry.RegisterService(service)
	
	endpoints := createTestEndpointsWithZones(5, []string{"us-west-1"})
	serviceKey := "concurrent-service.default"
	registry.endpoints[serviceKey] = endpoints

	// 并发路由测试
	concurrency := 10
	routesPerGoroutine := 50
	
	done := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for j := 0; j < routesPerGoroutine; j++ {
				_, err := router.RouteService("concurrent-service", "default", nil)
				if err != nil {
					t.Errorf("Concurrent routing failed: %v", err)
					done <- false
					return
				}
			}
			done <- true
		}()
	}

	// 等待所有goroutine完成
	for i := 0; i < concurrency; i++ {
		success := <-done
		if !success {
			t.Error("Concurrent routing should not fail")
		}
	}

	// 检查指标
	metrics := router.GetMetrics()
	expectedRequests := int64(concurrency * routesPerGoroutine)
	if metrics.TotalRequests != expectedRequests {
		t.Errorf("Expected %d total requests, got %d", expectedRequests, metrics.TotalRequests)
	}
}