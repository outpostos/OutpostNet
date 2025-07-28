package mesh

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// 创建测试端点
func createTestEndpoints(count int) []*types.Endpoint {
	endpoints := make([]*types.Endpoint, count)
	for i := 0; i < count; i++ {
		endpoints[i] = &types.Endpoint{
			ID:       generateNodeID(),
			IP:       "127.0.0.1",
			Port:     8080 + i,
			Protocol: "tcp",
			Labels:   make(map[string]string),
			Status: types.EndpointStatus{
				Ready: true,
			},
			LastSeen: time.Now(),
		}
	}
	return endpoints
}

func TestRoundRobinLoadBalancer(t *testing.T) {
	lb := &RoundRobinLoadBalancer{}
	endpoints := createTestEndpoints(3)

	// 测试空端点列表
	_, err := lb.Select([]*types.Endpoint{})
	if err == nil {
		t.Error("Should return error for empty endpoints")
	}

	// 测试轮询
	selectedPorts := make(map[int]int)
	for i := 0; i < 9; i++ { // 3轮完整轮询
		endpoint, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("Failed to select endpoint: %v", err)
		}
		selectedPorts[endpoint.Port]++
	}

	// 每个端点应该被选择3次
	for port, count := range selectedPorts {
		if count != 3 {
			t.Errorf("Port %d was selected %d times, expected 3", port, count)
		}
	}

	if lb.Name() != "round_robin" {
		t.Errorf("Expected name 'round_robin', got '%s'", lb.Name())
	}
}

func TestWeightedRoundRobinLoadBalancer(t *testing.T) {
	lb := &WeightedRoundRobinLoadBalancer{}
	endpoints := createTestEndpoints(3)

	// 设置权重标签
	endpoints[0].Labels["servicekeel.io/weight"] = "100"
	endpoints[1].Labels["servicekeel.io/weight"] = "200"
	endpoints[2].Labels["servicekeel.io/weight"] = "300"

	// 测试加权轮询
	selectedPorts := make(map[int]int)
	for i := 0; i < 60; i++ { // 多次选择以观察权重分布
		endpoint, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("Failed to select endpoint: %v", err)
		}
		selectedPorts[endpoint.Port]++
	}

	// 权重比例应该大致为1:2:3
	// 由于实现细节，这里只检查基本的权重效果
	if selectedPorts[8082] <= selectedPorts[8080] {
		t.Error("Higher weight endpoint should be selected more often")
	}

	if lb.Name() != "weighted_round_robin" {
		t.Errorf("Expected name 'weighted_round_robin', got '%s'", lb.Name())
	}
}

func TestLeastConnectionsLoadBalancer(t *testing.T) {
	lb := &LeastConnectionsLoadBalancer{}
	endpoints := createTestEndpoints(3)

	// 第一次选择应该选择第一个端点（连接数都是0）
	endpoint1, err := lb.Select(endpoints)
	if err != nil {
		t.Fatalf("Failed to select endpoint: %v", err)
	}

	// 第二次选择应该选择不同的端点
	endpoint2, err := lb.Select(endpoints)
	if err != nil {
		t.Fatalf("Failed to select endpoint: %v", err)
	}

	if endpoint1.ID == endpoint2.ID {
		t.Error("Should select different endpoints when connections are equal")
	}

	// 释放连接
	lb.ReleaseConnection(endpoint1.ID)

	// 下次选择应该倾向于选择释放了连接的端点
	_, err = lb.Select(endpoints)
	if err != nil {
		t.Fatalf("Failed to select endpoint: %v", err)
	}

	// 这里不强制要求选择特定端点，因为可能有多个端点连接数相同

	if lb.Name() != "least_connections" {
		t.Errorf("Expected name 'least_connections', got '%s'", lb.Name())
	}
}

func TestRandomLoadBalancer(t *testing.T) {
	lb := &RandomLoadBalancer{}
	endpoints := createTestEndpoints(3)

	// 测试随机选择
	selectedPorts := make(map[int]bool)
	for i := 0; i < 20; i++ {
		endpoint, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("Failed to select endpoint: %v", err)
		}
		selectedPorts[endpoint.Port] = true
	}

	// 应该选择到不同的端点（随机性测试）
	if len(selectedPorts) < 2 {
		t.Error("Random selection should select different endpoints")
	}

	if lb.Name() != "random" {
		t.Errorf("Expected name 'random', got '%s'", lb.Name())
	}
}

func TestConsistentHashLoadBalancer(t *testing.T) {
	lb := &ConsistentHashLoadBalancer{}
	endpoints := createTestEndpoints(3)

	// 测试一致性哈希
	selectedEndpoints := make([]*types.Endpoint, 10)
	for i := 0; i < 10; i++ {
		endpoint, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("Failed to select endpoint: %v", err)
		}
		selectedEndpoints[i] = endpoint
	}

	// 由于使用时间戳作为哈希键，每次结果可能不同
	// 这里主要测试不会出错
	for _, endpoint := range selectedEndpoints {
		if endpoint == nil {
			t.Error("Selected endpoint should not be nil")
		}
	}

	if lb.Name() != "consistent_hash" {
		t.Errorf("Expected name 'consistent_hash', got '%s'", lb.Name())
	}
}

func TestStickySessionLoadBalancer(t *testing.T) {
	baseBalancer := &RoundRobinLoadBalancer{}
	lb := NewStickySessionLoadBalancer(baseBalancer)
	endpoints := createTestEndpoints(3)

	// 测试会话粘性
	sessionID1 := "session-1"
	sessionID2 := "session-2"

	// 第一次选择
	endpoint1, err := lb.SelectWithSessionID(endpoints, sessionID1)
	if err != nil {
		t.Fatalf("Failed to select endpoint: %v", err)
	}

	// 同一会话应该返回相同端点
	endpoint1_2, err := lb.SelectWithSessionID(endpoints, sessionID1)
	if err != nil {
		t.Fatalf("Failed to select endpoint: %v", err)
	}

	if endpoint1.ID != endpoint1_2.ID {
		t.Error("Same session should return same endpoint")
	}

	// 不同会话可能返回不同端点
	_, err = lb.SelectWithSessionID(endpoints, sessionID2)
	if err != nil {
		t.Fatalf("Failed to select endpoint: %v", err)
	}

	// 这里不强制要求不同，因为可能随机选择到相同端点

	if lb.Name() != "sticky_session" {
		t.Errorf("Expected name 'sticky_session', got '%s'", lb.Name())
	}
}

func TestHealthAwareLoadBalancer(t *testing.T) {
	baseBalancer := &RoundRobinLoadBalancer{}
	lb := NewHealthAwareLoadBalancer(baseBalancer)
	endpoints := createTestEndpoints(3)

	// 设置一个端点为不健康
	endpoints[1].Status.Ready = false

	// 选择端点
	selectedPorts := make(map[int]int)
	for i := 0; i < 10; i++ {
		endpoint, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("Failed to select endpoint: %v", err)
		}
		selectedPorts[endpoint.Port]++
	}

	// 不健康的端点不应该被选择
	if selectedPorts[8081] > 0 {
		t.Error("Unhealthy endpoint should not be selected")
	}

	// 只有健康的端点应该被选择
	if selectedPorts[8080] == 0 && selectedPorts[8082] == 0 {
		t.Error("Healthy endpoints should be selected")
	}

	// 测试所有端点都不健康的情况
	for _, ep := range endpoints {
		ep.Status.Ready = false
	}

	_, err := lb.Select(endpoints)
	if err == nil {
		t.Error("Should return error when no healthy endpoints available")
	}

	if lb.Name() != "health_aware" {
		t.Errorf("Expected name 'health_aware', got '%s'", lb.Name())
	}
}

func TestGeolocationLoadBalancer(t *testing.T) {
	baseBalancer := &RoundRobinLoadBalancer{}
	localZone := "us-west-1"
	lb := NewGeolocationLoadBalancer(baseBalancer, localZone)
	
	endpoints := createTestEndpoints(4)
	
	// 设置地理位置标签
	endpoints[0].Labels["topology.kubernetes.io/zone"] = "us-west-1" // 本地区域
	endpoints[1].Labels["topology.kubernetes.io/zone"] = "us-west-1" // 本地区域
	endpoints[2].Labels["topology.kubernetes.io/zone"] = "us-east-1" // 远程区域
	endpoints[3].Labels["topology.kubernetes.io/zone"] = "eu-west-1" // 远程区域

	// 设置区域权重
	lb.SetZoneWeight("us-east-1", 10)

	selectedPorts := make(map[int]int)
	for i := 0; i < 20; i++ {
		endpoint, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("Failed to select endpoint: %v", err)
		}
		selectedPorts[endpoint.Port]++
	}

	// 本地区域的端点应该被优先选择
	localCount := selectedPorts[8080] + selectedPorts[8081]
	remoteCount := selectedPorts[8082] + selectedPorts[8083]

	if localCount == 0 {
		t.Error("Local zone endpoints should be selected")
	}

	// 在有本地区域端点的情况下，远程端点选择应该较少
	if localCount <= remoteCount {
		t.Error("Local zone endpoints should be preferred over remote ones")
	}

	if lb.Name() != "geolocation" {
		t.Errorf("Expected name 'geolocation', got '%s'", lb.Name())
	}
}

func TestLoadBalancerFactory(t *testing.T) {
	factory := NewLoadBalancerFactory()

	// 测试内置负载均衡器
	builtinBalancers := []string{
		"round_robin",
		"weighted_round_robin", 
		"least_connections",
		"random",
		"consistent_hash",
	}

	for _, name := range builtinBalancers {
		lb, err := factory.Create(name)
		if err != nil {
			t.Errorf("Failed to create %s load balancer: %v", name, err)
		}
		if lb == nil {
			t.Errorf("Load balancer %s should not be nil", name)
		}
		if lb.Name() != name {
			t.Errorf("Expected name %s, got %s", name, lb.Name())
		}
	}

	// 测试未知的负载均衡器
	_, err := factory.Create("unknown")
	if err == nil {
		t.Error("Should return error for unknown load balancer")
	}

	// 测试注册自定义负载均衡器
	factory.Register("custom", func() types.LoadBalancer {
		return &RoundRobinLoadBalancer{}
	})

	customLB, err := factory.Create("custom")
	if err != nil {
		t.Errorf("Failed to create custom load balancer: %v", err)
	}
	if customLB == nil {
		t.Error("Custom load balancer should not be nil")
	}

	// 测试获取可用的负载均衡器列表
	available := factory.GetAvailableBalancers()
	if len(available) < len(builtinBalancers)+1 { // +1 for custom
		t.Error("Should return all available load balancers")
	}
}

// 性能测试
func BenchmarkRoundRobinSelect(b *testing.B) {
	lb := &RoundRobinLoadBalancer{}
	endpoints := createTestEndpoints(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := lb.Select(endpoints)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWeightedRoundRobinSelect(b *testing.B) {
	lb := &WeightedRoundRobinLoadBalancer{}
	endpoints := createTestEndpoints(10)

	// 设置权重
	for _, ep := range endpoints {
		ep.Labels["servicekeel.io/weight"] = "100"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := lb.Select(endpoints)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeastConnectionsSelect(b *testing.B) {
	lb := &LeastConnectionsLoadBalancer{}
	endpoints := createTestEndpoints(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := lb.Select(endpoints)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomSelect(b *testing.B) {
	lb := &RandomLoadBalancer{}
	endpoints := createTestEndpoints(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := lb.Select(endpoints)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsistentHashSelect(b *testing.B) {
	lb := &ConsistentHashLoadBalancer{}
	endpoints := createTestEndpoints(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := lb.Select(endpoints)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// 并发测试
func TestLoadBalancerConcurrency(t *testing.T) {
	lb := &RoundRobinLoadBalancer{}
	endpoints := createTestEndpoints(5)

	// 并发选择
	concurrency := 10
	selectionsPerGoroutine := 100
	
	done := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for j := 0; j < selectionsPerGoroutine; j++ {
				_, err := lb.Select(endpoints)
				if err != nil {
					t.Errorf("Concurrent selection failed: %v", err)
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
			t.Error("Concurrent selection should not fail")
		}
	}
}

func TestStickySessionConcurrency(t *testing.T) {
	baseBalancer := &RoundRobinLoadBalancer{}
	lb := NewStickySessionLoadBalancer(baseBalancer)
	endpoints := createTestEndpoints(5)

	// 并发会话测试
	concurrency := 10
	selectionsPerGoroutine := 50
	
	done := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(sessionID string) {
			var firstEndpoint *types.Endpoint
			for j := 0; j < selectionsPerGoroutine; j++ {
				endpoint, err := lb.SelectWithSessionID(endpoints, sessionID)
				if err != nil {
					t.Errorf("Concurrent session selection failed: %v", err)
					done <- false
					return
				}
				
				if j == 0 {
					firstEndpoint = endpoint
				} else if endpoint.ID != firstEndpoint.ID {
					t.Errorf("Session %s should always return same endpoint", sessionID)
					done <- false
					return
				}
			}
			done <- true
		}(generateNodeID())
	}

	// 等待所有goroutine完成
	for i := 0; i < concurrency; i++ {
		success := <-done
		if !success {
			t.Error("Concurrent session selection should maintain consistency")
		}
	}
}