package mesh

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// MockTCPServer 用于测试的模拟TCP服务器
type MockTCPServer struct {
	listener net.Listener
	addr     string
	running  bool
}

func NewMockTCPServer() (*MockTCPServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &MockTCPServer{
		listener: listener,
		addr:     listener.Addr().String(),
		running:  true,
	}

	go server.serve()
	return server, nil
}

func (s *MockTCPServer) serve() {
	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running {
				continue
			}
			return
		}
		conn.Close() // 立即关闭连接，只是为了测试连接成功
	}
}

func (s *MockTCPServer) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *MockTCPServer) Addr() string {
	return s.addr
}

func TestNewHealthChecker(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	if hc.router != router {
		t.Error("Health checker should reference the correct router")
	}

	if hc.config == nil {
		t.Error("Health checker should have default config")
	}

	if hc.healthStatus == nil {
		t.Error("Health checker should initialize health status map")
	}

	if hc.checkQueue == nil {
		t.Error("Health checker should initialize check queue")
	}

	if hc.metrics == nil {
		t.Error("Health checker should initialize metrics")
	}
}

func TestDefaultHealthCheckConfig(t *testing.T) {
	config := DefaultHealthCheckConfig()

	if config.CheckInterval <= 0 {
		t.Error("Check interval should be positive")
	}

	if config.UnhealthyThreshold <= 0 {
		t.Error("Unhealthy threshold should be positive")
	}

	if config.HealthyThreshold <= 0 {
		t.Error("Healthy threshold should be positive")
	}

	if config.ConnectTimeout <= 0 {
		t.Error("Connect timeout should be positive")
	}

	if config.CheckType == "" {
		t.Error("Check type should not be empty")
	}

	if config.MaxConcurrentChecks <= 0 {
		t.Error("Max concurrent checks should be positive")
	}

	if config.QueueSize <= 0 {
		t.Error("Queue size should be positive")
	}
}

func TestHealthStatusManagement(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	endpointID := "test-endpoint-1"

	// 测试获取或创建健康状态
	health := hc.getOrCreateHealthStatus(endpointID)
	if health == nil {
		t.Error("Health status should not be nil")
	}

	if health.EndpointID != endpointID {
		t.Errorf("Expected endpoint ID '%s', got '%s'", endpointID, health.EndpointID)
	}

	if !health.IsHealthy {
		t.Error("New endpoint should be healthy by default")
	}

	if health.HealthScore <= 0 {
		t.Error("New endpoint should have positive health score")
	}

	// 测试重复获取相同端点
	health2 := hc.getOrCreateHealthStatus(endpointID)
	if health2 != health {
		t.Error("Should return same health status for same endpoint")
	}

	// 测试检查端点是否健康
	if !hc.IsHealthy(endpointID) {
		t.Error("Endpoint should be healthy initially")
	}

	// 测试获取健康状态
	retrievedHealth := hc.GetHealthStatus(endpointID)
	if retrievedHealth == nil {
		t.Error("Should be able to retrieve health status")
	}

	if retrievedHealth.EndpointID != endpointID {
		t.Error("Retrieved health status should match")
	}
}

func TestUpdateHealthStatus(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)
	hc.config.HealthyThreshold = 2
	hc.config.UnhealthyThreshold = 3

	endpointID := "test-endpoint-2"

	// 创建初始健康状态
	health := hc.getOrCreateHealthStatus(endpointID)

	// 测试健康检查成功
	hc.updateHealthStatus(endpointID, true, 10*time.Millisecond, "")
	
	if health.ConsecutiveSuccesses != 1 {
		t.Errorf("Expected 1 consecutive success, got %d", health.ConsecutiveSuccesses)
	}

	if health.ConsecutiveFailures != 0 {
		t.Errorf("Expected 0 consecutive failures, got %d", health.ConsecutiveFailures)
	}

	if health.ResponseTime != 10*time.Millisecond {
		t.Errorf("Expected response time 10ms, got %v", health.ResponseTime)
	}

	// 再次成功，应该达到健康阈值
	hc.updateHealthStatus(endpointID, true, 5*time.Millisecond, "")
	
	if health.ConsecutiveSuccesses != 2 {
		t.Errorf("Expected 2 consecutive successes, got %d", health.ConsecutiveSuccesses)
	}

	if !health.IsHealthy {
		t.Error("Endpoint should be marked as healthy after reaching threshold")
	}

	// 测试健康检查失败
	hc.updateHealthStatus(endpointID, false, 0, "Connection failed")
	
	if health.ConsecutiveFailures != 1 {
		t.Errorf("Expected 1 consecutive failure, got %d", health.ConsecutiveFailures)
	}

	if health.ConsecutiveSuccesses != 0 {
		t.Errorf("Expected 0 consecutive successes after failure, got %d", health.ConsecutiveSuccesses)
	}

	if health.ErrorMessage != "Connection failed" {
		t.Errorf("Expected error message 'Connection failed', got '%s'", health.ErrorMessage)
	}

	// 连续失败直到达到不健康阈值
	for i := 0; i < 2; i++ {
		hc.updateHealthStatus(endpointID, false, 0, "Connection failed")
	}

	if !health.IsHealthy {
		t.Error("Endpoint should still be healthy before reaching unhealthy threshold")
	}

	// 最后一次失败，应该标记为不健康
	hc.updateHealthStatus(endpointID, false, 0, "Connection failed")

	if health.IsHealthy {
		t.Error("Endpoint should be marked as unhealthy after reaching threshold")
	}

	if health.ConsecutiveFailures != 3 {
		t.Errorf("Expected 3 consecutive failures, got %d", health.ConsecutiveFailures)
	}
}

func TestCalculateHealthScore(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	// 测试健康端点的评分
	healthyHealth := &EndpointHealth{
		EndpointID:           "healthy-endpoint",
		IsHealthy:            true,
		ConsecutiveFailures:  0,
		ConsecutiveSuccesses: 5,
		ResponseTime:         50 * time.Millisecond,
	}

	healthyScore := hc.calculateHealthScore(healthyHealth)
	if healthyScore <= 90 {
		t.Errorf("Healthy endpoint should have high score, got %.1f", healthyScore)
	}

	// 测试有失败的端点评分
	failingHealth := &EndpointHealth{
		EndpointID:           "failing-endpoint",
		IsHealthy:            false,
		ConsecutiveFailures:  2,
		ConsecutiveSuccesses: 0,
		ResponseTime:         100 * time.Millisecond,
	}

	failingScore := hc.calculateHealthScore(failingHealth)
	if failingScore >= healthyScore {
		t.Errorf("Failing endpoint should have lower score than healthy endpoint")
	}

	// 测试高延迟端点评分
	slowHealth := &EndpointHealth{
		EndpointID:           "slow-endpoint",
		IsHealthy:            true,
		ConsecutiveFailures:  0,
		ConsecutiveSuccesses: 5,
		ResponseTime:         2 * time.Second,
	}

	slowScore := hc.calculateHealthScore(slowHealth)
	if slowScore >= healthyScore {
		t.Errorf("Slow endpoint should have lower score than fast healthy endpoint")
	}

	// 测试评分不会低于0
	badHealth := &EndpointHealth{
		EndpointID:           "bad-endpoint",
		IsHealthy:            false,
		ConsecutiveFailures:  10,
		ConsecutiveSuccesses: 0,
		ResponseTime:         5 * time.Second,
	}

	badScore := hc.calculateHealthScore(badHealth)
	if badScore < 0 {
		t.Errorf("Health score should not be negative, got %.1f", badScore)
	}
}

func TestTCPHealthCheck(t *testing.T) {
	// 启动模拟TCP服务器
	server, err := NewMockTCPServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)
	hc.config.ConnectTimeout = 1 * time.Second

	// 解析服务器地址
	host, portStr, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("Failed to parse server address: %v", err)
	}

	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// 创建健康的端点
	healthyEndpoint := &types.Endpoint{
		ID:       "healthy-tcp-endpoint",
		IP:       host,
		Port:     port,
		Protocol: "tcp",
		Status:   types.EndpointStatus{Ready: true},
	}

	// 执行TCP健康检查
	isHealthy, errorMsg := hc.performTCPCheck(healthyEndpoint)
	if !isHealthy {
		t.Errorf("TCP check should succeed for running server, error: %s", errorMsg)
	}

	if errorMsg != "" {
		t.Errorf("Error message should be empty for successful check, got: %s", errorMsg)
	}

	// 测试不可达的端点
	unreachableEndpoint := &types.Endpoint{
		ID:       "unreachable-tcp-endpoint",
		IP:       "127.0.0.1",
		Port:     0, // 无效端口
		Protocol: "tcp",
		Status:   types.EndpointStatus{Ready: true},
	}

	isHealthy2, errorMsg2 := hc.performTCPCheck(unreachableEndpoint)
	if isHealthy2 {
		t.Error("TCP check should fail for unreachable endpoint")
	}

	if errorMsg2 == "" {
		t.Error("Error message should not be empty for failed check")
	}
}

func TestPerformHealthCheck(t *testing.T) {
	// 启动模拟TCP服务器
	server, err := NewMockTCPServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)
	hc.config.CheckType = "tcp"
	hc.config.ConnectTimeout = 1 * time.Second

	// 解析服务器地址
	host, portStr, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("Failed to parse server address: %v", err)
	}

	var port int
	fmt.Sscanf(portStr, "%d", &port)

	endpoint := &types.Endpoint{
		ID:       "health-check-endpoint",
		IP:       host,
		Port:     port,
		Protocol: "tcp",
		Status:   types.EndpointStatus{Ready: true},
	}

	// 执行健康检查
	hc.performHealthCheck(endpoint)

	// 检查健康状态是否被更新
	health := hc.GetHealthStatus(endpoint.ID)
	if health == nil {
		t.Error("Health status should be created after check")
	}

	if health.LastCheck.IsZero() {
		t.Error("Last check time should be set")
	}

	if health.ResponseTime <= 0 {
		t.Error("Response time should be positive")
	}

	// 检查指标是否被更新
	metrics := hc.GetMetrics()
	if metrics.TotalChecks == 0 {
		t.Error("Total checks should be incremented")
	}
}

func TestHealthCheckerMetrics(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	// 创建一些健康状态
	healthyEndpoints := []string{"healthy-1", "healthy-2", "healthy-3"}
	unhealthyEndpoints := []string{"unhealthy-1", "unhealthy-2"}

	for _, id := range healthyEndpoints {
		health := hc.getOrCreateHealthStatus(id)
		health.IsHealthy = true
	}

	for _, id := range unhealthyEndpoints {
		health := hc.getOrCreateHealthStatus(id)
		health.IsHealthy = false
	}

	// 模拟一些检查
	hc.metrics.TotalChecks = 100
	hc.metrics.SuccessfulChecks = 75
	hc.metrics.FailedChecks = 25
	hc.metrics.AverageResponseTime = 50 * time.Millisecond

	metrics := hc.GetMetrics()

	if metrics.TotalChecks != 100 {
		t.Errorf("Expected 100 total checks, got %d", metrics.TotalChecks)
	}

	if metrics.SuccessfulChecks != 75 {
		t.Errorf("Expected 75 successful checks, got %d", metrics.SuccessfulChecks)
	}

	if metrics.FailedChecks != 25 {
		t.Errorf("Expected 25 failed checks, got %d", metrics.FailedChecks)
	}

	if metrics.HealthyEndpoints != len(healthyEndpoints) {
		t.Errorf("Expected %d healthy endpoints, got %d", len(healthyEndpoints), metrics.HealthyEndpoints)
	}

	if metrics.UnhealthyEndpoints != len(unhealthyEndpoints) {
		t.Errorf("Expected %d unhealthy endpoints, got %d", len(unhealthyEndpoints), metrics.UnhealthyEndpoints)
	}

	if metrics.AverageResponseTime != 50*time.Millisecond {
		t.Errorf("Expected average response time 50ms, got %v", metrics.AverageResponseTime)
	}
}

func TestGetAllHealthStatus(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	// 创建多个健康状态
	endpointIDs := []string{"endpoint-1", "endpoint-2", "endpoint-3"}
	
	for _, id := range endpointIDs {
		hc.getOrCreateHealthStatus(id)
	}

	allHealth := hc.GetAllHealthStatus()

	if len(allHealth) != len(endpointIDs) {
		t.Errorf("Expected %d health statuses, got %d", len(endpointIDs), len(allHealth))
	}

	for _, id := range endpointIDs {
		if _, exists := allHealth[id]; !exists {
			t.Errorf("Health status for endpoint %s should exist", id)
		}
	}

	// 检查返回的是副本而不是原始对象
	for id, health := range allHealth {
		originalHealth := hc.GetHealthStatus(id)
		if health == originalHealth {
			t.Error("Should return copies of health status, not original objects")
		}
	}
}

func TestRemoveEndpoint(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	endpointID := "removable-endpoint"

	// 创建健康状态
	hc.getOrCreateHealthStatus(endpointID)

	// 检查端点存在
	if !hc.IsHealthy(endpointID) {
		t.Error("Endpoint should exist and be healthy")
	}

	// 移除端点
	hc.RemoveEndpoint(endpointID)

	// 检查端点是否被移除
	health := hc.GetHealthStatus(endpointID)
	if health != nil {
		t.Error("Endpoint health status should be removed")
	}

	// 移除不存在的端点应该不会出错
	hc.RemoveEndpoint("nonexistent-endpoint")
}

func TestUpdateConfig(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	// 原始配置
	originalInterval := hc.config.CheckInterval

	// 更新配置
	newConfig := &HealthCheckConfig{
		CheckInterval:          5 * time.Second,
		UnhealthyThreshold:     5,
		HealthyThreshold:       3,
		ConnectTimeout:         2 * time.Second,
		CheckType:              "http",
		MaxConcurrentChecks:    20,
		QueueSize:              2000,
		RecoveryCheckInterval:  5 * time.Second,
		FastRecoveryEnabled:    false,
	}

	hc.UpdateConfig(newConfig)

	if hc.config.CheckInterval == originalInterval {
		t.Error("Config should be updated")
	}

	if hc.config.CheckInterval != 5*time.Second {
		t.Errorf("Expected check interval 5s, got %v", hc.config.CheckInterval)
	}

	if hc.config.CheckType != "http" {
		t.Errorf("Expected check type 'http', got '%s'", hc.config.CheckType)
	}

	if hc.config.FastRecoveryEnabled {
		t.Error("Fast recovery should be disabled")
	}
}

// 性能测试
func BenchmarkTCPHealthCheck(b *testing.B) {
	// 启动模拟TCP服务器
	server, err := NewMockTCPServer()
	if err != nil {
		b.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		b.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	// 解析服务器地址
	host, portStr, err := net.SplitHostPort(server.Addr())
	if err != nil {
		b.Fatalf("Failed to parse server address: %v", err)
	}

	var port int
	fmt.Sscanf(portStr, "%d", &port)

	endpoint := &types.Endpoint{
		ID:       "bench-endpoint",
		IP:       host,
		Port:     port,
		Protocol: "tcp",
		Status:   types.EndpointStatus{Ready: true},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hc.performTCPCheck(endpoint)
	}
}

func BenchmarkCalculateHealthScore(b *testing.B) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		b.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	health := &EndpointHealth{
		EndpointID:           "bench-endpoint",
		IsHealthy:            true,
		ConsecutiveFailures:  0,
		ConsecutiveSuccesses: 5,
		ResponseTime:         50 * time.Millisecond,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hc.calculateHealthScore(health)
	}
}

// 并发测试
func TestHealthCheckerConcurrency(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode, err := NewLocalNode(DefaultNodeConfig(), registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedRegistry := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedRegistry, cluster)

	hc := NewHealthChecker(router)

	// 并发创建健康状态
	concurrency := 10
	endpointsPerGoroutine := 50
	
	done := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			for j := 0; j < endpointsPerGoroutine; j++ {
				endpointID := fmt.Sprintf("concurrent-endpoint-%d-%d", workerID, j)
				health := hc.getOrCreateHealthStatus(endpointID)
				
				// 模拟健康检查更新
				isHealthy := j%2 == 0
				responseTime := time.Duration(j) * time.Millisecond
				hc.updateHealthStatus(endpointID, isHealthy, responseTime, "")
				
				// 检查健康状态
				_ = hc.IsHealthy(endpointID)
				_ = health.HealthScore
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < concurrency; i++ {
		<-done
	}

	// 检查最终状态
	allHealth := hc.GetAllHealthStatus()
	expectedCount := concurrency * endpointsPerGoroutine
	if len(allHealth) != expectedCount {
		t.Errorf("Expected %d health statuses, got %d", expectedCount, len(allHealth))
	}

	// 检查指标一致性
	metrics := hc.GetMetrics()
	if metrics.HealthyEndpoints+metrics.UnhealthyEndpoints != expectedCount {
		t.Error("Metrics should be consistent with health status count")
	}
}