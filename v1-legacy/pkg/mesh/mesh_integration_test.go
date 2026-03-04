package mesh

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// IntegrationTestSuite 集成测试套件
type IntegrationTestSuite struct {
	nodes              map[string]*LocalNode
	clusters           map[string]*MockMeshCluster
	distributedRegs    map[string]*DistributedServiceRegistry
	routers            map[string]*ServiceRouter
	gossipManagers     map[string]*GossipManager
	healthCheckers     map[string]*HealthChecker
	mockTCPServers     map[string]*MockTCPServer
}

func NewIntegrationTestSuite() *IntegrationTestSuite {
	return &IntegrationTestSuite{
		nodes:           make(map[string]*LocalNode),
		clusters:        make(map[string]*MockMeshCluster),
		distributedRegs: make(map[string]*DistributedServiceRegistry),
		routers:         make(map[string]*ServiceRouter),
		gossipManagers:  make(map[string]*GossipManager),
		healthCheckers:  make(map[string]*HealthChecker),
		mockTCPServers:  make(map[string]*MockTCPServer),
	}
}

func (suite *IntegrationTestSuite) CreateNode(nodeID string, zone string) error {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = nodeID
	config.Labels = map[string]string{
		"topology.kubernetes.io/zone": zone,
	}
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		return err
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedReg := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedReg, cluster)
	
	gossipManager, err := NewGossipManager(cluster)
	if err != nil {
		return err
	}
	
	healthChecker := NewHealthChecker(router)
	
	suite.nodes[nodeID] = localNode
	suite.clusters[nodeID] = cluster
	suite.distributedRegs[nodeID] = distributedReg
	suite.routers[nodeID] = router
	suite.gossipManagers[nodeID] = gossipManager
	suite.healthCheckers[nodeID] = healthChecker
	
	return nil
}

func (suite *IntegrationTestSuite) ConnectNodes(nodeID1, nodeID2 string) {
	// 模拟节点互相发现
	node1 := suite.nodes[nodeID1]
	node2 := suite.nodes[nodeID2]
	
	// 添加到对方的集群中
	suite.clusters[nodeID1].AddNode(node2.Node)
	suite.clusters[nodeID2].AddNode(node1.Node)
}

func (suite *IntegrationTestSuite) CreateMockService(nodeID string, serviceName string, port int) error {
	// 创建Mock TCP服务器
	server, err := NewMockTCPServer()
	if err != nil {
		return err
	}
	
	serverKey := fmt.Sprintf("%s-%s", nodeID, serviceName)
	suite.mockTCPServers[serverKey] = server
	
	// 注册服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      serviceName,
			Namespace: "default",
			Labels:    make(map[string]string),
		},
		Spec: types.ServiceSpec{
			Selector:     map[string]string{"app": serviceName},
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
			Ports: []types.ServicePort{
				{Name: "http", Port: port, TargetPort: port, Protocol: "tcp"},
			},
		},
	}
	
	err = suite.distributedRegs[nodeID].RegisterService(service)
	if err != nil {
		return err
	}
	
	// 解析服务器地址创建端点
	host, portStr, err := net.SplitHostPort(server.Addr())
	if err != nil {
		return err
	}
	
	var actualPort int
	fmt.Sscanf(portStr, "%d", &actualPort)
	
	endpoint := &types.Endpoint{
		ID:       fmt.Sprintf("%s-%s-endpoint", nodeID, serviceName),
		IP:       host,
		Port:     actualPort,
		Protocol: "tcp",
		Labels:   map[string]string{"app": serviceName},
		Status:   types.EndpointStatus{Ready: true},
		LastSeen: time.Now(),
	}
	
	// 添加端点到注册表
	reg := suite.distributedRegs[nodeID].localRegistry.(*MockServiceRegistry)
	serviceKey := serviceName + ".default"
	if reg.endpoints[serviceKey] == nil {
		reg.endpoints[serviceKey] = make([]*types.Endpoint, 0)
	}
	reg.endpoints[serviceKey] = append(reg.endpoints[serviceKey], endpoint)
	
	return nil
}

func (suite *IntegrationTestSuite) Cleanup() {
	for _, server := range suite.mockTCPServers {
		server.Close()
	}
}

// 测试：基础节点发现和连接
func TestBasicNodeDiscoveryAndConnection(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建三个节点
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	zones := []string{"us-west-1", "us-west-1", "us-east-1"}
	
	for i, nodeID := range nodeIDs {
		err := suite.CreateNode(nodeID, zones[i])
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
	}
	
	// 连接节点
	suite.ConnectNodes("node-1", "node-2")
	suite.ConnectNodes("node-1", "node-3")
	suite.ConnectNodes("node-2", "node-3")
	
	// 验证节点连接
	for _, nodeID := range nodeIDs {
		cluster := suite.clusters[nodeID]
		nodes := cluster.GetHealthyNodes()
		
		// 每个节点应该能看到其他节点
		if len(nodes) < 2 {
			t.Errorf("Node %s should see at least 2 other nodes, got %d", nodeID, len(nodes))
		}
	}
}

// 测试：跨节点服务发现
func TestCrossNodeServiceDiscovery(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建两个节点
	err := suite.CreateNode("provider-node", "us-west-1")
	if err != nil {
		t.Fatalf("Failed to create provider node: %v", err)
	}
	
	err = suite.CreateNode("consumer-node", "us-east-1")
	if err != nil {
		t.Fatalf("Failed to create consumer node: %v", err)
	}
	
	// 连接节点
	suite.ConnectNodes("provider-node", "consumer-node")
	
	// 在provider节点创建服务
	err = suite.CreateMockService("provider-node", "web-service", 8080)
	if err != nil {
		t.Fatalf("Failed to create mock service: %v", err)
	}
	
	// 等待服务传播
	time.Sleep(100 * time.Millisecond)
	
	// 从consumer节点发现服务
	consumerRouter := suite.routers["consumer-node"]
	services, err := consumerRouter.DiscoverServices("default")
	if err != nil {
		t.Fatalf("Failed to discover services: %v", err)
	}
	
	// 验证服务发现
	found := false
	for _, service := range services {
		if service.Metadata.Name == "web-service" {
			found = true
			break
		}
	}
	
	if !found {
		t.Error("web-service should be discovered by consumer node")
	}
}

// 测试：负载均衡和故障转移
func TestLoadBalancingAndFailover(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建一个节点
	err := suite.CreateNode("lb-node", "us-west-1")
	if err != nil {
		t.Fatalf("Failed to create lb node: %v", err)
	}
	
	// 创建服务和多个端点
	serviceName := "balanced-service"
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      serviceName,
			Namespace: "default",
			Labels:    make(map[string]string),
		},
		Spec: types.ServiceSpec{
			Selector:     map[string]string{"app": serviceName},
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
			Ports: []types.ServicePort{
				{Name: "http", Port: 8080, TargetPort: 8080, Protocol: "tcp"},
			},
		},
	}
	
	err = suite.distributedRegs["lb-node"].RegisterService(service)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	
	// 创建多个端点
	reg := suite.distributedRegs["lb-node"].localRegistry.(*MockServiceRegistry)
	serviceKey := serviceName + ".default"
	endpoints := make([]*types.Endpoint, 3)
	
	for i := 0; i < 3; i++ {
		endpoints[i] = &types.Endpoint{
			ID:       fmt.Sprintf("balanced-endpoint-%d", i),
			IP:       "127.0.0.1",
			Port:     8080 + i,
			Protocol: "tcp",
			Labels:   map[string]string{"app": serviceName},
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		}
	}
	reg.endpoints[serviceKey] = endpoints
	
	// 测试负载均衡
	router := suite.routers["lb-node"]
	selectedPorts := make(map[int]int)
	
	for i := 0; i < 30; i++ {
		endpoint, err := router.RouteService(serviceName, "default", nil)
		if err != nil {
			t.Fatalf("Failed to route service: %v", err)
		}
		selectedPorts[endpoint.Port]++
	}
	
	// 验证负载均衡分布
	if len(selectedPorts) != 3 {
		t.Errorf("Expected 3 different endpoints to be selected, got %d", len(selectedPorts))
	}
	
	// 验证分布均匀性（允许一定误差）
	for port, count := range selectedPorts {
		if count < 5 || count > 15 {
			t.Errorf("Port %d was selected %d times, expected 8-12 times", port, count)
		}
	}
	
	// 测试故障转移
	endpoints[0].Status.Ready = false // 标记第一个端点为不健康
	
	// 再次路由，不健康的端点不应该被选择
	for i := 0; i < 20; i++ {
		endpoint, err := router.RouteService(serviceName, "default", nil)
		if err != nil {
			t.Fatalf("Failed to route service during failover: %v", err)
		}
		
		if endpoint.Port == 8080 {
			t.Error("Unhealthy endpoint should not be selected")
		}
	}
}

// 测试：Gossip协议消息传播
func TestGossipMessagePropagation(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建三个节点形成网络
	nodeIDs := []string{"gossip-1", "gossip-2", "gossip-3"}
	
	for _, nodeID := range nodeIDs {
		err := suite.CreateNode(nodeID, "us-west-1")
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
	}
	
	// 连接节点形成网络
	suite.ConnectNodes("gossip-1", "gossip-2")
	suite.ConnectNodes("gossip-2", "gossip-3")
	// 注意：gossip-1和gossip-3没有直连，但应该通过gossip-2传播消息
	
	// 从gossip-1节点广播心跳消息
	heartbeat := &protocol.HeartbeatPayload{
		Status:            "active",
		ActiveConnections: 10,
		CpuUsage:          50,
		MemoryUsage:       60,
		Metrics: map[string]string{
			"test": "gossip-propagation",
		},
	}
	
	gossipManager1 := suite.gossipManagers["gossip-1"]
	gossipManager1.BroadcastHeartbeat(heartbeat)
	
	// 等待消息传播
	time.Sleep(200 * time.Millisecond)
	
	// 验证所有节点都收到了消息
	for _, nodeID := range nodeIDs {
		gossipManager := suite.gossipManagers[nodeID]
		stats := gossipManager.GetStats()
		
		if stats.CacheSize == 0 {
			t.Errorf("Node %s should have received gossip messages", nodeID)
		}
		
		if stats.MessageTypeStats["heartbeat"] == 0 {
			t.Errorf("Node %s should have received heartbeat messages", nodeID)
		}
	}
}

// 测试：健康检查和自动恢复
func TestHealthCheckAndAutoRecovery(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建节点
	err := suite.CreateNode("health-node", "us-west-1")
	if err != nil {
		t.Fatalf("Failed to create health node: %v", err)
	}
	
	// 创建服务和健康的端点
	err = suite.CreateMockService("health-node", "health-service", 8080)
	if err != nil {
		t.Fatalf("Failed to create mock service: %v", err)
	}
	
	healthChecker := suite.healthCheckers["health-node"]
	
	// 获取端点进行健康检查
	reg := suite.distributedRegs["health-node"].localRegistry.(*MockServiceRegistry)
	endpoints := reg.endpoints["health-service.default"]
	if len(endpoints) == 0 {
		t.Fatal("No endpoints found for health service")
	}
	
	endpoint := endpoints[0]
	
	// 执行健康检查
	healthChecker.performHealthCheck(endpoint)
	
	// 验证健康状态
	if !healthChecker.IsHealthy(endpoint.ID) {
		t.Error("Endpoint should be healthy after successful check")
	}
	
	health := healthChecker.GetHealthStatus(endpoint.ID)
	if health == nil {
		t.Error("Health status should exist")
	}
	
	if health.ConsecutiveSuccesses == 0 {
		t.Error("Should have at least one successful check")
	}
	
	// 模拟服务故障（关闭Mock服务器）
	serverKey := fmt.Sprintf("health-node-health-service")
	suite.mockTCPServers[serverKey].Close()
	
	// 再次执行健康检查
	healthChecker.performHealthCheck(endpoint)
	
	// 验证故障检测
	health = healthChecker.GetHealthStatus(endpoint.ID)
	if health.ConsecutiveFailures == 0 {
		t.Error("Should have detected failure")
	}
	
	if health.ErrorMessage == "" {
		t.Error("Should have error message for failed check")
	}
}

// 测试：大规模网格性能
func TestLargeScaleMeshPerformance(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	nodeCount := 20
	serviceCount := 50
	
	// 创建大量节点
	nodeIDs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("scale-node-%d", i)
		nodeIDs[i] = nodeID
		
		zone := "us-west-1"
		if i%3 == 1 {
			zone = "us-east-1"
		} else if i%3 == 2 {
			zone = "eu-west-1"
		}
		
		err := suite.CreateNode(nodeID, zone)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
	}
	
	// 连接所有节点（全连接网络）
	for i := 0; i < nodeCount; i++ {
		for j := i + 1; j < nodeCount; j++ {
			suite.ConnectNodes(nodeIDs[i], nodeIDs[j])
		}
	}
	
	// 在每个节点创建服务
	for i := 0; i < nodeCount && i < serviceCount; i++ {
		serviceName := fmt.Sprintf("scale-service-%d", i)
		err := suite.CreateMockService(nodeIDs[i], serviceName, 8080+i)
		if err != nil {
			t.Fatalf("Failed to create service %s: %v", serviceName, err)
		}
	}
	
	// 等待服务传播
	time.Sleep(1 * time.Second)
	
	// 验证服务发现性能
	start := time.Now()
	
	var wg sync.WaitGroup
	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(nID string) {
			defer wg.Done()
			
			router := suite.routers[nID]
			services, err := router.DiscoverServices("default")
			if err != nil {
				t.Errorf("Failed to discover services from node %s: %v", nID, err)
				return
			}
			
			if len(services) < serviceCount/2 {
				t.Errorf("Node %s should discover more services, got %d", nID, len(services))
			}
		}(nodeID)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	
	// 性能验证
	if elapsed > 5*time.Second {
		t.Errorf("Service discovery took too long: %v", elapsed)
	}
	
	// 验证路由性能
	start = time.Now()
	
	for i := 0; i < 1000; i++ {
		nodeIndex := i % nodeCount
		serviceIndex := i % serviceCount
		
		router := suite.routers[nodeIDs[nodeIndex]]
		serviceName := fmt.Sprintf("scale-service-%d", serviceIndex)
		
		_, err := router.RouteService(serviceName, "default", nil)
		// 允许一些路由失败，因为不是所有节点都有所有服务
		_ = err
	}
	
	elapsed = time.Since(start)
	
	if elapsed > 2*time.Second {
		t.Errorf("Routing 1000 requests took too long: %v", elapsed)
	}
}

// 测试：网络分区和恢复
func TestNetworkPartitionAndRecovery(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建四个节点
	nodeIDs := []string{"partition-1", "partition-2", "partition-3", "partition-4"}
	
	for _, nodeID := range nodeIDs {
		err := suite.CreateNode(nodeID, "us-west-1")
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
	}
	
	// 初始全连接网络
	for i := 0; i < len(nodeIDs); i++ {
		for j := i + 1; j < len(nodeIDs); j++ {
			suite.ConnectNodes(nodeIDs[i], nodeIDs[j])
		}
	}
	
	// 在每个节点创建服务
	for i, nodeID := range nodeIDs {
		serviceName := fmt.Sprintf("partition-service-%d", i)
		err := suite.CreateMockService(nodeID, serviceName, 8080+i)
		if err != nil {
			t.Fatalf("Failed to create service %s: %v", serviceName, err)
		}
	}
	
	// 等待服务传播
	time.Sleep(200 * time.Millisecond)
	
	// 验证初始状态：所有节点都能发现所有服务
	for _, nodeID := range nodeIDs {
		router := suite.routers[nodeID]
		services, err := router.DiscoverServices("default")
		if err != nil {
			t.Fatalf("Failed to discover services: %v", err)
		}
		
		if len(services) != 4 {
			t.Errorf("Node %s should discover 4 services, got %d", nodeID, len(services))
		}
	}
	
	// 模拟网络分区：将网络分为两个分区
	// 分区1: partition-1, partition-2
	// 分区2: partition-3, partition-4
	
	// 清除跨分区连接
	cluster1 := suite.clusters["partition-1"]
	cluster2 := suite.clusters["partition-2"]
	cluster3 := suite.clusters["partition-3"]
	cluster4 := suite.clusters["partition-4"]
	
	// 移除跨分区的节点
	delete(cluster1.nodes, "partition-3")
	delete(cluster1.nodes, "partition-4")
	delete(cluster2.nodes, "partition-3")
	delete(cluster2.nodes, "partition-4")
	delete(cluster3.nodes, "partition-1")
	delete(cluster3.nodes, "partition-2")
	delete(cluster4.nodes, "partition-1")
	delete(cluster4.nodes, "partition-2")
	
	// 等待分区生效
	time.Sleep(200 * time.Millisecond)
	
	// 验证分区状态：每个分区内的节点只能看到分区内的其他节点
	partition1Nodes := []string{"partition-1", "partition-2"}
	partition2Nodes := []string{"partition-3", "partition-4"}
	
	for _, nodeID := range partition1Nodes {
		cluster := suite.clusters[nodeID]
		nodes := cluster.GetHealthyNodes()
		
		// 分区1的节点应该只看到1个其他节点（分区内的）
		if len(nodes) != 1 {
			t.Errorf("Node %s in partition 1 should see 1 other node, got %d", nodeID, len(nodes))
		}
	}
	
	for _, nodeID := range partition2Nodes {
		cluster := suite.clusters[nodeID]
		nodes := cluster.GetHealthyNodes()
		
		// 分区2的节点应该只看到1个其他节点（分区内的）
		if len(nodes) != 1 {
			t.Errorf("Node %s in partition 2 should see 1 other node, got %d", nodeID, len(nodes))
		}
	}
	
	// 模拟网络恢复：重新连接分区
	suite.ConnectNodes("partition-1", "partition-3")
	suite.ConnectNodes("partition-2", "partition-4")
	
	// 等待网络恢复
	time.Sleep(300 * time.Millisecond)
	
	// 验证恢复状态：所有节点再次能够看到其他节点
	for _, nodeID := range nodeIDs {
		cluster := suite.clusters[nodeID]
		nodes := cluster.GetHealthyNodes()
		
		if len(nodes) < 2 {
			t.Errorf("Node %s should see at least 2 other nodes after recovery, got %d", nodeID, len(nodes))
		}
	}
}

// 测试：动态配置更新
func TestDynamicConfigurationUpdate(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 创建节点
	err := suite.CreateNode("config-node", "us-west-1")
	if err != nil {
		t.Fatalf("Failed to create config node: %v", err)
	}
	
	router := suite.routers["config-node"]
	healthChecker := suite.healthCheckers["config-node"]
	
	// 验证初始配置
	initialPolicy := router.routingPolicy
	if !initialPolicy.PreferLocal {
		t.Error("Initial policy should prefer local")
	}
	
	// 更新路由策略
	newPolicy := &RoutingPolicy{
		PreferLocal:        false,
		PreferSameZone:     false,
		PreferLowLatency:   true,
		EnableFailover:     true,
		MaxRetries:         5,
		DefaultLBAlgorithm: "random",
		LocalZone:          "eu-west-1",
	}
	
	router.UpdateRoutingPolicy(newPolicy)
	
	// 验证策略更新
	if router.routingPolicy.PreferLocal {
		t.Error("Updated policy should not prefer local")
	}
	
	if !router.routingPolicy.PreferLowLatency {
		t.Error("Updated policy should prefer low latency")
	}
	
	if router.routingPolicy.DefaultLBAlgorithm != "random" {
		t.Error("Updated policy should use random load balancer")
	}
	
	// 更新健康检查配置
	newHealthConfig := &HealthCheckConfig{
		CheckInterval:          5 * time.Second,
		UnhealthyThreshold:     5,
		HealthyThreshold:       3,
		ConnectTimeout:         3 * time.Second,
		CheckType:              "http",
		MaxConcurrentChecks:    15,
		QueueSize:              1500,
		RecoveryCheckInterval:  8 * time.Second,
		FastRecoveryEnabled:    false,
	}
	
	healthChecker.UpdateConfig(newHealthConfig)
	
	// 验证健康检查配置更新
	if healthChecker.config.CheckInterval != 5*time.Second {
		t.Error("Health check interval should be updated")
	}
	
	if healthChecker.config.CheckType != "http" {
		t.Error("Health check type should be updated")
	}
	
	if healthChecker.config.FastRecoveryEnabled {
		t.Error("Fast recovery should be disabled")
	}
}

// 端到端完整场景测试
func TestEndToEndCompleteScenario(t *testing.T) {
	suite := NewIntegrationTestSuite()
	defer suite.Cleanup()
	
	// 场景：三个区域，每个区域2个节点，部署微服务应用
	zones := []string{"us-west-1", "us-east-1", "eu-west-1"}
	nodeCount := 6
	
	// 创建节点
	nodeIDs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("e2e-node-%d", i)
		zone := zones[i/2] // 每两个节点一个区域
		nodeIDs[i] = nodeID
		
		err := suite.CreateNode(nodeID, zone)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
	}
	
	// 连接节点（跨区域连接）
	for i := 0; i < nodeCount; i++ {
		for j := i + 1; j < nodeCount; j++ {
			suite.ConnectNodes(nodeIDs[i], nodeIDs[j])
		}
	}
	
	// 部署微服务
	services := []struct {
		name     string
		nodeID   string
		port     int
		replicas int
	}{
		{"frontend", "e2e-node-0", 8080, 2},
		{"backend", "e2e-node-2", 8081, 2},
		{"database", "e2e-node-4", 8082, 1},
	}
	
	for _, svc := range services {
		for i := 0; i < svc.replicas; i++ {
			// 简化的副本分布逻辑
		nodeIndex := 0
		if nodeIDs[0] == svc.nodeID {
			nodeIndex = 1
		}
		nodeIndex = (nodeIndex + i) % len(nodeIDs)
			if nodeIndex >= len(nodeIDs) {
				nodeIndex = 0
			}
			
			targetNodeID := nodeIDs[nodeIndex]
			serviceName := fmt.Sprintf("%s-replica-%d", svc.name, i)
			
			err := suite.CreateMockService(targetNodeID, serviceName, svc.port+i)
			if err != nil {
				t.Fatalf("Failed to create service %s: %v", serviceName, err)
			}
		}
	}
	
	// 等待服务传播和发现
	time.Sleep(1 * time.Second)
	
	// 验证服务发现
	for _, nodeID := range nodeIDs {
		router := suite.routers[nodeID]
		discoveredServices, err := router.DiscoverServices("default")
		if err != nil {
			t.Fatalf("Failed to discover services from %s: %v", nodeID, err)
		}
		
		if len(discoveredServices) == 0 {
			t.Errorf("Node %s should discover some services", nodeID)
		}
	}
	
	// 模拟服务调用链：frontend -> backend -> database
	frontendNode := "e2e-node-0"
	backendNode := "e2e-node-2"
	
	// 1. Frontend调用Backend
	frontendRouter := suite.routers[frontendNode]
	backendServices, err := frontendRouter.DiscoverServices("default")
	if err != nil {
		t.Fatalf("Frontend failed to discover backend services: %v", err)
	}
	
	backendFound := false
	for _, service := range backendServices {
		if service.Metadata.Name == "backend-replica-0" {
			backendFound = true
			break
		}
	}
	
	if !backendFound {
		t.Error("Frontend should discover backend service")
	}
	
	// 2. Backend调用Database
	backendRouter := suite.routers[backendNode]
	dbServices, err := backendRouter.DiscoverServices("default")
	if err != nil {
		t.Fatalf("Backend failed to discover database services: %v", err)
	}
	
	dbFound := false
	for _, service := range dbServices {
		if service.Metadata.Name == "database-replica-0" {
			dbFound = true
			break
		}
	}
	
	if !dbFound {
		t.Error("Backend should discover database service")
	}
	
	// 验证负载均衡和故障转移
	// 模拟高并发调用
	var wg sync.WaitGroup
	errorCount := 0
	var errorMutex sync.Mutex
	
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			// 随机选择一个节点进行服务调用
			nodeIndex := i % len(nodeIDs)
			router := suite.routers[nodeIDs[nodeIndex]]
			
			// 尝试路由到不同的服务
			serviceNames := []string{"frontend-replica-0", "backend-replica-0", "database-replica-0"}
			for _, serviceName := range serviceNames {
				_, err := router.RouteService(serviceName, "default", nil)
				if err != nil {
					errorMutex.Lock()
					errorCount++
					errorMutex.Unlock()
				}
			}
		}()
	}
	
	wg.Wait()
	
	// 允许一定的错误率（由于服务可能不存在于所有节点）
	errorRate := float64(errorCount) / (100.0 * 3.0) // 100次调用 * 3个服务
	if errorRate > 0.5 {
		t.Errorf("Error rate too high: %.2f%%", errorRate*100)
	}
	
	// 验证系统指标
	for _, nodeID := range nodeIDs {
		router := suite.routers[nodeID]
		metrics := router.GetMetrics()
		
		if metrics.TotalRequests == 0 {
			t.Errorf("Node %s should have processed some requests", nodeID)
		}
		
		healthChecker := suite.healthCheckers[nodeID]
		healthMetrics := healthChecker.GetMetrics()
		
		if healthMetrics.TotalChecks == 0 {
			t.Errorf("Node %s should have performed health checks", nodeID)
		}
	}
}