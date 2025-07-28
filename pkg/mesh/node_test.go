package mesh

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// MockServiceRegistry 用于测试的mock服务注册表
type MockServiceRegistry struct {
	services  map[string]*types.Service
	workloads map[string]*types.Workload
	endpoints map[string][]*types.Endpoint
}

func NewMockServiceRegistry() *MockServiceRegistry {
	return &MockServiceRegistry{
		services:  make(map[string]*types.Service),
		workloads: make(map[string]*types.Workload),
		endpoints: make(map[string][]*types.Endpoint),
	}
}

func (m *MockServiceRegistry) RegisterService(service *types.Service) error {
	key := service.Metadata.Name + "." + service.Metadata.Namespace
	m.services[key] = service
	return nil
}

func (m *MockServiceRegistry) GetService(name, namespace string) (*types.Service, error) {
	key := name + "." + namespace
	if service, exists := m.services[key]; exists {
		return service, nil
	}
	return nil, nil
}

func (m *MockServiceRegistry) ListServices(namespace string) ([]*types.Service, error) {
	var result []*types.Service
	for _, service := range m.services {
		if namespace == "" || service.Metadata.Namespace == namespace {
			result = append(result, service)
		}
	}
	return result, nil
}

func (m *MockServiceRegistry) RegisterWorkload(workload *types.Workload) error {
	m.workloads[workload.Metadata.ID] = workload
	return nil
}

func (m *MockServiceRegistry) UnregisterWorkload(workloadID string) error {
	delete(m.workloads, workloadID)
	return nil
}

func (m *MockServiceRegistry) GetWorkload(workloadID string) (*types.Workload, error) {
	if workload, exists := m.workloads[workloadID]; exists {
		return workload, nil
	}
	return nil, nil
}

func (m *MockServiceRegistry) ListWorkloads(selector map[string]string) ([]*types.Workload, error) {
	var result []*types.Workload
	for _, workload := range m.workloads {
		if m.matchesSelector(workload.Metadata.Labels, selector) {
			result = append(result, workload)
		}
	}
	return result, nil
}

func (m *MockServiceRegistry) GetEndpoints(serviceName, namespace string, selector map[string]string) ([]*types.Endpoint, error) {
	key := serviceName + "." + namespace
	if endpoints, exists := m.endpoints[key]; exists {
		var result []*types.Endpoint
		for _, endpoint := range endpoints {
			if m.matchesSelector(endpoint.Labels, selector) {
				result = append(result, endpoint)
			}
		}
		return result, nil
	}
	return []*types.Endpoint{}, nil
}

func (m *MockServiceRegistry) SyncEndpoints() error {
	return nil
}

func (m *MockServiceRegistry) matchesSelector(labels map[string]string, selector map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func TestNewLocalNode(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "test-node-1"
	config.ListenAddr = ":0" // 使用动态端口

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	if node.ID != "test-node-1" {
		t.Errorf("Expected node ID 'test-node-1', got '%s'", node.ID)
	}

	if node.Status != NodeStatusUnknown {
		t.Errorf("Expected node status Unknown, got %v", node.Status)
	}

	if len(node.Capabilities) == 0 {
		t.Error("Expected node to have capabilities")
	}
}

func TestNodeLabels(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "test-node-2"
	config.Labels = map[string]string{
		"zone":        "us-west-1",
		"environment": "test",
	}

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	if node.Labels["zone"] != "us-west-1" {
		t.Errorf("Expected zone label 'us-west-1', got '%s'", node.Labels["zone"])
	}

	if node.Labels["environment"] != "test" {
		t.Errorf("Expected environment label 'test', got '%s'", node.Labels["environment"])
	}

	// 测试更新标签
	newLabels := map[string]string{
		"version": "1.0.0",
	}
	node.UpdateLabels(newLabels)

	if node.Labels["version"] != "1.0.0" {
		t.Errorf("Expected version label '1.0.0', got '%s'", node.Labels["version"])
	}

	// 原有标签应该保留
	if node.Labels["zone"] != "us-west-1" {
		t.Error("Original labels should be preserved")
	}
}

func TestNodeCapabilities(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "test-node-3"

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	// 检查默认能力
	if !node.HasCapability("tcp") {
		t.Error("Node should have tcp capability")
	}

	if !node.HasCapability("udp") {
		t.Error("Node should have udp capability")
	}

	// 添加新能力
	node.AddCapability("http")
	if !node.HasCapability("http") {
		t.Error("Node should have http capability after adding")
	}

	// 重复添加不应该导致重复
	originalCount := len(node.Capabilities)
	node.AddCapability("http")
	if len(node.Capabilities) != originalCount {
		t.Error("Adding duplicate capability should not increase count")
	}
}

func TestNodeHealth(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "test-node-4"
	config.HeartbeatInterval = 100 * time.Millisecond

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	// 新创建的节点应该是健康的
	if !node.IsHealthy() {
		t.Error("New node should be healthy")
	}

	// 设置状态为活跃
	node.Status = NodeStatusActive
	node.LastSeen = time.Now()

	if !node.IsHealthy() {
		t.Error("Active node with recent LastSeen should be healthy")
	}

	// 设置过期的LastSeen
	node.LastSeen = time.Now().Add(-10 * config.HeartbeatInterval)
	if node.IsHealthy() {
		t.Error("Node with old LastSeen should not be healthy")
	}

	// 设置状态为失败
	node.Status = NodeStatusFailed
	node.LastSeen = time.Now()
	if node.IsHealthy() {
		t.Error("Failed node should not be healthy")
	}
}

func TestNodeLoadScore(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "test-node-5"

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	// 设置CPU和内存使用率
	node.CPUUsage = 30.0
	node.MemoryUsage = 40.0

	// 模拟指标收集
	node.collectMetrics()

	score := node.GetLoadScore()
	expectedScore := (30.0 + 40.0) / 2 // 35.0

	if score != expectedScore {
		t.Errorf("Expected load score %.1f, got %.1f", expectedScore, score)
	}
}

func TestNodeStatus(t *testing.T) {
	testCases := []struct {
		status   NodeStatus
		expected string
	}{
		{NodeStatusUnknown, "unknown"},
		{NodeStatusJoining, "joining"},
		{NodeStatusActive, "active"},
		{NodeStatusLeaving, "leaving"},
		{NodeStatusFailed, "failed"},
	}

	for _, tc := range testCases {
		if tc.status.String() != tc.expected {
			t.Errorf("Expected status string '%s', got '%s'", tc.expected, tc.status.String())
		}
	}
}

func TestNodeEvent(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "test-node-6"

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	// 测试事件类型
	eventTypes := []NodeEventType{
		NodeEventJoined,
		NodeEventLeft,
		NodeEventFailed,
		NodeEventUpdated,
		NodeEventDiscovered,
	}

	expectedStrings := []string{
		"joined",
		"left",
		"failed",
		"updated",
		"discovered",
	}

	for i, eventType := range eventTypes {
		if eventType.String() != expectedStrings[i] {
			t.Errorf("Expected event type string '%s', got '%s'", expectedStrings[i], eventType.String())
		}
	}

	// 测试事件创建
	event := &NodeEvent{
		Type:      NodeEventJoined,
		Node:      node.Node,
		Timestamp: time.Now(),
		Details:   "test event",
	}

	if event.Type != NodeEventJoined {
		t.Error("Event type should be NodeEventJoined")
	}

	if event.Node.ID != node.ID {
		t.Error("Event node should match the test node")
	}
}

func TestDefaultNodeConfig(t *testing.T) {
	config := DefaultNodeConfig()

	if config.ListenAddr != ":7629" {
		t.Errorf("Expected default listen address ':7629', got '%s'", config.ListenAddr)
	}

	if config.ClusterName != "default" {
		t.Errorf("Expected default cluster name 'default', got '%s'", config.ClusterName)
	}

	if config.HeartbeatInterval != 10*time.Second {
		t.Errorf("Expected default heartbeat interval 10s, got %v", config.HeartbeatInterval)
	}

	if config.JoinTimeout != 30*time.Second {
		t.Errorf("Expected default join timeout 30s, got %v", config.JoinTimeout)
	}

	if config.MaxConnections != 1000 {
		t.Errorf("Expected default max connections 1000, got %d", config.MaxConnections)
	}
}

// 性能测试
func BenchmarkNodeCreation(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config.NodeID = generateNodeID()
		_, err := NewLocalNode(config, registry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNodeLabelUpdate(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "bench-node"

	node, err := NewLocalNode(config, registry)
	if err != nil {
		b.Fatal(err)
	}

	labels := map[string]string{
		"version": "1.0.0",
		"zone":    "us-west-1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node.UpdateLabels(labels)
	}
}

func BenchmarkNodeHealthCheck(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "bench-node"

	node, err := NewLocalNode(config, registry)
	if err != nil {
		b.Fatal(err)
	}

	node.Status = NodeStatusActive
	node.LastSeen = time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node.IsHealthy()
	}
}

// 并发测试
func TestNodeConcurrency(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "concurrent-node"

	node, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	// 并发更新标签
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			labels := map[string]string{
				"worker": string(rune('A' + id)),
			}
			node.UpdateLabels(labels)
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 检查节点状态
	nodeInfo := node.GetNodeInfo()
	if nodeInfo.ID != "concurrent-node" {
		t.Error("Node info should be accessible after concurrent updates")
	}
}

func TestGenerateNodeID(t *testing.T) {
	// 测试生成的节点ID是否唯一
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := generateNodeID()
		if len(id) != 16 { // 8字节hex编码 = 16字符
			t.Errorf("Expected node ID length 16, got %d", len(id))
		}
		if ids[id] {
			t.Errorf("Duplicate node ID generated: %s", id)
		}
		ids[id] = true
	}
}