package mesh

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MockMeshCluster 用于测试的mock集群
type MockMeshCluster struct {
	localNode *LocalNode
	nodes     map[string]*Node
	mu        sync.RWMutex
}

func NewMockMeshCluster(localNode *LocalNode) *MockMeshCluster {
	return &MockMeshCluster{
		localNode: localNode,
		nodes:     make(map[string]*Node),
	}
}

func (m *MockMeshCluster) GetHealthyNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var nodes []*Node
	for _, node := range m.nodes {
		if node.Status == NodeStatusActive {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}
	return nodes
}

func (m *MockMeshCluster) ConnectToNode(nodeID string) (net.Conn, error) {
	// 模拟连接，这里返回nil表示不实际建立连接
	return nil, nil
}

func (m *MockMeshCluster) BroadcastHeartbeat(heartbeat *protocol.HeartbeatPayload) {
	// Mock implementation
}

func (m *MockMeshCluster) GetLocalNode() *LocalNode {
	return m.localNode
}

func (m *MockMeshCluster) GetGossipManager() *GossipManager {
	return nil // Mock implementation
}

func (m *MockMeshCluster) AddNode(node *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
}

func TestNewGossipManager(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "gossip-test-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	if gm.cluster != cluster {
		t.Error("Gossip manager should reference the correct cluster")
	}

	if gm.config == nil {
		t.Error("Gossip manager should have default config")
	}

	if gm.messageCache == nil {
		t.Error("Gossip manager should initialize message cache")
	}

	if gm.propagationQueue == nil {
		t.Error("Gossip manager should initialize propagation queue")
	}
}

func TestGossipConfig(t *testing.T) {
	config := DefaultGossipConfig()

	if config.FanOut <= 0 {
		t.Error("FanOut should be positive")
	}

	if config.MaxPropagations <= 0 {
		t.Error("MaxPropagations should be positive")
	}

	if config.CacheSize <= 0 {
		t.Error("CacheSize should be positive")
	}

	if config.CacheTTL <= 0 {
		t.Error("CacheTTL should be positive")
	}

	if config.RetryCount <= 0 {
		t.Error("RetryCount should be positive")
	}
}

func TestGossipMessageType(t *testing.T) {
	testCases := []struct {
		msgType  GossipMessageType
		expected string
	}{
		{GossipMessageHeartbeat, "heartbeat"},
		{GossipMessageServiceSync, "service_sync"},
		{GossipMessageNodeJoin, "node_join"},
		{GossipMessageNodeLeave, "node_leave"},
		{GossipMessageCustom, "custom"},
	}

	for _, tc := range testCases {
		if tc.msgType.String() != tc.expected {
			t.Errorf("Expected message type string '%s', got '%s'", tc.expected, tc.msgType.String())
		}
	}
}

func TestGossipMessageCache(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "cache-test-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	// 测试消息缓存
	msg := &GossipMessage{
		ID:        "test-msg-1",
		Type:      GossipMessageHeartbeat,
		Payload:   []byte("test payload"),
		Timestamp: time.Now(),
		Source:    "test-node",
		TTL:       5,
	}

	// 第一次处理消息
	if gm.isMessageProcessed(msg.ID) {
		t.Error("Message should not be processed initially")
	}

	gm.addToCache(msg)

	// 现在消息应该被标记为已处理
	if !gm.isMessageProcessed(msg.ID) {
		t.Error("Message should be marked as processed after adding to cache")
	}

	// 测试缓存大小限制
	gm.config.CacheSize = 2
	msg2 := &GossipMessage{
		ID:        "test-msg-2",
		Type:      GossipMessageHeartbeat,
		Timestamp: time.Now(),
	}
	msg3 := &GossipMessage{
		ID:        "test-msg-3",
		Type:      GossipMessageHeartbeat,
		Timestamp: time.Now(),
	}

	gm.addToCache(msg2)
	gm.addToCache(msg3)

	// 第一个消息应该被淘汰
	if gm.isMessageProcessed(msg.ID) {
		t.Error("First message should be evicted from cache")
	}
}

func TestGossipPropagationTargetSelection(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "source-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	
	// 添加一些测试节点
	nodes := []*Node{
		{ID: "node-1", Status: NodeStatusActive},
		{ID: "node-2", Status: NodeStatusActive},
		{ID: "node-3", Status: NodeStatusActive},
		{ID: "node-4", Status: NodeStatusActive},
		{ID: "source-node", Status: NodeStatusActive}, // 源节点
	}

	for _, node := range nodes {
		cluster.AddNode(node)
	}

	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	msg := &GossipMessage{
		Source: "source-node",
	}

	healthyNodes := cluster.GetHealthyNodes()
	targets := gm.selectPropagationTargets(healthyNodes, msg)

	// 目标节点数量不应该超过扇出设置
	if len(targets) > gm.config.FanOut {
		t.Errorf("Target count %d should not exceed fan out %d", len(targets), gm.config.FanOut)
	}

	// 目标节点不应包含源节点和本地节点
	for _, target := range targets {
		if target.ID == "source-node" {
			t.Error("Target should not include source node")
		}
		if target.ID == localNode.ID {
			t.Error("Target should not include local node")
		}
	}
}

func TestGossipHeartbeatBroadcast(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "heartbeat-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	heartbeat := &protocol.HeartbeatPayload{
		Status:            "active",
		ActiveConnections: 10,
		CpuUsage:          50,
		MemoryUsage:       60,
		Metrics:           make(map[string]string),
	}

	// 广播心跳
	gm.BroadcastHeartbeat(heartbeat)

	// 等待消息处理
	time.Sleep(100 * time.Millisecond)

	// 检查消息是否被添加到缓存
	if len(gm.messageCache) == 0 {
		t.Error("Heartbeat message should be added to cache")
	}
}

func TestGossipServiceSyncBroadcast(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "sync-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	serviceSync := &protocol.ServiceSync{
		Services: []*protocol.Service{
			{
				Metadata: &protocol.ServiceMetadata{
					Name:      "test-service",
					Namespace: "default",
					Labels:    make(map[string]string),
				},
			},
		},
		Endpoints: []*protocol.Endpoint{},
		Version:   1,
		SyncTime:  timestamppb.Now(),
	}

	// 广播服务同步
	gm.BroadcastServiceSync(serviceSync)

	// 等待消息处理
	time.Sleep(100 * time.Millisecond)

	// 检查消息是否被添加到缓存
	if len(gm.messageCache) == 0 {
		t.Error("Service sync message should be added to cache")
	}
}

func TestGossipNodeJoinLeave(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "join-leave-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	nodeInfo := &protocol.NodeInfo{
		NodeId:       "new-node",
		NodeAddr:     "127.0.0.1:8080",
		Labels:       make(map[string]string),
		Capabilities: []string{"tcp", "udp"},
		LastSeen:     timestamppb.Now(),
	}

	// 广播节点加入
	gm.BroadcastNodeJoin(nodeInfo)

	// 等待消息处理
	time.Sleep(100 * time.Millisecond)

	joinMessageCount := 0
	for _, msg := range gm.messageCache {
		if msg.Type == GossipMessageNodeJoin {
			joinMessageCount++
		}
	}

	if joinMessageCount == 0 {
		t.Error("Node join message should be added to cache")
	}

	nodeHeartbeat := &protocol.NodeHeartbeat{
		NodeId: "leaving-node",
		NodeInfo: &protocol.NodeInfo{
			NodeId:   "leaving-node",
			NodeAddr: "127.0.0.1:8081",
		},
	}

	// 广播节点离开
	gm.BroadcastLeave(nodeHeartbeat)

	// 等待消息处理
	time.Sleep(100 * time.Millisecond)

	leaveMessageCount := 0
	for _, msg := range gm.messageCache {
		if msg.Type == GossipMessageNodeLeave {
			leaveMessageCount++
		}
	}

	if leaveMessageCount == 0 {
		t.Error("Node leave message should be added to cache")
	}
}

func TestGossipStats(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "stats-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	// 添加一些测试消息
	messages := []*GossipMessage{
		{ID: "msg-1", Type: GossipMessageHeartbeat, Timestamp: time.Now()},
		{ID: "msg-2", Type: GossipMessageServiceSync, Timestamp: time.Now()},
		{ID: "msg-3", Type: GossipMessageNodeJoin, Timestamp: time.Now()},
	}

	for _, msg := range messages {
		gm.addToCache(msg)
	}

	stats := gm.GetStats()

	if stats.CacheSize != len(messages) {
		t.Errorf("Expected cache size %d, got %d", len(messages), stats.CacheSize)
	}

	if stats.MaxCacheSize != gm.config.CacheSize {
		t.Errorf("Expected max cache size %d, got %d", gm.config.CacheSize, stats.MaxCacheSize)
	}

	if len(stats.MessageTypeStats) == 0 {
		t.Error("Message type stats should not be empty")
	}

	// 检查消息类型统计
	if stats.MessageTypeStats["heartbeat"] != 1 {
		t.Errorf("Expected 1 heartbeat message, got %d", stats.MessageTypeStats["heartbeat"])
	}

	if stats.MessageTypeStats["service_sync"] != 1 {
		t.Errorf("Expected 1 service_sync message, got %d", stats.MessageTypeStats["service_sync"])
	}

	if stats.MessageTypeStats["node_join"] != 1 {
		t.Errorf("Expected 1 node_join message, got %d", stats.MessageTypeStats["node_join"])
	}
}

func TestGossipCacheCleanup(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "cleanup-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	// 设置较短的TTL用于测试
	gm.config.CacheTTL = 100 * time.Millisecond

	// 添加过期消息
	oldMsg := &GossipMessage{
		ID:        "old-msg",
		Type:      GossipMessageHeartbeat,
		Timestamp: time.Now().Add(-200 * time.Millisecond),
	}
	gm.addToCache(oldMsg)

	// 添加新消息
	newMsg := &GossipMessage{
		ID:        "new-msg",
		Type:      GossipMessageHeartbeat,
		Timestamp: time.Now(),
	}
	gm.addToCache(newMsg)

	// 执行清理
	gm.cleanupExpiredMessages()

	// 检查过期消息是否被删除
	if gm.isMessageProcessed("old-msg") {
		t.Error("Expired message should be removed from cache")
	}

	// 检查新消息是否保留
	if !gm.isMessageProcessed("new-msg") {
		t.Error("Recent message should remain in cache")
	}
}

// 性能测试
func BenchmarkGossipMessagePropagation(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "bench-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		b.Fatal(err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		b.Fatal(err)
	}

	heartbeat := &protocol.HeartbeatPayload{
		Status: "active",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gm.BroadcastHeartbeat(heartbeat)
	}
}

func BenchmarkGossipCacheOperations(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "cache-bench-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		b.Fatal(err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		b.Fatal(err)
	}

	messages := make([]*GossipMessage, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = &GossipMessage{
			ID:        generateNodeID(),
			Type:      GossipMessageHeartbeat,
			Timestamp: time.Now(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gm.addToCache(messages[i])
	}
}

// 并发测试
func TestGossipConcurrency(t *testing.T) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "concurrent-gossip-node"

	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		t.Fatalf("Failed to create local node: %v", err)
	}

	cluster := NewMockMeshCluster(localNode)
	gm, err := NewGossipManager(cluster)
	if err != nil {
		t.Fatalf("Failed to create gossip manager: %v", err)
	}

	// 并发广播消息
	concurrency := 10
	messagesPerGoroutine := 100
	var wg sync.WaitGroup

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				heartbeat := &protocol.HeartbeatPayload{
					Status: "active",
					Metrics: map[string]string{
						"worker": string(rune('A' + workerID)),
						"count":  string(rune('0' + j%10)),
					},
				}
				gm.BroadcastHeartbeat(heartbeat)
			}
		}(i)
	}

	wg.Wait()

	// 等待消息处理完成
	time.Sleep(500 * time.Millisecond)

	// 检查缓存状态
	stats := gm.GetStats()
	if stats.CacheSize == 0 {
		t.Error("Cache should contain messages after concurrent operations")
	}

	// 缓存大小不应该超过配置的最大值
	if stats.CacheSize > stats.MaxCacheSize {
		t.Errorf("Cache size %d should not exceed max size %d", stats.CacheSize, stats.MaxCacheSize)
	}
}