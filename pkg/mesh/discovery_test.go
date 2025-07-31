package mesh

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDiscoveryManager_StaticDiscovery(t *testing.T) {
	// 创建静态节点配置
	config := &DiscoveryConfig{
		Methods: []string{"static"},
		StaticNodes: []StaticNodeConfig{
			{
				ID:       "node-1",
				Address:  "192.168.1.100:7629",
				Labels:   map[string]string{"zone": "east", "type": "server"},
				Priority: 1,
			},
			{
				ID:       "node-2", 
				Address:  "192.168.1.101:7629",
				Labels:   map[string]string{"zone": "west", "type": "server"},
				Priority: 2,
			},
		},
		DiscoveryInterval:   10 * time.Second,
		NodeTimeout:         30 * time.Second,
		HealthCheckInterval: 15 * time.Second,
	}

	// 创建本地节点
	localNode := &LocalNode{
		Node: &Node{
			ID:   "test-local-node",
			Addr: "127.0.0.1:7629",
		},
	}

	// 创建发现管理器
	dm, err := NewDiscoveryManager(config, localNode)
	if err != nil {
		t.Fatalf("Failed to create discovery manager: %v", err)
	}

	// 测试发现方法初始化
	if len(dm.methods) != 1 {
		t.Errorf("Expected 1 discovery method, got %d", len(dm.methods))
	}

	if dm.methods[0].Name() != "static" {
		t.Errorf("Expected static method, got %s", dm.methods[0].Name())
	}

	// 测试静态发现
	staticMethod := dm.methods[0]
	nodes, err := staticMethod.Discover(context.Background())
	if err != nil {
		t.Fatalf("Static discovery failed: %v", err)
	}

	if len(nodes) != 2 {
		t.Errorf("Expected 2 discovered nodes, got %d", len(nodes))
	}

	// 验证节点信息
	foundNode1 := false
	foundNode2 := false
	for _, node := range nodes {
		if node.ID == "node-1" {
			foundNode1 = true
			if node.Addr != "192.168.1.100:7629" {
				t.Errorf("Node-1 address mismatch: %s", node.Addr)
			}
			if node.Labels["zone"] != "east" {
				t.Errorf("Node-1 zone label mismatch: %s", node.Labels["zone"])
			}
		}
		if node.ID == "node-2" {
			foundNode2 = true
			if node.Addr != "192.168.1.101:7629" {
				t.Errorf("Node-2 address mismatch: %s", node.Addr)
			}
			if node.Labels["zone"] != "west" {
				t.Errorf("Node-2 zone label mismatch: %s", node.Labels["zone"])
			}
		}
	}

	if !foundNode1 || !foundNode2 {
		t.Errorf("Not all expected nodes were discovered")
	}
}

func TestDiscoveryManager_EnvDiscovery(t *testing.T) {
	// 设置环境变量
	os.Setenv("OUTPOSTNET_NODES", "192.168.1.10:7629, 192.168.1.11:7629")
	os.Setenv("OUTPOSTNET_CLUSTER_NODES", `[{"id":"cluster-node-1","address":"192.168.2.10:7629","labels":{"cluster":"prod"}}]`)
	defer func() {
		os.Unsetenv("OUTPOSTNET_NODES")
		os.Unsetenv("OUTPOSTNET_CLUSTER_NODES")
	}()

	config := &DiscoveryConfig{
		Methods: []string{"env"},
		EnvConfig: EnvConfig{
			Enabled:       true,
			NodesEnvVar:   "OUTPOSTNET_NODES",
			ClusterEnvVar: "OUTPOSTNET_CLUSTER_NODES",
			Separator:     ",",
		},
		DiscoveryInterval:   10 * time.Second,
		NodeTimeout:         30 * time.Second,
		HealthCheckInterval: 15 * time.Second,
	}

	localNode := &LocalNode{
		Node: &Node{
			ID:   "test-local-node",
			Addr: "127.0.0.1:7629",
		},
	}

	dm, err := NewDiscoveryManager(config, localNode)
	if err != nil {
		t.Fatalf("Failed to create discovery manager: %v", err)
	}

	// 测试环境变量发现
	envMethod := dm.methods[0]
	nodes, err := envMethod.Discover(context.Background())
	if err != nil {
		t.Fatalf("Env discovery failed: %v", err)
	}

	// 应该发现3个节点：2个来自OUTPOSTNET_NODES，1个来自OUTPOSTNET_CLUSTER_NODES
	if len(nodes) != 3 {
		t.Errorf("Expected 3 discovered nodes, got %d", len(nodes))
	}

	// 验证节点信息
	hasEnvNode := false
	hasClusterNode := false
	for _, node := range nodes {
		if node.Addr == "192.168.1.10:7629" || node.Addr == "192.168.1.11:7629" {
			hasEnvNode = true
			if node.Labels["source"] != "env" {
				t.Errorf("Env node should have source=env label")
			}
		}
		if node.ID == "cluster-node-1" {
			hasClusterNode = true
			if node.Labels["cluster"] != "prod" {
				t.Errorf("Cluster node label mismatch")
			}
		}
	}

	if !hasEnvNode || !hasClusterNode {
		t.Errorf("Not all expected node types were discovered")
	}
}

func TestDiscoveryManager_NodeManagement(t *testing.T) {
	config := DefaultDiscoveryConfig()
	config.Methods = []string{"static"}
	config.StaticNodes = []StaticNodeConfig{
		{
			ID:      "node-1",
			Address: "192.168.1.100:7629",
			Labels:  map[string]string{"zone": "east"},
		},
	}
	config.NodeTimeout = 2 * time.Second // 短超时用于测试

	localNode := &LocalNode{
		Node: &Node{
			ID:   "test-local-node",
			Addr: "127.0.0.1:7629",
		},
	}

	dm, err := NewDiscoveryManager(config, localNode)
	if err != nil {
		t.Fatalf("Failed to create discovery manager: %v", err)
	}

	// 测试事件监听器
	events := make(chan *NodeEvent, 10)
	listener := &TestNodeEventListener{events: events}
	dm.AddEventListener(listener)

	// 启动发现管理器
	if err := dm.Start(); err != nil {
		t.Fatalf("Failed to start discovery manager: %v", err)
	}
	defer dm.Stop()

	// 等待初始发现
	time.Sleep(100 * time.Millisecond)

	// 检查发现的节点
	nodes := dm.GetDiscoveredNodes()
	if len(nodes) != 1 {
		t.Errorf("Expected 1 discovered node, got %d", len(nodes))
	}

	if nodes["node-1"] == nil {
		t.Errorf("Node-1 should be discovered")
	}

	// 检查事件
	select {
	case event := <-events:
		if event.Type != NodeEventJoined {
			t.Errorf("Expected NodeEventJoined, got %v", event.Type)
		}
		if event.Node.ID != "node-1" {
			t.Errorf("Expected event for node-1, got %s", event.Node.ID)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Should have received a node joined event")
	}

	// 等待节点超时（模拟节点离线）
	time.Sleep(3 * time.Second)

	// 检查节点是否被标记为失败或移除
	nodes = dm.GetDiscoveredNodes()
	if len(nodes) > 0 {
		if node := nodes["node-1"]; node != nil && node.Status != NodeStatusFailed {
			t.Errorf("Node should be marked as failed or removed")
		}
	}
}

// TestNodeEventListener 测试用的事件监听器
type TestNodeEventListener struct {
	events chan *NodeEvent
}

func (l *TestNodeEventListener) OnNodeJoined(node *Node) {
	l.events <- &NodeEvent{
		Type:      NodeEventJoined,
		Node:      node,
		Timestamp: time.Now(),
	}
}

func (l *TestNodeEventListener) OnNodeLeft(node *Node) {
	l.events <- &NodeEvent{
		Type:      NodeEventLeft,
		Node:      node,
		Timestamp: time.Now(),
	}
}

func (l *TestNodeEventListener) OnNodeUpdated(node *Node) {
	l.events <- &NodeEvent{
		Type:      NodeEventUpdated,
		Node:      node,
		Timestamp: time.Now(),
	}
}

func (l *TestNodeEventListener) OnNodeFailed(node *Node) {
	l.events <- &NodeEvent{
		Type:      NodeEventFailed,
		Node:      node,
		Timestamp: time.Now(),
	}
}

func TestDefaultDiscoveryConfig(t *testing.T) {
	config := DefaultDiscoveryConfig()

	// 验证默认配置
	expectedMethods := []string{"static", "env"}
	if len(config.Methods) != len(expectedMethods) {
		t.Errorf("Expected %d methods, got %d", len(expectedMethods), len(config.Methods))
	}

	for i, method := range expectedMethods {
		if config.Methods[i] != method {
			t.Errorf("Expected method %s at index %d, got %s", method, i, config.Methods[i])
		}
	}

	// 验证默认时间间隔
	if config.DiscoveryInterval != 30*time.Second {
		t.Errorf("Expected discovery interval 30s, got %v", config.DiscoveryInterval)
	}

	if config.NodeTimeout != 5*time.Minute {
		t.Errorf("Expected node timeout 5m, got %v", config.NodeTimeout)
	}

	if config.HealthCheckInterval != 30*time.Second {
		t.Errorf("Expected health check interval 30s, got %v", config.HealthCheckInterval)
	}

	// 验证环境变量配置默认值
	if !config.EnvConfig.Enabled {
		t.Errorf("Env config should be enabled by default")
	}

	if config.EnvConfig.NodesEnvVar != "OUTPOSTNET_NODES" {
		t.Errorf("Expected default nodes env var OUTPOSTNET_NODES, got %s", config.EnvConfig.NodesEnvVar)
	}
}