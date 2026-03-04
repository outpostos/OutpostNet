package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/mesh"
)

func TestDiscoveryConfig(t *testing.T) {
	config := mesh.DefaultDiscoveryConfig()

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

func TestStaticDiscovery(t *testing.T) {
	// 创建静态节点配置
	staticNodes := []mesh.StaticNodeConfig{
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
	}

	// 创建静态发现实例
	discovery := mesh.NewStaticDiscovery(staticNodes)

	// 测试发现方法名称
	if discovery.Name() != "static" {
		t.Errorf("Expected static method name, got %s", discovery.Name())
	}

	// 测试发现
	nodes, err := discovery.Discover(context.Background())
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

	// 测试启动和停止（静态发现应该不做任何操作）
	if err := discovery.Start(context.Background()); err != nil {
		t.Errorf("Static discovery start should not fail: %v", err)
	}

	if err := discovery.Stop(); err != nil {
		t.Errorf("Static discovery stop should not fail: %v", err)
	}
}

func TestEnvDiscovery(t *testing.T) {
	// 设置环境变量
	os.Setenv("OUTPOSTNET_NODES", "192.168.1.10:7629, 192.168.1.11:7629")
	os.Setenv("OUTPOSTNET_CLUSTER_NODES", `[{"id":"cluster-node-1","address":"192.168.2.10:7629","labels":{"cluster":"prod"}}]`)
	defer func() {
		os.Unsetenv("OUTPOSTNET_NODES")
		os.Unsetenv("OUTPOSTNET_CLUSTER_NODES")
	}()

	config := &mesh.EnvConfig{
		Enabled:       true,
		NodesEnvVar:   "OUTPOSTNET_NODES",
		ClusterEnvVar: "OUTPOSTNET_CLUSTER_NODES",
		Separator:     ",",
	}

	// 创建环境变量发现实例
	discovery := mesh.NewEnvDiscovery(config)

	// 测试发现方法名称
	if discovery.Name() != "env" {
		t.Errorf("Expected env method name, got %s", discovery.Name())
	}

	// 测试发现
	nodes, err := discovery.Discover(context.Background())
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

	// 测试启动和停止（环境变量发现应该不做任何操作）
	if err := discovery.Start(context.Background()); err != nil {
		t.Errorf("Env discovery start should not fail: %v", err)
	}

	if err := discovery.Stop(); err != nil {
		t.Errorf("Env discovery stop should not fail: %v", err)
	}
}