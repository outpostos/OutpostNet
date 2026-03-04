package loadbalancer

import (
	"fmt"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

/*
* @TODO 一致性 Hash 不能改变已有的连接情况
*/

func TestConsistentHashLoadBalancer_SelectEndpoint(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
		{ID: "endpoint3"},
	}

	// 测试相同key总是返回相同端点
	key := "test-key"
	selected1 := lb.SelectEndpoint(endpoints, key)
	selected2 := lb.SelectEndpoint(endpoints, key)

	if selected1 == nil || selected2 == nil {
		t.Fatal("SelectEndpoint returned nil")
	}

	if selected1.ID != selected2.ID {
		t.Errorf("Same key should return same endpoint, got %s and %s", selected1.ID, selected2.ID)
	}
}

func TestConsistentHashLoadBalancer_EmptyEndpoints(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()
	
	selected := lb.SelectEndpoint([]*types.Endpoint{}, "test")
	if selected != nil {
		t.Error("Expected nil for empty endpoints")
	}
}

func TestConsistentHashLoadBalancer_EmptyKey(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
	}

	// 空key应该使用默认key
	selected := lb.SelectEndpoint(endpoints, "")
	if selected == nil {
		t.Error("Expected endpoint for empty key")
	}
}

func TestConsistentHashLoadBalancer_Distribution(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
		{ID: "endpoint3"},
	}

	// 测试分布均匀性
	selections := make(map[string]int)
	totalKeys := 1000

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		selected := lb.SelectEndpoint(endpoints, key)
		if selected != nil {
			selections[selected.ID]++
		}
	}

	// 检查每个端点都被选中了
	if len(selections) != len(endpoints) {
		t.Errorf("Expected %d endpoints to be selected, got %d", len(endpoints), len(selections))
	}

	// 检查分布相对均匀（允许30%的偏差）
	expectedPerEndpoint := totalKeys / len(endpoints)
	tolerance := expectedPerEndpoint * 30 / 100

	for endpointID, count := range selections {
		if abs(count-expectedPerEndpoint) > tolerance {
			t.Errorf("Endpoint %s selection count %d deviates too much from expected %d", 
				endpointID, count, expectedPerEndpoint)
		}
	}
}

func TestConsistentHashLoadBalancer_Consistency(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	// 初始端点
	endpoints1 := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
		{ID: "endpoint3"},
	}

	// 记录初始映射
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	initialMapping := make(map[string]string)

	for _, key := range keys {
		selected := lb.SelectEndpoint(endpoints1, key)
		if selected != nil {
			initialMapping[key] = selected.ID
		}
	}

	// 添加一个端点
	endpoints2 := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
		{ID: "endpoint3"},
		{ID: "endpoint4"}, // 新增
	}

	// 检查映射变化
	changedCount := 0
	for _, key := range keys {
		selected := lb.SelectEndpoint(endpoints2, key)
		if selected != nil && selected.ID != initialMapping[key] {
			changedCount++
		}
	}

	// 一致性哈希应该最小化映射变化
	// 理论上只有约1/4的映射会改变
	maxExpectedChanges := len(keys) / 2 // 允许最多一半的映射改变
	if changedCount > maxExpectedChanges {
		t.Errorf("Too many mappings changed: %d out of %d (expected <= %d)", 
			changedCount, len(keys), maxExpectedChanges)
	}
}

func TestConsistentHashLoadBalancer_VirtualNodes(t *testing.T) {
	// 测试不同虚拟节点数的效果
	virtualNodes := []int{50, 100, 200}
	
	for _, vn := range virtualNodes {
		lb := NewConsistentHashLoadBalancerWithOptions(vn, CRC32HashFunc)
		
		if lb.GetVirtualNodes() != vn {
			t.Errorf("Expected %d virtual nodes, got %d", vn, lb.GetVirtualNodes())
		}

		endpoints := []*types.Endpoint{
			{ID: "endpoint1"},
			{ID: "endpoint2"},
		}

		// 触发哈希环构建
		lb.SelectEndpoint(endpoints, "test")
		
		info := lb.GetHashRingInfo()
		ringSize := info["ring_size"].(int)
		expectedSize := len(endpoints) * vn
		
		if ringSize != expectedSize {
			t.Errorf("Expected ring size %d, got %d", expectedSize, ringSize)
		}
	}
}

func TestConsistentHashLoadBalancer_HashFunctions(t *testing.T) {
	// 测试不同哈希函数
	hashFuncs := []HashFunction{CRC32HashFunc, MD5HashFunc}
	
	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
	}

	key := "test-key"
	results := make([]string, len(hashFuncs))

	for i, hashFunc := range hashFuncs {
		lb := NewConsistentHashLoadBalancerWithOptions(150, hashFunc)
		selected := lb.SelectEndpoint(endpoints, key)
		if selected != nil {
			results[i] = selected.ID
		}
	}

	// 不同哈希函数可能产生不同结果（这是正常的）
	// 这里主要测试不会崩溃
	for i, result := range results {
		if result == "" {
			t.Errorf("Hash function %d returned empty result", i)
		}
	}
}

func TestConsistentHashLoadBalancer_SetVirtualNodes(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()
	
	// 设置虚拟节点数
	lb.SetVirtualNodes(200)
	if lb.GetVirtualNodes() != 200 {
		t.Errorf("Expected 200 virtual nodes, got %d", lb.GetVirtualNodes())
	}

	// 设置无效值
	lb.SetVirtualNodes(0)
	if lb.GetVirtualNodes() != 150 {
		t.Errorf("Expected 150 virtual nodes for invalid input, got %d", lb.GetVirtualNodes())
	}
}

func TestConsistentHashLoadBalancer_AddRemoveEndpoint(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoint1 := &types.Endpoint{ID: "endpoint1"}
	endpoint2 := &types.Endpoint{ID: "endpoint2"}

	// 添加端点
	lb.AddEndpoint(endpoint1)
	lb.AddEndpoint(endpoint2)

	endpoints := []*types.Endpoint{endpoint1, endpoint2}
	selected := lb.SelectEndpoint(endpoints, "test")
	if selected == nil {
		t.Error("Expected endpoint after adding")
	}

	// 移除端点
	lb.RemoveEndpoint("endpoint1")
	
	// 验证端点1不再被选中
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		selected := lb.SelectEndpoint([]*types.Endpoint{endpoint2}, key)
		if selected != nil && selected.ID == "endpoint1" {
			t.Error("Removed endpoint should not be selected")
		}
	}
}

func TestConsistentHashLoadBalancer_GetEndpointForKey(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
	}

	key := "test-key"
	endpointID := lb.GetEndpointForKey(endpoints, key)
	
	if endpointID == "" {
		t.Error("Expected non-empty endpoint ID")
	}

	// 验证与SelectEndpoint的一致性
	selected := lb.SelectEndpoint(endpoints, key)
	if selected == nil || selected.ID != endpointID {
		t.Errorf("GetEndpointForKey and SelectEndpoint should return same result")
	}
}

func TestConsistentHashLoadBalancer_GetHashRingInfo(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
	}

	// 触发哈希环构建
	lb.SelectEndpoint(endpoints, "test")

	info := lb.GetHashRingInfo()
	
	if info["virtual_nodes"].(int) != 150 {
		t.Errorf("Expected 150 virtual nodes in info")
	}

	if info["ring_size"].(int) != 300 { // 2 endpoints * 150 virtual nodes
		t.Errorf("Expected ring size 300, got %d", info["ring_size"].(int))
	}

	if info["endpoints_count"].(int) != 2 {
		t.Errorf("Expected 2 endpoints count, got %d", info["endpoints_count"].(int))
	}

	distribution := info["endpoint_distribution"].(map[string]int)
	if len(distribution) != 2 {
		t.Errorf("Expected 2 endpoints in distribution, got %d", len(distribution))
	}
}

func TestConsistentHashLoadBalancer_Name(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()
	
	if lb.Name() != "consistent_hash" {
		t.Errorf("Expected name 'consistent_hash', got '%s'", lb.Name())
	}
}

func TestConsistentHashLoadBalancer_UpdateStats(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoint := &types.Endpoint{ID: "test"}
	
	// UpdateStats 不应该崩溃
	lb.UpdateStats(endpoint, 100*time.Millisecond, true)
	lb.UpdateStats(endpoint, 200*time.Millisecond, false)
}

func TestConsistentHashLoadBalancer_ConcurrentAccess(t *testing.T) {
	lb := NewConsistentHashLoadBalancer()

	endpoints := []*types.Endpoint{
		{ID: "endpoint1"},
		{ID: "endpoint2"},
		{ID: "endpoint3"},
	}

	// 并发访问测试
	done := make(chan bool, 10)
	
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				selected := lb.SelectEndpoint(endpoints, key)
				if selected == nil {
					t.Errorf("Concurrent access failed for key %s", key)
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}
}

// 辅助函数
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}