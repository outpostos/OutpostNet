package loadbalancer

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestWeightedRoundRobinLoadBalancer_SelectEndpoint(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	// 创建测试端点
	endpoints := []*types.Endpoint{
		{
			ID: "endpoint1",
			Labels: map[string]string{
				"weight": "1",
			},
		},
		{
			ID: "endpoint2",
			Labels: map[string]string{
				"weight": "3",
			},
		},
		{
			ID: "endpoint3",
			Labels: map[string]string{
				"weight": "2",
			},
		},
	}

	// 测试多次选择，验证权重分布
	selections := make(map[string]int)
	totalSelections := 60 // 1+3+2=6的倍数，便于验证权重比例

	for i := 0; i < totalSelections; i++ {
		selected := lb.SelectEndpoint(endpoints, "")
		if selected != nil {
			selections[selected.ID]++
		}
	}

	// 验证权重比例 (1:3:2)
	expectedRatio1 := totalSelections * 1 / 6 // endpoint1 应该被选中 10 次
	expectedRatio2 := totalSelections * 3 / 6 // endpoint2 应该被选中 30 次
	expectedRatio3 := totalSelections * 2 / 6 // endpoint3 应该被选中 20 次

	// 允许一定的误差范围
	tolerance := 2

	if abs(selections["endpoint1"]-expectedRatio1) > tolerance {
		t.Errorf("endpoint1 selection count %d, expected around %d", selections["endpoint1"], expectedRatio1)
	}
	if abs(selections["endpoint2"]-expectedRatio2) > tolerance {
		t.Errorf("endpoint2 selection count %d, expected around %d", selections["endpoint2"], expectedRatio2)
	}
	if abs(selections["endpoint3"]-expectedRatio3) > tolerance {
		t.Errorf("endpoint3 selection count %d, expected around %d", selections["endpoint3"], expectedRatio3)
	}
}

func TestWeightedRoundRobinLoadBalancer_EmptyEndpoints(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()
	
	selected := lb.SelectEndpoint([]*types.Endpoint{}, "")
	if selected != nil {
		t.Error("Expected nil for empty endpoints")
	}
}

func TestWeightedRoundRobinLoadBalancer_SingleEndpoint(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()
	
	endpoints := []*types.Endpoint{
		{
			ID: "single",
			Labels: map[string]string{
				"weight": "5",
			},
		},
	}

	// 多次选择应该都返回同一个端点
	for i := 0; i < 10; i++ {
		selected := lb.SelectEndpoint(endpoints, "")
		if selected == nil || selected.ID != "single" {
			t.Errorf("Expected single endpoint, got %v", selected)
		}
	}
}

func TestWeightedRoundRobinLoadBalancer_PriorityLabels(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	endpoints := []*types.Endpoint{
		{
			ID: "low",
			Labels: map[string]string{
				"priority": "low",
			},
		},
		{
			ID: "high",
			Labels: map[string]string{
				"priority": "high",
			},
		},
		{
			ID: "medium",
			Labels: map[string]string{
				"priority": "medium",
			},
		},
	}

	// 验证权重设置
	lb.SelectEndpoint(endpoints, "") // 触发权重初始化
	
	if lb.GetWeight("low") != 1 {
		t.Errorf("Expected weight 1 for low priority, got %d", lb.GetWeight("low"))
	}
	if lb.GetWeight("high") != 5 {
		t.Errorf("Expected weight 5 for high priority, got %d", lb.GetWeight("high"))
	}
	if lb.GetWeight("medium") != 3 {
		t.Errorf("Expected weight 3 for medium priority, got %d", lb.GetWeight("medium"))
	}
}

func TestWeightedRoundRobinLoadBalancer_MetadataWeight(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	endpoints := []*types.Endpoint{
		{
			ID: "endpoint1",
			Metadata: types.EndpointMetadata{
				Weight: 4,
			},
		},
		{
			ID: "endpoint2",
			Metadata: types.EndpointMetadata{
				Priority: 2,
			},
		},
	}

	// 验证从元数据获取权重
	lb.SelectEndpoint(endpoints, "") // 触发权重初始化
	
	if lb.GetWeight("endpoint1") != 4 {
		t.Errorf("Expected weight 4 from metadata, got %d", lb.GetWeight("endpoint1"))
	}
	if lb.GetWeight("endpoint2") != 2 {
		t.Errorf("Expected weight 2 from priority, got %d", lb.GetWeight("endpoint2"))
	}
}

func TestWeightedRoundRobinLoadBalancer_SetWeight(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	// 设置权重
	lb.SetWeight("test", 10)
	
	if lb.GetWeight("test") != 10 {
		t.Errorf("Expected weight 10, got %d", lb.GetWeight("test"))
	}

	// 测试无效权重
	lb.SetWeight("test", 0)
	if lb.GetWeight("test") != 1 {
		t.Errorf("Expected weight 1 for invalid weight, got %d", lb.GetWeight("test"))
	}
}

func TestWeightedRoundRobinLoadBalancer_UpdateStats(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	endpoint := &types.Endpoint{
		ID: "test",
		Labels: map[string]string{
			"weight": "5",
		},
	}

	// 初始化权重
	lb.SelectEndpoint([]*types.Endpoint{endpoint}, "")
	initialWeight := lb.GetWeight("test")

	// 模拟失败，权重应该降低
	lb.UpdateStats(endpoint, 100*time.Millisecond, false)
	
	newWeight := lb.GetWeight("test")
	if newWeight >= initialWeight {
		t.Errorf("Expected weight to decrease after failure, initial: %d, new: %d", initialWeight, newWeight)
	}
}

func TestWeightedRoundRobinLoadBalancer_Name(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()
	
	if lb.Name() != "weighted_round_robin" {
		t.Errorf("Expected name 'weighted_round_robin', got '%s'", lb.Name())
	}
}

func TestWeightedRoundRobinLoadBalancer_GetWeights(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	lb.SetWeight("endpoint1", 3)
	lb.SetWeight("endpoint2", 5)

	weights := lb.GetWeights()
	
	if len(weights) != 2 {
		t.Errorf("Expected 2 weights, got %d", len(weights))
	}
	if weights["endpoint1"] != 3 {
		t.Errorf("Expected weight 3 for endpoint1, got %d", weights["endpoint1"])
	}
	if weights["endpoint2"] != 5 {
		t.Errorf("Expected weight 5 for endpoint2, got %d", weights["endpoint2"])
	}
}

func TestWeightedRoundRobinLoadBalancer_ResetWeights(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()

	endpoints := []*types.Endpoint{
		{
			ID: "endpoint1",
			Labels: map[string]string{
				"weight": "3",
			},
		},
	}

	// 选择几次以改变当前权重
	for i := 0; i < 3; i++ {
		lb.SelectEndpoint(endpoints, "")
	}

	// 重置权重
	lb.ResetWeights()

	// 验证当前权重被重置
	// 这里我们通过选择行为来间接验证重置是否生效
	selected := lb.SelectEndpoint(endpoints, "")
	if selected == nil {
		t.Error("Expected endpoint after reset")
	}
}

// 使用 consistent_hash_test.go 中定义的 abs 函数