package loadbalancer

import (
	"testing"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestRoundRobinLoadBalancer_Select(t *testing.T) {
	lb := NewRoundRobinLoadBalancer()

	// 测试空端点列表
	_, err := lb.Select([]*types.Endpoint{})
	if err == nil {
		t.Error("expected error for empty endpoints")
	}

	// 创建测试端点
	endpoints := []*types.Endpoint{
		{
			ID:     "endpoint-1",
			IP:     "192.168.1.1",
			Port:   8080,
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "endpoint-2",
			IP:     "192.168.1.2",
			Port:   8080,
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "endpoint-3",
			IP:     "192.168.1.3",
			Port:   8080,
			Status: types.EndpointStatus{Ready: false}, // 不就绪
		},
	}

	// 测试轮询选择
	expectedOrder := []string{"endpoint-1", "endpoint-2", "endpoint-1", "endpoint-2"}
	for i, expectedID := range expectedOrder {
		selected, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if selected.ID != expectedID {
			t.Errorf("iteration %d: expected %s, got %s", i, expectedID, selected.ID)
		}
	}
}

func TestRoundRobinLoadBalancer_NoReadyEndpoints(t *testing.T) {
	lb := NewRoundRobinLoadBalancer()

	// 所有端点都不就绪
	endpoints := []*types.Endpoint{
		{
			ID:     "endpoint-1",
			IP:     "192.168.1.1",
			Port:   8080,
			Status: types.EndpointStatus{Ready: false},
		},
		{
			ID:     "endpoint-2",
			IP:     "192.168.1.2",
			Port:   8080,
			Status: types.EndpointStatus{Ready: false},
		},
	}

	_, err := lb.Select(endpoints)
	if err == nil {
		t.Error("expected error for no ready endpoints")
	}
}

func TestRoundRobinLoadBalancer_Name(t *testing.T) {
	lb := NewRoundRobinLoadBalancer()
	if lb.Name() != "round_robin" {
		t.Errorf("expected name 'round_robin', got '%s'", lb.Name())
	}
}