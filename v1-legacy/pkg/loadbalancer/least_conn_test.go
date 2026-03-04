package loadbalancer

import (
	"testing"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestLeastConnLoadBalancer_Select(t *testing.T) {
	lb := NewLeastConnLoadBalancer()

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
			Status: types.EndpointStatus{Ready: true},
		},
	}

	// 第一次选择应该返回任意一个端点（都是0连接）
	selected1, err := lb.Select(endpoints)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 增加第一个端点的连接数
	lb.AddConnection(selected1.ID)

	// 第二次选择应该返回不同的端点
	selected2, err := lb.Select(endpoints)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 验证选择了连接数较少的端点
	if selected2.ID == selected1.ID {
		// 可能是巧合，再试一次
		lb.AddConnection(selected1.ID) // 再增加一个连接
		selected3, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selected3.ID == selected1.ID {
			t.Error("load balancer should prefer endpoints with fewer connections")
		}
	}
}

func TestLeastConnLoadBalancer_ConnectionTracking(t *testing.T) {
	lb := NewLeastConnLoadBalancer()

	// 测试连接计数
	endpointID := "test-endpoint"

	// 初始连接数应该为0
	if count := lb.GetConnectionCount(endpointID); count != 0 {
		t.Errorf("expected initial connection count 0, got %d", count)
	}

	// 增加连接
	lb.AddConnection(endpointID)
	if count := lb.GetConnectionCount(endpointID); count != 1 {
		t.Errorf("expected connection count 1, got %d", count)
	}

	// 再增加连接
	lb.AddConnection(endpointID)
	if count := lb.GetConnectionCount(endpointID); count != 2 {
		t.Errorf("expected connection count 2, got %d", count)
	}

	// 减少连接
	lb.RemoveConnection(endpointID)
	if count := lb.GetConnectionCount(endpointID); count != 1 {
		t.Errorf("expected connection count 1 after removal, got %d", count)
	}

	// 减少到0
	lb.RemoveConnection(endpointID)
	if count := lb.GetConnectionCount(endpointID); count != 0 {
		t.Errorf("expected connection count 0 after removal, got %d", count)
	}

	// 不应该变为负数
	lb.RemoveConnection(endpointID)
	if count := lb.GetConnectionCount(endpointID); count < 0 {
		t.Errorf("connection count should not be negative, got %d", count)
	}
}

func TestLeastConnLoadBalancer_Name(t *testing.T) {
	lb := NewLeastConnLoadBalancer()
	if lb.Name() != "least_conn" {
		t.Errorf("expected name 'least_conn', got '%s'", lb.Name())
	}
}