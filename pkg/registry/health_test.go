package registry

import (
	"context"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestHealthEventListener 测试用的健康事件监听器
type TestHealthEventListener struct {
	events []*HealthEvent
}

func (l *TestHealthEventListener) OnHealthEvent(event *HealthEvent) {
	l.events = append(l.events, event)
}

func TestHealthChecker_RegisterEndpoint(t *testing.T) {
	checker := NewHealthChecker(nil)

	// 注册端点
	checker.RegisterEndpoint("ep1")

	// 检查端点是否健康
	if !checker.IsEndpointHealthy("ep1") {
		t.Error("newly registered endpoint should be healthy")
	}

	// 获取健康状态
	status, exists := checker.GetEndpointHealthStatus("ep1")
	if !exists {
		t.Error("endpoint health status should exist")
	}

	if status.EndpointID != "ep1" {
		t.Errorf("expected endpoint ID 'ep1', got '%s'", status.EndpointID)
	}

	if !status.Ready {
		t.Error("newly registered endpoint should be ready")
	}
}

func TestHealthChecker_UnregisterEndpoint(t *testing.T) {
	checker := NewHealthChecker(nil)

	// 注册端点
	checker.RegisterEndpoint("ep1")
	if !checker.IsEndpointHealthy("ep1") {
		t.Error("registered endpoint should be healthy")
	}

	// 取消注册端点
	checker.UnregisterEndpoint("ep1")

	// 检查端点是否还存在（未注册的端点默认为健康）
	if !checker.IsEndpointHealthy("ep1") {
		t.Error("unregistered endpoint should default to healthy")
	}

	// 健康状态应该不存在
	_, exists := checker.GetEndpointHealthStatus("ep1")
	if exists {
		t.Error("unregistered endpoint health status should not exist")
	}
}

func TestHealthChecker_UpdateEndpointLastSeen(t *testing.T) {
	checker := NewHealthChecker(nil)
	listener := &TestHealthEventListener{}
	checker.AddEventListener(listener)

	// 注册端点
	checker.RegisterEndpoint("ep1")

	// 手动标记为不健康
	checker.MarkEndpointUnhealthy("ep1", "test", "test unhealthy")
	if checker.IsEndpointHealthy("ep1") {
		t.Error("endpoint should be unhealthy")
	}

	// 更新最后可见时间（模拟心跳）
	checker.UpdateEndpointLastSeen("ep1")

	// 端点应该恢复健康
	if !checker.IsEndpointHealthy("ep1") {
		t.Error("endpoint should be healthy after heartbeat")
	}

	// 检查事件
	if len(listener.events) < 2 {
		t.Errorf("expected at least 2 health events, got %d", len(listener.events))
	}
}

func TestHealthChecker_MarkEndpointUnhealthy(t *testing.T) {
	checker := NewHealthChecker(nil)
	listener := &TestHealthEventListener{}
	checker.AddEventListener(listener)

	// 注册端点
	checker.RegisterEndpoint("ep1")

	// 标记为不健康
	checker.MarkEndpointUnhealthy("ep1", "connection_failed", "connection timeout")

	// 检查状态
	if checker.IsEndpointHealthy("ep1") {
		t.Error("endpoint should be unhealthy")
	}

	status, exists := checker.GetEndpointHealthStatus("ep1")
	if !exists {
		t.Error("health status should exist")
	}

	if status.Ready {
		t.Error("endpoint should not be ready")
	}

	if status.Reason != "connection_failed" {
		t.Errorf("expected reason 'connection_failed', got '%s'", status.Reason)
	}

	// 检查事件
	if len(listener.events) != 1 {
		t.Errorf("expected 1 health event, got %d", len(listener.events))
	}

	if listener.events[0].Type != HealthEventBecameUnhealthy {
		t.Errorf("expected event type '%s', got '%s'", HealthEventBecameUnhealthy, listener.events[0].Type)
	}
}

func TestHealthChecker_MarkEndpointHealthy(t *testing.T) {
	checker := NewHealthChecker(nil)
	listener := &TestHealthEventListener{}
	checker.AddEventListener(listener)

	// 注册端点并标记为不健康
	checker.RegisterEndpoint("ep1")
	checker.MarkEndpointUnhealthy("ep1", "test", "test")

	// 标记为健康
	checker.MarkEndpointHealthy("ep1", "recovered", "endpoint recovered")

	// 检查状态
	if !checker.IsEndpointHealthy("ep1") {
		t.Error("endpoint should be healthy")
	}

	status, exists := checker.GetEndpointHealthStatus("ep1")
	if !exists {
		t.Error("health status should exist")
	}

	if !status.Ready {
		t.Error("endpoint should be ready")
	}

	if status.Reason != "recovered" {
		t.Errorf("expected reason 'recovered', got '%s'", status.Reason)
	}

	// 检查事件
	if len(listener.events) != 2 {
		t.Errorf("expected 2 health events, got %d", len(listener.events))
	}

	if listener.events[1].Type != HealthEventBecameHealthy {
		t.Errorf("expected event type '%s', got '%s'", HealthEventBecameHealthy, listener.events[1].Type)
	}
}

func TestHealthChecker_FilterHealthyEndpoints(t *testing.T) {
	checker := NewHealthChecker(nil)

	// 创建测试端点
	endpoints := []*types.Endpoint{
		{
			ID:     "ep1",
			IP:     "10.0.0.1",
			Port:   8080,
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "ep2",
			IP:     "10.0.0.2",
			Port:   8080,
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "ep3",
			IP:     "10.0.0.3",
			Port:   8080,
			Status: types.EndpointStatus{Ready: true},
		},
	}

	// 注册端点
	checker.RegisterEndpoint("ep1")
	checker.RegisterEndpoint("ep2")
	checker.RegisterEndpoint("ep3")

	// 标记一个端点为不健康
	checker.MarkEndpointUnhealthy("ep2", "test", "test unhealthy")

	// 过滤健康端点
	healthyEndpoints := checker.FilterHealthyEndpoints(endpoints)

	// 应该只有2个健康端点
	if len(healthyEndpoints) != 2 {
		t.Errorf("expected 2 healthy endpoints, got %d", len(healthyEndpoints))
	}

	// 检查健康端点ID
	healthyIDs := make(map[string]bool)
	for _, ep := range healthyEndpoints {
		healthyIDs[ep.ID] = true
	}

	if !healthyIDs["ep1"] {
		t.Error("ep1 should be healthy")
	}

	if healthyIDs["ep2"] {
		t.Error("ep2 should be unhealthy")
	}

	if !healthyIDs["ep3"] {
		t.Error("ep3 should be healthy")
	}
}

func TestHealthChecker_GetAllHealthStatus(t *testing.T) {
	checker := NewHealthChecker(nil)

	// 注册多个端点
	checker.RegisterEndpoint("ep1")
	checker.RegisterEndpoint("ep2")
	checker.RegisterEndpoint("ep3")

	// 标记一个为不健康
	checker.MarkEndpointUnhealthy("ep2", "test", "test")

	// 获取所有健康状态
	allStatus := checker.GetAllHealthStatus()

	if len(allStatus) != 3 {
		t.Errorf("expected 3 health statuses, got %d", len(allStatus))
	}

	if !allStatus["ep1"].Ready {
		t.Error("ep1 should be ready")
	}

	if allStatus["ep2"].Ready {
		t.Error("ep2 should not be ready")
	}

	if !allStatus["ep3"].Ready {
		t.Error("ep3 should be ready")
	}
}

func TestHealthChecker_GetHealthStats(t *testing.T) {
	checker := NewHealthChecker(nil)

	// 注册多个端点
	checker.RegisterEndpoint("ep1")
	checker.RegisterEndpoint("ep2")
	checker.RegisterEndpoint("ep3")

	// 标记一个为不健康
	checker.MarkEndpointUnhealthy("ep2", "test", "test")

	// 获取统计信息
	stats := checker.GetHealthStats()

	if stats["total_endpoints"] != 3 {
		t.Errorf("expected 3 total endpoints, got %v", stats["total_endpoints"])
	}

	if stats["healthy_endpoints"] != 2 {
		t.Errorf("expected 2 healthy endpoints, got %v", stats["healthy_endpoints"])
	}

	if stats["unhealthy_endpoints"] != 1 {
		t.Errorf("expected 1 unhealthy endpoint, got %v", stats["unhealthy_endpoints"])
	}
}

func TestHealthChecker_StartStop(t *testing.T) {
	config := &HealthCheckConfig{
		CheckInterval:    100 * time.Millisecond, // 快速检查间隔用于测试
		Timeout:          1 * time.Second,
		RetryCount:       2,
		HeartbeatTimeout: 200 * time.Millisecond,
	}

	checker := NewHealthChecker(config)
	ctx, cancel := context.WithCancel(context.Background())

	// 启动健康检查器
	err := checker.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start health checker: %v", err)
	}

	// 注册端点
	checker.RegisterEndpoint("ep1")
	checker.UpdateEndpointLastSeen("ep1")

	// 等待一段时间让健康检查运行
	time.Sleep(50 * time.Millisecond)

	// 端点应该还是健康的
	if !checker.IsEndpointHealthy("ep1") {
		t.Error("endpoint should still be healthy")
	}

	// 等待心跳超时
	time.Sleep(300 * time.Millisecond)

	// 端点应该变为不健康
	if checker.IsEndpointHealthy("ep1") {
		t.Error("endpoint should be unhealthy after heartbeat timeout")
	}

	// 停止健康检查器
	cancel()
	checker.Stop()

	// 验证统计信息
	stats := checker.GetHealthStats()
	if stats["running"] != false {
		t.Error("health checker should not be running after stop")
	}
}