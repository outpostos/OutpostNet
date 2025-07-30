package registry

import (
	"context"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestCleanupEventListener 测试用的清理事件监听器
type TestCleanupEventListener struct {
	events []*CleanupEvent
}

func (l *TestCleanupEventListener) OnCleanupEvent(event *CleanupEvent) {
	l.events = append(l.events, event)
}

func TestCleanupManager_NewCleanupManager(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewCleanupManager(registry, nil)

	if manager == nil {
		t.Fatal("cleanup manager should not be nil")
	}

	if manager.registry != registry {
		t.Error("registry should be set correctly")
	}

	if !manager.config.EnableAutoCleanup {
		t.Error("auto cleanup should be enabled by default")
	}

	// 检查默认清理规则
	rules := manager.GetCleanupRules()
	if len(rules) != 3 {
		t.Errorf("expected 3 default cleanup rules, got %d", len(rules))
	}

	expectedRules := []string{"service", "workload", "endpoint"}
	for _, ruleType := range expectedRules {
		if _, exists := rules[ruleType]; !exists {
			t.Errorf("missing default cleanup rule for %s", ruleType)
		}
	}
}

func TestCleanupManager_GetStats(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewCleanupManager(registry, nil)

	stats := manager.GetStats()
	if stats == nil {
		t.Fatal("stats should not be nil")
	}

	if stats.TotalCleanupRuns != 0 {
		t.Error("initial cleanup runs should be 0")
	}

	if stats.TotalCleaned != 0 {
		t.Error("initial total cleaned should be 0")
	}
}

func TestCleanupManager_UpdateCleanupRule(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.MinRetentionTime = 1 * time.Minute
	manager := NewCleanupManager(registry, config)

	// 测试有效的规则更新
	newRule := &CleanupRule{
		ResourceType: "workload",
		TTL:          5 * time.Minute,
		Enabled:      true,
		GracePeriod:  1 * time.Minute,
		MaxBatchSize: 50,
	}

	err := manager.UpdateCleanupRule("workload", newRule)
	if err != nil {
		t.Fatalf("failed to update cleanup rule: %v", err)
	}

	// 验证规则已更新
	rules := manager.GetCleanupRules()
	updatedRule := rules["workload"]
	if updatedRule.TTL != 5*time.Minute {
		t.Errorf("expected TTL 5m, got %v", updatedRule.TTL)
	}

	if updatedRule.MaxBatchSize != 50 {
		t.Errorf("expected batch size 50, got %d", updatedRule.MaxBatchSize)
	}
}

func TestCleanupManager_UpdateCleanupRule_ValidationError(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.MinRetentionTime = 5 * time.Minute
	manager := NewCleanupManager(registry, config)

	// 测试 TTL 小于最小保留时间的情况
	invalidRule := &CleanupRule{
		ResourceType: "workload",
		TTL:          1 * time.Minute, // 小于 MinRetentionTime
		Enabled:      true,
	}

	err := manager.UpdateCleanupRule("workload", invalidRule)
	if err == nil {
		t.Error("expected error for TTL less than minimum retention time")
	}

	// 测试 nil 规则
	err = manager.UpdateCleanupRule("workload", nil)
	if err == nil {
		t.Error("expected error for nil cleanup rule")
	}
}

func TestCleanupManager_AddEventListener(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewCleanupManager(registry, nil)
	listener := &TestCleanupEventListener{}

	manager.AddEventListener(listener)

	// 验证监听器已添加（通过触发事件来验证）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start cleanup manager: %v", err)
	}

	// 等待启动事件
	time.Sleep(10 * time.Millisecond)

	if len(listener.events) == 0 {
		t.Error("expected at least one event after starting manager")
	}

	if listener.events[0].Type != CleanupEventStarted {
		t.Errorf("expected first event to be started, got %s", listener.events[0].Type)
	}

	manager.Stop()
}

func TestCleanupManager_ManualCleanup(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.WorkloadTTL = 100 * time.Millisecond // 很短的 TTL 用于测试
	config.GracePeriod = 10 * time.Millisecond
	manager := NewCleanupManager(registry, config)

	listener := &TestCleanupEventListener{}
	manager.AddEventListener(listener)

	// 创建一个过期的工作负载
	expiredWorkload := &types.Workload{
		Metadata: types.WorkloadMetadata{
			ID:        "expired-workload",
			Labels:    map[string]string{"app": "test"},
			CreatedAt: time.Now().Add(-1 * time.Hour),
			TTL:       100 * time.Millisecond, // 明确设置很短的TTL
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 8080,
			Protocol:  "tcp",
		},
		Status: types.WorkloadStatus{
			Ready:      false, // 不健康
			LastSeen:   time.Now().Add(-1 * time.Hour), // 很久以前
			RegisterIP: "127.0.0.1",
		},
	}

	err := registry.RegisterWorkload(expiredWorkload)
	if err != nil {
		t.Fatalf("failed to register workload: %v", err)
	}

	// 等待确保工作负载过期
	time.Sleep(200 * time.Millisecond)

	// 执行手动清理
	result, err := manager.ManualCleanup()
	if err != nil {
		t.Fatalf("manual cleanup failed: %v", err)
	}

	if result == nil {
		t.Fatal("cleanup result should not be nil")
	}

	if result.TotalCleaned == 0 {
		t.Error("expected at least one resource to be cleaned")
	}

	if result.Duration <= 0 {
		t.Error("cleanup duration should be positive")
	}

	// 检查清理事件
	var cleanupStarted, cleanupCompleted bool
	for _, event := range listener.events {
		switch event.Type {
		case CleanupEventStarted:
			cleanupStarted = true
		case CleanupEventCompleted:
			cleanupCompleted = true
		}
	}

	if !cleanupStarted {
		t.Error("expected cleanup started event")
	}

	if !cleanupCompleted {
		t.Error("expected cleanup completed event")
	}
}

func TestCleanupManager_ProtectedNamespaces(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.ProtectedNamespaces = []string{"protected-ns"}
	manager := NewCleanupManager(registry, config)

	// 测试受保护命名空间的检查
	if !manager.isProtectedNamespace("protected-ns") {
		t.Error("protected-ns should be protected")
	}

	if manager.isProtectedNamespace("regular-ns") {
		t.Error("regular-ns should not be protected")
	}
}

func TestCleanupManager_ShouldCleanupWorkload(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.WorkloadTTL = 1 * time.Hour
	config.GracePeriod = 5 * time.Minute
	manager := NewCleanupManager(registry, config)

	rule := manager.cleanupRules["workload"]
	now := time.Now()

	tests := []struct {
		name     string
		workload *types.Workload
		expected bool
	}{
		{
			name: "recent workload should not be cleaned",
			workload: &types.Workload{
				Status: types.WorkloadStatus{
					Ready:    true,
					LastSeen: now.Add(-30 * time.Minute), // 30分钟前
				},
			},
			expected: false,
		},
		{
			name: "old unhealthy workload should be cleaned",
			workload: &types.Workload{
				Status: types.WorkloadStatus{
					Ready:    false, // 不健康
					LastSeen: now.Add(-2 * time.Hour), // 2小时前
				},
			},
			expected: true,
		},
		{
			name: "old but healthy workload should be cleaned after TTL",
			workload: &types.Workload{
				Status: types.WorkloadStatus{
					Ready:    true, // 健康但过期
					LastSeen: now.Add(-2 * time.Hour), // 2小时前
				},
			},
			expected: true,
		},
		{
			name: "workload in grace period should not be cleaned",
			workload: &types.Workload{
				Metadata: types.WorkloadMetadata{
					CreatedAt: now.Add(-1*time.Hour - 2*time.Minute), // 创建时间超过TTL
					TTL:       1 * time.Hour,                         // 1小时TTL
				},
				Status: types.WorkloadStatus{
					Ready:    false,
					LastSeen: now.Add(-1*time.Hour - 2*time.Minute), // 最后心跳也超过1小时
				},
			},
			expected: false, // 虽然过期，但在宽限期内，不应该清理
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.shouldCleanupWorkload(tt.workload, rule, now)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCleanupManager_StartStop(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.CleanupInterval = 50 * time.Millisecond // 快速清理间隔用于测试
	config.EnableAutoCleanup = true
	manager := NewCleanupManager(registry, config)

	ctx, cancel := context.WithCancel(context.Background())

	// 启动管理器
	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start cleanup manager: %v", err)
	}

	// 验证正在运行
	if !manager.running {
		t.Error("manager should be running after start")
	}

	// 等待一段时间让清理循环运行
	time.Sleep(100 * time.Millisecond)

	// 停止管理器
	cancel()
	manager.Stop()

	// 验证已停止
	if manager.running {
		t.Error("manager should not be running after stop")
	}
}

func TestCleanupManager_DisabledAutoCleanup(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultCleanupConfig()
	config.EnableAutoCleanup = false
	manager := NewCleanupManager(registry, config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start cleanup manager: %v", err)
	}

	// 即使启动了，自动清理应该被禁用
	// 这里我们主要验证不会出错，实际的禁用逻辑在 Start 方法中
	time.Sleep(10 * time.Millisecond)

	manager.Stop()
}