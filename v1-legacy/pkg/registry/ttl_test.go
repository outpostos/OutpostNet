package registry

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestTTLManager_NewTTLManager(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewTTLManager(registry, nil)

	if manager == nil {
		t.Fatal("TTL manager should not be nil")
	}

	if manager.registry != registry {
		t.Error("registry should be set correctly")
	}

	// 检查默认 TTL
	serviceTTL := manager.GetDefaultTTL("service")
	if serviceTTL != 24*time.Hour {
		t.Errorf("expected service TTL 24h, got %v", serviceTTL)
	}

	workloadTTL := manager.GetDefaultTTL("workload")
	if workloadTTL != 2*time.Hour {
		t.Errorf("expected workload TTL 2h, got %v", workloadTTL)
	}
}

func TestTTLManager_SetDefaultTTL(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewTTLManager(registry, nil)

	// 设置新的 TTL
	newTTL := 4 * time.Hour
	err := manager.SetDefaultTTL("service", newTTL)
	if err != nil {
		t.Fatalf("failed to set default TTL: %v", err)
	}

	// 验证已更新
	currentTTL := manager.GetDefaultTTL("service")
	if currentTTL != newTTL {
		t.Errorf("expected TTL %v, got %v", newTTL, currentTTL)
	}

	// 测试无效 TTL
	err = manager.SetDefaultTTL("service", 0)
	if err == nil {
		t.Error("expected error for zero TTL")
	}

	err = manager.SetDefaultTTL("service", -1*time.Hour)
	if err == nil {
		t.Error("expected error for negative TTL")
	}
}

func TestTTLManager_SetHeartbeatTTL(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewTTLManager(registry, nil)

	// 设置新的心跳 TTL
	newTTL := 10 * time.Minute
	err := manager.SetHeartbeatTTL("workload", newTTL)
	if err != nil {
		t.Fatalf("failed to set heartbeat TTL: %v", err)
	}

	// 验证已更新
	currentTTL := manager.GetHeartbeatTTL("workload")
	if currentTTL != newTTL {
		t.Errorf("expected heartbeat TTL %v, got %v", newTTL, currentTTL)
	}
}

func TestTTLManager_IsServiceExpired(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultTTLConfig()
	config.ServiceTTL = 1 * time.Hour
	config.ServiceHeartbeatTTL = 30 * time.Minute
	manager := NewTTLManager(registry, config)

	now := time.Now()

	tests := []struct {
		name     string
		service  *types.Service
		expected bool
	}{
		{
			name: "recent service should not be expired",
			service: &types.Service{
				Metadata: types.ServiceMetadata{
					CreatedAt: now.Add(-30 * time.Minute),
					TTL:       1 * time.Hour,
					LastSeen:  now.Add(-5 * time.Minute),
				},
			},
			expected: false,
		},
		{
			name: "old service should be expired by TTL",
			service: &types.Service{
				Metadata: types.ServiceMetadata{
					CreatedAt: now.Add(-2 * time.Hour),
					TTL:       1 * time.Hour,
					LastSeen:  now.Add(-5 * time.Minute),
				},
			},
			expected: true,
		},
		{
			name: "service should be expired by heartbeat timeout",
			service: &types.Service{
				Metadata: types.ServiceMetadata{
					CreatedAt: now.Add(-30 * time.Minute),
					TTL:       2 * time.Hour,
					LastSeen:  now.Add(-45 * time.Minute), // 超过心跳 TTL
				},
			},
			expected: true,
		},
		{
			name: "service without TTL and recent heartbeat should not be expired",
			service: &types.Service{
				Metadata: types.ServiceMetadata{
					LastSeen: now.Add(-10 * time.Minute),
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.IsServiceExpired(tt.service, now)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTTLManager_IsWorkloadExpired(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultTTLConfig()
	config.WorkloadTTL = 2 * time.Hour
	config.WorkloadHeartbeatTTL = 30 * time.Minute
	manager := NewTTLManager(registry, config)

	now := time.Now()

	tests := []struct {
		name     string
		workload *types.Workload
		expected bool
	}{
		{
			name: "recent workload should not be expired",
			workload: &types.Workload{
				Metadata: types.WorkloadMetadata{
					CreatedAt: now.Add(-1 * time.Hour),
					TTL:       2 * time.Hour,
					LastSeen:  now.Add(-5 * time.Minute),
				},
				Status: types.WorkloadStatus{
					LastSeen: now.Add(-5 * time.Minute),
				},
			},
			expected: false,
		},
		{
			name: "old workload should be expired by TTL",
			workload: &types.Workload{
				Metadata: types.WorkloadMetadata{
					CreatedAt: now.Add(-3 * time.Hour),
					TTL:       2 * time.Hour,
					LastSeen:  now.Add(-5 * time.Minute),
				},
				Status: types.WorkloadStatus{
					LastSeen: now.Add(-5 * time.Minute),
				},
			},
			expected: true,
		},
		{
			name: "workload should be expired by heartbeat timeout",
			workload: &types.Workload{
				Metadata: types.WorkloadMetadata{
					CreatedAt: now.Add(-1 * time.Hour),
					TTL:       4 * time.Hour,
					LastSeen:  now.Add(-45 * time.Minute), // 超过心跳 TTL
				},
				Status: types.WorkloadStatus{
					LastSeen: now.Add(-45 * time.Minute),
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.IsWorkloadExpired(tt.workload, now)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTTLManager_IsEndpointExpired(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultTTLConfig()
	config.EndpointHeartbeatTTL = 5 * time.Minute
	manager := NewTTLManager(registry, config)

	now := time.Now()

	tests := []struct {
		name     string
		endpoint *types.Endpoint
		expected bool
	}{
		{
			name: "recent endpoint should not be expired",
			endpoint: &types.Endpoint{
				ID:       "ep1",
				LastSeen: now.Add(-2 * time.Minute),
			},
			expected: false,
		},
		{
			name: "old endpoint should be expired",
			endpoint: &types.Endpoint{
				ID:       "ep2",
				LastSeen: now.Add(-10 * time.Minute), // 超过 5 分钟
			},
			expected: true,
		},
		{
			name: "endpoint without LastSeen should not be expired",
			endpoint: &types.Endpoint{
				ID: "ep3",
				// LastSeen 为零值
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.IsEndpointExpired(tt.endpoint, now)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTTLManager_UpdateLastSeen(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewTTLManager(registry, nil)

	// 注册一个服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
			CreatedAt: time.Now(),
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{"app": "test"},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 注册一个工作负载
	workload := &types.Workload{
		Metadata: types.WorkloadMetadata{
			ID:        "test-workload",
			Labels:    map[string]string{"app": "test"},
			CreatedAt: time.Now(),
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 8080,
		},
		Status: types.WorkloadStatus{
			Ready:      true,
			LastSeen:   time.Now(),
			RegisterIP: "127.0.0.1",
		},
	}

	err = registry.RegisterWorkload(workload)
	if err != nil {
		t.Fatalf("failed to register workload: %v", err)
	}

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	// 更新服务最后可见时间
	err = manager.UpdateServiceLastSeen("test-service", "default")
	if err != nil {
		t.Fatalf("failed to update service last seen: %v", err)
	}

	// 更新工作负载最后可见时间
	err = manager.UpdateWorkloadLastSeen("test-workload")
	if err != nil {
		t.Fatalf("failed to update workload last seen: %v", err)
	}

	// 验证时间已更新
	updatedService, err := registry.GetService("test-service", "default")
	if err != nil {
		t.Fatalf("failed to get service: %v", err)
	}

	if updatedService.Metadata.LastSeen.Before(service.Metadata.LastSeen) {
		t.Error("service last seen time should be updated")
	}

	updatedWorkload, err := registry.GetWorkload("test-workload")
	if err != nil {
		t.Fatalf("failed to get workload: %v", err)
	}

	if updatedWorkload.Status.LastSeen.Before(workload.Status.LastSeen) {
		t.Error("workload last seen time should be updated")
	}
}

func TestTTLManager_GetExpiredResources(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	config := DefaultTTLConfig()
	config.ServiceTTL = 100 * time.Millisecond
	config.WorkloadTTL = 100 * time.Millisecond
	config.ServiceHeartbeatTTL = 50 * time.Millisecond
	config.WorkloadHeartbeatTTL = 50 * time.Millisecond
	manager := NewTTLManager(registry, config)

	// 注册一个服务（立即过期）
	expiredService := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "expired-service",
			Namespace: "default",
			CreatedAt: time.Now().Add(-200 * time.Millisecond),
			TTL:       100 * time.Millisecond,
			LastSeen:  time.Now().Add(-200 * time.Millisecond),
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{"app": "expired"},
		},
	}

	err := registry.RegisterService(expiredService)
	if err != nil {
		t.Fatalf("failed to register expired service: %v", err)
	}

	// 注册一个工作负载（立即过期）
	expiredWorkload := &types.Workload{
		Metadata: types.WorkloadMetadata{
			ID:        "expired-workload",
			Labels:    map[string]string{"app": "expired"},
			CreatedAt: time.Now().Add(-200 * time.Millisecond),
			TTL:       100 * time.Millisecond,
			LastSeen:  time.Now().Add(-200 * time.Millisecond),
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 8080,
		},
		Status: types.WorkloadStatus{
			Ready:      true,
			LastSeen:   time.Now().Add(-200 * time.Millisecond),
			RegisterIP: "127.0.0.1",
		},
	}

	err = registry.RegisterWorkload(expiredWorkload)
	if err != nil {
		t.Fatalf("failed to register expired workload: %v", err)
	}

	// 等待确保过期
	time.Sleep(150 * time.Millisecond)

	// 获取过期的服务
	expiredServices, err := manager.GetExpiredServices()
	if err != nil {
		t.Fatalf("failed to get expired services: %v", err)
	}

	if len(expiredServices) == 0 {
		t.Error("expected at least one expired service")
	}

	// 获取过期的工作负载
	expiredWorkloads, err := manager.GetExpiredWorkloads()
	if err != nil {
		t.Fatalf("failed to get expired workloads: %v", err)
	}

	if len(expiredWorkloads) == 0 {
		t.Error("expected at least one expired workload")
	}
}

func TestTTLManager_GetTTLStats(t *testing.T) {
	registry := NewK8sMemoryRegistry()
	manager := NewTTLManager(registry, nil)

	// 注册一些资源
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
			CreatedAt: time.Now(),
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{"app": "test"},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	workload := &types.Workload{
		Metadata: types.WorkloadMetadata{
			ID:        "test-workload",
			Labels:    map[string]string{"app": "test"},
			CreatedAt: time.Now(),
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 8080,
		},
		Status: types.WorkloadStatus{
			Ready:      true,
			LastSeen:   time.Now(),
			RegisterIP: "127.0.0.1",
		},
	}

	err = registry.RegisterWorkload(workload)
	if err != nil {
		t.Fatalf("failed to register workload: %v", err)
	}

	// 获取统计信息
	stats := manager.GetTTLStats()

	if stats["total_services"] != 1 {
		t.Errorf("expected 1 total service, got %v", stats["total_services"])
	}

	if stats["total_workloads"] != 1 {
		t.Errorf("expected 1 total workload, got %v", stats["total_workloads"])
	}

	if stats["expired_services"] == nil {
		t.Error("expired_services should be present in stats")
	}

	if stats["expired_workloads"] == nil {
		t.Error("expired_workloads should be present in stats")
	}
}