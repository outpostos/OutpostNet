package registry

import (
	"testing"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestK8sMemoryRegistry_WorkloadServiceMapping(t *testing.T) {
	registry := NewK8sMemoryRegistry()

	// 1. 注册服务定义 (管理员配置)
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "web-service",
			Namespace: "default",
			Labels: map[string]string{
				"app": "web",
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app":     "web",
				"version": "v1",
			},
			Ports: []types.ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: 8080,
					Protocol:   "tcp",
				},
			},
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 2. 注册工作负载实例 (不指定服务名，只有标签)
	workloads := []*types.Workload{
		{
			Metadata: types.WorkloadMetadata{
				Labels: map[string]string{
					"app":     "web",
					"version": "v1",
					"node":    "edge-node-1",
				},
			},
			Spec: types.WorkloadSpec{
				LocalAddr: "127.0.0.1",
				LocalPort: 8080,
				Protocol:  "tcp",
			},
			Status: types.WorkloadStatus{
				RegisterIP: "10.1.1.1",
			},
		},
		{
			Metadata: types.WorkloadMetadata{
				Labels: map[string]string{
					"app":     "web",
					"version": "v1",
					"node":    "edge-node-2",
				},
			},
			Spec: types.WorkloadSpec{
				LocalAddr: "127.0.0.1",
				LocalPort: 8080,
				Protocol:  "tcp",
			},
			Status: types.WorkloadStatus{
				RegisterIP: "10.1.1.2",
			},
		},
		{
			Metadata: types.WorkloadMetadata{
				Labels: map[string]string{
					"app":     "web",
					"version": "v2", // 不匹配服务选择器
					"node":    "edge-node-3",
				},
			},
			Spec: types.WorkloadSpec{
				LocalAddr: "127.0.0.1",
				LocalPort: 8080,
				Protocol:  "tcp",
			},
			Status: types.WorkloadStatus{
				RegisterIP: "10.1.1.3",
			},
		},
	}

	for _, workload := range workloads {
		err = registry.RegisterWorkload(workload)
		if err != nil {
			t.Fatalf("failed to register workload: %v", err)
		}
	}

	// 3. 验证端点自动发现 - 应该只有匹配选择器的工作负载被包含
	endpoints, err := registry.GetEndpoints("web-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	// 只有前两个工作负载应该匹配 (app=web AND version=v1)
	if len(endpoints) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(endpoints))
	}

	// 验证端点信息
	expectedIPs := map[string]bool{"10.1.1.1": false, "10.1.1.2": false}
	for _, ep := range endpoints {
		if _, exists := expectedIPs[ep.IP]; exists {
			expectedIPs[ep.IP] = true
		} else {
			t.Errorf("unexpected endpoint IP: %s", ep.IP)
		}

		// 验证端点标签包含工作负载标签
		if ep.Labels["app"] != "web" || ep.Labels["version"] != "v1" {
			t.Errorf("endpoint labels incorrect: %v", ep.Labels)
		}
	}

	// 确保所有预期的 IP 都被找到
	for ip, found := range expectedIPs {
		if !found {
			t.Errorf("expected endpoint IP %s not found", ip)
		}
	}
}

func TestK8sMemoryRegistry_DynamicEndpointUpdates(t *testing.T) {
	registry := NewK8sMemoryRegistry()

	// 注册服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "api-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "api",
			},
			Ports: []types.ServicePort{
				{
					Name:       "api",
					Port:       3000,
					TargetPort: 3000,
					Protocol:   "tcp",
				},
			},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 初始状态：没有端点
	endpoints, err := registry.GetEndpoints("api-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}
	if len(endpoints) != 0 {
		t.Fatalf("expected 0 endpoints initially, got %d", len(endpoints))
	}

	// 添加第一个工作负载
	workload1 := &types.Workload{
		Metadata: types.WorkloadMetadata{
			Labels: map[string]string{
				"app": "api",
			},
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 3000,
		},
		Status: types.WorkloadStatus{
			RegisterIP: "10.1.1.1",
		},
	}

	err = registry.RegisterWorkload(workload1)
	if err != nil {
		t.Fatalf("failed to register workload1: %v", err)
	}

	// 验证端点被自动创建
	endpoints, err = registry.GetEndpoints("api-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint after adding workload1, got %d", len(endpoints))
	}

	// 添加第二个工作负载
	workload2 := &types.Workload{
		Metadata: types.WorkloadMetadata{
			Labels: map[string]string{
				"app": "api",
			},
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 3000,
		},
		Status: types.WorkloadStatus{
			RegisterIP: "10.1.1.2",
		},
	}

	err = registry.RegisterWorkload(workload2)
	if err != nil {
		t.Fatalf("failed to register workload2: %v", err)
	}

	// 验证端点数量增加
	endpoints, err = registry.GetEndpoints("api-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("expected 2 endpoints after adding workload2, got %d", len(endpoints))
	}

	// 删除第一个工作负载
	err = registry.UnregisterWorkload(workload1.Metadata.ID)
	if err != nil {
		t.Fatalf("failed to unregister workload1: %v", err)
	}

	// 验证端点数量减少
	endpoints, err = registry.GetEndpoints("api-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint after removing workload1, got %d", len(endpoints))
	}

	// 验证剩余的端点是 workload2
	if endpoints[0].IP != "10.1.1.2" {
		t.Errorf("expected remaining endpoint IP 10.1.1.2, got %s", endpoints[0].IP)
	}
}

func TestK8sMemoryRegistry_LabelSelectors(t *testing.T) {
	registry := NewK8sMemoryRegistry()

	// 注册服务，只选择 zone=east 的工作负载
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "zone-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app":  "web",
				"zone": "east",
			},
			Ports: []types.ServicePort{
				{
					Name:     "http",
					Port:     80,
					Protocol: "tcp",
				},
			},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 注册不同区域的工作负载
	workloads := []*types.Workload{
		{
			Metadata: types.WorkloadMetadata{
				Labels: map[string]string{
					"app":  "web",
					"zone": "east", // 匹配
				},
			},
			Spec: types.WorkloadSpec{LocalPort: 8080},
			Status: types.WorkloadStatus{RegisterIP: "10.1.1.1"},
		},
		{
			Metadata: types.WorkloadMetadata{
				Labels: map[string]string{
					"app":  "web",
					"zone": "west", // 不匹配
				},
			},
			Spec: types.WorkloadSpec{LocalPort: 8080},
			Status: types.WorkloadStatus{RegisterIP: "10.1.1.2"},
		},
		{
			Metadata: types.WorkloadMetadata{
				Labels: map[string]string{
					"app":  "web",
					"zone": "east", // 匹配
				},
			},
			Spec: types.WorkloadSpec{LocalPort: 8080},
			Status: types.WorkloadStatus{RegisterIP: "10.1.1.3"},
		},
	}

	for _, workload := range workloads {
		err = registry.RegisterWorkload(workload)
		if err != nil {
			t.Fatalf("failed to register workload: %v", err)
		}
	}

	// 验证只有 zone=east 的工作负载被选中
	endpoints, err := registry.GetEndpoints("zone-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	if len(endpoints) != 2 {
		t.Fatalf("expected 2 endpoints (zone=east), got %d", len(endpoints))
	}

	// 验证选中的端点都是 zone=east
	for _, ep := range endpoints {
		if ep.Labels["zone"] != "east" {
			t.Errorf("expected zone=east, got zone=%s", ep.Labels["zone"])
		}
	}
}

func TestK8sMemoryRegistry_WorkloadManagement(t *testing.T) {
	registry := NewK8sMemoryRegistry()

	// 注册工作负载
	workload := &types.Workload{
		Metadata: types.WorkloadMetadata{
			Labels: map[string]string{
				"app": "test",
			},
		},
		Spec: types.WorkloadSpec{
			LocalAddr: "127.0.0.1",
			LocalPort: 8080,
		},
	}

	err := registry.RegisterWorkload(workload)
	if err != nil {
		t.Fatalf("failed to register workload: %v", err)
	}

	// 验证工作负载被分配了 ID
	if workload.Metadata.ID == "" {
		t.Error("workload ID should be generated")
	}

	// 获取工作负载
	retrieved, err := registry.GetWorkload(workload.Metadata.ID)
	if err != nil {
		t.Fatalf("failed to get workload: %v", err)
	}

	if retrieved.Metadata.ID != workload.Metadata.ID {
		t.Errorf("expected workload ID %s, got %s", workload.Metadata.ID, retrieved.Metadata.ID)
	}

	// 列出工作负载
	workloads, err := registry.ListWorkloads(map[string]string{"app": "test"})
	if err != nil {
		t.Fatalf("failed to list workloads: %v", err)
	}

	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}

	// 注销工作负载
	err = registry.UnregisterWorkload(workload.Metadata.ID)
	if err != nil {
		t.Fatalf("failed to unregister workload: %v", err)
	}

	// 验证工作负载已被删除
	_, err = registry.GetWorkload(workload.Metadata.ID)
	if err == nil {
		t.Error("expected error when getting deleted workload")
	}
}