package registry

import (
	"testing"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestMemoryRegistry_RegisterService(t *testing.T) {
	registry := NewMemoryRegistry()

	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
			Labels: map[string]string{
				"app": "test",
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "test",
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

	// 获取服务验证注册成功
	retrievedService, err := registry.GetService("test-service", "default")
	if err != nil {
		t.Fatalf("failed to get service: %v", err)
	}

	if retrievedService.Metadata.Name != service.Metadata.Name {
		t.Errorf("expected service name %s, got %s", service.Metadata.Name, retrievedService.Metadata.Name)
	}
}

func TestMemoryRegistry_RegisterEndpoint(t *testing.T) {
	registry := NewMemoryRegistry()

	// 先注册服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "test",
			},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 注册端点
	endpoint := &types.Endpoint{
		ID:       "endpoint-1",
		IP:       "192.168.1.100",
		Port:     8080,
		Protocol: "tcp",
		Labels: map[string]string{
			"app": "test",
		},
		Status: types.EndpointStatus{
			Ready: true,
		},
	}

	err = registry.RegisterEndpoint("test-service", "default", endpoint)
	if err != nil {
		t.Fatalf("failed to register endpoint: %v", err)
	}

	// 获取端点验证注册成功
	endpoints, err := registry.GetEndpoints("test-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}

	if endpoints[0].ID != endpoint.ID {
		t.Errorf("expected endpoint ID %s, got %s", endpoint.ID, endpoints[0].ID)
	}
}

func TestMemoryRegistry_GetEndpointsWithSelector(t *testing.T) {
	registry := NewMemoryRegistry()

	// 注册服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "test",
			},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 注册多个端点
	endpoints := []*types.Endpoint{
		{
			ID:       "endpoint-1",
			IP:       "192.168.1.100",
			Port:     8080,
			Protocol: "tcp",
			Labels: map[string]string{
				"app":     "test",
				"version": "v1",
			},
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:       "endpoint-2",
			IP:       "192.168.1.101",
			Port:     8080,
			Protocol: "tcp",
			Labels: map[string]string{
				"app":     "test",
				"version": "v2",
			},
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:       "endpoint-3",
			IP:       "192.168.1.102",
			Port:     8080,
			Protocol: "tcp",
			Labels: map[string]string{
				"app":     "other",
				"version": "v1",
			},
			Status: types.EndpointStatus{Ready: true},
		},
	}

	for _, ep := range endpoints {
		err = registry.RegisterEndpoint("test-service", "default", ep)
		if err != nil {
			t.Fatalf("failed to register endpoint %s: %v", ep.ID, err)
		}
	}

	// 测试无选择器 - 应该返回所有就绪的端点
	allEndpoints, err := registry.GetEndpoints("test-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get all endpoints: %v", err)
	}

	if len(allEndpoints) != 3 {
		t.Fatalf("expected 3 endpoints, got %d", len(allEndpoints))
	}

	// 测试选择器 - 只匹配 version=v1 的端点
	v1Endpoints, err := registry.GetEndpoints("test-service", "default", map[string]string{
		"version": "v1",
	})
	if err != nil {
		t.Fatalf("failed to get v1 endpoints: %v", err)
	}

	if len(v1Endpoints) != 2 {
		t.Fatalf("expected 2 v1 endpoints, got %d", len(v1Endpoints))
	}

	// 测试多重选择器
	specificEndpoints, err := registry.GetEndpoints("test-service", "default", map[string]string{
		"app":     "test",
		"version": "v1",
	})
	if err != nil {
		t.Fatalf("failed to get specific endpoints: %v", err)
	}

	if len(specificEndpoints) != 1 {
		t.Fatalf("expected 1 specific endpoint, got %d", len(specificEndpoints))
	}

	if specificEndpoints[0].ID != "endpoint-1" {
		t.Errorf("expected endpoint-1, got %s", specificEndpoints[0].ID)
	}
}

func TestMemoryRegistry_UnregisterEndpoint(t *testing.T) {
	registry := NewMemoryRegistry()

	// 注册服务和端点
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	endpoint := &types.Endpoint{
		ID:       "endpoint-1",
		IP:       "192.168.1.100",
		Port:     8080,
		Protocol: "tcp",
		Status:   types.EndpointStatus{Ready: true},
	}

	err = registry.RegisterEndpoint("test-service", "default", endpoint)
	if err != nil {
		t.Fatalf("failed to register endpoint: %v", err)
	}

	// 验证端点存在
	endpoints, err := registry.GetEndpoints("test-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}

	// 删除端点
	err = registry.UnregisterEndpoint("test-service", "default", "endpoint-1")
	if err != nil {
		t.Fatalf("failed to unregister endpoint: %v", err)
	}

	// 验证端点已删除
	endpoints, err = registry.GetEndpoints("test-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints after unregister: %v", err)
	}

	if len(endpoints) != 0 {
		t.Fatalf("expected 0 endpoints after unregister, got %d", len(endpoints))
	}
}

func TestMemoryRegistry_ListServices(t *testing.T) {
	registry := NewMemoryRegistry()

	services := []*types.Service{
		{
			Metadata: types.ServiceMetadata{
				Name:      "service-1",
				Namespace: "default",
			},
		},
		{
			Metadata: types.ServiceMetadata{
				Name:      "service-2",
				Namespace: "default",
			},
		},
		{
			Metadata: types.ServiceMetadata{
				Name:      "service-3",
				Namespace: "kube-system",
			},
		},
	}

	// 注册服务
	for _, service := range services {
		err := registry.RegisterService(service)
		if err != nil {
			t.Fatalf("failed to register service %s: %v", service.Metadata.Name, err)
		}
	}

	// 列出所有服务
	allServices, err := registry.ListServices("")
	if err != nil {
		t.Fatalf("failed to list all services: %v", err)
	}

	if len(allServices) != 3 {
		t.Fatalf("expected 3 services, got %d", len(allServices))
	}

	// 列出特定命名空间的服务
	defaultServices, err := registry.ListServices("default")
	if err != nil {
		t.Fatalf("failed to list default services: %v", err)
	}

	if len(defaultServices) != 2 {
		t.Fatalf("expected 2 default services, got %d", len(defaultServices))
	}
}

func TestMemoryRegistry_ErrorCases(t *testing.T) {
	registry := NewMemoryRegistry()

	// 测试注册 nil 服务
	err := registry.RegisterService(nil)
	if err == nil {
		t.Error("expected error for nil service")
	}

	// 测试注册没有名称的服务
	err = registry.RegisterService(&types.Service{
		Metadata: types.ServiceMetadata{
			Namespace: "default",
		},
	})
	if err == nil {
		t.Error("expected error for service without name")
	}

	// 测试获取不存在的服务
	_, err = registry.GetService("non-existent", "default")
	if err == nil {
		t.Error("expected error for non-existent service")
	}

	// 测试向不存在的服务注册端点
	err = registry.RegisterEndpoint("non-existent", "default", &types.Endpoint{
		ID:   "test",
		IP:   "192.168.1.1",
		Port: 8080,
	})
	if err == nil {
		t.Error("expected error for registering endpoint to non-existent service")
	}
}