package main

import (
	"fmt"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/loadbalancer"
	"github.com/outpostos/OutpostNet/pkg/registry"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestPhase1Complete 验证第一阶段的所有功能是否完整
func TestPhase1Complete(t *testing.T) {
	t.Run("Configuration Parsing", testConfigurationParsing)
	t.Run("Service Registry", testServiceRegistry)
	t.Run("Load Balancer", testLoadBalancer)
	t.Run("Integration", testBasicIntegration)
}

func testConfigurationParsing(t *testing.T) {
	// 测试服务器配置验证
	serverConfig := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "0.0.0.0:8080",
			AuthToken:  "test-token",
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      "web-service",
					Namespace: "default",
				},
				Spec: types.ServiceSpec{
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
			},
		},
	}

	err := config.ValidateServerConfig(serverConfig)
	if err != nil {
		t.Fatalf("server config validation failed: %v", err)
	}

	// 测试客户端配置验证
	clientConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:8080",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "web-proxy",
				ServiceName:      "web-service",
				ServiceNamespace: "default",
				LocalPort:        8080,
				ProxyMode:        "tcp",
			},
		},
	}

	err = config.ValidateClientConfig(clientConfig)
	if err != nil {
		t.Fatalf("client config validation failed: %v", err)
	}

	t.Log("✓ Configuration parsing and validation works correctly")
}

func testServiceRegistry(t *testing.T) {
	registry := registry.NewMemoryRegistry()

	// 注册服务
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
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 注册端点
	endpoints := []*types.Endpoint{
		{
			ID:       "endpoint-1",
			IP:       "192.168.1.1",
			Port:     8080,
			Protocol: "tcp",
			Labels: map[string]string{
				"app": "test",
			},
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:       "endpoint-2",
			IP:       "192.168.1.2",
			Port:     8080,
			Protocol: "tcp",
			Labels: map[string]string{
				"app": "test",
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

	// 验证服务查询
	retrievedService, err := registry.GetService("test-service", "default")
	if err != nil {
		t.Fatalf("failed to get service: %v", err)
	}

	if retrievedService.Metadata.Name != "test-service" {
		t.Errorf("expected service name 'test-service', got '%s'", retrievedService.Metadata.Name)
	}

	// 验证端点查询
	retrievedEndpoints, err := registry.GetEndpoints("test-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	if len(retrievedEndpoints) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(retrievedEndpoints))
	}

	// 验证选择器查询
	filteredEndpoints, err := registry.GetEndpoints("test-service", "default", map[string]string{
		"app": "test",
	})
	if err != nil {
		t.Fatalf("failed to get filtered endpoints: %v", err)
	}

	if len(filteredEndpoints) != 2 {
		t.Fatalf("expected 2 filtered endpoints, got %d", len(filteredEndpoints))
	}

	t.Log("✓ Service registry and endpoint management works correctly")
}

func testLoadBalancer(t *testing.T) {
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

	// 测试所有负载均衡算法
	algorithms := []string{"round_robin", "random", "least_conn"}

	for _, algorithm := range algorithms {
		t.Run(algorithm, func(t *testing.T) {
			lb, err := loadbalancer.NewLoadBalancer(algorithm)
			if err != nil {
				t.Fatalf("failed to create load balancer %s: %v", algorithm, err)
			}

			// 测试选择功能
			for i := 0; i < 10; i++ {
				selected, err := lb.Select(endpoints)
				if err != nil {
					t.Fatalf("load balancer %s failed to select endpoint: %v", algorithm, err)
				}

				if selected == nil {
					t.Fatalf("load balancer %s returned nil endpoint", algorithm)
				}

				// 验证选择的端点是有效的
				found := false
				for _, ep := range endpoints {
					if ep.ID == selected.ID {
						found = true
						break
					}
				}

				if !found {
					t.Fatalf("load balancer %s selected invalid endpoint: %s", algorithm, selected.ID)
				}
			}
		})
	}

	t.Log("✓ Load balancer algorithms work correctly")
}

func testBasicIntegration(t *testing.T) {
	// 创建服务注册表
	serviceRegistry := registry.NewMemoryRegistry()

	// 注册服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "integration-service",
			Namespace: "default",
			Labels: map[string]string{
				"app": "integration",
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "integration",
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

	err := serviceRegistry.RegisterService(service)
	if err != nil {
		t.Fatalf("failed to register service: %v", err)
	}

	// 注册多个端点
	for i := 1; i <= 3; i++ {
		endpoint := &types.Endpoint{
			ID:       fmt.Sprintf("endpoint-%d", i),
			IP:       fmt.Sprintf("192.168.1.%d", i),
			Port:     8080,
			Protocol: "tcp",
			Labels: map[string]string{
				"app": "integration",
			},
			Status: types.EndpointStatus{Ready: true},
		}

		err = serviceRegistry.RegisterEndpoint("integration-service", "default", endpoint)
		if err != nil {
			t.Fatalf("failed to register endpoint %d: %v", i, err)
		}
	}

	// 创建负载均衡器
	lb, err := loadbalancer.NewLoadBalancer(service.Spec.LoadBalancer)
	if err != nil {
		t.Fatalf("failed to create load balancer: %v", err)
	}

	// 获取端点并进行负载均衡选择
	endpoints, err := serviceRegistry.GetEndpoints("integration-service", "default", service.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	if len(endpoints) != 3 {
		t.Fatalf("expected 3 endpoints, got %d", len(endpoints))
	}

	// 测试负载均衡选择
	selectedEndpoints := make(map[string]int)
	for i := 0; i < 10; i++ {
		selected, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("failed to select endpoint: %v", err)
		}
		selectedEndpoints[selected.ID]++
	}

	// 验证所有端点都被选择过（对于轮询算法）
	if len(selectedEndpoints) != 3 {
		t.Errorf("expected all 3 endpoints to be selected, only %d were selected", len(selectedEndpoints))
	}

	t.Log("✓ Integration test passed - all components work together")
}