package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestNewServer 测试服务器创建
func TestNewServer(t *testing.T) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:0", // 使用随机端口
			AuthToken:  "test-token",
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      "test-service",
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
				},
			},
		},
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server == nil {
		t.Fatal("Server is nil")
	}

	// 验证服务注册
	registry := server.GetRegistry()
	if registry == nil {
		t.Fatal("Registry is nil")
	}

	service, err := registry.GetService("test-service", "default")
	if err != nil {
		t.Fatalf("Failed to get service: %v", err)
	}

	if service.Metadata.Name != "test-service" {
		t.Errorf("Expected service name 'test-service', got '%s'", service.Metadata.Name)
	}
}

// TestServerStartStop 测试服务器启动和停止
func TestServerStartStop(t *testing.T) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:0",
			AuthToken:  "test-token",
		},
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// 启动服务器
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Run(ctx)
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	// 停止服务器
	server.Stop()

	// 等待服务器完全停止
	select {
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			t.Fatalf("Server run failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Server did not stop within timeout")
	}
}

// TestServerConnectionHandling 测试服务器连接处理
func TestServerConnectionHandling(t *testing.T) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:18080",
			AuthToken:  "test-token",
		},
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// 启动服务器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := server.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("Server run error: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(500 * time.Millisecond)

	// 尝试连接到服务器
	conn, err := net.DialTimeout("tcp", "127.0.0.1:18080", 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// 发送测试数据
	testData := "test message"
	_, err = conn.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Failed to write to server: %v", err)
	}

	// 停止服务器
	cancel()
}

// TestServerServiceRegistration 测试服务注册功能
func TestServerServiceRegistration(t *testing.T) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:0",
			AuthToken:  "test-token",
		},
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	registry := server.GetRegistry()

	// 测试服务注册
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "dynamic-service",
			Namespace: "test",
			Labels: map[string]string{
				"app": "test-app",
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "test-app",
			},
			Ports: []types.ServicePort{
				{
					Name:       "http",
					Port:       8080,
					TargetPort: 8080,
					Protocol:   "tcp",
				},
			},
		},
	}

	err = registry.RegisterService(service)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}

	// 验证服务注册
	registeredService, err := registry.GetService("dynamic-service", "test")
	if err != nil {
		t.Fatalf("Failed to get registered service: %v", err)
	}

	if registeredService.Metadata.Name != "dynamic-service" {
		t.Errorf("Expected service name 'dynamic-service', got '%s'", registeredService.Metadata.Name)
	}

	if registeredService.Metadata.Labels["app"] != "test-app" {
		t.Errorf("Expected label app=test-app, got %s", registeredService.Metadata.Labels["app"])
	}
}

// TestServerEndpointManagement 测试端点管理
func TestServerEndpointManagement(t *testing.T) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:0",
			AuthToken:  "test-token",
		},
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	registry := server.GetRegistry()

	// 先注册一个服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "endpoint-test-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "endpoint-test",
			},
			Ports: []types.ServicePort{
				{
					Name:       "http",
					Port:       8080,
					TargetPort: 8080,
					Protocol:   "tcp",
				},
			},
		},
	}

	err = registry.RegisterService(service)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}

	// 注册端点
	endpoint := &types.Endpoint{
		ID:       "test-endpoint-1",
		IP:       "10.1.1.1",
		Port:     8080,
		Protocol: "tcp",
		Labels: map[string]string{
			"app": "endpoint-test",
		},
		Status: types.EndpointStatus{
			Ready: true,
		},
	}

	err = registry.RegisterEndpoint("endpoint-test-service", "default", endpoint)
	if err != nil {
		t.Fatalf("Failed to register endpoint: %v", err)
	}

	// 获取端点
	endpoints, err := registry.GetEndpoints("endpoint-test-service", "default", map[string]string{
		"app": "endpoint-test",
	})
	if err != nil {
		t.Fatalf("Failed to get endpoints: %v", err)
	}

	if len(endpoints) != 1 {
		t.Fatalf("Expected 1 endpoint, got %d", len(endpoints))
	}

	if endpoints[0].ID != "test-endpoint-1" {
		t.Errorf("Expected endpoint ID 'test-endpoint-1', got '%s'", endpoints[0].ID)
	}

	if endpoints[0].IP != "10.1.1.1" {
		t.Errorf("Expected endpoint IP '10.1.1.1', got '%s'", endpoints[0].IP)
	}
}