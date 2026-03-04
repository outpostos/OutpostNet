package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/server"
	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestServerStartup(t *testing.T) {
	// 创建服务器配置
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:18080",
			AuthToken:  "test-token",
		},
		Services: []types.Service{
			{
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
			},
		},
	}

	// 创建服务器
	srv, err := server.NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// 启动服务器
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		if err := srv.Run(ctx); err != nil {
			t.Logf("server run error: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(1 * time.Second)

	// 测试连接到服务器
	conn, err := net.DialTimeout("tcp", "127.0.0.1:18080", 2*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	// 发送测试消息
	testMsg := "hello server"
	_, err = conn.Write([]byte(testMsg))
	if err != nil {
		t.Fatalf("failed to write to server: %v", err)
	}

	// 验证服务注册表中的服务
	registry := srv.GetRegistry()
	service, err := registry.GetService("test-service", "default")
	if err != nil {
		t.Fatalf("failed to get service from registry: %v", err)
	}

	if service.Metadata.Name != "test-service" {
		t.Errorf("expected service name 'test-service', got '%s'", service.Metadata.Name)
	}

	// 停止服务器
	cancel()
	time.Sleep(100 * time.Millisecond)
}