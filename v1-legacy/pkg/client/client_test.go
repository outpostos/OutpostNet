package client

import (
	"context"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestNewClient 测试客户端创建
func TestNewClient(t *testing.T) {
	// 测试 register 模式客户端
	registerConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 8080,
					Protocol:  "tcp",
				},
			},
		},
	}

	client, err := NewClient(registerConfig)
	if err != nil {
		t.Fatalf("Failed to create register client: %v", err)
	}

	if client == nil {
		t.Fatal("Client is nil")
	}

	// 测试 proxy 模式客户端
	proxyConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "test-proxy",
				ServiceName:      "test-service",
				ServiceNamespace: "default",
				LocalPort:        8081,
				ProxyMode:        "tcp",
			},
		},
	}

	proxyClient, err := NewClient(proxyConfig)
	if err != nil {
		t.Fatalf("Failed to create proxy client: %v", err)
	}

	if proxyClient == nil {
		t.Fatal("Proxy client is nil")
	}
}

// TestClientValidation 测试客户端配置验证
func TestClientValidation(t *testing.T) {
	// 测试无效的模式
	invalidConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "invalid-mode",
		},
	}

	_, err := NewClient(invalidConfig)
	if err == nil {
		t.Fatal("Expected error for invalid mode, got nil")
	}

	// 测试缺少服务器地址
	noServerConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			AuthToken: "test-token",
			Mode:      "register",
		},
	}

	_, err = NewClient(noServerConfig)
	if err == nil {
		t.Fatal("Expected error for missing server address, got nil")
	}

	// 测试缺少认证令牌
	noTokenConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			Mode:       "register",
		},
	}

	_, err = NewClient(noTokenConfig)
	if err == nil {
		t.Fatal("Expected error for missing auth token, got nil")
	}
}

// TestClientModeValidation 测试客户端模式验证
func TestClientModeValidation(t *testing.T) {
	// 测试 register 模式缺少 workloads
	registerConfigNoWorkloads := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		// Workloads 为空
	}

	_, err := NewClient(registerConfigNoWorkloads)
	if err == nil {
		t.Fatal("Expected error for register mode without workloads, got nil")
	}

	// 测试 proxy 模式缺少 proxies
	proxyConfigNoProxies := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		// Proxies 为空
	}

	_, err = NewClient(proxyConfigNoProxies)
	if err == nil {
		t.Fatal("Expected error for proxy mode without proxies, got nil")
	}
}

// TestClientStartStop 测试客户端启动和停止
func TestClientStartStop(t *testing.T) {
	config := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000", // 不存在的服务器，但可以测试启动逻辑
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 8080,
					Protocol:  "tcp",
				},
			},
		},
	}

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// 启动客户端（会尝试连接到不存在的服务器，但不应该崩溃）
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Run(ctx)
	}()

	// 等待一段时间
	time.Sleep(100 * time.Millisecond)

	// 停止客户端
	client.Stop()

	// 等待客户端停止
	select {
	case err := <-errChan:
		// 预期会有连接错误，因为服务器不存在
		if err != nil {
			t.Logf("Expected connection error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Client did not stop within timeout")
	}
}

// TestClientConfigurationParsing 测试客户端配置解析
func TestClientConfigurationParsing(t *testing.T) {
	// 测试完整的 register 配置
	registerConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app":     "web",
						"version": "v1",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 8080,
					Protocol:  "tcp",
				},
			},
		},
		HealthCheck: config.HealthCheckConfig{
			Enabled:  true,
			Interval: "10s",
			Timeout:  "5s",
		},
	}

	client, err := NewClient(registerConfig)
	if err != nil {
		t.Fatalf("Failed to create client with full config: %v", err)
	}

	if client == nil {
		t.Fatal("Client is nil")
	}

	// 测试完整的 proxy 配置
	proxyConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
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
				Selector: map[string]string{
					"app": "web",
				},
				LoadBalancer: "round_robin",
			},
		},
		HealthCheck: config.HealthCheckConfig{
			Enabled:  true,
			Interval: "10s",
			Timeout:  "5s",
		},
	}

	proxyClient, err := NewClient(proxyConfig)
	if err != nil {
		t.Fatalf("Failed to create proxy client with full config: %v", err)
	}

	if proxyClient == nil {
		t.Fatal("Proxy client is nil")
	}
}

// TestClientWorkloadValidation 测试工作负载配置验证
func TestClientWorkloadValidation(t *testing.T) {
	// 测试无效的本地端口
	invalidPortConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 0, // 无效端口
					Protocol:  "tcp",
				},
			},
		},
	}

	_, err := NewClient(invalidPortConfig)
	if err == nil {
		t.Fatal("Expected error for invalid port, got nil")
	}

	// 测试无效的协议
	invalidProtocolConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:17000",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 8080,
					Protocol:  "invalid", // 无效协议
				},
			},
		},
	}

	_, err = NewClient(invalidProtocolConfig)
	if err == nil {
		t.Fatal("Expected error for invalid protocol, got nil")
	}
}