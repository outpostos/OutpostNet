package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/client"
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/server"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestK8sStyleArchitecture 测试 K8s 风格的服务发现架构
func TestK8sStyleArchitecture(t *testing.T) {
	// 1. 启动 outpostnet-server
	serverConfig := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:18081",
			AuthToken:  "test-token",
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      "web-service",
					Namespace: "default",
					Labels: map[string]string{
						"app": "web",
					},
				},
				Spec: types.ServiceSpec{
					Selector: map[string]string{
						"app": "web",
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

	srv, err := server.NewServer(serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动服务器
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond) // 等待服务器启动

	// 2. 启动 register 模式的客户端（工作负载提供方）
	registerConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:18081",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app":     "web",
						"version": "v1",
						"node":    "test-node",
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

	registerClient, err := client.NewClient(registerConfig)
	if err != nil {
		t.Fatalf("failed to create register client: %v", err)
	}

	// 启动 register 客户端
	go registerClient.Run(ctx)
	time.Sleep(200 * time.Millisecond) // 等待客户端连接和注册

	// 3. 验证 K8s Controller 自动创建了端点
	registry := srv.GetRegistry()
	endpoints, err := registry.GetEndpoints("web-service", "default", nil)
	if err != nil {
		t.Fatalf("failed to get endpoints: %v", err)
	}

	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}

	endpoint := endpoints[0]
	if endpoint.Labels["app"] != "web" {
		t.Errorf("expected endpoint label app=web, got %s", endpoint.Labels["app"])
	}
	if endpoint.Labels["version"] != "v1" {
		t.Errorf("expected endpoint label version=v1, got %s", endpoint.Labels["version"])
	}
	if endpoint.Port != 8080 {
		t.Errorf("expected endpoint port 8080, got %d", endpoint.Port)
	}

	t.Logf("✓ K8s Controller automatically created endpoint: %s:%d with labels %v", 
		endpoint.IP, endpoint.Port, endpoint.Labels)

	// 4. 测试 proxy 模式的客户端（服务消费方）能够发现服务
	proxyConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:18081",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "web-proxy",
				ServiceName:      "web-service",
				ServiceNamespace: "default",
				LocalPort:        8081,
				ProxyMode:        "tcp",
				Selector: map[string]string{
					"app": "web",
				},
				LoadBalancer: "round_robin",
			},
		},
	}

	_, err = client.NewClient(proxyConfig)
	if err != nil {
		t.Fatalf("failed to create proxy client: %v", err)
	}

	// 测试查询服务功能
	conn, err := net.DialTimeout("tcp", "127.0.0.1:18081", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// 5. 测试认证
	authMsg := types.Message{
		Type: types.MsgTypeAuth,
		ID:   "auth-test",
		Payload: types.AuthPayload{
			Token: "test-token",
			Mode:  "proxy",
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(authMsg); err != nil {
		t.Fatalf("failed to send auth message: %v", err)
	}

	var authResponse types.Message
	if err := decoder.Decode(&authResponse); err != nil {
		t.Fatalf("failed to receive auth response: %v", err)
	}

	if authResponse.Type != types.MsgTypeAuthResponse {
		t.Fatalf("unexpected auth response type: %s", authResponse.Type)
	}

	t.Logf("✓ Authentication successful")

	// 6. 测试服务查询
	queryMsg := types.Message{
		Type: types.MsgTypeQueryService,
		ID:   "query-test",
		Payload: types.QueryServicePayload{
			ServiceName:      "web-service",
			ServiceNamespace: "default",
			Selector: map[string]string{
				"app": "web",
			},
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(queryMsg); err != nil {
		t.Fatalf("failed to send query message: %v", err)
	}

	var queryResponse types.Message
	if err := decoder.Decode(&queryResponse); err != nil {
		t.Fatalf("failed to receive query response: %v", err)
	}

	if queryResponse.Type == types.MsgTypeError {
		var errorPayload types.ErrorPayload
		payloadBytes, _ := json.Marshal(queryResponse.Payload)
		json.Unmarshal(payloadBytes, &errorPayload)
		t.Fatalf("server error: %s", errorPayload.Message)
	}

	if queryResponse.Type != types.MsgTypeServiceInfo {
		t.Fatalf("unexpected query response type: %s", queryResponse.Type)
	}

	var serviceInfo types.ServiceInfoPayload
	payloadBytes, err := json.Marshal(queryResponse.Payload)
	if err != nil {
		t.Fatalf("failed to marshal service info: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &serviceInfo); err != nil {
		t.Fatalf("failed to unmarshal service info: %v", err)
	}

	if len(serviceInfo.Endpoints) != 1 {
		t.Fatalf("expected 1 endpoint in service info, got %d", len(serviceInfo.Endpoints))
	}

	discoveredEndpoint := serviceInfo.Endpoints[0]
	if discoveredEndpoint.Labels["app"] != "web" {
		t.Errorf("expected discovered endpoint label app=web, got %s", discoveredEndpoint.Labels["app"])
	}

	t.Logf("✓ Service discovery successful: found %d endpoints for service %s", 
		len(serviceInfo.Endpoints), serviceInfo.Service.Metadata.Name)

	// 7. 验证完整的 K8s 风格工作流程
	t.Logf("✓ K8s-style service discovery architecture test passed!")
	t.Logf("  - Workloads register with labels (no service name specified)")
	t.Logf("  - Controller automatically creates endpoints based on service selectors")  
	t.Logf("  - Proxy clients can discover services through selectors")
	t.Logf("  - Service registry supports dynamic endpoint management")

	// 清理
	registerClient.Stop()
	srv.Stop()
}