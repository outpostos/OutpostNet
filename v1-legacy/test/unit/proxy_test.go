package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/client"
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/server"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestProxyDataForwarding 测试代理数据转发功能
func TestProxyDataForwarding(t *testing.T) {
	// 1. 启动一个简单的 HTTP 服务作为后端
	backendServer := &http.Server{
		Addr: ":18200",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "Hello from backend server! Path: %s", r.URL.Path)
		}),
	}

	go func() {
		if err := backendServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Logf("backend server error: %v", err)
		}
	}()
	defer backendServer.Shutdown(context.Background())

	// 等待后端服务启动
	time.Sleep(100 * time.Millisecond)

	// 2. 启动 outpostnet-server
	serverConfig := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:18201",
			AuthToken:  "test-token",
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      "backend-service",
					Namespace: "default",
				},
				Spec: types.ServiceSpec{
					Selector: map[string]string{
						"app": "backend",
					},
					Ports: []types.ServicePort{
						{
							Name:       "http",
							Port:       80,
							TargetPort: 18200,
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

	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// 3. 启动 register 模式的客户端
	registerConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:18201",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app": "backend",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 18200,
					Protocol:  "tcp",
				},
			},
		},
	}

	registerClient, err := client.NewClient(registerConfig)
	if err != nil {
		t.Fatalf("failed to create register client: %v", err)
	}

	go registerClient.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	// 4. 启动 proxy 模式的客户端
	proxyConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:18201",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "backend-proxy",
				ServiceName:      "backend-service",
				ServiceNamespace: "default",
				LocalPort:        18202,
				ProxyMode:        "tcp",
				Selector: map[string]string{
					"app": "backend",
				},
				LoadBalancer: "round_robin",
			},
		},
	}

	proxyClient, err := client.NewClient(proxyConfig)
	if err != nil {
		t.Fatalf("failed to create proxy client: %v", err)
	}

	go proxyClient.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	// 5. 测试通过代理访问后端服务
	testCases := []struct {
		path     string
		expected string
	}{
		{"/", "Hello from backend server! Path: /"},
		{"/api/test", "Hello from backend server! Path: /api/test"},
		{"/health", "Hello from backend server! Path: /health"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("path_%s", tc.path), func(t *testing.T) {
			// 通过代理端口访问
			url := fmt.Sprintf("http://127.0.0.1:18202%s", tc.path)
			resp, err := http.Get(url)
			if err != nil {
				t.Fatalf("failed to make request through proxy: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected status 200, got %d", resp.StatusCode)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}

			if strings.TrimSpace(string(body)) != tc.expected {
				t.Errorf("expected response %q, got %q", tc.expected, string(body))
			}

			t.Logf("✓ Successfully proxied request to %s", tc.path)
		})
	}

	// 6. 测试并发连接
	t.Run("concurrent_connections", func(t *testing.T) {
		const numConnections = 10
		done := make(chan error, numConnections)

		for i := 0; i < numConnections; i++ {
			go func(id int) {
				path := fmt.Sprintf("/concurrent/%d", id)
				url := fmt.Sprintf("http://127.0.0.1:18202%s", path)
				resp, err := http.Get(url)
				if err != nil {
					done <- fmt.Errorf("connection %d failed: %v", id, err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					done <- fmt.Errorf("connection %d got status %d", id, resp.StatusCode)
					return
				}

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					done <- fmt.Errorf("connection %d failed to read body: %v", id, err)
					return
				}

				expected := fmt.Sprintf("Hello from backend server! Path: %s", path)
				if strings.TrimSpace(string(body)) != expected {
					done <- fmt.Errorf("connection %d got unexpected response: %s", id, string(body))
					return
				}

				done <- nil
			}(i)
		}

		// 等待所有连接完成
		for i := 0; i < numConnections; i++ {
			if err := <-done; err != nil {
				t.Errorf("concurrent connection failed: %v", err)
			}
		}

		t.Logf("✓ Successfully handled %d concurrent connections", numConnections)
	})

	// 清理
	registerClient.Stop()
	proxyClient.Stop()
	srv.Stop()

	t.Logf("✓ Proxy data forwarding test completed successfully!")
}

// TestTCPProxyBasic 测试基本的 TCP 代理功能
func TestTCPProxyBasic(t *testing.T) {
	// 1. 启动一个简单的 TCP echo 服务
	listener, err := net.Listen("tcp", "127.0.0.1:18300")
	if err != nil {
		t.Fatalf("failed to create TCP listener: %v", err)
	}
	defer listener.Close()

	// Echo 服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // echo back
			}(conn)
		}
	}()

	// 2. 启动 outpostnet-server
	serverConfig := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: "127.0.0.1:18301",
			AuthToken:  "test-token",
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      "echo-service",
					Namespace: "default",
				},
				Spec: types.ServiceSpec{
					Selector: map[string]string{
						"app": "echo",
					},
					Ports: []types.ServicePort{
						{
							Name:       "tcp",
							Port:       18300,
							TargetPort: 18300,
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

	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// 3. 启动 register 客户端
	registerConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:18301",
			AuthToken:  "test-token",
			Mode:       "register",
		},
		Workloads: []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: map[string]string{
						"app": "echo",
					},
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 18300,
					Protocol:  "tcp",
				},
			},
		},
	}

	registerClient, err := client.NewClient(registerConfig)
	if err != nil {
		t.Fatalf("failed to create register client: %v", err)
	}

	go registerClient.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	// 4. 启动 proxy 客户端
	proxyConfig := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:18301",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "echo-proxy",
				ServiceName:      "echo-service",
				ServiceNamespace: "default",
				LocalPort:        18302,
				ProxyMode:        "tcp",
				Selector: map[string]string{
					"app": "echo",
				},
				LoadBalancer: "round_robin",
			},
		},
	}

	proxyClient, err := client.NewClient(proxyConfig)
	if err != nil {
		t.Fatalf("failed to create proxy client: %v", err)
	}

	go proxyClient.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	// 5. 测试 TCP 连接和数据传输
	conn, err := net.Dial("tcp", "127.0.0.1:18302")
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer conn.Close()

	// 发送测试数据
	testData := "Hello, TCP proxy!"
	_, err = conn.Write([]byte(testData))
	if err != nil {
		t.Fatalf("failed to write to proxy: %v", err)
	}

	// 读取响应
	buffer := make([]byte, len(testData))
	_, err = io.ReadFull(conn, buffer)
	if err != nil {
		t.Fatalf("failed to read from proxy: %v", err)
	}

	if string(buffer) != testData {
		t.Errorf("expected %q, got %q", testData, string(buffer))
	}

	t.Logf("✓ TCP proxy successfully echoed: %s", testData)

	// 清理
	registerClient.Stop()
	proxyClient.Stop()
	srv.Stop()
}