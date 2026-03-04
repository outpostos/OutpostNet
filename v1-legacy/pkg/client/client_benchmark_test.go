package client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// BenchmarkClient_NewClient 测试客户端创建性能
func BenchmarkClient_NewClient(b *testing.B) {
	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: "127.0.0.1:7629",
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "test-proxy",
				ServiceName:      "test-service",
				ServiceNamespace: "default",
				LocalPort:        8080,
				ProxyMode:        "tcp",
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		client, err := NewClient(cfg)
		if err != nil {
			b.Fatalf("创建客户端失败: %v", err)
		}
		_ = client
	}
}

// BenchmarkClient_ConnectToServer 测试服务器连接性能
func BenchmarkClient_ConnectToServer(b *testing.B) {
	// 启动测试服务器
	server := startMockServer(b)
	defer server.Close()

	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: server.Addr().String(),
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		client, err := NewClient(cfg)
		if err != nil {
			b.Fatalf("创建客户端失败: %v", err)
		}

		err = client.connectToServer()
		if err != nil {
			b.Fatalf("连接服务器失败: %v", err)
		}

		client.closeConnection()
	}
}

// BenchmarkClient_TCPProxy 测试TCP代理性能
func BenchmarkClient_TCPProxy(b *testing.B) {
	// 启动后端服务
	backend := startMockBackend(b, "tcp", "Hello World")
	defer backend.Close()

	// 启动outpostnet服务器
	server := startMockServerWithService(b, backend.Addr().String())
	defer server.Close()

	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: server.Addr().String(),
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "test-proxy",
				ServiceName:      "test-service",
				ServiceNamespace: "default",
				LocalPort:        18080,
				ProxyMode:        "tcp",
			},
		},
	}

	client, err := NewClient(cfg)
	if err != nil {
		b.Fatalf("创建客户端失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动客户端
	go client.Run(ctx)
	time.Sleep(100 * time.Millisecond) // 等待客户端启动

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 连接到代理端口
			conn, err := net.Dial("tcp", "127.0.0.1:18080")
			if err != nil {
				b.Fatalf("连接代理失败: %v", err)
			}

			// 发送数据
			_, err = conn.Write([]byte("test"))
			if err != nil {
				b.Fatalf("发送数据失败: %v", err)
			}

			// 读取响应
			buffer := make([]byte, 11)
			_, err = conn.Read(buffer)
			if err != nil {
				b.Fatalf("读取响应失败: %v", err)
			}

			conn.Close()
		}
	})
}

// BenchmarkClient_UDPProxy 测试UDP代理性能
func BenchmarkClient_UDPProxy(b *testing.B) {
	// 启动UDP后端服务
	backend := startMockUDPBackend(b, "Hello UDP")
	defer backend.Close()

	// 启动outpostnet服务器
	server := startMockServerWithUDPService(b, backend.LocalAddr().String())
	defer server.Close()

	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: server.Addr().String(),
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "udp-proxy",
				ServiceName:      "udp-service",
				ServiceNamespace: "default",
				LocalPort:        18081,
				ProxyMode:        "udp",
			},
		},
	}

	client, err := NewClient(cfg)
	if err != nil {
		b.Fatalf("创建客户端失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动客户端
	go client.Run(ctx)
	time.Sleep(100 * time.Millisecond) // 等待客户端启动

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 连接到UDP代理端口
			conn, err := net.Dial("udp", "127.0.0.1:18081")
			if err != nil {
				b.Fatalf("连接UDP代理失败: %v", err)
			}

			// 发送数据
			_, err = conn.Write([]byte("test"))
			if err != nil {
				b.Fatalf("发送数据失败: %v", err)
			}

			// 读取响应
			buffer := make([]byte, 9)
			_, err = conn.Read(buffer)
			if err != nil {
				b.Fatalf("读取响应失败: %v", err)
			}

			conn.Close()
		}
	})
}

// BenchmarkClient_ServiceQuery 测试服务查询性能
func BenchmarkClient_ServiceQuery(b *testing.B) {
	server := startMockServerWithService(b, "127.0.0.1:8080")
	defer server.Close()

	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: server.Addr().String(),
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
	}

	client, err := NewClient(cfg)
	if err != nil {
		b.Fatalf("创建客户端失败: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		service, endpoints, err := client.queryService("test-service", "default", nil)
		if err != nil {
			b.Fatalf("查询服务失败: %v", err)
		}
		_ = service
		_ = endpoints
	}
}

// BenchmarkClient_LoadBalancerSelection 测试负载均衡选择性能
func BenchmarkClient_LoadBalancerSelection(b *testing.B) {
	client, err := NewClient(&config.ClientConfig{})
	if err != nil {
		b.Fatalf("创建客户端失败: %v", err)
	}

	// 创建测试服务和端点
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			LoadBalancer: "round_robin",
		},
	}

	endpoints := make([]*types.Endpoint, 5)
	for i := 0; i < 5; i++ {
		endpoints[i] = &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", i+1),
			Port: 8080,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			endpoint := client.selectEndpoint(service, endpoints, "test-client")
			_ = endpoint
		}
	})
}

// BenchmarkClient_ConcurrentProxies 测试并发代理性能
func BenchmarkClient_ConcurrentProxies(b *testing.B) {
	// 启动多个后端服务
	backends := make([]net.Listener, 3)
	for i := 0; i < 3; i++ {
		backends[i] = startMockBackend(b, "tcp", fmt.Sprintf("Backend %d", i))
		defer backends[i].Close()
	}

	// 启动outpostnet服务器
	server := startMockServerWithMultipleServices(b, backends)
	defer server.Close()

	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: server.Addr().String(),
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             "proxy-1",
				ServiceName:      "service-1",
				ServiceNamespace: "default",
				LocalPort:        18082,
				ProxyMode:        "tcp",
			},
			{
				Name:             "proxy-2",
				ServiceName:      "service-2",
				ServiceNamespace: "default",
				LocalPort:        18083,
				ProxyMode:        "tcp",
			},
			{
				Name:             "proxy-3",
				ServiceName:      "service-3",
				ServiceNamespace: "default",
				LocalPort:        18084,
				ProxyMode:        "tcp",
			},
		},
	}

	client, err := NewClient(cfg)
	if err != nil {
		b.Fatalf("创建客户端失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动客户端
	go client.Run(ctx)
	time.Sleep(200 * time.Millisecond) // 等待客户端启动

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 随机选择一个代理端口
			port := 18082 + (time.Now().Nanosecond() % 3)
			
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				b.Fatalf("连接代理失败: %v", err)
			}

			_, err = conn.Write([]byte("test"))
			if err != nil {
				b.Fatalf("发送数据失败: %v", err)
			}

			buffer := make([]byte, 100)
			_, err = conn.Read(buffer)
			if err != nil {
				b.Fatalf("读取响应失败: %v", err)
			}

			conn.Close()
		}
	})
}

// BenchmarkClient_WorkloadRegistration 测试工作负载注册性能
func BenchmarkClient_WorkloadRegistration(b *testing.B) {
	server := startMockServer(b)
	defer server.Close()

	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: server.Addr().String(),
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		client, err := NewClient(cfg)
		if err != nil {
			b.Fatalf("创建客户端失败: %v", err)
		}

		err = client.connectToServer()
		if err != nil {
			b.Fatalf("连接服务器失败: %v", err)
		}

		err = client.registerWorkload(cfg.Workloads[0])
		if err != nil {
			b.Fatalf("注册工作负载失败: %v", err)
		}

		client.closeConnection()
	}
}

// BenchmarkClient_MemoryUsage 测试内存使用情况
func BenchmarkClient_MemoryUsage(b *testing.B) {
	for _, numProxies := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("Proxies_%d", numProxies), func(b *testing.B) {
			// 创建多个代理配置
			proxies := make([]types.Proxy, numProxies)
			for i := 0; i < numProxies; i++ {
				proxies[i] = types.Proxy{
					Name:             fmt.Sprintf("proxy-%d", i),
					ServiceName:      fmt.Sprintf("service-%d", i),
					ServiceNamespace: "default",
					LocalPort:        19000 + i,
					ProxyMode:        "tcp",
				}
			}

			cfg := &config.ClientConfig{
				Client: config.ClientSpec{
					ServerAddr: "127.0.0.1:7629",
					AuthToken:  "test-token",
					Mode:       "proxy",
				},
				Proxies: proxies,
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				client, err := NewClient(cfg)
				if err != nil {
					b.Fatalf("创建客户端失败: %v", err)
				}
				_ = client
			}
		})
	}
}

// 辅助函数：启动模拟服务器
func startMockServer(b *testing.B) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("启动模拟服务器失败: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleMockConnection(conn)
		}
	}()

	return listener
}

// 辅助函数：处理模拟连接
func handleMockConnection(conn net.Conn) {
	defer conn.Close()
	
	// 简单的认证响应
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		return
	}

	// 发送成功认证响应
	response := `{"type":"auth_response","id":"test","payload":{"success":true,"message":"ok","client_id":"test"},"timestamp":"2025-01-01T00:00:00Z"}`
	conn.Write([]byte(response))
}

// 辅助函数：启动模拟后端服务
func startMockBackend(b *testing.B, protocol, response string) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("启动模拟后端失败: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buffer := make([]byte, 1024)
				c.Read(buffer)
				c.Write([]byte(response))
			}(conn)
		}
	}()

	return listener
}

// 辅助函数：启动模拟UDP后端服务
func startMockUDPBackend(b *testing.B, response string) *net.UDPConn {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("解析UDP地址失败: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		b.Fatalf("启动UDP后端失败: %v", err)
	}

	go func() {
		buffer := make([]byte, 1024)
		for {
			n, clientAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if n > 0 {
				conn.WriteToUDP([]byte(response), clientAddr)
			}
		}
	}()

	return conn
}

// 辅助函数：启动带服务的模拟服务器
func startMockServerWithService(b *testing.B, backendAddr string) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("启动模拟服务器失败: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleMockServerConnection(conn, backendAddr)
		}
	}()

	return listener
}

// 辅助函数：启动带UDP服务的模拟服务器
func startMockServerWithUDPService(b *testing.B, backendAddr string) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("启动模拟服务器失败: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleMockUDPServerConnection(conn, backendAddr)
		}
	}()

	return listener
}

// 辅助函数：启动多服务模拟服务器
func startMockServerWithMultipleServices(b *testing.B, backends []net.Listener) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("启动多服务模拟服务器失败: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleMockMultiServiceConnection(conn, backends)
		}
	}()

	return listener
}

// 辅助函数：处理模拟服务器连接
func handleMockServerConnection(conn net.Conn, backendAddr string) {
	defer conn.Close()
	
	// 读取第一个消息（认证）
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		return
	}

	// 发送认证响应
	authResponse := `{"type":"auth_response","id":"test","payload":{"success":true,"message":"ok","client_id":"test"},"timestamp":"2025-01-01T00:00:00Z"}`
	conn.Write([]byte(authResponse))

	// 读取查询服务消息
	_, err = conn.Read(buffer)
	if err != nil {
		return
	}

	// 发送服务信息响应
	serviceResponse := fmt.Sprintf(`{"type":"service_info","id":"test","payload":{"service":{"metadata":{"name":"test-service","namespace":"default"},"spec":{"load_balancer":"round_robin"}},"endpoints":[{"id":"endpoint-1","ip":"%s","port":8080}]},"timestamp":"2025-01-01T00:00:00Z"}`, parseIPFromAddr(backendAddr))
	conn.Write([]byte(serviceResponse))
}

// 辅助函数：处理模拟UDP服务器连接
func handleMockUDPServerConnection(conn net.Conn, backendAddr string) {
	defer conn.Close()
	
	// 读取第一个消息（认证）
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		return
	}

	// 发送认证响应
	authResponse := `{"type":"auth_response","id":"test","payload":{"success":true,"message":"ok","client_id":"test"},"timestamp":"2025-01-01T00:00:00Z"}`
	conn.Write([]byte(authResponse))

	// 读取查询服务消息
	_, err = conn.Read(buffer)
	if err != nil {
		return
	}

	// 发送UDP服务信息响应
	serviceResponse := fmt.Sprintf(`{"type":"service_info","id":"test","payload":{"service":{"metadata":{"name":"udp-service","namespace":"default"},"spec":{"load_balancer":"round_robin"}},"endpoints":[{"id":"endpoint-1","ip":"%s","port":8081}]},"timestamp":"2025-01-01T00:00:00Z"}`, parseIPFromAddr(backendAddr))
	conn.Write([]byte(serviceResponse))
}

// 辅助函数：处理多服务模拟连接
func handleMockMultiServiceConnection(conn net.Conn, backends []net.Listener) {
	defer conn.Close()
	
	// 简化处理：只响应认证和基本服务查询
	buffer := make([]byte, 1024)
	
	// 认证
	conn.Read(buffer)
	authResponse := `{"type":"auth_response","id":"test","payload":{"success":true,"message":"ok","client_id":"test"},"timestamp":"2025-01-01T00:00:00Z"}`
	conn.Write([]byte(authResponse))

	// 处理后续的服务查询
	for {
		n, err := conn.Read(buffer)
		if err != nil || n == 0 {
			return
		}
		
		// 简单响应一个服务信息
		serviceResponse := `{"type":"service_info","id":"test","payload":{"service":{"metadata":{"name":"test-service","namespace":"default"},"spec":{"load_balancer":"round_robin"}},"endpoints":[{"id":"endpoint-1","ip":"127.0.0.1","port":8080}]},"timestamp":"2025-01-01T00:00:00Z"}`
		conn.Write([]byte(serviceResponse))
	}
}

// 辅助函数：从地址字符串解析IP
func parseIPFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1"
	}
	return host
}