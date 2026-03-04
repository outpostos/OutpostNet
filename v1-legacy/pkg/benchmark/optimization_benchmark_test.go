package benchmark

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/client"
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/pool"
	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
	"google.golang.org/protobuf/proto"
)

// BenchmarkJSONvsProtobuf 对比JSON和Protocol Buffers的性能
func BenchmarkJSONvsProtobuf(b *testing.B) {
	// 创建测试数据
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
			Labels:    map[string]string{"app": "test", "version": "v1"},
		},
		Spec: types.ServiceSpec{
			Selector:     map[string]string{"app": "test"},
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
			Ports: []types.ServicePort{
				{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
			},
		},
	}
	
	endpoints := []types.Endpoint{
		{ID: "ep1", IP: "10.1.1.1", Port: 8080, Protocol: "tcp", Labels: map[string]string{"app": "test"}},
		{ID: "ep2", IP: "10.1.1.2", Port: 8080, Protocol: "tcp", Labels: map[string]string{"app": "test"}},
		{ID: "ep3", IP: "10.1.1.3", Port: 8080, Protocol: "tcp", Labels: map[string]string{"app": "test"}},
	}
	
	// 转换为Protocol Buffers格式
	pbService := convertToProtobufService(service)
	pbEndpoints := convertToProtobufEndpoints(endpoints)
	
	pbServiceInfo := &protocol.ServiceInfoPayload{
		Service:   pbService,
		Endpoints: pbEndpoints,
	}
	
	jsonServiceInfo := &types.ServiceInfoPayload{
		Service:   *service,
		Endpoints: endpoints,
	}
	
	b.Run("JSON_Serialization", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := json.Marshal(jsonServiceInfo)
			if err != nil {
				b.Fatal(err)
			}
			_ = data
		}
	})
	
	b.Run("Protobuf_Serialization", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := proto.Marshal(pbServiceInfo)
			if err != nil {
				b.Fatal(err)
			}
			_ = data
		}
	})
	
	// 预序列化数据用于反序列化测试
	jsonData, _ := json.Marshal(jsonServiceInfo)
	pbData, _ := proto.Marshal(pbServiceInfo)
	
	b.Run("JSON_Deserialization", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var decoded types.ServiceInfoPayload
			err := json.Unmarshal(jsonData, &decoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("Protobuf_Deserialization", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded := &protocol.ServiceInfoPayload{}
			err := proto.Unmarshal(pbData, decoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	// 数据大小对比
	b.Run("DataSize", func(b *testing.B) {
		b.Logf("JSON size: %d bytes", len(jsonData))
		b.Logf("Protobuf size: %d bytes", len(pbData))
		b.Logf("Space savings: %.2f%%", float64(len(jsonData)-len(pbData))/float64(len(jsonData))*100)
	})
}

// BenchmarkConnectionPool 测试连接池性能
func BenchmarkConnectionPool(b *testing.B) {
	// 启动测试服务器
	server, serverAddr := startTestServer(b)
	defer server.Close()
	
	// 创建连接池
	poolConfig := &pool.ConnectionPoolConfig{
		MaxConnections:      100,
		MaxIdleConnections:  20,
		MaxConnectionAge:    30 * time.Minute,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
	
	connectionPool := pool.NewConnectionPool(poolConfig)
	defer connectionPool.Close()
	
	b.Run("DirectConnection", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := net.Dial("tcp", serverAddr)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()
		}
	})
	
	b.Run("ConnectionPool_FirstTime", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := connectionPool.GetConnection(serverAddr)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()
		}
	})
	
	// 预热连接池
	for i := 0; i < 10; i++ {
		conn, _ := connectionPool.GetConnection(serverAddr)
		conn.Close()
	}
	
	b.Run("ConnectionPool_Reuse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := connectionPool.GetConnection(serverAddr)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()
		}
	})
}

// BenchmarkOptimizedClient 测试优化客户端性能
func BenchmarkOptimizedClient(b *testing.B) {
	// 启动测试服务器
	server, serverAddr := startTestServer(b)
	defer server.Close()
	
	// 创建优化客户端配置
	config := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: serverAddr,
			AuthToken:  "test-token",
			Mode:       "proxy",
		},
	}
	
	// 创建优化客户端
	optimizedClient, err := client.NewOptimizedClient(config)
	if err != nil {
		b.Fatal(err)
	}
	defer optimizedClient.Close()
	
	b.Run("OptimizedClient_ServiceQuery", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 模拟服务查询
			serviceName := "test-service"
			namespace := "default"
			selector := map[string]string{"app": "test"}
			
			// 这里需要实际的服务查询逻辑
			_ = serviceName
			_ = namespace
			_ = selector
		}
	})
}

// BenchmarkOptimizedVsOriginal 对比优化前后的性能
func BenchmarkOptimizedVsOriginal(b *testing.B) {
	// 创建测试环境
	backendServer := startBackendServer(b)
	defer backendServer.Close()
	
	proxyServerOriginal := startOriginalProxyServer(b, backendServer.Addr().String())
	defer proxyServerOriginal.Close()
	
	proxyServerOptimized := startOptimizedProxyServer(b, backendServer.Addr().String())
	defer proxyServerOptimized.Close()
	
	// 测试数据
	testData := make([]byte, 1024) // 1KB测试数据
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	
	b.Run("Original_Proxy_1KB", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := net.Dial("tcp", proxyServerOriginal.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			
			// 发送数据
			_, err = conn.Write(testData)
			if err != nil {
				b.Fatal(err)
			}
			
			// 读取响应
			response := make([]byte, len(testData))
			_, err = io.ReadFull(conn, response)
			if err != nil {
				b.Fatal(err)
			}
			
			conn.Close()
		}
	})
	
	b.Run("Optimized_Proxy_1KB", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := net.Dial("tcp", proxyServerOptimized.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			
			// 发送数据
			_, err = conn.Write(testData)
			if err != nil {
				b.Fatal(err)
			}
			
			// 读取响应
			response := make([]byte, len(testData))
			_, err = io.ReadFull(conn, response)
			if err != nil {
				b.Fatal(err)
			}
			
			conn.Close()
		}
	})
}

// BenchmarkConcurrentConnections 并发连接性能测试
func BenchmarkConcurrentConnections(b *testing.B) {
	// 启动测试服务器
	server, serverAddr := startTestServer(b)
	defer server.Close()
	
	// 创建连接池
	poolConfig := &pool.ConnectionPoolConfig{
		MaxConnections:      200,
		MaxIdleConnections:  50,
		MaxConnectionAge:    30 * time.Minute,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
	
	connectionPool := pool.NewConnectionPool(poolConfig)
	defer connectionPool.Close()
	
	concurrencyLevels := []int{1, 5, 10, 20, 50}
	
	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d_Direct", concurrency), func(b *testing.B) {
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					conn, err := net.Dial("tcp", serverAddr)
					if err != nil {
						b.Fatal(err)
					}
					conn.Close()
				}
			})
		})
		
		b.Run(fmt.Sprintf("Concurrency_%d_Pool", concurrency), func(b *testing.B) {
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					conn, err := connectionPool.GetConnection(serverAddr)
					if err != nil {
						b.Fatal(err)
					}
					conn.Close()
				}
			})
		})
	}
}

// BenchmarkMessageCodec 消息编解码性能测试
func BenchmarkMessageCodec(b *testing.B) {
	// 创建测试连接
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	
	// 创建编解码器
	codec := protocol.NewCodec(clientConn)
	defer codec.Close()
	
	// 创建测试消息
	authPayload := &protocol.AuthPayload{
		Token:          "test-token",
		Mode:           "proxy",
		ClientVersion:  "1.0.0",
		Capabilities:   map[string]string{"test": "true"},
	}
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeAuth).
		WithPayload(authPayload).
		Build()
	
	b.Run("Message_Encoding", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := codec.WriteMessage(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
	
	// 为解码测试准备数据
	go func() {
		for i := 0; i < b.N; i++ {
			codec.WriteMessage(msg)
		}
	}()
	
	b.Run("Message_Decoding", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			receivedMsg, err := codec.ReadMessage()
			if err != nil {
				b.Fatal(err)
			}
			codec.ReturnMessage(receivedMsg)
		}
	})
}

// BenchmarkDataTransfer 数据传输性能测试
func BenchmarkDataTransfer(b *testing.B) {
	dataSizes := []int{1024, 4096, 16384, 65536, 1048576} // 1KB, 4KB, 16KB, 64KB, 1MB
	
	for _, size := range dataSizes {
		testData := make([]byte, size)
		for i := range testData {
			testData[i] = byte(i % 256)
		}
		
		b.Run(fmt.Sprintf("DataSize_%dB_Direct", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// 模拟直接数据传输
				client, server := net.Pipe()
				
				go func() {
					defer server.Close()
					io.Copy(server, server)
				}()
				
				_, err := client.Write(testData)
				if err != nil {
					b.Fatal(err)
				}
				
				response := make([]byte, size)
				_, err = io.ReadFull(client, response)
				if err != nil {
					b.Fatal(err)
				}
				
				client.Close()
			}
		})
		
		b.Run(fmt.Sprintf("DataSize_%dB_Optimized", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// 模拟优化后的数据传输
				client, server := net.Pipe()
				
				go func() {
					defer server.Close()
					buffer := make([]byte, 64*1024) // 64KB 缓冲区
					for {
						n, err := server.Read(buffer)
						if err != nil {
							break
						}
						server.Write(buffer[:n])
					}
				}()
				
				_, err := client.Write(testData)
				if err != nil {
					b.Fatal(err)
				}
				
				response := make([]byte, size)
				_, err = io.ReadFull(client, response)
				if err != nil {
					b.Fatal(err)
				}
				
				client.Close()
			}
		})
	}
}

// 辅助函数

// startTestServer 启动测试服务器
func startTestServer(b *testing.B) (net.Listener, string) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // 回显服务器
			}(conn)
		}
	}()
	
	return listener, listener.Addr().String()
}

// startBackendServer 启动后端服务器
func startBackendServer(b *testing.B) net.Listener {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // 回显服务器
			}(conn)
		}
	}()
	
	return listener
}

// startOriginalProxyServer 启动原始代理服务器
func startOriginalProxyServer(b *testing.B, backendAddr string) net.Listener {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	
	go func() {
		for {
			clientConn, err := listener.Accept()
			if err != nil {
				return
			}
			
			go func(client net.Conn) {
				defer client.Close()
				
				// 模拟原始代理：每次连接都创建新的后端连接
				backend, err := net.Dial("tcp", backendAddr)
				if err != nil {
					return
				}
				defer backend.Close()
				
				// 双向数据转发
				done := make(chan struct{}, 2)
				
				go func() {
					defer func() { done <- struct{}{} }()
					io.Copy(backend, client)
				}()
				
				go func() {
					defer func() { done <- struct{}{} }()
					io.Copy(client, backend)
				}()
				
				<-done
			}(clientConn)
		}
	}()
	
	return listener
}

// startOptimizedProxyServer 启动优化代理服务器
func startOptimizedProxyServer(b *testing.B, backendAddr string) net.Listener {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	
	// 创建连接池
	poolConfig := &pool.ConnectionPoolConfig{
		MaxConnections:      50,
		MaxIdleConnections:  10,
		MaxConnectionAge:    30 * time.Minute,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
	
	connectionPool := pool.NewConnectionPool(poolConfig)
	
	go func() {
		defer connectionPool.Close()
		
		for {
			clientConn, err := listener.Accept()
			if err != nil {
				return
			}
			
			go func(client net.Conn) {
				defer client.Close()
				
				// 模拟优化代理：使用连接池
				backend, err := connectionPool.GetConnection(backendAddr)
				if err != nil {
					return
				}
				defer backend.Close()
				
				// 使用更大的缓冲区进行双向数据转发
				done := make(chan struct{}, 2)
				bufferSize := 64 * 1024
				
				go func() {
					defer func() { done <- struct{}{} }()
					buffer := make([]byte, bufferSize)
					for {
						n, err := client.Read(buffer)
						if err != nil {
							return
						}
						if _, err := backend.Write(buffer[:n]); err != nil {
							return
						}
					}
				}()
				
				go func() {
					defer func() { done <- struct{}{} }()
					buffer := make([]byte, bufferSize)
					for {
						n, err := backend.Read(buffer)
						if err != nil {
							return
						}
						if _, err := client.Write(buffer[:n]); err != nil {
							return
						}
					}
				}()
				
				<-done
			}(clientConn)
		}
	}()
	
	return listener
}

// 类型转换辅助函数
func convertToProtobufService(service *types.Service) *protocol.Service {
	pbPorts := make([]*protocol.ServicePort, len(service.Spec.Ports))
	for i, port := range service.Spec.Ports {
		pbPorts[i] = &protocol.ServicePort{
			Name:       port.Name,
			Port:       int32(port.Port),
			TargetPort: int32(port.TargetPort),
			Protocol:   port.Protocol,
		}
	}
	
	return &protocol.Service{
		Metadata: &protocol.ServiceMetadata{
			Name:      service.Metadata.Name,
			Namespace: service.Metadata.Namespace,
			Labels:    service.Metadata.Labels,
		},
		Spec: &protocol.ServiceSpec{
			Selector:     service.Spec.Selector,
			Ports:        pbPorts,
			ProxyMode:    service.Spec.ProxyMode,
			LoadBalancer: service.Spec.LoadBalancer,
		},
	}
}

func convertToProtobufEndpoints(endpoints []types.Endpoint) []*protocol.Endpoint {
	pbEndpoints := make([]*protocol.Endpoint, len(endpoints))
	for i, ep := range endpoints {
		pbEndpoints[i] = &protocol.Endpoint{
			Id:       ep.ID,
			Ip:       ep.IP,
			Port:     int32(ep.Port),
			Protocol: ep.Protocol,
			Labels:   ep.Labels,
			Status: &protocol.EndpointStatus{
				Ready:       ep.Status.Ready,
				Reason:      ep.Status.Reason,
				Message:     ep.Status.Message,
				HealthScore: 100,
			},
		}
	}
	return pbEndpoints
}