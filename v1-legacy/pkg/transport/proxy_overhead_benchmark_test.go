package transport

import (
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// BenchmarkProxyOverhead_Throughput 对比直接连接 vs outpostnet代理的吞吐量开销
func BenchmarkProxyOverhead_Throughput(b *testing.B) {
	payloadSizes := []int{1024, 4096, 16384, 65536, 1048576} // 1KB to 1MB

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("PayloadSize_%dB", size), func(b *testing.B) {
			// 创建测试数据
			payload := make([]byte, size)
			rand.Read(payload)

			// 子测试：直接连接
			b.Run("DirectConnection", func(b *testing.B) {
				// 启动后端服务
				backend := startEchoServer(b, "tcp")
				defer backend.Close()

				b.ResetTimer()
				b.SetBytes(int64(size))
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// 直接连接到后端
					conn, err := net.Dial("tcp", backend.Addr().String())
					if err != nil {
						b.Fatalf("Failed to connect directly: %v", err)
					}

					// 发送数据
					_, err = conn.Write(payload)
					if err != nil {
						b.Fatalf("Failed to write: %v", err)
					}

					// 读取响应
					response := make([]byte, size)
					_, err = io.ReadFull(conn, response)
					if err != nil {
						b.Fatalf("Failed to read: %v", err)
					}

					conn.Close()
				}
			})

			// 子测试：outpostnet代理连接 (模拟)
			b.Run("ProxiedConnection", func(b *testing.B) {
				// 启动后端服务
				backend := startEchoServer(b, "tcp")
				defer backend.Close()

				// 启动模拟outpostnet代理
				proxy := startSimpleProxy(b, backend.Addr().String())
				defer proxy.Close()

				b.ResetTimer()
				b.SetBytes(int64(size))
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// 通过代理连接
					conn, err := net.Dial("tcp", proxy.Addr().String())
					if err != nil {
						b.Fatalf("Failed to connect via proxy: %v", err)
					}

					// 发送数据
					_, err = conn.Write(payload)
					if err != nil {
						b.Fatalf("Failed to write: %v", err)
					}

					// 读取响应
					response := make([]byte, size)
					_, err = io.ReadFull(conn, response)
					if err != nil {
						b.Fatalf("Failed to read: %v", err)
					}

					conn.Close()
				}
			})
		})
	}
}

// BenchmarkProxyOverhead_Latency 对比直接连接 vs outpostnet代理的延迟开销
func BenchmarkProxyOverhead_Latency(b *testing.B) {
	// 小包测试延迟开销
	payload := make([]byte, 64)
	rand.Read(payload)

	// 子测试：直接连接延迟
	b.Run("DirectConnection", func(b *testing.B) {
		backend := startEchoServer(b, "tcp")
		defer backend.Close()

		// 预建立连接减少连接建立开销
		conn, err := net.Dial("tcp", backend.Addr().String())
		if err != nil {
			b.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		b.ResetTimer()
		b.ReportAllocs()

		var totalLatency time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()

			_, err = conn.Write(payload)
			if err != nil {
				b.Fatalf("Failed to write: %v", err)
			}

			response := make([]byte, 64)
			_, err = io.ReadFull(conn, response)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			latency := time.Since(start)
			totalLatency += latency

			if i == 0 {
				b.Logf("Direct connection first request latency: %v", latency)
			}
		}
		b.Logf("Direct connection average latency: %v", totalLatency/time.Duration(b.N))
	})

	// 子测试：代理连接延迟
	b.Run("ProxiedConnection", func(b *testing.B) {
		backend := startEchoServer(b, "tcp")
		defer backend.Close()

		proxy := startSimpleProxy(b, backend.Addr().String())
		defer proxy.Close()

		// 预建立连接
		conn, err := net.Dial("tcp", proxy.Addr().String())
		if err != nil {
			b.Fatalf("Failed to connect via proxy: %v", err)
		}
		defer conn.Close()

		b.ResetTimer()
		b.ReportAllocs()

		var totalLatency time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()

			_, err = conn.Write(payload)
			if err != nil {
				b.Fatalf("Failed to write: %v", err)
			}

			response := make([]byte, 64)
			_, err = io.ReadFull(conn, response)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			latency := time.Since(start)
			totalLatency += latency

			if i == 0 {
				b.Logf("Proxied connection first request latency: %v", latency)
			}
		}
		b.Logf("Proxied connection average latency: %v", totalLatency/time.Duration(b.N))
	})
}

// BenchmarkProxyOverhead_ConnectionEstablishment 对比连接建立开销
func BenchmarkProxyOverhead_ConnectionEstablishment(b *testing.B) {
	// 子测试：直接连接建立
	b.Run("DirectConnection", func(b *testing.B) {
		backend := startEchoServer(b, "tcp")
		defer backend.Close()

		b.ResetTimer()
		b.ReportAllocs()

		var totalTime time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			conn, err := net.Dial("tcp", backend.Addr().String())
			if err != nil {
				b.Fatalf("Failed to connect: %v", err)
			}
			establishTime := time.Since(start)
			totalTime += establishTime
			conn.Close()

			if i == 0 {
				b.Logf("Direct connection establishment time: %v", establishTime)
			}
		}
		b.Logf("Direct connection average establishment time: %v", totalTime/time.Duration(b.N))
	})

	// 子测试：代理连接建立
	b.Run("ProxiedConnection", func(b *testing.B) {
		backend := startEchoServer(b, "tcp")
		defer backend.Close()

		proxy := startSimpleProxy(b, backend.Addr().String())
		defer proxy.Close()

		b.ResetTimer()
		b.ReportAllocs()

		var totalTime time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			conn, err := net.Dial("tcp", proxy.Addr().String())
			if err != nil {
				b.Fatalf("Failed to connect via proxy: %v", err)
			}
			establishTime := time.Since(start)
			totalTime += establishTime
			conn.Close()

			if i == 0 {
				b.Logf("Proxied connection establishment time: %v", establishTime)
			}
		}
		b.Logf("Proxied connection average establishment time: %v", totalTime/time.Duration(b.N))
	})
}

// BenchmarkProxyOverhead_Memory 对比内存使用开销
func BenchmarkProxyOverhead_Memory(b *testing.B) {
	payload := make([]byte, 1024)
	rand.Read(payload)

	// 子测试：直接连接内存使用
	b.Run("DirectConnection", func(b *testing.B) {
		backend := startEchoServer(b, "tcp")
		defer backend.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			conn, err := net.Dial("tcp", backend.Addr().String())
			if err != nil {
				b.Fatalf("Failed to connect: %v", err)
			}

			_, err = conn.Write(payload)
			if err != nil {
				b.Fatalf("Failed to write: %v", err)
			}

			response := make([]byte, 1024)
			_, err = io.ReadFull(conn, response)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			conn.Close()
		}
	})

	// 子测试：代理连接内存使用
	b.Run("ProxiedConnection", func(b *testing.B) {
		backend := startEchoServer(b, "tcp")
		defer backend.Close()

		proxy := startSimpleProxy(b, backend.Addr().String())
		defer proxy.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			conn, err := net.Dial("tcp", proxy.Addr().String())
			if err != nil {
				b.Fatalf("Failed to connect via proxy: %v", err)
			}

			_, err = conn.Write(payload)
			if err != nil {
				b.Fatalf("Failed to write: %v", err)
			}

			response := make([]byte, 1024)
			_, err = io.ReadFull(conn, response)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			conn.Close()
		}
	})
}

// BenchmarkProxyOverhead_ConcurrentLoad 对比并发负载下的开销
func BenchmarkProxyOverhead_ConcurrentLoad(b *testing.B) {
	concurrencyLevels := []int{1, 5, 10, 20}
	
	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			payload := make([]byte, 1024)
			rand.Read(payload)

			// 子测试：直接连接并发
			b.Run("DirectConnection", func(b *testing.B) {
				backend := startEchoServer(b, "tcp")
				defer backend.Close()

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					done := make(chan struct{}, concurrency)
					
					for j := 0; j < concurrency; j++ {
						go func() {
							defer func() { done <- struct{}{} }()

							conn, err := net.Dial("tcp", backend.Addr().String())
							if err != nil {
								b.Errorf("Failed to connect: %v", err)
								return
							}
							defer conn.Close()

							_, err = conn.Write(payload)
							if err != nil {
								b.Errorf("Failed to write: %v", err)
								return
							}

							response := make([]byte, 1024)
							_, err = io.ReadFull(conn, response)
							if err != nil {
								b.Errorf("Failed to read: %v", err)
								return
							}
						}()
					}

					// 等待所有协程完成
					for j := 0; j < concurrency; j++ {
						<-done
					}
				}
			})

			// 子测试：代理连接并发
			b.Run("ProxiedConnection", func(b *testing.B) {
				backend := startEchoServer(b, "tcp")
				defer backend.Close()

				proxy := startSimpleProxy(b, backend.Addr().String())
				defer proxy.Close()

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					done := make(chan struct{}, concurrency)
					
					for j := 0; j < concurrency; j++ {
						go func() {
							defer func() { done <- struct{}{} }()

							conn, err := net.Dial("tcp", proxy.Addr().String())
							if err != nil {
								b.Errorf("Failed to connect via proxy: %v", err)
								return
							}
							defer conn.Close()

							_, err = conn.Write(payload)
							if err != nil {
								b.Errorf("Failed to write: %v", err)
								return
							}

							response := make([]byte, 1024)
							_, err = io.ReadFull(conn, response)
							if err != nil {
								b.Errorf("Failed to read: %v", err)
								return
							}
						}()
					}

					// 等待所有协程完成
					for j := 0; j < concurrency; j++ {
						<-done
					}
				}
			})
		})
	}
}

// BenchmarkProxyOverhead_UDP UDP协议代理开销对比
func BenchmarkProxyOverhead_UDP(b *testing.B) {
	payloadSizes := []int{512, 1024, 8192}

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("PayloadSize_%dB", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)

			// 子测试：直接UDP连接
			b.Run("DirectConnection", func(b *testing.B) {
				backend := startUDPEchoServer(b)
				defer backend.Close()

				b.ResetTimer()
				b.SetBytes(int64(size))
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					conn, err := net.Dial("udp", backend.LocalAddr().String())
					if err != nil {
						b.Fatalf("Failed to connect to UDP backend: %v", err)
					}

					_, err = conn.Write(payload)
					if err != nil {
						b.Fatalf("Failed to write UDP: %v", err)
					}

					response := make([]byte, size)
					_, err = conn.Read(response)
					if err != nil {
						b.Fatalf("Failed to read UDP: %v", err)
					}

					conn.Close()
				}
			})

			// 子测试：UDP代理连接 (简化实现)
			b.Run("ProxiedConnection", func(b *testing.B) {
				backend := startUDPEchoServer(b)
				defer backend.Close()

				proxy := startSimpleUDPProxy(b, backend.LocalAddr().String())
				defer proxy.Close()

				b.ResetTimer()
				b.SetBytes(int64(size))
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					conn, err := net.Dial("udp", proxy.LocalAddr().String())
					if err != nil {
						b.Fatalf("Failed to connect to UDP proxy: %v", err)
					}

					_, err = conn.Write(payload)
					if err != nil {
						b.Fatalf("Failed to write UDP via proxy: %v", err)
					}

					response := make([]byte, size)
					_, err = conn.Read(response)
					if err != nil {
						b.Fatalf("Failed to read UDP via proxy: %v", err)
					}

					conn.Close()
				}
			})
		})
	}
}

// 辅助函数：启动简单代理服务器
func startSimpleProxy(b *testing.B, backendAddr string) net.Listener {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to start proxy: %v", err)
	}

	go func() {
		for {
			clientConn, err := listener.Accept()
			if err != nil {
				return
			}

			go func(client net.Conn) {
				defer client.Close()

				// 连接到后端
				backend, err := net.Dial("tcp", backendAddr)
				if err != nil {
					return
				}
				defer backend.Close()

				// 双向数据转发
				done := make(chan struct{}, 2)

				// 客户端 -> 后端
				go func() {
					defer func() { done <- struct{}{} }()
					io.Copy(backend, client)
				}()

				// 后端 -> 客户端
				go func() {
					defer func() { done <- struct{}{} }()
					io.Copy(client, backend)
				}()

				// 等待任一方向完成
				<-done
			}(clientConn)
		}
	}()

	return listener
}

// 辅助函数：启动简单UDP代理
func startSimpleUDPProxy(b *testing.B, backendAddr string) *net.UDPConn {
	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to resolve UDP proxy address: %v", err)
	}

	proxyConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		b.Fatalf("Failed to start UDP proxy: %v", err)
	}

	go func() {
		buffer := make([]byte, 65536)
		for {
			n, clientAddr, err := proxyConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}

			// 转发到后端
			backendConn, err := net.Dial("udp", backendAddr)
			if err != nil {
				continue
			}

			_, err = backendConn.Write(buffer[:n])
			if err != nil {
				backendConn.Close()
				continue
			}

			// 读取后端响应
			response := make([]byte, 65536)
			respN, err := backendConn.Read(response)
			if err != nil {
				backendConn.Close()
				continue
			}

			// 发送回客户端
			proxyConn.WriteToUDP(response[:respN], clientAddr)
			backendConn.Close()
		}
	}()

	return proxyConn
}

// 辅助函数：启动UDP回音服务器
func startUDPEchoServer(b *testing.B) *net.UDPConn {
	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		b.Fatalf("Failed to start UDP echo server: %v", err)
	}

	go func() {
		buffer := make([]byte, 65536)
		for {
			n, clientAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			conn.WriteToUDP(buffer[:n], clientAddr)
		}
	}()

	return conn
}

// 辅助函数：启动TCP回音服务器
func startEchoServer(b *testing.B, protocol string) net.Listener {
	listener, err := net.Listen(protocol, "localhost:0")
	if err != nil {
		b.Fatalf("Failed to start echo server: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // 回音服务
			}(conn)
		}
	}()

	return listener
}