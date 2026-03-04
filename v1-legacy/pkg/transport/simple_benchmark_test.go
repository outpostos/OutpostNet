package transport

import (
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// BenchmarkDirectTCPThroughput 直接 TCP 连接吞吐量基准测试
func BenchmarkDirectTCPThroughput(b *testing.B) {
	payloadSizes := []int{1024, 4096, 16384, 65536, 1048576} // 1KB to 1MB

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("PayloadSize_%dB", size), func(b *testing.B) {
			// 创建测试数据
			payload := make([]byte, size)
			rand.Read(payload)

			// 启动回音服务器
			listener, err := net.Listen("tcp", "localhost:0")
			if err != nil {
				b.Fatalf("Failed to start server: %v", err)
			}
			defer listener.Close()

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

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 建立连接
				conn, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					b.Fatalf("Failed to connect: %v", err)
				}

				// 发送数据
				_, err = conn.Write(payload)
				if err != nil {
					b.Fatalf("Failed to write: %v", err)
				}

				// 读取回音
				response := make([]byte, size)
				_, err = io.ReadFull(conn, response)
				if err != nil {
					b.Fatalf("Failed to read: %v", err)
				}

				conn.Close()
			}
		})
	}
}

// BenchmarkDirectTCPLatency 直接 TCP 连接延迟基准测试
func BenchmarkDirectTCPLatency(b *testing.B) {
	// 小包测试延迟
	payload := make([]byte, 64)
	rand.Read(payload)

	// 启动回音服务器
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()

	// 预建立连接
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()

		// 发送数据
		_, err = conn.Write(payload)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}

		// 读取响应
		response := make([]byte, 64)
		_, err = io.ReadFull(conn, response)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}

		latency := time.Since(start)
		if i == 0 {
			b.Logf("First request latency: %v", latency)
		}
	}
}

// BenchmarkUDPThroughput UDP 吞吐量基准测试
func BenchmarkUDPThroughput(b *testing.B) {
	payloadSizes := []int{512, 1024, 8192, 32768} // UDP 包大小限制

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("PayloadSize_%dB", size), func(b *testing.B) {
			payload := make([]byte, size)
			rand.Read(payload)

			// 启动 UDP 回音服务器
			serverAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
			if err != nil {
				b.Fatalf("Failed to resolve UDP address: %v", err)
			}

			serverConn, err := net.ListenUDP("udp", serverAddr)
			if err != nil {
				b.Fatalf("Failed to start UDP server: %v", err)
			}
			defer serverConn.Close()

			go func() {
				buffer := make([]byte, 65536)
				for {
					n, clientAddr, err := serverConn.ReadFromUDP(buffer)
					if err != nil {
						return
					}
					serverConn.WriteToUDP(buffer[:n], clientAddr)
				}
			}()

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 建立客户端连接
				clientConn, err := net.Dial("udp", serverConn.LocalAddr().String())
				if err != nil {
					b.Fatalf("Failed to connect to UDP server: %v", err)
				}

				// 发送数据
				_, err = clientConn.Write(payload)
				if err != nil {
					b.Fatalf("Failed to write UDP data: %v", err)
				}

				// 读取响应
				response := make([]byte, size)
				_, err = clientConn.Read(response)
				if err != nil {
					b.Fatalf("Failed to read UDP response: %v", err)
				}

				clientConn.Close()
			}
		})
	}
}

// BenchmarkConnectionEstablishment 连接建立性能测试
func BenchmarkConnectionEstablishment(b *testing.B) {
	// 启动服务器
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close() // 立即关闭，只测试连接建立
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			b.Fatalf("Failed to connect: %v", err)
		}
		conn.Close()
	}
}

// BenchmarkConcurrentConnections 并发连接性能测试
func BenchmarkConcurrentConnections(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			// 启动服务器
			listener, err := net.Listen("tcp", "localhost:0")
			if err != nil {
				b.Fatalf("Failed to start server: %v", err)
			}
			defer listener.Close()

			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					go func(c net.Conn) {
						defer c.Close()
						io.Copy(c, c)
					}(conn)
				}
			}()

			payload := make([]byte, 1024)
			rand.Read(payload)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 并发连接
				done := make(chan struct{}, concurrency)
				for j := 0; j < concurrency; j++ {
					go func() {
						defer func() { done <- struct{}{} }()

						conn, err := net.Dial("tcp", listener.Addr().String())
						if err != nil {
							b.Errorf("Failed to connect: %v", err)
							return
						}
						defer conn.Close()

						// 发送和接收数据
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

				// 等待所有连接完成
				for j := 0; j < concurrency; j++ {
					<-done
				}
			}
		})
	}
}

// BenchmarkBandwidthUtilization 带宽利用率测试
func BenchmarkBandwidthUtilization(b *testing.B) {
	// 大量数据传输测试
	payload := make([]byte, 1048576) // 1MB
	rand.Read(payload)

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()

	b.ResetTimer()
	b.SetBytes(1048576) // 1MB per operation
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			b.Fatalf("Failed to connect: %v", err)
		}

		// 发送 1MB 数据
		_, err = conn.Write(payload)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}

		// 读取 1MB 响应
		response := make([]byte, 1048576)
		_, err = io.ReadFull(conn, response)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}

		conn.Close()
	}
}