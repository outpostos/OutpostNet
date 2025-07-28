package protocol

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// TestProtocolManager 测试协议管理器
func TestProtocolManager(t *testing.T) {
	pm := NewProtocolManager()

	// 测试获取支持的协议
	protocols := []string{"tcp", "udp", "stcp", "sudp"}
	for _, protocol := range protocols {
		handler, err := pm.GetHandler(protocol)
		if err != nil {
			t.Errorf("Failed to get handler for protocol %s: %v", protocol, err)
		}
		if handler.GetProtocol() != protocol {
			t.Errorf("Handler protocol mismatch: expected %s, got %s", protocol, handler.GetProtocol())
		}
	}

	// 测试不支持的协议
	_, err := pm.GetHandler("unknown")
	if err == nil {
		t.Error("Expected error for unknown protocol")
	}
}

// TestTCPHandler 测试TCP处理器
func TestTCPHandler(t *testing.T) {
	handler := NewTCPHandler()

	// 测试协议信息
	if handler.GetProtocol() != "tcp" {
		t.Errorf("Expected protocol 'tcp', got '%s'", handler.GetProtocol())
	}

	if handler.SupportsEncryption() {
		t.Error("TCP handler should not support encryption")
	}

	// 测试关闭
	if err := handler.Close(); err != nil {
		t.Errorf("Failed to close TCP handler: %v", err)
	}
}

// TestUDPHandler 测试UDP处理器
func TestUDPHandler(t *testing.T) {
	handler := NewUDPHandler()

	// 测试协议信息
	if handler.GetProtocol() != "udp" {
		t.Errorf("Expected protocol 'udp', got '%s'", handler.GetProtocol())
	}

	if handler.SupportsEncryption() {
		t.Error("UDP handler should not support encryption")
	}

	// 测试会话管理
	if len(handler.sessions) != 0 {
		t.Error("New UDP handler should have no sessions")
	}

	// 测试关闭
	if err := handler.Close(); err != nil {
		t.Errorf("Failed to close UDP handler: %v", err)
	}
}

// TestSTCPHandler 测试STCP处理器
func TestSTCPHandler(t *testing.T) {
	handler := NewSTCPHandler()

	// 测试协议信息
	if handler.GetProtocol() != "stcp" {
		t.Errorf("Expected protocol 'stcp', got '%s'", handler.GetProtocol())
	}

	if !handler.SupportsEncryption() {
		t.Error("STCP handler should support encryption")
	}

	// 测试TLS配置
	if handler.tlsConfig == nil {
		t.Error("STCP handler should have TLS config")
	}

	// 测试关闭
	if err := handler.Close(); err != nil {
		t.Errorf("Failed to close STCP handler: %v", err)
	}
}

// TestSUDPHandler 测试SUDP处理器
func TestSUDPHandler(t *testing.T) {
	handler := NewSUDPHandler()

	// 测试协议信息
	if handler.GetProtocol() != "sudp" {
		t.Errorf("Expected protocol 'sudp', got '%s'", handler.GetProtocol())
	}

	if !handler.SupportsEncryption() {
		t.Error("SUDP handler should support encryption")
	}

	// 测试关闭
	if err := handler.Close(); err != nil {
		t.Errorf("Failed to close SUDP handler: %v", err)
	}
}

// TestProtocolHandlerIntegration 集成测试
func TestProtocolHandlerIntegration(t *testing.T) {
	// 跳过需要实际网络连接的测试
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pm := NewProtocolManager()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建测试端点
	endpoint := &types.Endpoint{
		IP:   "127.0.0.1",
		Port: 22, // SSH端口，通常是开放的
	}

	// 测试TCP连接（如果SSH服务可用）
	handler, err := pm.GetHandler("tcp")
	if err != nil {
		t.Fatalf("Failed to get TCP handler: %v", err)
	}

	// 创建模拟客户端连接
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// 在goroutine中处理连接
	go func() {
		err := handler.HandleConnection(ctx, serverConn, endpoint)
		if err != nil {
			t.Logf("TCP handler returned error (expected): %v", err)
		}
	}()

	// 测试数据传输
	testData := []byte("test")
	clientConn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	_, err = clientConn.Write(testData)
	if err != nil {
		t.Logf("Write to client connection failed (may be expected): %v", err)
	}
}

// TestProtocolStats 测试协议统计
func TestProtocolStats(t *testing.T) {
	pm := NewProtocolManager()

	// 获取初始统计
	stats := pm.GetStats()
	if len(stats) != 4 {
		t.Errorf("Expected 4 protocol stats, got %d", len(stats))
	}

	// 检查每个协议的统计
	for protocol, stat := range stats {
		if stat.Protocol != protocol {
			t.Errorf("Protocol mismatch in stats: expected %s, got %s", protocol, stat.Protocol)
		}
		if stat.ConnectionCount != 0 {
			t.Errorf("Initial connection count should be 0 for %s", protocol)
		}
	}
}

// BenchmarkTCPHandler TCP处理器性能测试
func BenchmarkTCPHandler(b *testing.B) {
	handler := NewTCPHandler()
	ctx := context.Background()
	
	// 创建测试端点
	endpoint := &types.Endpoint{
		IP:   "127.0.0.1",
		Port: 22,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientConn, serverConn := net.Pipe()
		
		// 启动处理器
		go func() {
			handler.HandleConnection(ctx, serverConn, endpoint)
			serverConn.Close()
		}()
		
		// 关闭连接
		clientConn.Close()
	}
}

// BenchmarkUDPSession UDP会话管理性能测试
func BenchmarkUDPSession(b *testing.B) {
	handler := NewUDPHandler()
	
	// 创建测试地址
	clientAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	targetConn, _ := net.DialUDP("udp", nil, clientAddr)
	defer targetConn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sessionKey := "test-session"
		handler.updateSession(sessionKey, clientAddr, targetConn, 100)
	}
}

// TestProtocolCompatibility 测试协议兼容性
func TestProtocolCompatibility(t *testing.T) {
	protocols := []string{"tcp", "udp", "stcp", "sudp"}
	
	for _, protocol := range protocols {
		pm := NewProtocolManager()
		handler, err := pm.GetHandler(protocol)
		if err != nil {
			t.Errorf("Protocol %s not supported: %v", protocol, err)
			continue
		}

		// 测试基本接口
		if handler.GetProtocol() != protocol {
			t.Errorf("Protocol name mismatch for %s", protocol)
		}

		// 测试加密支持
		expectedEncryption := protocol == "stcp" || protocol == "sudp"
		if handler.SupportsEncryption() != expectedEncryption {
			t.Errorf("Encryption support mismatch for %s: expected %v, got %v", 
				protocol, expectedEncryption, handler.SupportsEncryption())
		}

		// 测试关闭
		if err := handler.Close(); err != nil {
			t.Errorf("Failed to close handler for %s: %v", protocol, err)
		}
	}
}

// TestConcurrentProtocolHandling 测试并发协议处理
func TestConcurrentProtocolHandling(t *testing.T) {
	pm := NewProtocolManager()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 创建测试端点
	endpoint := &types.Endpoint{
		IP:   "127.0.0.1",
		Port: 22,
	}

	// 并发测试多个协议
	protocols := []string{"tcp", "udp", "stcp", "sudp"}
	
	for _, protocol := range protocols {
		t.Run(protocol, func(t *testing.T) {
			t.Parallel()
			
			handler, err := pm.GetHandler(protocol)
			if err != nil {
				t.Fatalf("Failed to get handler for %s: %v", protocol, err)
			}

			// 创建模拟连接
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			// 测试连接处理
			go func() {
				err := handler.HandleConnection(ctx, serverConn, endpoint)
				if err != nil {
					t.Logf("Handler for %s returned error (may be expected): %v", protocol, err)
				}
			}()

			// 短暂等待
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// TestProtocolErrorHandling 测试协议错误处理
func TestProtocolErrorHandling(t *testing.T) {
	pm := NewProtocolManager()
	ctx := context.Background()

	// 创建无效端点
	invalidEndpoint := &types.Endpoint{
		IP:   "192.0.2.1", // RFC5737测试地址，不可路由
		Port: 9999,
	}

	protocols := []string{"tcp", "stcp"}
	for _, protocol := range protocols {
		handler, err := pm.GetHandler(protocol)
		if err != nil {
			t.Fatalf("Failed to get handler for %s: %v", protocol, err)
		}

		// 创建模拟连接
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()

		// 测试错误处理
		err = handler.HandleConnection(ctx, serverConn, invalidEndpoint)
		if err == nil {
			t.Errorf("Expected error for invalid endpoint with protocol %s", protocol)
		}

		serverConn.Close()
	}
}