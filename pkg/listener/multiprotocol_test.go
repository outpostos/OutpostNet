package listener

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
)

// TestMultiProtocolListener 测试多协议监听器
func TestMultiProtocolListener(t *testing.T) {
	// 创建测试配置
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr:   "127.0.0.1:0", // 使用随机端口
			UnixSockPath: "",            // 不使用Unix Socket
		},
	}

	// 创建多协议监听器
	mpl, err := NewMultiProtocolListener(cfg)
	if err != nil {
		t.Fatalf("failed to create multi-protocol listener: %v", err)
	}

	// 启动监听器
	if err := mpl.Start(); err != nil {
		t.Fatalf("failed to start multi-protocol listener: %v", err)
	}
	defer mpl.Stop()

	// 验证监听器信息
	listeners := mpl.GetActiveListeners()
	if len(listeners) != 1 {
		t.Errorf("expected 1 listener, got %d", len(listeners))
	}

	if listeners[0].Protocol != "tcp" {
		t.Errorf("expected tcp protocol, got %s", listeners[0].Protocol)
	}

	// 获取实际监听地址
	listenAddr := listeners[0].Address

	// 测试TCP连接
	t.Run("TCPConnection", func(t *testing.T) {
		testTCPConnection(t, mpl, listenAddr)
	})

	// 测试统计信息
	t.Run("Statistics", func(t *testing.T) {
		testStatistics(t, mpl)
	})
}

// TestMultiProtocolListenerDual 测试双协议监听器
func TestMultiProtocolListenerDual(t *testing.T) {
	// 创建临时Unix Socket路径
	tempDir := t.TempDir()
	unixSockPath := filepath.Join(tempDir, "test.sock")

	// 创建测试配置
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr:   "127.0.0.1:0",
			UnixSockPath: unixSockPath,
		},
	}

	// 创建多协议监听器
	mpl, err := NewMultiProtocolListener(cfg)
	if err != nil {
		t.Fatalf("failed to create multi-protocol listener: %v", err)
	}

	// 启动监听器
	if err := mpl.Start(); err != nil {
		t.Fatalf("failed to start multi-protocol listener: %v", err)
	}
	defer mpl.Stop()

	// 验证监听器信息
	listeners := mpl.GetActiveListeners()
	if len(listeners) != 2 {
		t.Errorf("expected 2 listeners, got %d", len(listeners))
	}

	// 验证协议类型
	protocols := make(map[string]bool)
	for _, listener := range listeners {
		protocols[listener.Protocol] = true
	}

	if !protocols["tcp"] {
		t.Error("tcp protocol not found")
	}
	if !protocols["unix"] {
		t.Error("unix protocol not found")
	}

	// 测试TCP连接
	t.Run("TCPConnection", func(t *testing.T) {
		var tcpAddr string
		for _, listener := range listeners {
			if listener.Protocol == "tcp" {
				tcpAddr = listener.Address
				break
			}
		}
		testTCPConnection(t, mpl, tcpAddr)
	})

	// 测试Unix Socket连接
	t.Run("UnixConnection", func(t *testing.T) {
		testUnixConnection(t, mpl, unixSockPath)
	})
}

// testTCPConnection 测试TCP连接
func testTCPConnection(t *testing.T, mpl *MultiProtocolListener, addr string) {
	// 连接到监听器
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to TCP listener: %v", err)
	}
	defer conn.Close()

	// 等待连接被处理
	time.Sleep(100 * time.Millisecond)

	// 验证连接通道中有连接信息
	select {
	case connInfo := <-mpl.GetConnectionChannel():
		if connInfo.Protocol != "tcp" {
			t.Errorf("expected tcp protocol, got %s", connInfo.Protocol)
		}
		if connInfo.Conn == nil {
			t.Error("connection is nil")
		}
		// 关闭接收到的连接
		connInfo.Conn.Close()
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for connection info")
	}
}

// testUnixConnection 测试Unix Socket连接
func testUnixConnection(t *testing.T, mpl *MultiProtocolListener, sockPath string) {
	// 连接到Unix Socket
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to connect to Unix socket: %v", err)
	}
	defer conn.Close()

	// 等待连接被处理
	time.Sleep(100 * time.Millisecond)

	// 验证连接通道中有连接信息
	select {
	case connInfo := <-mpl.GetConnectionChannel():
		if connInfo.Protocol != "unix" {
			t.Errorf("expected unix protocol, got %s", connInfo.Protocol)
		}
		if connInfo.Conn == nil {
			t.Error("connection is nil")
		}
		// 关闭接收到的连接
		connInfo.Conn.Close()
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for connection info")
	}
}

// testStatistics 测试统计信息
func testStatistics(t *testing.T, mpl *MultiProtocolListener) {
	stats := mpl.GetStats()
	if stats == nil {
		t.Error("stats is nil")
		return
	}

	if stats.StartTime.IsZero() {
		t.Error("start time is zero")
	}

	// 初始统计应该为0或正数
	if stats.TotalConnections < 0 {
		t.Errorf("total connections should be >= 0, got %d", stats.TotalConnections)
	}

	if stats.ActiveConnections < 0 {
		t.Errorf("active connections should be >= 0, got %d", stats.ActiveConnections)
	}
}

// TestUnixSocketPermissions 测试Unix Socket权限设置
func TestUnixSocketPermissions(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	// 创建临时Unix Socket路径
	tempDir := t.TempDir()
	unixSockPath := filepath.Join(tempDir, "perm_test.sock")

	// 创建测试配置
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr:   "",
			UnixSockPath: unixSockPath,
		},
	}

	// 创建多协议监听器
	mpl, err := NewMultiProtocolListener(cfg)
	if err != nil {
		t.Fatalf("failed to create multi-protocol listener: %v", err)
	}

	// 设置权限
	mpl.SetUnixSocketPermissions(0600, &UnixSocketOwner{
		UID: os.Getuid(),
		GID: os.Getgid(),
	})

	// 启动监听器
	if err := mpl.Start(); err != nil {
		t.Fatalf("failed to start multi-protocol listener: %v", err)
	}
	defer mpl.Stop()

	// 验证Unix Socket文件权限
	info, err := os.Stat(unixSockPath)
	if err != nil {
		t.Fatalf("failed to stat unix socket: %v", err)
	}

	if info.Mode().Perm() != 0600 {
		t.Errorf("expected permission 0600, got %o", info.Mode().Perm())
	}
}

// TestConnectionTracker 测试连接跟踪器
func TestConnectionTracker(t *testing.T) {
	tracker := NewConnectionTracker()

	// 创建测试连接信息
	connInfo := ConnectionInfo{
		Protocol:   "tcp",
		RemoteAddr: "127.0.0.1:12345",
		LocalAddr:  "127.0.0.1:7000",
		Timestamp:  time.Now(),
	}

	// 跟踪连接
	connID := tracker.TrackConnection(connInfo)
	if connID == "" {
		t.Error("connection ID is empty")
	}

	// 验证连接存在
	connections := tracker.GetConnections()
	if len(connections) != 1 {
		t.Errorf("expected 1 connection, got %d", len(connections))
	}

	if _, exists := connections[connID]; !exists {
		t.Error("connection not found in tracker")
	}

	// 按协议获取连接
	tcpConnections := tracker.GetConnectionsByProtocol("tcp")
	if len(tcpConnections) != 1 {
		t.Errorf("expected 1 TCP connection, got %d", len(tcpConnections))
	}

	udpConnections := tracker.GetConnectionsByProtocol("udp")
	if len(udpConnections) != 0 {
		t.Errorf("expected 0 UDP connections, got %d", len(udpConnections))
	}

	// 更新活跃度
	tracker.UpdateActivity(connID, 100, 200)

	// 验证更新
	updatedConnections := tracker.GetConnections()
	conn := updatedConnections[connID]
	if conn.BytesSent != 100 {
		t.Errorf("expected 100 bytes sent, got %d", conn.BytesSent)
	}
	if conn.BytesReceived != 200 {
		t.Errorf("expected 200 bytes received, got %d", conn.BytesReceived)
	}

	// 取消跟踪连接
	tracker.UntrackConnection(connID)

	// 验证连接已移除
	finalConnections := tracker.GetConnections()
	if len(finalConnections) != 0 {
		t.Errorf("expected 0 connections after untracking, got %d", len(finalConnections))
	}
}

// TestUnixSocketUtils 测试Unix Socket工具函数
func TestUnixSocketUtils(t *testing.T) {
	utils := &UnixSocketUtils{}
	tempDir := t.TempDir()
	socketPath := filepath.Join(tempDir, "test_utils.sock")

	// 测试目录创建
	if err := utils.CreateUnixSocketDir(socketPath, 0755); err != nil {
		t.Fatalf("failed to create unix socket directory: %v", err)
	}

	// 验证目录存在
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Error("unix socket directory was not created")
	}

	// 创建监听器测试socket是否在使用
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to create unix listener: %v", err)
	}

	// 测试socket是否在使用中
	if !utils.IsUnixSocketInUse(socketPath) {
		t.Error("socket should be in use")
	}

	// 关闭监听器
	listener.Close()

	// 测试socket不再使用
	if utils.IsUnixSocketInUse(socketPath) {
		t.Error("socket should not be in use after closing")
	}

	// 重新创建socket测试获取信息
	listener, err = net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to recreate unix listener: %v", err)
	}
	defer listener.Close()

	// 测试获取socket信息
	info, err := utils.GetUnixSocketInfo(socketPath)
	if err != nil {
		t.Fatalf("failed to get unix socket info: %v", err)
	}

	if info.Path != socketPath {
		t.Errorf("expected path %s, got %s", socketPath, info.Path)
	}

	if info.Mode&os.ModeSocket == 0 {
		t.Error("file should be a socket")
	}
}

// BenchmarkMultiProtocolListener 多协议监听器性能测试
func BenchmarkMultiProtocolListener(b *testing.B) {
	// 创建测试配置
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr:   "127.0.0.1:0",
			UnixSockPath: "",
		},
	}

	// 创建多协议监听器
	mpl, err := NewMultiProtocolListener(cfg)
	if err != nil {
		b.Fatalf("failed to create multi-protocol listener: %v", err)
	}

	// 启动监听器
	if err := mpl.Start(); err != nil {
		b.Fatalf("failed to start multi-protocol listener: %v", err)
	}
	defer mpl.Stop()

	// 获取监听地址
	listeners := mpl.GetActiveListeners()
	if len(listeners) == 0 {
		b.Fatal("no active listeners")
	}
	addr := listeners[0].Address

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 创建连接
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err != nil {
				b.Errorf("failed to connect: %v", err)
				continue
			}

			// 立即关闭连接
			conn.Close()

			// 清理连接信息
			select {
			case connInfo := <-mpl.GetConnectionChannel():
				connInfo.Conn.Close()
			case <-time.After(100 * time.Millisecond):
				// 超时，继续下一次测试
			}
		}
	})
}

// BenchmarkConnectionTracker 连接跟踪器性能测试
func BenchmarkConnectionTracker(b *testing.B) {
	tracker := NewConnectionTracker()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			connInfo := ConnectionInfo{
				Protocol:   "tcp",
				RemoteAddr: "127.0.0.1:12345",
				LocalAddr:  "127.0.0.1:7000",
				Timestamp:  time.Now(),
			}

			// 跟踪连接
			connID := tracker.TrackConnection(connInfo)

			// 更新活跃度
			tracker.UpdateActivity(connID, 100, 200)

			// 取消跟踪
			tracker.UntrackConnection(connID)
		}
	})
}

// TestMultiProtocolListenerEdgeCases 测试边缘情况
func TestMultiProtocolListenerEdgeCases(t *testing.T) {
	t.Run("InvalidConfig", func(t *testing.T) {
		// 测试空配置
		_, err := NewMultiProtocolListener(nil)
		if err == nil {
			t.Error("expected error for nil config")
		}
	})

	t.Run("InvalidUnixSocketPath", func(t *testing.T) {
		// 测试无效Unix Socket路径
		cfg := &config.ServerConfig{
			Server: config.ServerSpec{
				ListenAddr:   "",
				UnixSockPath: "/invalid/path/that/does/not/exist/test.sock",
			},
		}

		mpl, err := NewMultiProtocolListener(cfg)
		if err != nil {
			t.Fatalf("failed to create listener: %v", err)
		}

		// 启动应该失败
		if err := mpl.Start(); err == nil {
			t.Error("expected error for invalid unix socket path")
			mpl.Stop()
		}
	})

	t.Run("StopWithoutStart", func(t *testing.T) {
		cfg := &config.ServerConfig{
			Server: config.ServerSpec{
				ListenAddr:   "127.0.0.1:0",
				UnixSockPath: "",
			},
		}

		mpl, err := NewMultiProtocolListener(cfg)
		if err != nil {
			t.Fatalf("failed to create listener: %v", err)
		}

		// 直接停止不应该报错
		if err := mpl.Stop(); err != nil {
			t.Errorf("stop without start should not error: %v", err)
		}
	})
}

// TestConcurrentConnections 测试并发连接
func TestConcurrentConnections(t *testing.T) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr:   "127.0.0.1:0",
			UnixSockPath: "",
		},
	}

	mpl, err := NewMultiProtocolListener(cfg)
	if err != nil {
		t.Fatalf("failed to create multi-protocol listener: %v", err)
	}

	if err := mpl.Start(); err != nil {
		t.Fatalf("failed to start multi-protocol listener: %v", err)
	}
	defer mpl.Stop()

	listeners := mpl.GetActiveListeners()
	if len(listeners) == 0 {
		t.Fatal("no active listeners")
	}
	addr := listeners[0].Address

	// 并发创建多个连接
	numConnections := 10
	done := make(chan bool, numConnections)

	for i := 0; i < numConnections; i++ {
		go func(id int) {
			defer func() { done <- true }()

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Errorf("connection %d failed: %v", id, err)
				return
			}
			defer conn.Close()

			// 等待一段时间
			time.Sleep(100 * time.Millisecond)
		}(i)
	}

	// 处理连接
	var receivedConnections int
	timeout := time.After(10 * time.Second)

connectionLoop:
	for receivedConnections < numConnections {
		select {
		case connInfo := <-mpl.GetConnectionChannel():
			connInfo.Conn.Close()
			receivedConnections++
		case <-timeout:
			t.Errorf("timeout waiting for connections, received %d of %d", receivedConnections, numConnections)
			break connectionLoop
		}
	}

	// 等待所有goroutine完成
	for i := 0; i < numConnections; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for goroutines to complete")
		}
	}

	// 验证统计信息
	stats := mpl.GetStats()
	if stats.TotalConnections < int64(receivedConnections) {
		t.Errorf("expected at least %d total connections, got %d", receivedConnections, stats.TotalConnections)
	}
}