package listener

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
)

// MultiProtocolListener 多协议监听器
type MultiProtocolListener struct {
	config          *config.ServerConfig
	addressResolver *config.AddressResolver
	
	// 监听器列表
	listeners    []net.Listener
	listenersMu  sync.RWMutex
	
	// 连接通道
	connectionChan chan ConnectionInfo
	errorChan      chan error
	
	// 控制
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopped   bool
	stopMu    sync.Mutex
	
	// 统计信息
	stats    *ListenerStats
	statsMu  sync.RWMutex
	
	// Unix Socket特殊处理
	unixSocketPerms os.FileMode
	unixSocketOwner *UnixSocketOwner
}

// ConnectionInfo 连接信息
type ConnectionInfo struct {
	Conn     net.Conn
	Protocol string // "tcp", "unix"
	RemoteAddr string
	LocalAddr  string
	Timestamp  time.Time
}

// ListenerStats 监听器统计信息
type ListenerStats struct {
	TCPConnections       int64 `json:"tcp_connections"`
	UnixConnections      int64 `json:"unix_connections"`
	TotalConnections     int64 `json:"total_connections"`
	ActiveConnections    int64 `json:"active_connections"`
	ConnectionErrors     int64 `json:"connection_errors"`
	BytesTransferred     int64 `json:"bytes_transferred"`
	StartTime           time.Time `json:"start_time"`
	LastConnectionTime  time.Time `json:"last_connection_time"`
}

// UnixSocketOwner Unix Socket所有权配置
type UnixSocketOwner struct {
	UID int
	GID int
}

// MultiProtocolConfig 多协议监听器配置
type MultiProtocolConfig struct {
	ListenAddr      string
	UnixSocketPath  string
	UnixSocketPerms os.FileMode
	UnixSocketOwner *UnixSocketOwner
	MaxConnections  int
	ConnectionTimeout time.Duration
	KeepAlive       bool
}

// NewMultiProtocolListener 创建多协议监听器
func NewMultiProtocolListener(cfg *config.ServerConfig) (*MultiProtocolListener, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	mpl := &MultiProtocolListener{
		config:          cfg,
		addressResolver: config.NewAddressResolver("tcp"),
		connectionChan:  make(chan ConnectionInfo, 100),
		errorChan:       make(chan error, 10),
		ctx:             ctx,
		cancel:          cancel,
		stats: &ListenerStats{
			StartTime: time.Now(),
		},
		unixSocketPerms: 0666, // 默认权限
	}
	
	return mpl, nil
}

// SetUnixSocketPermissions 设置Unix Socket权限
func (mpl *MultiProtocolListener) SetUnixSocketPermissions(perms os.FileMode, owner *UnixSocketOwner) {
	mpl.unixSocketPerms = perms
	mpl.unixSocketOwner = owner
}

// Start 启动监听器
func (mpl *MultiProtocolListener) Start() error {
	// 确定监听协议
	listenProto := "tcp"
	if mpl.config.Server.UnixSockPath != "" {
		listenProto = "dual"
	}
	
	// 创建监听器
	listeners, err := mpl.addressResolver.Listen(
		listenProto,
		mpl.config.Server.ListenAddr,
		mpl.config.Server.UnixSockPath,
	)
	if err != nil {
		return fmt.Errorf("failed to create listeners: %w", err)
	}
	
	mpl.listenersMu.Lock()
	mpl.listeners = listeners
	mpl.listenersMu.Unlock()
	
	// 设置Unix Socket权限
	if err := mpl.setupUnixSocketPermissions(); err != nil {
		return fmt.Errorf("failed to setup unix socket permissions: %w", err)
	}
	
	// 启动监听goroutines
	for _, listener := range listeners {
		mpl.wg.Add(1)
		go mpl.acceptConnections(listener)
	}
	
	log.Printf("Multi-protocol listener started with %d listeners", len(listeners))
	return nil
}

// Stop 停止监听器
func (mpl *MultiProtocolListener) Stop() error {
	mpl.stopMu.Lock()
	defer mpl.stopMu.Unlock()
	
	if mpl.stopped {
		return nil // 已经停止，直接返回
	}
	mpl.stopped = true
	
	// 取消上下文
	if mpl.cancel != nil {
		mpl.cancel()
	}
	
	// 关闭所有监听器
	mpl.listenersMu.Lock()
	for _, listener := range mpl.listeners {
		listener.Close()
	}
	mpl.listenersMu.Unlock()
	
	// 等待所有goroutines结束
	mpl.wg.Wait()
	
	// 清理Unix Socket文件
	if err := mpl.cleanupUnixSockets(); err != nil {
		log.Printf("Warning: failed to cleanup unix sockets: %v", err)
	}
	
	// 关闭通道
	if mpl.connectionChan != nil {
		close(mpl.connectionChan)
		mpl.connectionChan = nil
	}
	if mpl.errorChan != nil {
		close(mpl.errorChan)
		mpl.errorChan = nil
	}
	
	log.Println("Multi-protocol listener stopped")
	return nil
}

// GetConnectionChannel 获取连接通道
func (mpl *MultiProtocolListener) GetConnectionChannel() <-chan ConnectionInfo {
	mpl.stopMu.Lock()
	defer mpl.stopMu.Unlock()
	return mpl.connectionChan
}

// GetErrorChannel 获取错误通道
func (mpl *MultiProtocolListener) GetErrorChannel() <-chan error {
	mpl.stopMu.Lock()
	defer mpl.stopMu.Unlock()
	return mpl.errorChan
}

// GetStats 获取统计信息
func (mpl *MultiProtocolListener) GetStats() *ListenerStats {
	mpl.statsMu.RLock()
	defer mpl.statsMu.RUnlock()
	
	// 返回统计信息的副本
	statsCopy := *mpl.stats
	return &statsCopy
}

// GetActiveListeners 获取活跃监听器信息
func (mpl *MultiProtocolListener) GetActiveListeners() []ListenerInfo {
	mpl.listenersMu.RLock()
	defer mpl.listenersMu.RUnlock()
	
	info := make([]ListenerInfo, 0, len(mpl.listeners))
	for _, listener := range mpl.listeners {
		addr := listener.Addr()
		protocol := "tcp"
		if addr.Network() == "unix" {
			protocol = "unix"
		}
		
		info = append(info, ListenerInfo{
			Protocol: protocol,
			Address:  addr.String(),
			Network:  addr.Network(),
		})
	}
	
	return info
}

// ListenerInfo 监听器信息
type ListenerInfo struct {
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
	Network  string `json:"network"`
}

// acceptConnections 接受连接
func (mpl *MultiProtocolListener) acceptConnections(listener net.Listener) {
	defer mpl.wg.Done()
	
	protocol := "tcp"
	if listener.Addr().Network() == "unix" {
		protocol = "unix"
	}
	
	log.Printf("Starting to accept %s connections on %s", protocol, listener.Addr())
	
	for {
		select {
		case <-mpl.ctx.Done():
			return
		default:
		}
		
		// 设置接受超时
		if tcpListener, ok := listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}
		
		conn, err := listener.Accept()
		if err != nil {
			// 检查是否是超时错误或者上下文取消
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			
			select {
			case <-mpl.ctx.Done():
				return
			default:
				mpl.recordError(fmt.Errorf("accept error on %s listener: %w", protocol, err))
				continue
			}
		}
		
		// 配置连接
		if err := mpl.configureConnection(conn, protocol); err != nil {
			mpl.recordError(fmt.Errorf("failed to configure %s connection: %w", protocol, err))
			conn.Close()
			continue
		}
		
		// 创建连接信息
		connInfo := ConnectionInfo{
			Conn:      conn,
			Protocol:  protocol,
			RemoteAddr: conn.RemoteAddr().String(),
			LocalAddr:  conn.LocalAddr().String(),
			Timestamp: time.Now(),
		}
		
		// 更新统计信息
		mpl.updateStats(protocol, true)
		
		// 发送到连接通道
		select {
		case mpl.connectionChan <- connInfo:
		case <-mpl.ctx.Done():
			conn.Close()
			return
		default:
			// 通道满了，记录警告但不阻塞
			mpl.recordError(fmt.Errorf("connection channel full, dropping %s connection", protocol))
			conn.Close()
		}
	}
}

// configureConnection 配置连接
func (mpl *MultiProtocolListener) configureConnection(conn net.Conn, protocol string) error {
	// TCP连接配置
	if tcpConn, ok := conn.(*net.TCPConn); ok && protocol == "tcp" {
		// 启用Keep-Alive
		if err := tcpConn.SetKeepAlive(true); err != nil {
			log.Printf("Warning: failed to set keep-alive: %v", err)
		}
		
		// 设置Keep-Alive间隔
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			log.Printf("Warning: failed to set keep-alive period: %v", err)
		}
		
		// 设置读写缓冲区
		if err := tcpConn.SetReadBuffer(64 * 1024); err != nil {
			log.Printf("Warning: failed to set read buffer: %v", err)
		}
		
		if err := tcpConn.SetWriteBuffer(64 * 1024); err != nil {
			log.Printf("Warning: failed to set write buffer: %v", err)
		}
	}
	
	return nil
}

// setupUnixSocketPermissions 设置Unix Socket权限
func (mpl *MultiProtocolListener) setupUnixSocketPermissions() error {
	if mpl.config.Server.UnixSockPath == "" {
		return nil
	}
	
	socketPath := mpl.config.Server.UnixSockPath
	
	// 设置文件权限
	if err := os.Chmod(socketPath, mpl.unixSocketPerms); err != nil {
		return fmt.Errorf("failed to set socket permissions: %w", err)
	}
	
	// 设置所有者（如果指定）
	if mpl.unixSocketOwner != nil {
		if err := os.Chown(socketPath, mpl.unixSocketOwner.UID, mpl.unixSocketOwner.GID); err != nil {
			return fmt.Errorf("failed to set socket owner: %w", err)
		}
	}
	
	log.Printf("Unix socket permissions set: %o", mpl.unixSocketPerms)
	return nil
}

// cleanupUnixSockets 清理Unix Socket文件
func (mpl *MultiProtocolListener) cleanupUnixSockets() error {
	if mpl.config.Server.UnixSockPath == "" {
		return nil
	}
	
	return os.Remove(mpl.config.Server.UnixSockPath)
}

// updateStats 更新统计信息
func (mpl *MultiProtocolListener) updateStats(protocol string, isNewConnection bool) {
	mpl.statsMu.Lock()
	defer mpl.statsMu.Unlock()
	
	if isNewConnection {
		mpl.stats.TotalConnections++
		mpl.stats.ActiveConnections++
		mpl.stats.LastConnectionTime = time.Now()
		
		switch protocol {
		case "tcp":
			mpl.stats.TCPConnections++
		case "unix":
			mpl.stats.UnixConnections++
		}
	} else {
		// 连接关闭
		mpl.stats.ActiveConnections--
		if mpl.stats.ActiveConnections < 0 {
			mpl.stats.ActiveConnections = 0
		}
	}
}

// recordError 记录错误
func (mpl *MultiProtocolListener) recordError(err error) {
	mpl.statsMu.Lock()
	mpl.stats.ConnectionErrors++
	mpl.statsMu.Unlock()
	
	select {
	case mpl.errorChan <- err:
	default:
		// 错误通道满了，记录到日志
		log.Printf("Listener error (channel full): %v", err)
	}
}

// UnixSocketUtils Unix Socket工具函数
type UnixSocketUtils struct{}

// IsUnixSocketInUse 检查Unix Socket是否在使用中
func (u *UnixSocketUtils) IsUnixSocketInUse(path string) bool {
	conn, err := net.Dial("unix", path)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// CreateUnixSocketDir 创建Unix Socket目录
func (u *UnixSocketUtils) CreateUnixSocketDir(socketPath string, perm os.FileMode) error {
	dir := filepath.Dir(socketPath)
	return os.MkdirAll(dir, perm)
}

// GetUnixSocketInfo 获取Unix Socket信息
func (u *UnixSocketUtils) GetUnixSocketInfo(path string) (*UnixSocketInfo, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	
	info := &UnixSocketInfo{
		Path:    path,
		Mode:    stat.Mode(),
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}
	
	// 获取系统信息
	if sysStat, ok := stat.Sys().(*syscall.Stat_t); ok {
		info.UID = int(sysStat.Uid)
		info.GID = int(sysStat.Gid)
	}
	
	return info, nil
}

// UnixSocketInfo Unix Socket信息
type UnixSocketInfo struct {
	Path    string      `json:"path"`
	Mode    os.FileMode `json:"mode"`
	Size    int64       `json:"size"`
	ModTime time.Time   `json:"mod_time"`
	UID     int         `json:"uid"`
	GID     int         `json:"gid"`
}

// ConnectionTracker 连接跟踪器
type ConnectionTracker struct {
	connections map[string]*TrackedConnection
	mu          sync.RWMutex
}

// TrackedConnection 被跟踪的连接
type TrackedConnection struct {
	Info        ConnectionInfo
	StartTime   time.Time
	LastActivity time.Time
	BytesSent   int64
	BytesReceived int64
}

// NewConnectionTracker 创建连接跟踪器
func NewConnectionTracker() *ConnectionTracker {
	return &ConnectionTracker{
		connections: make(map[string]*TrackedConnection),
	}
}

// TrackConnection 跟踪连接
func (ct *ConnectionTracker) TrackConnection(info ConnectionInfo) string {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	connID := fmt.Sprintf("%s-%d", info.Protocol, time.Now().UnixNano())
	ct.connections[connID] = &TrackedConnection{
		Info:         info,
		StartTime:    time.Now(),
		LastActivity: time.Now(),
	}
	
	return connID
}

// UntrackConnection 取消跟踪连接
func (ct *ConnectionTracker) UntrackConnection(connID string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	delete(ct.connections, connID)
}

// GetConnections 获取所有连接
func (ct *ConnectionTracker) GetConnections() map[string]*TrackedConnection {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	
	// 返回副本
	result := make(map[string]*TrackedConnection)
	for id, conn := range ct.connections {
		connCopy := *conn
		result[id] = &connCopy
	}
	
	return result
}

// GetConnectionsByProtocol 按协议获取连接
func (ct *ConnectionTracker) GetConnectionsByProtocol(protocol string) []*TrackedConnection {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	
	var result []*TrackedConnection
	for _, conn := range ct.connections {
		if conn.Info.Protocol == protocol {
			connCopy := *conn
			result = append(result, &connCopy)
		}
	}
	
	return result
}

// UpdateActivity 更新连接活跃度
func (ct *ConnectionTracker) UpdateActivity(connID string, bytesSent, bytesReceived int64) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	if conn, exists := ct.connections[connID]; exists {
		conn.LastActivity = time.Now()
		conn.BytesSent += bytesSent
		conn.BytesReceived += bytesReceived
	}
}