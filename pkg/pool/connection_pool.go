package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// ConnectionPool 管理到目标地址的连接池
type ConnectionPool struct {
	// 连接池配置
	maxConnections      int
	maxIdleConnections  int
	maxConnectionAge    time.Duration
	idleTimeout         time.Duration
	connectionTimeout   time.Duration
	healthCheckInterval time.Duration

	// 连接池状态
	pools   map[string]*targetPool // 按目标地址分组的连接池
	poolsMu sync.RWMutex
	
	// 统计信息
	stats *PoolStats
	
	// 控制通道
	ctx    context.Context
	cancel context.CancelFunc
}

// targetPool 针对特定目标地址的连接池
type targetPool struct {
	target            string
	connections       []*PooledConnection
	activeConnections int
	mu                sync.RWMutex
	
	// 配置
	maxConnections     int
	maxIdleConnections int
	maxConnectionAge   time.Duration
	idleTimeout        time.Duration
	connectionTimeout  time.Duration
	
	// 统计
	totalCreated int64
	totalClosed  int64
	totalReused  int64
	
	// 健康检查
	lastHealthCheck time.Time
	isHealthy       bool
}

// PooledConnection 池化的连接对象
type PooledConnection struct {
	conn       net.Conn
	target     string
	createdAt  time.Time
	lastUsed   time.Time
	useCount   int64
	isHealthy  bool
	inUse      bool
	
	// 元数据
	metadata map[string]interface{}
}

// PoolStats 连接池统计信息
type PoolStats struct {
	mu                    sync.RWMutex
	activeConnections     int64
	idleConnections       int64
	totalConnectionsEver  int64
	totalConnectionsClosed int64
	totalConnectionsReused int64
	connectionsPerTarget  map[string]int64
	
	// 性能指标
	avgConnectionAge   time.Duration
	avgConnectionReuse float64
	connectionErrors   int64
	healthCheckErrors  int64
}

// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
	MaxConnections      int           `yaml:"max_connections" json:"max_connections"`
	MaxIdleConnections  int           `yaml:"max_idle_connections" json:"max_idle_connections"`
	MaxConnectionAge    time.Duration `yaml:"max_connection_age" json:"max_connection_age"`
	IdleTimeout         time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	ConnectionTimeout   time.Duration `yaml:"connection_timeout" json:"connection_timeout"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval" json:"health_check_interval"`
}

// NewConnectionPool 创建新的连接池
func NewConnectionPool(config *ConnectionPoolConfig) *ConnectionPool {
	if config == nil {
		config = &ConnectionPoolConfig{
			MaxConnections:      50,
			MaxIdleConnections:  10,
			MaxConnectionAge:    30 * time.Minute,
			IdleTimeout:         5 * time.Minute,
			ConnectionTimeout:   10 * time.Second,
			HealthCheckInterval: 30 * time.Second,
		}
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	pool := &ConnectionPool{
		maxConnections:      config.MaxConnections,
		maxIdleConnections:  config.MaxIdleConnections,
		maxConnectionAge:    config.MaxConnectionAge,
		idleTimeout:         config.IdleTimeout,
		connectionTimeout:   config.ConnectionTimeout,
		healthCheckInterval: config.HealthCheckInterval,
		
		pools: make(map[string]*targetPool),
		stats: &PoolStats{
			connectionsPerTarget: make(map[string]int64),
		},
		
		ctx:    ctx,
		cancel: cancel,
	}
	
	// 启动后台清理和健康检查
	go pool.maintenanceLoop()
	
	return pool
}

// GetConnection 获取到目标地址的连接
func (p *ConnectionPool) GetConnection(target string) (net.Conn, error) {
	return p.GetConnectionWithContext(context.Background(), target)
}

// GetConnectionWithContext 使用上下文获取连接
func (p *ConnectionPool) GetConnectionWithContext(ctx context.Context, target string) (net.Conn, error) {
	// 尝试从连接池获取连接
	if conn := p.getPooledConnection(target); conn != nil {
		p.updateStats(target, true) // 记录连接复用
		return conn, nil
	}
	
	// 创建新连接
	conn, err := p.createConnection(ctx, target)
	if err != nil {
		p.stats.mu.Lock()
		p.stats.connectionErrors++
		p.stats.mu.Unlock()
		return nil, fmt.Errorf("failed to create connection to %s: %w", target, err)
	}
	
	p.updateStats(target, false) // 记录新连接创建
	return conn, nil
}

// getPooledConnection 从连接池获取可用连接
func (p *ConnectionPool) getPooledConnection(target string) net.Conn {
	p.poolsMu.RLock()
	pool, exists := p.pools[target]
	p.poolsMu.RUnlock()
	
	if !exists {
		return nil
	}
	
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	for i, conn := range pool.connections {
		if !conn.inUse && conn.isHealthy && p.isConnectionUsable(conn) {
			// 标记为使用中
			conn.inUse = true
			conn.lastUsed = time.Now()
			conn.useCount++
			
			// 从池中移除
			pool.connections = append(pool.connections[:i], pool.connections[i+1:]...)
			pool.activeConnections++
			pool.totalReused++
			
			return &pooledConnectionWrapper{
				PooledConnection: conn,
				pool:            p,
				target:          target,
			}
		}
	}
	
	return nil
}

// createConnection 创建新连接
func (p *ConnectionPool) createConnection(ctx context.Context, target string) (net.Conn, error) {
	// 检查连接数限制
	if !p.canCreateConnection(target) {
		return nil, fmt.Errorf("connection limit reached for target %s", target)
	}
	
	// 创建连接
	dialer := &net.Dialer{
		Timeout: p.connectionTimeout,
	}
	
	rawConn, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, err
	}
	
	// 包装为池化连接
	conn := &PooledConnection{
		conn:      rawConn,
		target:    target,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		useCount:  1,
		isHealthy: true,
		inUse:     true,
		metadata:  make(map[string]interface{}),
	}
	
	// 更新目标连接池
	p.updateTargetPool(target, conn)
	
	return &pooledConnectionWrapper{
		PooledConnection: conn,
		pool:            p,
		target:          target,
	}, nil
}

// canCreateConnection 检查是否可以创建新连接
func (p *ConnectionPool) canCreateConnection(target string) bool {
	p.poolsMu.RLock()
	pool, exists := p.pools[target]
	p.poolsMu.RUnlock()
	
	if !exists {
		return true
	}
	
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	
	return pool.activeConnections < pool.maxConnections
}

// updateTargetPool 更新目标连接池
func (p *ConnectionPool) updateTargetPool(target string, conn *PooledConnection) {
	p.poolsMu.Lock()
	pool, exists := p.pools[target]
	if !exists {
		pool = &targetPool{
			target:             target,
			connections:        make([]*PooledConnection, 0),
			maxConnections:     p.maxConnections,
			maxIdleConnections: p.maxIdleConnections,
			maxConnectionAge:   p.maxConnectionAge,
			idleTimeout:        p.idleTimeout,
			connectionTimeout:  p.connectionTimeout,
			isHealthy:          true,
		}
		p.pools[target] = pool
	}
	p.poolsMu.Unlock()
	
	pool.mu.Lock()
	pool.activeConnections++
	pool.totalCreated++
	pool.mu.Unlock()
}

// returnConnection 将连接返回到池中
func (p *ConnectionPool) returnConnection(conn *PooledConnection, target string) {
	p.poolsMu.RLock()
	pool, exists := p.pools[target]
	p.poolsMu.RUnlock()
	
	if !exists {
		conn.conn.Close()
		return
	}
	
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	// 标记为非使用状态
	conn.inUse = false
	conn.lastUsed = time.Now()
	pool.activeConnections--
	
	// 检查连接是否还可用
	if !p.isConnectionUsable(conn) {
		conn.conn.Close()
		pool.totalClosed++
		return
	}
	
	// 检查空闲连接数限制
	if len(pool.connections) >= pool.maxIdleConnections {
		conn.conn.Close()
		pool.totalClosed++
		return
	}
	
	// 返回到池中
	pool.connections = append(pool.connections, conn)
}

// isConnectionUsable 检查连接是否可用
func (p *ConnectionPool) isConnectionUsable(conn *PooledConnection) bool {
	now := time.Now()
	
	// 检查连接年龄
	if now.Sub(conn.createdAt) > p.maxConnectionAge {
		return false
	}
	
	// 检查空闲时间
	if now.Sub(conn.lastUsed) > p.idleTimeout {
		return false
	}
	
	// 检查连接健康状态
	if !conn.isHealthy {
		return false
	}
	
	return true
}

// updateStats 更新统计信息
func (p *ConnectionPool) updateStats(target string, reused bool) {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()
	
	if reused {
		p.stats.totalConnectionsReused++
	} else {
		p.stats.totalConnectionsEver++
	}
	
	p.stats.connectionsPerTarget[target]++
}

// maintenanceLoop 后台维护循环
func (p *ConnectionPool) maintenanceLoop() {
	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performMaintenance()
		}
	}
}

// performMaintenance 执行维护任务
func (p *ConnectionPool) performMaintenance() {
	p.poolsMu.RLock()
	targets := make([]string, 0, len(p.pools))
	for target := range p.pools {
		targets = append(targets, target)
	}
	p.poolsMu.RUnlock()
	
	// 清理过期连接
	for _, target := range targets {
		p.cleanupExpiredConnections(target)
	}
	
	// 执行健康检查
	for _, target := range targets {
		p.performHealthCheck(target)
	}
}

// cleanupExpiredConnections 清理过期连接
func (p *ConnectionPool) cleanupExpiredConnections(target string) {
	p.poolsMu.RLock()
	pool, exists := p.pools[target]
	p.poolsMu.RUnlock()
	
	if !exists {
		return
	}
	
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	validConnections := make([]*PooledConnection, 0, len(pool.connections))
	
	for _, conn := range pool.connections {
		if p.isConnectionUsable(conn) {
			validConnections = append(validConnections, conn)
		} else {
			conn.conn.Close()
			pool.totalClosed++
		}
	}
	
	pool.connections = validConnections
}

// performHealthCheck 执行健康检查
func (p *ConnectionPool) performHealthCheck(target string) {
	p.poolsMu.RLock()
	pool, exists := p.pools[target]
	p.poolsMu.RUnlock()
	
	if !exists {
		return
	}
	
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	// 简单的健康检查：尝试读取连接状态
	healthy := 0
	total := len(pool.connections)
	
	for _, conn := range pool.connections {
		if p.checkConnectionHealth(conn) {
			healthy++
		}
	}
	
	pool.lastHealthCheck = time.Now()
	pool.isHealthy = total == 0 || float64(healthy)/float64(total) > 0.5
}

// checkConnectionHealth 检查单个连接的健康状态
func (p *ConnectionPool) checkConnectionHealth(conn *PooledConnection) bool {
	// 设置短暂的读取超时
	conn.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer conn.conn.SetReadDeadline(time.Time{})
	
	// 尝试读取一个字节（非阻塞）
	buffer := make([]byte, 1)
	_, err := conn.conn.Read(buffer)
	
	// 如果出现超时错误，认为连接正常
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	
	// 如果连接关闭，标记为不健康
	if err != nil {
		conn.isHealthy = false
		return false
	}
	
	return true
}

// GetStats 获取连接池统计信息
func (p *ConnectionPool) GetStats() *PoolStats {
	p.stats.mu.RLock()
	defer p.stats.mu.RUnlock()
	
	// 复制统计信息
	stats := &PoolStats{
		activeConnections:     p.stats.activeConnections,
		idleConnections:       p.stats.idleConnections,
		totalConnectionsEver:  p.stats.totalConnectionsEver,
		totalConnectionsClosed: p.stats.totalConnectionsClosed,
		totalConnectionsReused: p.stats.totalConnectionsReused,
		connectionsPerTarget:  make(map[string]int64),
		avgConnectionAge:      p.stats.avgConnectionAge,
		avgConnectionReuse:    p.stats.avgConnectionReuse,
		connectionErrors:      p.stats.connectionErrors,
		healthCheckErrors:     p.stats.healthCheckErrors,
	}
	
	for target, count := range p.stats.connectionsPerTarget {
		stats.connectionsPerTarget[target] = count
	}
	
	return stats
}

// Close 关闭连接池
func (p *ConnectionPool) Close() error {
	p.cancel()
	
	p.poolsMu.Lock()
	defer p.poolsMu.Unlock()
	
	// 关闭所有连接
	for _, pool := range p.pools {
		pool.mu.Lock()
		for _, conn := range pool.connections {
			conn.conn.Close()
		}
		pool.mu.Unlock()
	}
	
	return nil
}

// pooledConnectionWrapper 包装池化连接，实现连接返回
type pooledConnectionWrapper struct {
	*PooledConnection
	pool   *ConnectionPool
	target string
	closed bool
}

func (w *pooledConnectionWrapper) Close() error {
	if w.closed {
		return nil
	}
	
	w.closed = true
	
	// 将连接返回到池中而不是关闭
	w.pool.returnConnection(w.PooledConnection, w.target)
	return nil
}

// Read 实现 net.Conn 接口
func (w *pooledConnectionWrapper) Read(b []byte) (n int, err error) {
	return w.conn.Read(b)
}

// Write 实现 net.Conn 接口
func (w *pooledConnectionWrapper) Write(b []byte) (n int, err error) {
	return w.conn.Write(b)
}

// LocalAddr 实现 net.Conn 接口
func (w *pooledConnectionWrapper) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

// RemoteAddr 实现 net.Conn 接口
func (w *pooledConnectionWrapper) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}

// SetDeadline 实现 net.Conn 接口
func (w *pooledConnectionWrapper) SetDeadline(t time.Time) error {
	return w.conn.SetDeadline(t)
}

// SetReadDeadline 实现 net.Conn 接口
func (w *pooledConnectionWrapper) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

// SetWriteDeadline 实现 net.Conn 接口
func (w *pooledConnectionWrapper) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

// 为了支持节点互联，添加高级连接池功能
// NodeConnectionPool 节点间连接池
type NodeConnectionPool struct {
	*ConnectionPool
	nodeConnections map[string]*PersistentConnection
	mu              sync.RWMutex
}

// PersistentConnection 持久连接
type PersistentConnection struct {
	conn      net.Conn
	nodeID    string
	isHealthy bool
	lastPing  time.Time
}

// NewNodeConnectionPool 创建节点连接池
func NewNodeConnectionPool(config *ConnectionPoolConfig) *NodeConnectionPool {
	return &NodeConnectionPool{
		ConnectionPool:  NewConnectionPool(config),
		nodeConnections: make(map[string]*PersistentConnection),
	}
}

// GetNodeConnection 获取到指定节点的持久连接
func (p *NodeConnectionPool) GetNodeConnection(nodeID, nodeAddr string) (net.Conn, error) {
	p.mu.RLock()
	persistentConn, exists := p.nodeConnections[nodeID]
	p.mu.RUnlock()
	
	if exists && persistentConn.isHealthy {
		return persistentConn.conn, nil
	}
	
	// 创建新的持久连接
	conn, err := p.GetConnection(nodeAddr)
	if err != nil {
		return nil, err
	}
	
	p.mu.Lock()
	p.nodeConnections[nodeID] = &PersistentConnection{
		conn:      conn,
		nodeID:    nodeID,
		isHealthy: true,
		lastPing:  time.Now(),
	}
	p.mu.Unlock()
	
	return conn, nil
}