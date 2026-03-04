package mesh

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegionNetworkManager 区域网络管理器 (Phase 2)
// 负责管理区域内节点间的网络通信和服务同步
type RegionNetworkManager struct {
	localNodeID    string
	regionID       string
	listenAddr     string
	
	// 网络连接管理
	server         net.Listener
	connections    map[string]*NodeConnection  // nodeID -> connection
	connMu         sync.RWMutex
	
	// 消息处理
	messageHandler RegionMessageHandler
	
	// 健康检查
	healthChecker  *RegionHealthChecker
	
	// 状态管理
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	
	// 统计信息
	stats          *RegionNetworkStats
	statsMu        sync.RWMutex
}

// NodeConnection 节点连接
type NodeConnection struct {
	NodeID       string
	RemoteAddr   string
	Conn         net.Conn
	Codec        *protocol.Codec
	Status       string // "connecting", "connected", "disconnected", "failed"
	LastPing     time.Time
	RTT          time.Duration
	ConnectedAt  time.Time
	MessagesSent int64
	MessagesRecv int64
	BytesSent    int64
	BytesRecv    int64
	mu           sync.RWMutex
}

// RegionMessageHandler 区域消息处理器接口
type RegionMessageHandler interface {
	// 服务同步消息处理
	HandleServiceSync(nodeID string, sync *protocol.ServiceSync) error
	HandleRegionServiceQuery(nodeID string, query *protocol.RegionServiceQuery) (*protocol.RegionServiceResponse, error)
	
	// 健康检查消息处理
	HandleNodeHealthCheck(nodeID string, check *protocol.NodeHealthCheck) (*protocol.NodeHealthResponse, error)
	
	// 故障通知处理
	HandleServiceFailureNotification(notification *protocol.ServiceFailureNotification) error
	HandleServiceRecoveryNotification(notification *protocol.ServiceRecoveryNotification) error
	
	// 拓扑发现处理
	HandleRegionTopologyQuery(query *protocol.RegionTopologyQuery) (*protocol.RegionTopologyResponse, error)
}

// RegionNetworkStats 区域网络统计信息
type RegionNetworkStats struct {
	ActiveConnections    int       `json:"active_connections"`
	TotalConnections     int       `json:"total_connections"`
	FailedConnections    int       `json:"failed_connections"`
	MessagesSent         int64     `json:"messages_sent"`
	MessagesReceived     int64     `json:"messages_received"`
	BytesSent            int64     `json:"bytes_sent"`
	BytesReceived        int64     `json:"bytes_received"`
	AverageRTT           float64   `json:"average_rtt_ms"`
	LastSyncTime         time.Time `json:"last_sync_time"`
	SyncErrors           int       `json:"sync_errors"`
	UpdateTime           time.Time `json:"update_time"`
}

// NewRegionNetworkManager 创建区域网络管理器
func NewRegionNetworkManager(
	localNodeID, regionID, listenAddr string,
	messageHandler RegionMessageHandler,
) *RegionNetworkManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RegionNetworkManager{
		localNodeID:    localNodeID,
		regionID:       regionID,
		listenAddr:     listenAddr,
		connections:    make(map[string]*NodeConnection),
		messageHandler: messageHandler,
		ctx:            ctx,
		cancel:         cancel,
		stats:          &RegionNetworkStats{},
	}
}

// Start 启动区域网络管理器
func (m *RegionNetworkManager) Start() error {
	// 启动TCP服务器监听
	listener, err := net.Listen("tcp", m.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", m.listenAddr, err)
	}
	m.server = listener
	
	// 启动健康检查器
	m.healthChecker = NewRegionHealthChecker(m)
	m.healthChecker.Start()
	
	// 启动连接接受器
	m.wg.Add(1)
	go m.acceptConnections()
	
	// 启动统计信息更新器
	m.wg.Add(1)
	go m.updateStats()
	
	return nil
}

// Stop 停止区域网络管理器
func (m *RegionNetworkManager) Stop() error {
	m.cancel()
	
	// 停止健康检查器
	if m.healthChecker != nil {
		m.healthChecker.Stop()
	}
	
	// 关闭服务器
	if m.server != nil {
		m.server.Close()
	}
	
	// 关闭所有连接
	m.connMu.Lock()
	for _, conn := range m.connections {
		conn.Close()
	}
	m.connMu.Unlock()
	
	// 等待所有goroutine退出
	m.wg.Wait()
	
	return nil
}

// ConnectToNode 连接到指定节点
func (m *RegionNetworkManager) ConnectToNode(nodeID, address string) error {
	// 检查是否已连接
	m.connMu.RLock()
	if conn, exists := m.connections[nodeID]; exists && conn.IsConnected() {
		m.connMu.RUnlock()
		return nil // 已连接
	}
	m.connMu.RUnlock()
	
	// 建立连接
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
	}
	
	conn, err := dialer.DialContext(m.ctx, "tcp", address)
	if err != nil {
		m.updateConnectionStats(false)
		return fmt.Errorf("failed to connect to node %s at %s: %w", nodeID, address, err)
	}
	
	// 创建节点连接
	nodeConn := &NodeConnection{
		NodeID:      nodeID,
		RemoteAddr:  address,
		Conn:        conn,
		Codec:       protocol.NewCodec(conn),
		Status:      "connecting",
		ConnectedAt: time.Now(),
	}
	
	// 保存连接
	m.connMu.Lock()
	m.connections[nodeID] = nodeConn
	m.connMu.Unlock()
	
	// 启动连接处理
	m.wg.Add(1)
	go m.handleConnection(nodeConn)
	
	// 发送初始化消息
	err = m.sendNodeHandshake(nodeConn)
	if err != nil {
		nodeConn.Close()
		return fmt.Errorf("failed to handshake with node %s: %w", nodeID, err)
	}
	
	nodeConn.SetStatus("connected")
	m.updateConnectionStats(true)
	
	return nil
}

// DisconnectFromNode 断开与指定节点的连接
func (m *RegionNetworkManager) DisconnectFromNode(nodeID string) error {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	
	if conn, exists := m.connections[nodeID]; exists {
		conn.Close()
		delete(m.connections, nodeID)
		return nil
	}
	
	return fmt.Errorf("node %s not connected", nodeID)
}

// SendServiceSync 发送服务同步消息
func (m *RegionNetworkManager) SendServiceSync(nodeID string, sync *protocol.ServiceSync) error {
	conn := m.getConnection(nodeID)
	if conn == nil {
		return fmt.Errorf("no connection to node %s", nodeID)
	}
	
	// 设置发送节点信息
	sync.SourceNodeId = m.localNodeID
	sync.RegionId = m.regionID
	sync.SyncTime = timestamppb.Now()
	
	return m.sendMessage(conn, protocol.MsgTypeServiceSync, sync)
}

// BroadcastServiceSync 广播服务同步消息到所有节点
func (m *RegionNetworkManager) BroadcastServiceSync(sync *protocol.ServiceSync) error {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	
	sync.SourceNodeId = m.localNodeID
	sync.RegionId = m.regionID
	sync.SyncTime = timestamppb.Now()
	
	var errors []error
	for nodeID, conn := range m.connections {
		if conn.IsConnected() {
			if err := m.sendMessage(conn, protocol.MsgTypeServiceSync, sync); err != nil {
				errors = append(errors, fmt.Errorf("failed to sync to node %s: %w", nodeID, err))
			}
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("broadcast failed to %d nodes: %v", len(errors), errors)
	}
	
	return nil
}

// QueryRegionService 查询区域服务
func (m *RegionNetworkManager) QueryRegionService(
	nodeID string,
	query *protocol.RegionServiceQuery,
) (*protocol.RegionServiceResponse, error) {
	conn := m.getConnection(nodeID)
	if conn == nil {
		return nil, fmt.Errorf("no connection to node %s", nodeID)
	}
	
	// 发送查询消息
	if err := m.sendMessage(conn, protocol.MsgTypeRegionServiceQuery, query); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}
	
	// 等待响应 (简化实现，实际应该使用请求ID匹配)
	msg, err := conn.Codec.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	if msg.Type != protocol.MsgTypeRegionServiceResponse {
		return nil, fmt.Errorf("unexpected response type: %s", msg.Type)
	}
	
	response := &protocol.RegionServiceResponse{}
	if err := proto.Unmarshal(msg.Payload, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	
	return response, nil
}

// SendHealthCheck 发送健康检查
func (m *RegionNetworkManager) SendHealthCheck(nodeID string, check *protocol.NodeHealthCheck) (*protocol.NodeHealthResponse, error) {
	conn := m.getConnection(nodeID)
	if conn == nil {
		return nil, fmt.Errorf("no connection to node %s", nodeID)
	}
	
	check.RegionId = m.regionID
	check.CheckTime = timestamppb.Now()
	
	if err := m.sendMessage(conn, protocol.MsgTypeNodeHealthCheck, check); err != nil {
		return nil, fmt.Errorf("failed to send health check: %w", err)
	}
	
	// 等待响应
	msg, err := conn.Codec.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read health response: %w", err)
	}
	
	if msg.Type != protocol.MsgTypeNodeHealthResponse {
		return nil, fmt.Errorf("unexpected response type: %s", msg.Type)
	}
	
	response := &protocol.NodeHealthResponse{}
	if err := proto.Unmarshal(msg.Payload, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal health response: %w", err)
	}
	
	// 更新RTT
	if conn != nil {
		rtt := time.Since(check.CheckTime.AsTime())
		conn.UpdateRTT(rtt)
	}
	
	return response, nil
}

// NotifyServiceFailure 通知服务故障
func (m *RegionNetworkManager) NotifyServiceFailure(notification *protocol.ServiceFailureNotification) error {
	notification.ReporterNode = m.localNodeID
	notification.FailureTime = timestamppb.Now()
	
	return m.broadcastMessage(protocol.MsgTypeServiceFailureNotification, notification)
}

// NotifyServiceRecovery 通知服务恢复
func (m *RegionNetworkManager) NotifyServiceRecovery(notification *protocol.ServiceRecoveryNotification) error {
	notification.ReporterNode = m.localNodeID
	notification.RecoveryTime = timestamppb.Now()
	
	return m.broadcastMessage(protocol.MsgTypeServiceRecoveryNotification, notification)
}

// GetConnectedNodes 获取已连接的节点列表
func (m *RegionNetworkManager) GetConnectedNodes() []string {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	
	var nodes []string
	for nodeID, conn := range m.connections {
		if conn.IsConnected() {
			nodes = append(nodes, nodeID)
		}
	}
	
	return nodes
}

// GetNetworkStats 获取网络统计信息
func (m *RegionNetworkManager) GetNetworkStats() *RegionNetworkStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	
	// 返回统计信息副本
	stats := *m.stats
	return &stats
}

// 内部方法

// acceptConnections 接受连接
func (m *RegionNetworkManager) acceptConnections() {
	defer m.wg.Done()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			conn, err := m.server.Accept()
			if err != nil {
				if m.ctx.Err() != nil {
					return // 正常关闭
				}
				continue
			}
			
			// 处理新连接
			m.wg.Add(1)
			go m.handleIncomingConnection(conn)
		}
	}
}

// handleIncomingConnection 处理入站连接
func (m *RegionNetworkManager) handleIncomingConnection(conn net.Conn) {
	defer m.wg.Done()
	defer conn.Close()
	
	codec := protocol.NewCodec(conn)
	defer codec.Close()
	
	// 等待握手消息以识别节点
	msg, err := codec.ReadMessage()
	if err != nil {
		return
	}
	
	// 解析节点ID (简化实现，实际应该有专门的握手协议)
	if msg.Type != protocol.MsgTypeNodeHeartbeat {
		return
	}
	
	heartbeat := &protocol.NodeHeartbeat{}
	if err := proto.Unmarshal(msg.Payload, heartbeat); err != nil {
		return
	}
	
	nodeID := heartbeat.NodeId
	
	// 创建节点连接
	nodeConn := &NodeConnection{
		NodeID:      nodeID,
		RemoteAddr:  conn.RemoteAddr().String(),
		Conn:        conn,
		Codec:       codec,
		Status:      "connected",
		ConnectedAt: time.Now(),
	}
	
	// 保存连接
	m.connMu.Lock()
	m.connections[nodeID] = nodeConn
	m.connMu.Unlock()
	
	// 处理连接
	m.handleConnection(nodeConn)
}

// handleConnection 处理节点连接
func (m *RegionNetworkManager) handleConnection(nodeConn *NodeConnection) {
	defer m.wg.Done()
	defer nodeConn.Close()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			msg, err := nodeConn.Codec.ReadMessage()
			if err != nil {
				nodeConn.SetStatus("disconnected")
				return
			}
			
			// 更新接收统计
			nodeConn.UpdateRecvStats(int64(len(msg.Payload)))
			
			// 处理消息
			if err := m.handleMessage(nodeConn.NodeID, msg); err != nil {
				// 记录错误但继续处理
				continue
			}
		}
	}
}

// handleMessage 处理收到的消息
func (m *RegionNetworkManager) handleMessage(nodeID string, msg *protocol.Message) error {
	switch msg.Type {
	case protocol.MsgTypeServiceSync:
		sync := &protocol.ServiceSync{}
		if err := proto.Unmarshal(msg.Payload, sync); err != nil {
			return err
		}
		return m.messageHandler.HandleServiceSync(nodeID, sync)
		
	case protocol.MsgTypeRegionServiceQuery:
		query := &protocol.RegionServiceQuery{}
		if err := proto.Unmarshal(msg.Payload, query); err != nil {
			return err
		}
		response, err := m.messageHandler.HandleRegionServiceQuery(nodeID, query)
		if err != nil {
			return err
		}
		return m.sendMessage(m.getConnection(nodeID), protocol.MsgTypeRegionServiceResponse, response)
		
	case protocol.MsgTypeNodeHealthCheck:
		check := &protocol.NodeHealthCheck{}
		if err := proto.Unmarshal(msg.Payload, check); err != nil {
			return err
		}
		response, err := m.messageHandler.HandleNodeHealthCheck(nodeID, check)
		if err != nil {
			return err
		}
		return m.sendMessage(m.getConnection(nodeID), protocol.MsgTypeNodeHealthResponse, response)
		
	case protocol.MsgTypeServiceFailureNotification:
		notification := &protocol.ServiceFailureNotification{}
		if err := proto.Unmarshal(msg.Payload, notification); err != nil {
			return err
		}
		return m.messageHandler.HandleServiceFailureNotification(notification)
		
	case protocol.MsgTypeServiceRecoveryNotification:
		notification := &protocol.ServiceRecoveryNotification{}
		if err := proto.Unmarshal(msg.Payload, notification); err != nil {
			return err
		}
		return m.messageHandler.HandleServiceRecoveryNotification(notification)
		
	case protocol.MsgTypeRegionTopologyQuery:
		query := &protocol.RegionTopologyQuery{}
		if err := proto.Unmarshal(msg.Payload, query); err != nil {
			return err
		}
		response, err := m.messageHandler.HandleRegionTopologyQuery(query)
		if err != nil {
			return err
		}
		return m.sendMessage(m.getConnection(nodeID), protocol.MsgTypeRegionTopologyResponse, response)
		
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// sendMessage 发送消息
func (m *RegionNetworkManager) sendMessage(conn *NodeConnection, msgType string, payload proto.Message) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection not available")
	}
	
	msg := protocol.NewMessageBuilder(msgType).
		WithPayload(payload).
		Build()
	
	if err := conn.Codec.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	
	// 更新发送统计
	// TODO: 实现数据长度统计，暂时使用固定值
	conn.UpdateSentStats(int64(1024))
	
	return nil
}

// broadcastMessage 广播消息
func (m *RegionNetworkManager) broadcastMessage(msgType string, payload proto.Message) error {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	
	var errors []error
	for nodeID, conn := range m.connections {
		if conn.IsConnected() {
			if err := m.sendMessage(conn, msgType, payload); err != nil {
				errors = append(errors, fmt.Errorf("failed to send to node %s: %w", nodeID, err))
			}
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("broadcast failed to %d nodes: %v", len(errors), errors)
	}
	
	return nil
}

// sendNodeHandshake 发送节点握手
func (m *RegionNetworkManager) sendNodeHandshake(conn *NodeConnection) error {
	heartbeat := &protocol.NodeHeartbeat{
		NodeId: m.localNodeID,
		NodeInfo: &protocol.NodeInfo{
			NodeId:   m.localNodeID,
			NodeAddr: m.listenAddr,
			Labels: map[string]string{
				"region": m.regionID,
			},
		},
	}
	
	return m.sendMessage(conn, protocol.MsgTypeNodeHeartbeat, heartbeat)
}

// getConnection 获取连接
func (m *RegionNetworkManager) getConnection(nodeID string) *NodeConnection {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	
	return m.connections[nodeID]
}

// updateConnectionStats 更新连接统计
func (m *RegionNetworkManager) updateConnectionStats(success bool) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	
	m.stats.TotalConnections++
	if success {
		m.stats.ActiveConnections++
	} else {
		m.stats.FailedConnections++
	}
}

// updateStats 更新统计信息
func (m *RegionNetworkManager) updateStats() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.calculateStats()
		}
	}
}

// calculateStats 计算统计信息
func (m *RegionNetworkManager) calculateStats() {
	m.connMu.RLock()
	m.statsMu.Lock()
	
	activeConns := 0
	var totalRTT time.Duration
	var totalSent, totalRecv int64
	
	for _, conn := range m.connections {
		if conn.IsConnected() {
			activeConns++
			totalRTT += conn.GetRTT()
			totalSent += conn.MessagesSent
			totalRecv += conn.MessagesRecv
		}
	}
	
	m.stats.ActiveConnections = activeConns
	m.stats.MessagesSent = totalSent
	m.stats.MessagesReceived = totalRecv
	
	if activeConns > 0 {
		m.stats.AverageRTT = float64(totalRTT/time.Duration(activeConns)) / float64(time.Millisecond)
	}
	
	m.stats.UpdateTime = time.Now()
	
	m.statsMu.Unlock()
	m.connMu.RUnlock()
}

// NodeConnection 方法

// IsConnected 检查连接是否活跃
func (c *NodeConnection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Status == "connected"
}

// SetStatus 设置连接状态
func (c *NodeConnection) SetStatus(status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Status = status
}

// UpdateRTT 更新RTT
func (c *NodeConnection) UpdateRTT(rtt time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.RTT = rtt
	c.LastPing = time.Now()
}

// GetRTT 获取RTT
func (c *NodeConnection) GetRTT() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.RTT
}

// UpdateSentStats 更新发送统计
func (c *NodeConnection) UpdateSentStats(bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MessagesSent++
	c.BytesSent += bytes
}

// UpdateRecvStats 更新接收统计
func (c *NodeConnection) UpdateRecvStats(bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MessagesRecv++
	c.BytesRecv += bytes
}

// Close 关闭连接
func (c *NodeConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.Status = "disconnected"
	
	if c.Codec != nil {
		c.Codec.Close()
	}
	
	if c.Conn != nil {
		return c.Conn.Close()
	}
	
	return nil
}