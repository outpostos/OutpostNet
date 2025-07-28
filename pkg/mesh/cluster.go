package mesh

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/pool"
	"github.com/outpostos/OutpostNet/pkg/types"
	"google.golang.org/protobuf/proto"
)

// MeshCluster 管理mesh集群中的节点发现和通信
type MeshCluster struct {
	localNode *LocalNode
	config    *NodeConfig
	
	// 集群状态
	nodes    map[string]*Node
	nodesMu  sync.RWMutex
	
	// 网络管理
	listener       net.Listener
	connectionPool *pool.NodeConnectionPool
	
	// Gossip协议
	gossipManager *GossipManager
	
	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMeshCluster 创建mesh集群
func NewMeshCluster(config *NodeConfig, localNode *LocalNode) (*MeshCluster, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	// 创建连接池
	poolConfig := &pool.ConnectionPoolConfig{
		MaxConnections:      config.MaxConnections,
		MaxIdleConnections:  config.MaxConnections / 4,
		MaxConnectionAge:    30 * time.Minute,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
	
	cluster := &MeshCluster{
		localNode:      localNode,
		config:         config,
		nodes:          make(map[string]*Node),
		connectionPool: pool.NewNodeConnectionPool(poolConfig),
		ctx:            ctx,
		cancel:         cancel,
	}
	
	// 创建gossip管理器
	gossipManager, err := NewGossipManager(cluster)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create gossip manager: %w", err)
	}
	cluster.gossipManager = gossipManager
	
	return cluster, nil
}

// Start 启动mesh集群
func (c *MeshCluster) Start() error {
	// 启动TCP监听器
	listener, err := net.Listen("tcp", c.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", c.config.ListenAddr, err)
	}
	c.listener = listener
	
	// 启动gossip管理器
	if err := c.gossipManager.Start(); err != nil {
		c.listener.Close()
		return fmt.Errorf("failed to start gossip manager: %w", err)
	}
	
	// 启动连接处理器
	c.wg.Add(1)
	go c.handleConnections()
	
	// 启动节点健康检查
	c.wg.Add(1)
	go c.healthChecker()
	
	return nil
}

// Stop 停止mesh集群
func (c *MeshCluster) Stop() error {
	c.cancel()
	
	// 关闭监听器
	if c.listener != nil {
		c.listener.Close()
	}
	
	// 停止gossip管理器
	if c.gossipManager != nil {
		c.gossipManager.Stop()
	}
	
	// 关闭连接池
	if c.connectionPool != nil {
		c.connectionPool.Close()
	}
	
	// 等待所有goroutine结束
	c.wg.Wait()
	
	return nil
}

// Leave 离开集群
func (c *MeshCluster) Leave() {
	// 广播离开消息
	leaveMsg := &protocol.NodeHeartbeat{
		NodeId: c.localNode.ID,
		NodeInfo: &protocol.NodeInfo{
			NodeId:   c.localNode.ID,
			NodeAddr: c.localNode.Addr,
			Labels:   c.localNode.Labels,
		},
	}
	
	c.gossipManager.BroadcastLeave(leaveMsg)
}

// handleConnections 处理入站连接
func (c *MeshCluster) handleConnections() {
	defer c.wg.Done()
	
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			conn, err := c.listener.Accept()
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				continue
			}
			
			// 处理连接
			go c.handleConnection(conn)
		}
	}
}

// handleConnection 处理单个连接
func (c *MeshCluster) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	codec := protocol.NewCodec(conn)
	defer codec.Close()
	
	// 设置超时
	conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := codec.ReadMessage()
			if err != nil {
				return
			}
			
			// 处理消息
			if err := c.handleMessage(msg, codec); err != nil {
				c.sendError(codec, err)
				return
			}
			
			// 重置超时
			conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		}
	}
}

// handleMessage 处理消息
func (c *MeshCluster) handleMessage(msg *protocol.Message, codec *protocol.Codec) error {
	switch msg.Type {
	case protocol.MsgTypeNodeHeartbeat:
		return c.handleNodeHeartbeat(msg, codec)
	case protocol.MsgTypeServiceSync:
		return c.handleServiceSync(msg, codec)
	case protocol.MsgTypeAuth:
		return c.handleAuth(msg, codec)
	case protocol.MsgTypeQueryService:
		return c.handleQueryService(msg, codec)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleNodeHeartbeat 处理节点心跳
func (c *MeshCluster) handleNodeHeartbeat(msg *protocol.Message, codec *protocol.Codec) error {
	heartbeat := &protocol.NodeHeartbeat{}
	if err := proto.Unmarshal(msg.Payload, heartbeat); err != nil {
		return fmt.Errorf("failed to unmarshal node heartbeat: %w", err)
	}
	
	// 更新节点信息
	c.updateNode(heartbeat.NodeInfo)
	
	// 通过gossip协议传播
	c.gossipManager.PropagateHeartbeat(heartbeat)
	
	return nil
}

// handleServiceSync 处理服务同步
func (c *MeshCluster) handleServiceSync(msg *protocol.Message, codec *protocol.Codec) error {
	sync := &protocol.ServiceSync{}
	if err := proto.Unmarshal(msg.Payload, sync); err != nil {
		return fmt.Errorf("failed to unmarshal service sync: %w", err)
	}
	
	// 更新本地服务信息
	c.updateServices(sync.Services, sync.Endpoints)
	
	return nil
}

// handleAuth 处理认证
func (c *MeshCluster) handleAuth(msg *protocol.Message, codec *protocol.Codec) error {
	auth := &protocol.AuthPayload{}
	if err := proto.Unmarshal(msg.Payload, auth); err != nil {
		return fmt.Errorf("failed to unmarshal auth: %w", err)
	}
	
	// 简单的认证逻辑
	success := auth.Token != "" // 简化：只要有token就认为认证成功
	
	response := &protocol.AuthResponse{
		Success:       success,
		Message:       "Authentication successful",
		ClientId:      msg.Id,
		ServerVersion: c.localNode.Version,
	}
	
	responseMsg := protocol.NewMessageBuilder(protocol.MsgTypeAuthResponse).
		WithPayload(response).
		WithID(msg.Id).
		Build()
	
	return codec.WriteMessage(responseMsg)
}

// handleQueryService 处理服务查询
func (c *MeshCluster) handleQueryService(msg *protocol.Message, codec *protocol.Codec) error {
	query := &protocol.QueryServicePayload{}
	if err := proto.Unmarshal(msg.Payload, query); err != nil {
		return fmt.Errorf("failed to unmarshal query service: %w", err)
	}
	
	// 查询服务
	service, err := c.localNode.serviceRegistry.GetService(query.ServiceName, query.ServiceNamespace)
	if err != nil {
		return fmt.Errorf("service not found: %w", err)
	}
	
	// 获取端点
	endpoints, err := c.localNode.serviceRegistry.GetEndpoints(
		query.ServiceName, 
		query.ServiceNamespace, 
		query.Selector,
	)
	if err != nil {
		return fmt.Errorf("failed to get endpoints: %w", err)
	}
	
	// 转换为协议格式
	protoService := c.convertToProtoService(service)
	protoEndpoints := c.convertToProtoEndpoints(endpoints)
	
	response := &protocol.ServiceInfoPayload{
		Service:   protoService,
		Endpoints: protoEndpoints,
	}
	
	responseMsg := protocol.NewMessageBuilder(protocol.MsgTypeServiceInfo).
		WithPayload(response).
		WithID(msg.Id).
		Build()
	
	return codec.WriteMessage(responseMsg)
}

// updateNode 更新节点信息
func (c *MeshCluster) updateNode(nodeInfo *protocol.NodeInfo) {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	
	node := &Node{
		ID:           nodeInfo.NodeId,
		Addr:         nodeInfo.NodeAddr,
		Labels:       nodeInfo.Labels,
		Capabilities: nodeInfo.Capabilities,
		Status:       NodeStatusActive,
		LastSeen:     nodeInfo.LastSeen.AsTime(),
	}
	
	c.nodes[node.ID] = node
	
	// 发送事件
	select {
	case c.localNode.eventChan <- &NodeEvent{
		Type:      NodeEventDiscovered,
		Node:      node,
		Timestamp: time.Now(),
		Details:   "node discovered via gossip",
	}:
	default:
	}
}

// updateServices 更新服务信息
func (c *MeshCluster) updateServices(services []*protocol.Service, endpoints []*protocol.Endpoint) {
	// TODO: 实现服务同步逻辑
	// 这里需要将远程服务信息同步到本地注册表
}

// GetNodes 获取所有节点
func (c *MeshCluster) GetNodes() []*Node {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	
	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	
	return nodes
}

// GetNode 获取指定节点
func (c *MeshCluster) GetNode(nodeID string) (*Node, bool) {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	
	node, exists := c.nodes[nodeID]
	if !exists {
		return nil, false
	}
	
	nodeCopy := *node
	return &nodeCopy, true
}

// GetHealthyNodes 获取健康的节点
func (c *MeshCluster) GetHealthyNodes() []*Node {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	
	var healthyNodes []*Node
	now := time.Now()
	timeout := 3 * c.config.HeartbeatInterval
	
	for _, node := range c.nodes {
		if node.Status == NodeStatusActive && now.Sub(node.LastSeen) < timeout {
			nodeCopy := *node
			healthyNodes = append(healthyNodes, &nodeCopy)
		}
	}
	
	return healthyNodes
}

// BroadcastHeartbeat 广播心跳
func (c *MeshCluster) BroadcastHeartbeat(heartbeat *protocol.HeartbeatPayload) {
	c.gossipManager.BroadcastHeartbeat(heartbeat)
}

// ConnectToNode 连接到指定节点
func (c *MeshCluster) ConnectToNode(nodeID string) (net.Conn, error) {
	c.nodesMu.RLock()
	node, exists := c.nodes[nodeID]
	c.nodesMu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	
	return c.connectionPool.GetNodeConnection(nodeID, node.Addr)
}

// healthChecker 健康检查器
func (c *MeshCluster) healthChecker() {
	defer c.wg.Done()
	
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkNodeHealth()
		}
	}
}

// checkNodeHealth 检查节点健康状态
func (c *MeshCluster) checkNodeHealth() {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	
	now := time.Now()
	timeout := 3 * c.config.HeartbeatInterval
	
	for nodeID, node := range c.nodes {
		if now.Sub(node.LastSeen) > timeout {
			if node.Status == NodeStatusActive {
				node.Status = NodeStatusFailed
				
				// 发送事件
				select {
				case c.localNode.eventChan <- &NodeEvent{
					Type:      NodeEventFailed,
					Node:      node,
					Timestamp: time.Now(),
					Details:   "node health check failed",
				}:
				default:
				}
			}
			
			// 如果节点长时间未响应，从集群中移除
			if now.Sub(node.LastSeen) > 5*timeout {
				delete(c.nodes, nodeID)
			}
		}
	}
}

// sendError 发送错误响应
func (c *MeshCluster) sendError(codec *protocol.Codec, err error) {
	errorPayload := &protocol.ErrorPayload{
		Code:    500,
		Message: err.Error(),
		Details: "internal server error",
	}
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeError).
		WithPayload(errorPayload).
		Build()
	
	codec.WriteMessage(msg)
}

// convertToProtoService 转换为协议服务格式
func (c *MeshCluster) convertToProtoService(service *types.Service) *protocol.Service {
	protoService := &protocol.Service{
		Metadata: &protocol.ServiceMetadata{
			Name:      service.Metadata.Name,
			Namespace: service.Metadata.Namespace,
			Labels:    service.Metadata.Labels,
		},
		Spec: &protocol.ServiceSpec{
			Selector:     service.Spec.Selector,
			ProxyMode:    service.Spec.ProxyMode,
			LoadBalancer: service.Spec.LoadBalancer,
			Ports:        make([]*protocol.ServicePort, len(service.Spec.Ports)),
		},
	}
	
	for i, port := range service.Spec.Ports {
		protoService.Spec.Ports[i] = &protocol.ServicePort{
			Name:       port.Name,
			Port:       int32(port.Port),
			TargetPort: int32(port.TargetPort),
			Protocol:   port.Protocol,
		}
	}
	
	return protoService
}

// convertToProtoEndpoints 转换为协议端点格式
func (c *MeshCluster) convertToProtoEndpoints(endpoints []*types.Endpoint) []*protocol.Endpoint {
	protoEndpoints := make([]*protocol.Endpoint, len(endpoints))
	
	for i, endpoint := range endpoints {
		protoEndpoints[i] = &protocol.Endpoint{
			Id:       endpoint.ID,
			Ip:       endpoint.IP,
			Port:     int32(endpoint.Port),
			Protocol: endpoint.Protocol,
			Labels:   endpoint.Labels,
			Status: &protocol.EndpointStatus{
				Ready:   endpoint.Status.Ready,
				Reason:  endpoint.Status.Reason,
				Message: endpoint.Status.Message,
			},
		}
	}
	
	return protoEndpoints
}

// GetLocalNode 获取本地节点
func (c *MeshCluster) GetLocalNode() *LocalNode {
	return c.localNode
}

// GetGossipManager 获取gossip管理器
func (c *MeshCluster) GetGossipManager() *GossipManager {
	return c.gossipManager
}

// GetClusterStats 获取集群统计信息
func (c *MeshCluster) GetClusterStats() *ClusterStats {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	
	stats := &ClusterStats{
		TotalNodes:   len(c.nodes),
		ActiveNodes:  0,
		FailedNodes:  0,
		ClusterName:  c.config.ClusterName,
		LocalNodeID:  c.localNode.ID,
		UpdateTime:   time.Now(),
	}
	
	for _, node := range c.nodes {
		switch node.Status {
		case NodeStatusActive:
			stats.ActiveNodes++
		case NodeStatusFailed:
			stats.FailedNodes++
		}
	}
	
	return stats
}

// ClusterStats 集群统计信息
type ClusterStats struct {
	TotalNodes   int       `json:"total_nodes"`
	ActiveNodes  int       `json:"active_nodes"`
	FailedNodes  int       `json:"failed_nodes"`
	ClusterName  string    `json:"cluster_name"`
	LocalNodeID  string    `json:"local_node_id"`
	UpdateTime   time.Time `json:"update_time"`
}