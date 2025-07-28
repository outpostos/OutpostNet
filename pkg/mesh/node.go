package mesh

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NodeStatus 节点状态
type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusJoining
	NodeStatusActive
	NodeStatusLeaving
	NodeStatusFailed
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusJoining:
		return "joining"
	case NodeStatusActive:
		return "active"
	case NodeStatusLeaving:
		return "leaving"
	case NodeStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Node 表示mesh网络中的一个节点
type Node struct {
	ID           string            `json:"id"`
	Addr         string            `json:"addr"`
	Labels       map[string]string `json:"labels"`
	Capabilities []string          `json:"capabilities"`
	Status       NodeStatus        `json:"status"`
	LastSeen     time.Time         `json:"last_seen"`
	Version      string            `json:"version"`
	
	// 性能指标
	CPUUsage    float64 `json:"cpu_usage"`
	MemoryUsage float64 `json:"memory_usage"`
	LoadScore   float64 `json:"load_score"`
	
	// 网络信息
	RTT       time.Duration `json:"rtt"`
	Bandwidth int64         `json:"bandwidth"`
	
	// 安全信息
	TLSEnabled    bool   `json:"tls_enabled"`
	CertificateID string `json:"certificate_id"`
}

// LocalNode 本地节点信息
type LocalNode struct {
	*Node
	
	// 本地配置
	config     *NodeConfig
	meshCluster *MeshCluster
	
	// 状态管理
	mu           sync.RWMutex
	isRunning    bool
	ctx          context.Context
	cancel       context.CancelFunc
	
	// 服务注册表
	serviceRegistry types.ServiceRegistry
	
	// 事件通道
	eventChan chan *NodeEvent
}

// NodeConfig 节点配置
type NodeConfig struct {
	// 基本配置
	NodeID       string            `yaml:"node_id" json:"node_id"`
	ListenAddr   string            `yaml:"listen_addr" json:"listen_addr"`
	AdvertiseAddr string           `yaml:"advertise_addr" json:"advertise_addr"`
	Labels       map[string]string `yaml:"labels" json:"labels"`
	
	// 集群配置
	ClusterName     string   `yaml:"cluster_name" json:"cluster_name"`
	SeedNodes       []string `yaml:"seed_nodes" json:"seed_nodes"`
	JoinTimeout     time.Duration `yaml:"join_timeout" json:"join_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" json:"heartbeat_interval"`
	
	// 安全配置
	TLSEnabled   bool   `yaml:"tls_enabled" json:"tls_enabled"`
	CertFile     string `yaml:"cert_file" json:"cert_file"`
	KeyFile      string `yaml:"key_file" json:"key_file"`
	CAFile       string `yaml:"ca_file" json:"ca_file"`
	
	// 性能配置
	MaxConnections int           `yaml:"max_connections" json:"max_connections"`
	BufferSize     int           `yaml:"buffer_size" json:"buffer_size"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout"`
}

// NodeEvent 节点事件
type NodeEvent struct {
	Type      NodeEventType `json:"type"`
	Node      *Node         `json:"node"`
	Timestamp time.Time     `json:"timestamp"`
	Details   string        `json:"details"`
}

// NodeEventType 节点事件类型
type NodeEventType int

const (
	NodeEventJoined NodeEventType = iota
	NodeEventLeft
	NodeEventFailed
	NodeEventUpdated
	NodeEventDiscovered
)

func (t NodeEventType) String() string {
	switch t {
	case NodeEventJoined:
		return "joined"
	case NodeEventLeft:
		return "left"
	case NodeEventFailed:
		return "failed"
	case NodeEventUpdated:
		return "updated"
	case NodeEventDiscovered:
		return "discovered"
	default:
		return "unknown"
	}
}

// NewLocalNode 创建本地节点
func NewLocalNode(config *NodeConfig, registry types.ServiceRegistry) (*LocalNode, error) {
	if config == nil {
		return nil, fmt.Errorf("node config is required")
	}
	
	// 生成节点ID（如果未提供）
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = generateNodeID()
	}
	
	// 解析监听地址
	listenAddr := config.ListenAddr
	if listenAddr == "" {
		listenAddr = ":7629"
	}
	
	// 确定广播地址
	advertiseAddr := config.AdvertiseAddr
	if advertiseAddr == "" {
		host, port, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid listen address: %w", err)
		}
		
		if host == "" || host == "0.0.0.0" {
			// 自动检测本地IP
			if localIP, err := getLocalIP(); err == nil {
				advertiseAddr = net.JoinHostPort(localIP, port)
			} else {
				advertiseAddr = net.JoinHostPort("127.0.0.1", port)
			}
		} else {
			advertiseAddr = listenAddr
		}
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	node := &Node{
		ID:           nodeID,
		Addr:         advertiseAddr,
		Labels:       make(map[string]string),
		Capabilities: []string{"tcp", "udp", "stcp", "sudp"},
		Status:       NodeStatusUnknown,
		LastSeen:     time.Now(),
		Version:      "1.0.0",
		TLSEnabled:   config.TLSEnabled,
	}
	
	// 复制标签
	for k, v := range config.Labels {
		node.Labels[k] = v
	}
	
	localNode := &LocalNode{
		Node:            node,
		config:          config,
		serviceRegistry: registry,
		ctx:             ctx,
		cancel:          cancel,
		eventChan:       make(chan *NodeEvent, 100),
	}
	
	return localNode, nil
}

// Start 启动本地节点
func (n *LocalNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.isRunning {
		return fmt.Errorf("node is already running")
	}
	
	// 创建mesh集群
	meshCluster, err := NewMeshCluster(n.config, n)
	if err != nil {
		return fmt.Errorf("failed to create mesh cluster: %w", err)
	}
	n.meshCluster = meshCluster
	
	// 启动集群
	if err := meshCluster.Start(); err != nil {
		return fmt.Errorf("failed to start mesh cluster: %w", err)
	}
	
	// 更新节点状态
	n.Status = NodeStatusJoining
	n.LastSeen = time.Now()
	n.isRunning = true
	
	// 启动后台任务
	go n.heartbeatLoop()
	go n.eventHandler()
	go n.metricsCollector()
	
	// 加入集群
	if err := n.joinCluster(); err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}
	
	// 更新状态为活跃
	n.Status = NodeStatusActive
	
	return nil
}

// Stop 停止本地节点
func (n *LocalNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if !n.isRunning {
		return nil
	}
	
	// 更新状态
	n.Status = NodeStatusLeaving
	
	// 离开集群
	if n.meshCluster != nil {
		n.meshCluster.Leave()
		n.meshCluster.Stop()
	}
	
	// 取消上下文
	n.cancel()
	n.isRunning = false
	
	// 关闭事件通道
	close(n.eventChan)
	
	return nil
}

// joinCluster 加入集群
func (n *LocalNode) joinCluster() error {
	if len(n.config.SeedNodes) == 0 {
		// 单节点模式，直接标记为活跃
		n.Status = NodeStatusActive
		return nil
	}
	
	// 尝试连接到种子节点
	for _, seedNode := range n.config.SeedNodes {
		if err := n.connectToSeedNode(seedNode); err != nil {
			continue
		}
		return nil
	}
	
	return fmt.Errorf("failed to connect to any seed node")
}

// connectToSeedNode 连接到种子节点
func (n *LocalNode) connectToSeedNode(seedAddr string) error {
	// 连接到种子节点
	conn, err := net.DialTimeout("tcp", seedAddr, n.config.JoinTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect to seed node %s: %w", seedAddr, err)
	}
	defer conn.Close()
	
	// 创建协议编解码器
	codec := protocol.NewCodec(conn)
	defer codec.Close()
	
	// 发送加入请求
	joinRequest := &protocol.NodeHeartbeat{
		NodeId: n.ID,
		NodeInfo: &protocol.NodeInfo{
			NodeId:       n.ID,
			NodeAddr:     n.Addr,
			Labels:       n.Labels,
			Capabilities: n.Capabilities,
			LastSeen:     timestamppb.New(n.LastSeen),
		},
	}
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeNodeHeartbeat).
		WithPayload(joinRequest).
		Build()
	
	if err := codec.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to send join request: %w", err)
	}
	
	// 等待响应
	response, err := codec.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read join response: %w", err)
	}
	
	if response.Type == protocol.MsgTypeError {
		return fmt.Errorf("join rejected by seed node")
	}
	
	return nil
}

// heartbeatLoop 心跳循环
func (n *LocalNode) heartbeatLoop() {
	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.sendHeartbeat()
		}
	}
}

// sendHeartbeat 发送心跳
func (n *LocalNode) sendHeartbeat() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	if !n.isRunning || n.meshCluster == nil {
		return
	}
	
	// 更新最后可见时间
	n.LastSeen = time.Now()
	
	// 创建心跳消息
	heartbeat := &protocol.HeartbeatPayload{
		Status:            n.Status.String(),
		ActiveConnections: 0, // TODO: 从连接池获取
		CpuUsage:          int32(n.CPUUsage),
		MemoryUsage:       int32(n.MemoryUsage),
		Metrics:           make(map[string]string),
	}
	
	// 添加自定义指标
	heartbeat.Metrics["node_id"] = n.ID
	heartbeat.Metrics["version"] = n.Version
	
	// 通过gossip协议广播心跳
	n.meshCluster.BroadcastHeartbeat(heartbeat)
}

// eventHandler 事件处理器
func (n *LocalNode) eventHandler() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case event := <-n.eventChan:
			if event != nil {
				n.handleNodeEvent(event)
			}
		}
	}
}

// handleNodeEvent 处理节点事件
func (n *LocalNode) handleNodeEvent(event *NodeEvent) {
	switch event.Type {
	case NodeEventJoined:
		// 新节点加入
	case NodeEventLeft:
		// 节点离开
	case NodeEventFailed:
		// 节点失败
	case NodeEventUpdated:
		// 节点更新
	case NodeEventDiscovered:
		// 发现新节点
	}
}

// metricsCollector 指标收集器
func (n *LocalNode) metricsCollector() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.collectMetrics()
		}
	}
}

// collectMetrics 收集性能指标
func (n *LocalNode) collectMetrics() {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// TODO: 实现真实的性能指标收集
	// 这里只是模拟数据
	n.CPUUsage = 20.0
	n.MemoryUsage = 30.0
	n.LoadScore = (n.CPUUsage + n.MemoryUsage) / 2
}

// GetNodeInfo 获取节点信息
func (n *LocalNode) GetNodeInfo() *Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	// 复制节点信息
	node := *n.Node
	return &node
}

// UpdateLabels 更新节点标签
func (n *LocalNode) UpdateLabels(labels map[string]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// 更新标签
	for k, v := range labels {
		n.Labels[k] = v
	}
	
	// 发送事件
	select {
	case n.eventChan <- &NodeEvent{
		Type:      NodeEventUpdated,
		Node:      n.Node,
		Timestamp: time.Now(),
		Details:   "labels updated",
	}:
	default:
	}
}

// AddCapability 添加能力
func (n *LocalNode) AddCapability(capability string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// 检查是否已存在
	for _, cap := range n.Capabilities {
		if cap == capability {
			return
		}
	}
	
	n.Capabilities = append(n.Capabilities, capability)
}

// HasCapability 检查是否有指定能力
func (n *LocalNode) HasCapability(capability string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	for _, cap := range n.Capabilities {
		if cap == capability {
			return true
		}
	}
	return false
}

// IsHealthy 检查节点是否健康
func (n *LocalNode) IsHealthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return n.Status == NodeStatusActive && 
		   time.Since(n.LastSeen) < 3*n.config.HeartbeatInterval
}

// GetLoadScore 获取负载评分
func (n *LocalNode) GetLoadScore() float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return n.LoadScore
}

// generateNodeID 生成节点ID
func generateNodeID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// getLocalIP 获取本地IP地址
func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	
	return "", fmt.Errorf("no valid IP address found")
}

// DefaultNodeConfig 默认节点配置
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		ListenAddr:        ":7629",
		Labels:            make(map[string]string),
		ClusterName:       "default",
		SeedNodes:         []string{},
		JoinTimeout:       30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		TLSEnabled:        false,
		MaxConnections:    1000,
		BufferSize:        8192,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
}