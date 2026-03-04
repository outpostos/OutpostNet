package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// DiscoveryManager 节点发现管理器
type DiscoveryManager struct {
	mu           sync.RWMutex
	config       *DiscoveryConfig
	localNode    *LocalNode
	discoveredNodes map[string]*Node
	
	// 发现方法
	methods      []DiscoveryMethod
	
	// 控制
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	
	// 事件监听器
	listeners    []NodeEventListener
}

// DiscoveryConfig 节点发现配置
type DiscoveryConfig struct {
	// 发现方法
	Methods []string `yaml:"methods" json:"methods"` // "static", "multicast", "env", "dns"
	
	// 静态节点配置
	StaticNodes []StaticNodeConfig `yaml:"static_nodes" json:"static_nodes"`
	
	// 多播发现配置
	MulticastConfig MulticastConfig `yaml:"multicast" json:"multicast"`
	
	// DNS发现配置
	DNSConfig DNSConfig `yaml:"dns" json:"dns"`
	
	// 环境变量发现配置
	EnvConfig EnvConfig `yaml:"env" json:"env"`
	
	// 发现间隔
	DiscoveryInterval time.Duration `yaml:"discovery_interval" json:"discovery_interval"`
	
	// 节点超时时间
	NodeTimeout time.Duration `yaml:"node_timeout" json:"node_timeout"`
	
	// 健康检查间隔
	HealthCheckInterval time.Duration `yaml:"health_check_interval" json:"health_check_interval"`
}

// StaticNodeConfig 静态节点配置
type StaticNodeConfig struct {
	ID       string            `yaml:"id" json:"id"`
	Address  string            `yaml:"address" json:"address"`
	Labels   map[string]string `yaml:"labels" json:"labels"`
	Priority int               `yaml:"priority" json:"priority"`
}

// MulticastConfig 多播发现配置
type MulticastConfig struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	Group       string `yaml:"group" json:"group"`
	Port        int    `yaml:"port" json:"port"`
	Interface   string `yaml:"interface" json:"interface"`
	TTL         int    `yaml:"ttl" json:"ttl"`
	Interval    time.Duration `yaml:"interval" json:"interval"`
}

// DNSConfig DNS发现配置
type DNSConfig struct {
	Enabled    bool     `yaml:"enabled" json:"enabled"`
	Domain     string   `yaml:"domain" json:"domain"`
	Servers    []string `yaml:"servers" json:"servers"`
	Interval   time.Duration `yaml:"interval" json:"interval"`
	RecordType string   `yaml:"record_type" json:"record_type"` // "A", "SRV", "TXT"
}

// EnvConfig 环境变量发现配置
type EnvConfig struct {
	Enabled          bool   `yaml:"enabled" json:"enabled"`
	NodesEnvVar      string `yaml:"nodes_env_var" json:"nodes_env_var"`
	ClusterEnvVar    string `yaml:"cluster_env_var" json:"cluster_env_var"`
	Separator        string `yaml:"separator" json:"separator"`
}

// DiscoveryMethod 发现方法接口
type DiscoveryMethod interface {
	Name() string
	Discover(ctx context.Context) ([]*Node, error)
	Start(ctx context.Context) error
	Stop() error
}

// NodeEventListener 节点事件监听器
type NodeEventListener interface {
	OnNodeJoined(node *Node)
	OnNodeLeft(node *Node)
	OnNodeUpdated(node *Node)
	OnNodeFailed(node *Node)
}

// 使用 node.go 中定义的 NodeEvent 和 NodeEventType

// DefaultDiscoveryConfig 返回默认发现配置
func DefaultDiscoveryConfig() *DiscoveryConfig {
	return &DiscoveryConfig{
		Methods: []string{"static", "env"},
		StaticNodes: []StaticNodeConfig{},
		MulticastConfig: MulticastConfig{
			Enabled:   false,
			Group:     "224.0.0.1",
			Port:      7630,
			Interface: "",
			TTL:       1,
			Interval:  30 * time.Second,
		},
		DNSConfig: DNSConfig{
			Enabled:    false,
			Domain:     "",
			Servers:    []string{"8.8.8.8:53"},
			Interval:   60 * time.Second,
			RecordType: "A",
		},
		EnvConfig: EnvConfig{
			Enabled:       true,
			NodesEnvVar:   "OUTPOSTNET_NODES",
			ClusterEnvVar: "OUTPOSTNET_CLUSTER_NODES",
			Separator:     ",",
		},
		DiscoveryInterval:   30 * time.Second,
		NodeTimeout:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
	}
}

// NewDiscoveryManager 创建节点发现管理器
func NewDiscoveryManager(config *DiscoveryConfig, localNode *LocalNode) (*DiscoveryManager, error) {
	if config == nil {
		config = DefaultDiscoveryConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	dm := &DiscoveryManager{
		config:          config,
		localNode:       localNode,
		discoveredNodes: make(map[string]*Node),
		methods:         make([]DiscoveryMethod, 0),
		ctx:             ctx,
		cancel:          cancel,
		listeners:       make([]NodeEventListener, 0),
	}

	// 初始化发现方法
	if err := dm.initDiscoveryMethods(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize discovery methods: %w", err)
	}

	return dm, nil
}

// initDiscoveryMethods 初始化发现方法
func (dm *DiscoveryManager) initDiscoveryMethods() error {
	for _, methodName := range dm.config.Methods {
		switch methodName {
		case "static":
			method := NewStaticDiscovery(dm.config.StaticNodes)
			dm.methods = append(dm.methods, method)
		case "multicast":
			if dm.config.MulticastConfig.Enabled {
				method, err := NewMulticastDiscovery(&dm.config.MulticastConfig, dm.localNode)
				if err != nil {
					return fmt.Errorf("failed to create multicast discovery: %w", err)
				}
				dm.methods = append(dm.methods, method)
			}
		case "dns":
			if dm.config.DNSConfig.Enabled {
				method := NewDNSDiscovery(&dm.config.DNSConfig)
				dm.methods = append(dm.methods, method)
			}
		case "env":
			if dm.config.EnvConfig.Enabled {
				method := NewEnvDiscovery(&dm.config.EnvConfig)
				dm.methods = append(dm.methods, method)
			}
		default:
			return fmt.Errorf("unknown discovery method: %s", methodName)
		}
	}

	if len(dm.methods) == 0 {
		return fmt.Errorf("no discovery methods configured")
	}

	return nil
}

// Start 启动发现管理器
func (dm *DiscoveryManager) Start() error {
	// 启动所有发现方法
	for _, method := range dm.methods {
		if err := method.Start(dm.ctx); err != nil {
			return fmt.Errorf("failed to start discovery method %s: %w", method.Name(), err)
		}
	}

	// 启动发现循环
	dm.wg.Add(1)
	go dm.discoveryLoop()

	// 启动健康检查循环
	dm.wg.Add(1)
	go dm.healthCheckLoop()

	return nil
}

// Stop 停止发现管理器
func (dm *DiscoveryManager) Stop() error {
	dm.cancel()
	dm.wg.Wait()

	// 停止所有发现方法
	for _, method := range dm.methods {
		if err := method.Stop(); err != nil {
			// 记录错误但继续停止其他方法
			fmt.Printf("Error stopping discovery method %s: %v\n", method.Name(), err)
		}
	}

	return nil
}

// GetDiscoveredNodes 获取发现的节点
func (dm *DiscoveryManager) GetDiscoveredNodes() map[string]*Node {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	nodes := make(map[string]*Node)
	for id, node := range dm.discoveredNodes {
		nodeCopy := *node
		nodes[id] = &nodeCopy
	}
	return nodes
}

// AddEventListener 添加事件监听器
func (dm *DiscoveryManager) AddEventListener(listener NodeEventListener) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.listeners = append(dm.listeners, listener)
}

// discoveryLoop 发现循环
func (dm *DiscoveryManager) discoveryLoop() {
	defer dm.wg.Done()

	ticker := time.NewTicker(dm.config.DiscoveryInterval)
	defer ticker.Stop()

	// 初始发现
	dm.performDiscovery()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.performDiscovery()
		}
	}
}

// performDiscovery 执行发现
func (dm *DiscoveryManager) performDiscovery() {
	allNodes := make(map[string]*Node)

	// 从所有发现方法收集节点
	for _, method := range dm.methods {
		nodes, err := method.Discover(dm.ctx)
		if err != nil {
			fmt.Printf("Discovery error from method %s: %v\n", method.Name(), err)
			continue
		}

		for _, node := range nodes {
			// 跳过自己
			if node.ID == dm.localNode.ID {
				continue
			}
			allNodes[node.ID] = node
		}
	}

	// 更新发现的节点
	dm.updateDiscoveredNodes(allNodes)
}

// updateDiscoveredNodes 更新发现的节点
func (dm *DiscoveryManager) updateDiscoveredNodes(newNodes map[string]*Node) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	now := time.Now()

	// 检查新加入的节点
	for id, node := range newNodes {
		if existingNode, exists := dm.discoveredNodes[id]; exists {
			// 更新现有节点
			if !nodeEqual(existingNode, node) {
				existingNode.Addr = node.Addr
				existingNode.Labels = node.Labels
				existingNode.Capabilities = node.Capabilities
				existingNode.LastSeen = now
				dm.notifyNodeUpdated(existingNode)
			} else {
				existingNode.LastSeen = now
			}
		} else {
			// 新节点加入
			node.LastSeen = now
			node.Status = NodeStatusActive
			dm.discoveredNodes[id] = node
			dm.notifyNodeJoined(node)
		}
	}

	// 检查离开的节点（超时）
	for nodeID, node := range dm.discoveredNodes {
		if _, exists := newNodes[nodeID]; !exists {
			if now.Sub(node.LastSeen) > dm.config.NodeTimeout {
				delete(dm.discoveredNodes, nodeID)
				node.Status = NodeStatusFailed
				dm.notifyNodeLeft(node)
			}
		}
	}
}

// healthCheckLoop 健康检查循环
func (dm *DiscoveryManager) healthCheckLoop() {
	defer dm.wg.Done()

	ticker := time.NewTicker(dm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (dm *DiscoveryManager) performHealthCheck() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	now := time.Now()
	for _, node := range dm.discoveredNodes {
		// 检查节点是否超时
		if now.Sub(node.LastSeen) > dm.config.NodeTimeout {
			if node.Status != NodeStatusFailed {
				node.Status = NodeStatusFailed
				dm.notifyNodeFailed(node)
			}
		}

		// 可以在这里添加更多健康检查逻辑，比如ping测试
		if err := dm.pingNode(node); err != nil {
			if node.Status == NodeStatusActive {
				node.Status = NodeStatusFailed
				dm.notifyNodeFailed(node)
			}
		} else {
			if node.Status == NodeStatusFailed {
				node.Status = NodeStatusActive
				node.LastSeen = now
				dm.notifyNodeJoined(node) // 重新加入
			}
		}
	}
}

// pingNode 检查节点连通性
func (dm *DiscoveryManager) pingNode(node *Node) error {
	// 简单的TCP连接测试
	conn, err := net.DialTimeout("tcp", node.Addr, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// 通知方法
func (dm *DiscoveryManager) notifyNodeJoined(node *Node) {
	for _, listener := range dm.listeners {
		go listener.OnNodeJoined(node)
	}
}

func (dm *DiscoveryManager) notifyNodeLeft(node *Node) {
	for _, listener := range dm.listeners {
		go listener.OnNodeLeft(node)
	}
}

func (dm *DiscoveryManager) notifyNodeUpdated(node *Node) {
	for _, listener := range dm.listeners {
		go listener.OnNodeUpdated(node)
	}
}

func (dm *DiscoveryManager) notifyNodeFailed(node *Node) {
	for _, listener := range dm.listeners {
		go listener.OnNodeFailed(node)
	}
}

// 辅助函数
func nodeEqual(a, b *Node) bool {
	if a.Addr != b.Addr {
		return false
	}
	
	if len(a.Labels) != len(b.Labels) {
		return false
	}
	for k, v := range a.Labels {
		if b.Labels[k] != v {
			return false
		}
	}
	
	if len(a.Capabilities) != len(b.Capabilities) {
		return false
	}
	for i, cap := range a.Capabilities {
		if b.Capabilities[i] != cap {
			return false
		}
	}
	
	return true
}

// =============================================================================
// 具体发现方法实现
// =============================================================================

// StaticDiscovery 静态节点发现
type StaticDiscovery struct {
	nodes []StaticNodeConfig
}

func NewStaticDiscovery(nodes []StaticNodeConfig) *StaticDiscovery {
	return &StaticDiscovery{nodes: nodes}
}

func (sd *StaticDiscovery) Name() string {
	return "static"
}

func (sd *StaticDiscovery) Discover(ctx context.Context) ([]*Node, error) {
	nodes := make([]*Node, 0, len(sd.nodes))
	for _, config := range sd.nodes {
		node := &Node{
			ID:           config.ID,
			Addr:         config.Address,
			Labels:       config.Labels,
			Capabilities: []string{"static"},
			Status:       NodeStatusActive,
			LastSeen:     time.Now(),
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (sd *StaticDiscovery) Start(ctx context.Context) error {
	return nil // 静态发现不需要启动
}

func (sd *StaticDiscovery) Stop() error {
	return nil // 静态发现不需要停止
}

// EnvDiscovery 环境变量发现
type EnvDiscovery struct {
	config *EnvConfig
}

func NewEnvDiscovery(config *EnvConfig) *EnvDiscovery {
	return &EnvDiscovery{config: config}
}

func (ed *EnvDiscovery) Name() string {
	return "env"
}

func (ed *EnvDiscovery) Discover(ctx context.Context) ([]*Node, error) {
	nodes := make([]*Node, 0)

	// 从环境变量读取节点列表
	nodesEnv := os.Getenv(ed.config.NodesEnvVar)
	if nodesEnv != "" {
		nodeAddrs := strings.Split(nodesEnv, ed.config.Separator)
		for i, addr := range nodeAddrs {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			
			node := &Node{
				ID:           fmt.Sprintf("env-node-%d", i),
				Addr:         addr,
				Labels:       map[string]string{"source": "env"},
				Capabilities: []string{"env"},
				Status:       NodeStatusActive,
				LastSeen:     time.Now(),
			}
			nodes = append(nodes, node)
		}
	}

	// 从集群环境变量读取
	clusterEnv := os.Getenv(ed.config.ClusterEnvVar)
	if clusterEnv != "" {
		// 支持JSON格式的节点配置
		var clusterNodes []struct {
			ID      string            `json:"id"`
			Address string            `json:"address"`
			Labels  map[string]string `json:"labels"`
		}
		
		if err := json.Unmarshal([]byte(clusterEnv), &clusterNodes); err == nil {
			for _, config := range clusterNodes {
				node := &Node{
					ID:           config.ID,
					Addr:         config.Address,
					Labels:       config.Labels,
					Capabilities: []string{"env", "cluster"},
					Status:       NodeStatusActive,
					LastSeen:     time.Now(),
				}
				nodes = append(nodes, node)
			}
		}
	}

	return nodes, nil
}

func (ed *EnvDiscovery) Start(ctx context.Context) error {
	return nil
}

func (ed *EnvDiscovery) Stop() error {
	return nil
}

// MulticastDiscovery 多播发现
type MulticastDiscovery struct {
	config    *MulticastConfig
	localNode *LocalNode
	conn      *net.UDPConn
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewMulticastDiscovery(config *MulticastConfig, localNode *LocalNode) (*MulticastDiscovery, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &MulticastDiscovery{
		config:    config,
		localNode: localNode,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (md *MulticastDiscovery) Name() string {
	return "multicast"
}

func (md *MulticastDiscovery) Discover(ctx context.Context) ([]*Node, error) {
	// 多播发现实现（简化版）
	// 在实际实现中，这里会监听多播消息并解析节点信息
	return []*Node{}, nil
}

func (md *MulticastDiscovery) Start(ctx context.Context) error {
	// 启动多播监听
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", md.config.Group, md.config.Port))
	if err != nil {
		return err
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	md.conn = conn

	// 启动广播循环
	go md.broadcastLoop()
	
	return nil
}

func (md *MulticastDiscovery) Stop() error {
	md.cancel()
	if md.conn != nil {
		return md.conn.Close()
	}
	return nil
}

func (md *MulticastDiscovery) broadcastLoop() {
	ticker := time.NewTicker(md.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-md.ctx.Done():
			return
		case <-ticker.C:
			md.broadcastPresence()
		}
	}
}

func (md *MulticastDiscovery) broadcastPresence() {
	// 广播本节点信息
	nodeInfo := map[string]interface{}{
		"id":      md.localNode.ID,
		"address": md.localNode.Addr,
		"labels":  md.localNode.Labels,
	}
	
	data, err := json.Marshal(nodeInfo)
	if err != nil {
		return
	}

	if md.conn != nil {
		md.conn.Write(data)
	}
}

// DNSDiscovery DNS发现
type DNSDiscovery struct {
	config *DNSConfig
}

func NewDNSDiscovery(config *DNSConfig) *DNSDiscovery {
	return &DNSDiscovery{config: config}
}

func (dd *DNSDiscovery) Name() string {
	return "dns"
}

func (dd *DNSDiscovery) Discover(ctx context.Context) ([]*Node, error) {
	// DNS发现实现（简化版）
	nodes := make([]*Node, 0)

	// 解析域名
	if dd.config.Domain != "" {
		addrs, err := net.LookupHost(dd.config.Domain)
		if err != nil {
			return nil, err
		}

		for i, addr := range addrs {
			node := &Node{
				ID:           fmt.Sprintf("dns-node-%d", i),
				Addr:         fmt.Sprintf("%s:7629", addr), // 假设默认端口
				Labels:       map[string]string{"source": "dns"},
				Capabilities: []string{"dns"},
				Status:       NodeStatusActive,
				LastSeen:     time.Now(),
			}
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

func (dd *DNSDiscovery) Start(ctx context.Context) error {
	return nil
}

func (dd *DNSDiscovery) Stop() error {
	return nil
}