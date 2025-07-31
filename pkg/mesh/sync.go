package mesh

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ServiceSyncManager 服务数据同步管理器
type ServiceSyncManager struct {
	mu           sync.RWMutex
	localNode    *LocalNode
	cluster      *MeshCluster
	registry     types.ServiceRegistry
	
	// 同步状态
	syncData     map[string]*SyncEntry
	syncVersion  uint64
	
	// 配置
	config       *SyncConfig
	
	// 控制
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	
	// 事件监听器
	listeners    []ServiceSyncListener
}

// SyncConfig 同步配置
type SyncConfig struct {
	// 同步间隔
	SyncInterval         time.Duration `yaml:"sync_interval" json:"sync_interval"`
	
	// Gossip配置
	GossipInterval       time.Duration `yaml:"gossip_interval" json:"gossip_interval"`
	GossipFanout         int           `yaml:"gossip_fanout" json:"gossip_fanout"`
	GossipRetransmitMult int           `yaml:"gossip_retransmit_mult" json:"gossip_retransmit_mult"`
	
	// 数据管理
	MaxSyncEntries       int           `yaml:"max_sync_entries" json:"max_sync_entries"`
	EntryTTL             time.Duration `yaml:"entry_ttl" json:"entry_ttl"`
	ConflictResolution   string        `yaml:"conflict_resolution" json:"conflict_resolution"`
	
	// 压缩配置
	EnableCompression    bool          `yaml:"enable_compression" json:"enable_compression"`
	CompressionThreshold int           `yaml:"compression_threshold" json:"compression_threshold"`
}

// SyncEntry 同步条目
type SyncEntry struct {
	ID          string                 `json:"id"`
	Type        SyncEntryType          `json:"type"`
	NodeID      string                 `json:"node_id"`
	Data        map[string]interface{} `json:"data"`
	Version     uint64                 `json:"version"`
	Timestamp   time.Time              `json:"timestamp"`
	TTL         time.Duration          `json:"ttl"`
	Checksum    string                 `json:"checksum"`
}

// SyncEntryType 同步条目类型
type SyncEntryType string

const (
	SyncEntryService  SyncEntryType = "service"
	SyncEntryEndpoint SyncEntryType = "endpoint"
	SyncEntryNode     SyncEntryType = "node"
	SyncEntryConfig   SyncEntryType = "config"
)

// ServiceSyncListener 服务同步事件监听器
type ServiceSyncListener interface {
	OnServiceSynced(service *types.Service)
	OnEndpointSynced(endpoint *types.Endpoint)
	OnSyncConflict(localEntry, remoteEntry *SyncEntry)
	OnSyncError(err error)
}

// SyncGossipMessage 服务同步的Gossip消息
type SyncGossipMessage struct {
	MessageID   string                 `json:"message_id"`
	Type        SyncGossipMessageType  `json:"type"`
	SenderID    string                 `json:"sender_id"`
	Timestamp   time.Time              `json:"timestamp"`
	Data        map[string]interface{} `json:"data"`
	Entries     []*SyncEntry           `json:"entries,omitempty"`
	Version     uint64                 `json:"version"`
	Compressed  bool                   `json:"compressed,omitempty"`
}

// SyncGossipMessageType 同步Gossip消息类型
type SyncGossipMessageType string

const (
	SyncGossipMessageSync    SyncGossipMessageType = "sync"
	SyncGossipMessageRequest SyncGossipMessageType = "request"
	SyncGossipMessageAck     SyncGossipMessageType = "ack"
	SyncGossipMessagePing    SyncGossipMessageType = "ping"
	SyncGossipMessagePong    SyncGossipMessageType = "pong"
)

// DefaultSyncConfig 返回默认同步配置
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		SyncInterval:         30 * time.Second,
		GossipInterval:       10 * time.Second,
		GossipFanout:         3,
		GossipRetransmitMult: 4,
		MaxSyncEntries:       10000,
		EntryTTL:             10 * time.Minute,
		ConflictResolution:   "timestamp_wins",
		EnableCompression:    true,
		CompressionThreshold: 1024,
	}
}

// NewServiceSyncManager 创建服务同步管理器
func NewServiceSyncManager(config *SyncConfig, localNode *LocalNode, cluster *MeshCluster, registry types.ServiceRegistry) (*ServiceSyncManager, error) {
	if config == nil {
		config = DefaultSyncConfig()
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	sm := &ServiceSyncManager{
		localNode:   localNode,
		cluster:     cluster,
		registry:    registry,
		syncData:    make(map[string]*SyncEntry),
		syncVersion: 1,
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		listeners:   make([]ServiceSyncListener, 0),
	}
	
	return sm, nil
}

// Start 启动同步管理器
func (sm *ServiceSyncManager) Start() error {
	// 启动同步循环
	sm.wg.Add(1)
	go sm.syncLoop()
	
	// 启动Gossip循环
	sm.wg.Add(1)
	go sm.gossipLoop()
	
	// 启动清理循环
	sm.wg.Add(1)
	go sm.cleanupLoop()
	
	return nil
}

// Stop 停止同步管理器
func (sm *ServiceSyncManager) Stop() error {
	sm.cancel()
	sm.wg.Wait()
	return nil
}

// AddListener 添加事件监听器
func (sm *ServiceSyncManager) AddListener(listener ServiceSyncListener) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.listeners = append(sm.listeners, listener)
}

// syncLoop 同步循环
func (sm *ServiceSyncManager) syncLoop() {
	defer sm.wg.Done()
	
	ticker := time.NewTicker(sm.config.SyncInterval)
	defer ticker.Stop()
	
	// 初始同步
	sm.performFullSync()
	
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performIncrementalSync()
		}
	}
}

// gossipLoop Gossip循环
func (sm *ServiceSyncManager) gossipLoop() {
	defer sm.wg.Done()
	
	ticker := time.NewTicker(sm.config.GossipInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performGossip()
		}
	}
}

// cleanupLoop 清理循环
func (sm *ServiceSyncManager) cleanupLoop() {
	defer sm.wg.Done()
	
	ticker := time.NewTicker(5 * time.Minute) // 每5分钟清理一次
	defer ticker.Stop()
	
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performCleanup()
		}
	}
}

// performFullSync 执行全量同步
func (sm *ServiceSyncManager) performFullSync() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	// 同步本地服务到同步数据
	sm.syncLocalServices()
	
	// 同步本地端点到同步数据
	sm.syncLocalEndpoints()
	
	// 增加版本号
	sm.syncVersion++
}

// performIncrementalSync 执行增量同步
func (sm *ServiceSyncManager) performIncrementalSync() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	// 检查本地服务变化
	if sm.hasLocalChanges() {
		sm.syncLocalServices()
		sm.syncLocalEndpoints()
		sm.syncVersion++
	}
}

// performGossip 执行Gossip传播
func (sm *ServiceSyncManager) performGossip() {
	// 获取健康的节点
	nodes := sm.cluster.GetHealthyNodes()
	if len(nodes) == 0 {
		return
	}
	
	// 选择Gossip目标节点
	targets := sm.selectGossipTargets(nodes)
	
	// 创建Gossip消息
	message := sm.createGossipMessage()
	
	// 向目标节点发送消息
	for _, target := range targets {
		go sm.sendGossipMessage(target, message)
	}
}

// performCleanup 执行清理
func (sm *ServiceSyncManager) performCleanup() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	now := time.Now()
	expired := make([]string, 0)
	
	// 查找过期条目
	for id, entry := range sm.syncData {
		if entry.TTL > 0 && now.Sub(entry.Timestamp) > entry.TTL {
			expired = append(expired, id)
		}
	}
	
	// 删除过期条目
	for _, id := range expired {
		delete(sm.syncData, id)
	}
	
	// 限制条目数量
	if len(sm.syncData) > sm.config.MaxSyncEntries {
		sm.limitSyncEntries()
	}
}

// syncLocalServices 同步本地服务
func (sm *ServiceSyncManager) syncLocalServices() {
	services, err := sm.registry.ListServices("")
	if err != nil {
		sm.notifyError(fmt.Errorf("failed to list local services: %w", err))
		return
	}
	
	for _, service := range services {
		entryID := fmt.Sprintf("service:%s:%s", service.Metadata.Namespace, service.Metadata.Name)
		
		// 序列化服务数据
		serviceData, err := sm.serializeService(service)
		if err != nil {
			sm.notifyError(fmt.Errorf("failed to serialize service %s: %w", service.Metadata.Name, err))
			continue
		}
		
		// 创建同步条目
		entry := &SyncEntry{
			ID:        entryID,
			Type:      SyncEntryService,
			NodeID:    sm.localNode.ID,
			Data:      serviceData,
			Version:   sm.syncVersion,
			Timestamp: time.Now(),
			TTL:       sm.config.EntryTTL,
			Checksum:  sm.calculateChecksum(serviceData),
		}
		
		sm.syncData[entryID] = entry
	}
}

// syncLocalEndpoints 同步本地端点
func (sm *ServiceSyncManager) syncLocalEndpoints() {
	// 获取所有服务的端点
	services, err := sm.registry.ListServices("")
	if err != nil {
		return
	}
	
	for _, service := range services {
		endpoints, err := sm.registry.GetEndpoints(service.Metadata.Name, service.Metadata.Namespace, nil)
		if err != nil {
			continue
		}
		
		for _, endpoint := range endpoints {
			entryID := fmt.Sprintf("endpoint:%s:%s:%s", service.Metadata.Namespace, service.Metadata.Name, endpoint.ID)
			
			// 序列化端点数据
			endpointData, err := sm.serializeEndpoint(endpoint)
			if err != nil {
				continue
			}
			
			// 创建同步条目
			entry := &SyncEntry{
				ID:        entryID,
				Type:      SyncEntryEndpoint,
				NodeID:    sm.localNode.ID,
				Data:      endpointData,
				Version:   sm.syncVersion,
				Timestamp: time.Now(),
				TTL:       sm.config.EntryTTL,
				Checksum:  sm.calculateChecksum(endpointData),
			}
			
			sm.syncData[entryID] = entry
		}
	}
}

// hasLocalChanges 检查是否有本地变化
func (sm *ServiceSyncManager) hasLocalChanges() bool {
	// 简化实现：假设总是有变化
	// 实际实现中应该比较本地数据的版本/时间戳
	return true
}

// selectGossipTargets 选择Gossip目标节点
func (sm *ServiceSyncManager) selectGossipTargets(nodes []*Node) []*Node {
	if len(nodes) == 0 {
		return nil
	}
	
	fanout := sm.config.GossipFanout
	if fanout > len(nodes) {
		fanout = len(nodes)
	}
	
	// 随机选择目标节点
	targets := make([]*Node, 0, fanout)
	used := make(map[string]bool)
	
	for len(targets) < fanout {
		idx := sm.randomInt(len(nodes))
		node := nodes[idx]
		
		if !used[node.ID] && node.ID != sm.localNode.ID {
			targets = append(targets, node)
			used[node.ID] = true
		}
		
		// 防止无限循环
		if len(used) >= len(nodes) {
			break
		}
	}
	
	return targets
}

// createGossipMessage 创建Gossip消息
func (sm *ServiceSyncManager) createGossipMessage() *SyncGossipMessage {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	// 选择要传播的条目（最新的条目）
	entries := sm.selectSyncEntries()
	
	message := &SyncGossipMessage{
		MessageID: sm.generateMessageID(),
		Type:      SyncGossipMessageSync,
		SenderID:  sm.localNode.ID,
		Timestamp: time.Now(),
		Entries:   entries,
		Version:   sm.syncVersion,
	}
	
	return message
}

// sendGossipMessage 发送Gossip消息
func (sm *ServiceSyncManager) sendGossipMessage(target *Node, message *SyncGossipMessage) {
	// 连接到目标节点
	conn, err := sm.cluster.ConnectToNode(target.ID)
	if err != nil {
		return
	}
	defer conn.Close()
	
	// 序列化消息
	data, err := json.Marshal(message)
	if err != nil {
		return
	}
	
	// 发送消息
	if _, err := conn.Write(data); err != nil {
		return
	}
}

// selectSyncEntries 选择要同步的条目
func (sm *ServiceSyncManager) selectSyncEntries() []*SyncEntry {
	entries := make([]*SyncEntry, 0)
	maxEntries := 100 // 限制每次传播的条目数量
	
	// 选择最新的条目
	for _, entry := range sm.syncData {
		if len(entries) >= maxEntries {
			break
		}
		entries = append(entries, entry)
	}
	
	return entries
}

// ProcessGossipMessage 处理收到的Gossip消息
func (sm *ServiceSyncManager) ProcessGossipMessage(message *SyncGossipMessage) error {
	switch message.Type {
	case SyncGossipMessageSync:
		return sm.processSyncMessage(message)
	case SyncGossipMessageRequest:
		return sm.processRequestMessage(message)
	case SyncGossipMessageAck:
		return sm.processAckMessage(message)
	case SyncGossipMessagePing:
		return sm.processPingMessage(message)
	case SyncGossipMessagePong:
		return sm.processPongMessage(message)
	default:
		return fmt.Errorf("unknown gossip message type: %s", message.Type)
	}
}

// processSyncMessage 处理同步消息
func (sm *ServiceSyncManager) processSyncMessage(message *SyncGossipMessage) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	updated := false
	
	for _, remoteEntry := range message.Entries {
		if remoteEntry.NodeID == sm.localNode.ID {
			continue // 跳过自己的条目
		}
		
		localEntry, exists := sm.syncData[remoteEntry.ID]
		
		if !exists {
			// 新条目，直接添加
			sm.syncData[remoteEntry.ID] = remoteEntry
			sm.applySyncEntry(remoteEntry)
			updated = true
		} else {
			// 条目已存在，检查是否需要更新
			if sm.shouldUpdateEntry(localEntry, remoteEntry) {
				sm.syncData[remoteEntry.ID] = remoteEntry
				sm.applySyncEntry(remoteEntry)
				updated = true
			} else if sm.hasConflict(localEntry, remoteEntry) {
				// 有冲突，通知监听器
				sm.notifyConflict(localEntry, remoteEntry)
			}
		}
	}
	
	if updated {
		sm.syncVersion++
	}
	
	return nil
}

// shouldUpdateEntry 判断是否应该更新条目
func (sm *ServiceSyncManager) shouldUpdateEntry(local, remote *SyncEntry) bool {
	switch sm.config.ConflictResolution {
	case "timestamp_wins":
		return remote.Timestamp.After(local.Timestamp)
	case "version_wins":
		return remote.Version > local.Version
	case "node_priority":
		// 可以基于节点优先级决定
		return remote.NodeID < local.NodeID // 简化实现
	default:
		return remote.Timestamp.After(local.Timestamp)
	}
}

// hasConflict 检查是否有冲突
func (sm *ServiceSyncManager) hasConflict(local, remote *SyncEntry) bool {
	return local.Checksum != remote.Checksum && 
		   local.Timestamp.Equal(remote.Timestamp) && 
		   local.Version == remote.Version
}

// applySyncEntry 应用同步条目
func (sm *ServiceSyncManager) applySyncEntry(entry *SyncEntry) {
	switch entry.Type {
	case SyncEntryService:
		sm.applyServiceEntry(entry)
	case SyncEntryEndpoint:
		sm.applyEndpointEntry(entry)
	case SyncEntryNode:
		sm.applyNodeEntry(entry)
	}
}

// applyServiceEntry 应用服务条目
func (sm *ServiceSyncManager) applyServiceEntry(entry *SyncEntry) {
	service, err := sm.deserializeService(entry.Data)
	if err != nil {
		sm.notifyError(fmt.Errorf("failed to deserialize service entry: %w", err))
		return
	}
	
	// 更新本地注册表
	if err := sm.registry.RegisterService(service); err != nil {
		sm.notifyError(fmt.Errorf("failed to register synced service: %w", err))
		return
	}
	
	// 通知监听器
	sm.notifyServiceSynced(service)
}

// applyEndpointEntry 应用端点条目
func (sm *ServiceSyncManager) applyEndpointEntry(entry *SyncEntry) {
	endpoint, err := sm.deserializeEndpoint(entry.Data)
	if err != nil {
		sm.notifyError(fmt.Errorf("failed to deserialize endpoint entry: %w", err))
		return
	}
	
	// 从entry ID中解析服务名称和命名空间
	// entryID格式: "endpoint:namespace:serviceName:endpointID"
	parts := strings.Split(entry.ID, ":")
	if len(parts) < 4 {
		sm.notifyError(fmt.Errorf("invalid endpoint entry ID format: %s", entry.ID))
		return
	}
	
	namespace := parts[1]
	serviceName := parts[2]
	
	// 更新本地注册表
	if err := sm.registry.RegisterEndpoint(serviceName, namespace, endpoint); err != nil {
		sm.notifyError(fmt.Errorf("failed to register synced endpoint: %w", err))
		return
	}
	
	// 通知监听器
	sm.notifyEndpointSynced(endpoint)
}

// applyNodeEntry 应用节点条目
func (sm *ServiceSyncManager) applyNodeEntry(entry *SyncEntry) {
	// TODO: 实现节点信息同步
}

// processRequestMessage 处理请求消息
func (sm *ServiceSyncManager) processRequestMessage(message *SyncGossipMessage) error {
	// TODO: 实现请求处理
	return nil
}

// processAckMessage 处理确认消息
func (sm *ServiceSyncManager) processAckMessage(message *SyncGossipMessage) error {
	// TODO: 实现确认处理
	return nil
}

// processPingMessage 处理Ping消息
func (sm *ServiceSyncManager) processPingMessage(message *SyncGossipMessage) error {
	// TODO: 实现Ping处理
	return nil
}

// processPongMessage 处理Pong消息
func (sm *ServiceSyncManager) processPongMessage(message *SyncGossipMessage) error {
	// TODO: 实现Pong处理
	return nil
}

// limitSyncEntries 限制同步条目数量
func (sm *ServiceSyncManager) limitSyncEntries() {
	// 按时间戳排序，保留最新的条目
	if len(sm.syncData) <= sm.config.MaxSyncEntries {
		return
	}
	
	// 简化实现：删除最旧的条目
	oldestTime := time.Now()
	oldestID := ""
	
	for id, entry := range sm.syncData {
		if entry.Timestamp.Before(oldestTime) {
			oldestTime = entry.Timestamp
			oldestID = id
		}
	}
	
	if oldestID != "" {
		delete(sm.syncData, oldestID)
	}
}

// 序列化和反序列化方法 (公共接口用于测试)
func (sm *ServiceSyncManager) SerializeService(service *types.Service) (map[string]interface{}, error) {
	return sm.serializeService(service)
}

func (sm *ServiceSyncManager) DeserializeService(data map[string]interface{}) (*types.Service, error) {
	return sm.deserializeService(data)
}

func (sm *ServiceSyncManager) SerializeEndpoint(endpoint *types.Endpoint) (map[string]interface{}, error) {
	return sm.serializeEndpoint(endpoint)
}

func (sm *ServiceSyncManager) DeserializeEndpoint(data map[string]interface{}) (*types.Endpoint, error) {
	return sm.deserializeEndpoint(data)
}

// 内部序列化和反序列化方法
func (sm *ServiceSyncManager) serializeService(service *types.Service) (map[string]interface{}, error) {
	data, err := json.Marshal(service)
	if err != nil {
		return nil, err
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	
	return result, nil
}

func (sm *ServiceSyncManager) deserializeService(data map[string]interface{}) (*types.Service, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	
	var service types.Service
	if err := json.Unmarshal(jsonData, &service); err != nil {
		return nil, err
	}
	
	return &service, nil
}

func (sm *ServiceSyncManager) serializeEndpoint(endpoint *types.Endpoint) (map[string]interface{}, error) {
	data, err := json.Marshal(endpoint)
	if err != nil {
		return nil, err
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	
	return result, nil
}

func (sm *ServiceSyncManager) deserializeEndpoint(data map[string]interface{}) (*types.Endpoint, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	
	var endpoint types.Endpoint
	if err := json.Unmarshal(jsonData, &endpoint); err != nil {
		return nil, err
	}
	
	return &endpoint, nil
}

// calculateChecksum 计算校验和
func (sm *ServiceSyncManager) calculateChecksum(data map[string]interface{}) string {
	jsonData, _ := json.Marshal(data)
	// 简化实现：使用数据长度作为校验和
	return fmt.Sprintf("%x", len(jsonData))
}

// generateMessageID 生成消息ID
func (sm *ServiceSyncManager) generateMessageID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// randomInt 生成随机整数
func (sm *ServiceSyncManager) randomInt(max int) int {
	if max <= 0 {
		return 0
	}
	bytes := make([]byte, 1)
	rand.Read(bytes)
	return int(bytes[0]) % max
}

// 通知方法
func (sm *ServiceSyncManager) notifyServiceSynced(service *types.Service) {
	for _, listener := range sm.listeners {
		go listener.OnServiceSynced(service)
	}
}

func (sm *ServiceSyncManager) notifyEndpointSynced(endpoint *types.Endpoint) {
	for _, listener := range sm.listeners {
		go listener.OnEndpointSynced(endpoint)
	}
}

func (sm *ServiceSyncManager) notifyConflict(local, remote *SyncEntry) {
	for _, listener := range sm.listeners {
		go listener.OnSyncConflict(local, remote)
	}
}

func (sm *ServiceSyncManager) notifyError(err error) {
	for _, listener := range sm.listeners {
		go listener.OnSyncError(err)
	}
}

// GetSyncStats 获取同步统计信息
func (sm *ServiceSyncManager) GetSyncStats() *SyncStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	stats := &SyncStats{
		TotalEntries:    len(sm.syncData),
		ServiceEntries:  0,
		EndpointEntries: 0,
		NodeEntries:     0,
		SyncVersion:     sm.syncVersion,
		LastSyncTime:    time.Now(),
	}
	
	for _, entry := range sm.syncData {
		switch entry.Type {
		case SyncEntryService:
			stats.ServiceEntries++
		case SyncEntryEndpoint:
			stats.EndpointEntries++
		case SyncEntryNode:
			stats.NodeEntries++
		}
	}
	
	return stats
}

// SyncStats 同步统计信息
type SyncStats struct {
	TotalEntries    int       `json:"total_entries"`
	ServiceEntries  int       `json:"service_entries"`
	EndpointEntries int       `json:"endpoint_entries"`
	NodeEntries     int       `json:"node_entries"`
	SyncVersion     uint64    `json:"sync_version"`
	LastSyncTime    time.Time `json:"last_sync_time"`
}