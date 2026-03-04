package mesh

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"google.golang.org/protobuf/proto"
)

// ClusterInterface 定义集群接口，供gossip管理器使用
type ClusterInterface interface {
	GetHealthyNodes() []*Node
	ConnectToNode(nodeID string) (net.Conn, error)
	BroadcastHeartbeat(heartbeat *protocol.HeartbeatPayload)
	GetLocalNode() *LocalNode
	GetGossipManager() *GossipManager
}

// GossipManager 管理gossip协议的消息传播
type GossipManager struct {
	cluster     ClusterInterface
	config      *GossipConfig
	
	// 消息缓存
	messageCache map[string]*GossipMessage
	cacheMu      sync.RWMutex
	
	// 传播队列
	propagationQueue chan *GossipMessage
	
	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// GossipConfig Gossip协议配置
type GossipConfig struct {
	// 传播参数
	FanOut           int           `yaml:"fan_out" json:"fan_out"`                       // 每轮传播的节点数
	PropagationDelay time.Duration `yaml:"propagation_delay" json:"propagation_delay"` // 传播延迟
	MaxPropagations  int           `yaml:"max_propagations" json:"max_propagations"`   // 最大传播轮数
	
	// 消息缓存
	CacheSize    int           `yaml:"cache_size" json:"cache_size"`       // 缓存大小
	CacheTTL     time.Duration `yaml:"cache_ttl" json:"cache_ttl"`         // 缓存TTL
	CleanupInterval time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"` // 清理间隔
	
	// 网络参数
	RetryCount     int           `yaml:"retry_count" json:"retry_count"`         // 重试次数
	RetryDelay     time.Duration `yaml:"retry_delay" json:"retry_delay"`         // 重试延迟
	MessageTimeout time.Duration `yaml:"message_timeout" json:"message_timeout"` // 消息超时
}

// GossipMessage Gossip消息
type GossipMessage struct {
	ID          string                 `json:"id"`
	Type        GossipMessageType      `json:"type"`
	Payload     []byte                 `json:"payload"`
	Timestamp   time.Time              `json:"timestamp"`
	Source      string                 `json:"source"`
	TTL         int                    `json:"ttl"`
	Propagated  int                    `json:"propagated"`
	Metadata    map[string]string      `json:"metadata"`
}

// GossipMessageType Gossip消息类型
type GossipMessageType int

const (
	GossipMessageHeartbeat GossipMessageType = iota
	GossipMessageServiceSync
	GossipMessageNodeJoin
	GossipMessageNodeLeave
	GossipMessageCustom
)

func (t GossipMessageType) String() string {
	switch t {
	case GossipMessageHeartbeat:
		return "heartbeat"
	case GossipMessageServiceSync:
		return "service_sync"
	case GossipMessageNodeJoin:
		return "node_join"
	case GossipMessageNodeLeave:
		return "node_leave"
	case GossipMessageCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// NewGossipManager 创建Gossip管理器
func NewGossipManager(cluster ClusterInterface) (*GossipManager, error) {
	config := DefaultGossipConfig()
	ctx, cancel := context.WithCancel(context.Background())
	
	gm := &GossipManager{
		cluster:          cluster,
		config:           config,
		messageCache:     make(map[string]*GossipMessage),
		propagationQueue: make(chan *GossipMessage, config.CacheSize),
		ctx:              ctx,
		cancel:           cancel,
	}
	
	return gm, nil
}

// Start 启动Gossip管理器
func (gm *GossipManager) Start() error {
	// 启动消息传播器
	gm.wg.Add(1)
	go gm.propagationWorker()
	
	// 启动缓存清理器
	gm.wg.Add(1)
	go gm.cacheCleanupWorker()
	
	return nil
}

// Stop 停止Gossip管理器
func (gm *GossipManager) Stop() {
	gm.cancel()
	close(gm.propagationQueue)
	gm.wg.Wait()
}

// BroadcastHeartbeat 广播心跳消息
func (gm *GossipManager) BroadcastHeartbeat(heartbeat *protocol.HeartbeatPayload) {
	payload, err := proto.Marshal(heartbeat)
	if err != nil {
		return
	}
	
	msg := &GossipMessage{
		ID:        generateNodeID(),
		Type:      GossipMessageHeartbeat,
		Payload:   payload,
		Timestamp: time.Now(),
		Source:    gm.cluster.GetLocalNode().ID,
		TTL:       gm.config.MaxPropagations,
		Metadata:  make(map[string]string),
	}
	
	gm.propagateMessage(msg)
}

// BroadcastServiceSync 广播服务同步消息
func (gm *GossipManager) BroadcastServiceSync(serviceSync *protocol.ServiceSync) {
	payload, err := proto.Marshal(serviceSync)
	if err != nil {
		return
	}
	
	msg := &GossipMessage{
		ID:        generateNodeID(),
		Type:      GossipMessageServiceSync,
		Payload:   payload,
		Timestamp: time.Now(),
		Source:    gm.cluster.GetLocalNode().ID,
		TTL:       gm.config.MaxPropagations,
		Metadata:  make(map[string]string),
	}
	
	gm.propagateMessage(msg)
}

// BroadcastNodeJoin 广播节点加入消息
func (gm *GossipManager) BroadcastNodeJoin(nodeInfo *protocol.NodeInfo) {
	payload, err := proto.Marshal(nodeInfo)
	if err != nil {
		return
	}
	
	msg := &GossipMessage{
		ID:        generateNodeID(),
		Type:      GossipMessageNodeJoin,
		Payload:   payload,
		Timestamp: time.Now(),
		Source:    gm.cluster.GetLocalNode().ID,
		TTL:       gm.config.MaxPropagations,
		Metadata:  make(map[string]string),
	}
	
	gm.propagateMessage(msg)
}

// BroadcastLeave 广播离开消息
func (gm *GossipManager) BroadcastLeave(nodeHeartbeat *protocol.NodeHeartbeat) {
	payload, err := proto.Marshal(nodeHeartbeat)
	if err != nil {
		return
	}
	
	msg := &GossipMessage{
		ID:        generateNodeID(),
		Type:      GossipMessageNodeLeave,
		Payload:   payload,
		Timestamp: time.Now(),
		Source:    gm.cluster.GetLocalNode().ID,
		TTL:       gm.config.MaxPropagations,
		Metadata:  make(map[string]string),
	}
	
	gm.propagateMessage(msg)
}

// PropagateHeartbeat 传播心跳消息
func (gm *GossipManager) PropagateHeartbeat(heartbeat *protocol.NodeHeartbeat) {
	payload, err := proto.Marshal(heartbeat)
	if err != nil {
		return
	}
	
	msg := &GossipMessage{
		ID:        generateNodeID(),
		Type:      GossipMessageHeartbeat,
		Payload:   payload,
		Timestamp: time.Now(),
		Source:    heartbeat.NodeId,
		TTL:       gm.config.MaxPropagations - 1, // 减少TTL
		Metadata:  make(map[string]string),
	}
	
	gm.propagateMessage(msg)
}

// propagateMessage 传播消息
func (gm *GossipManager) propagateMessage(msg *GossipMessage) {
	// 检查是否已经处理过这个消息
	if gm.isMessageProcessed(msg.ID) {
		return
	}
	
	// 添加到缓存
	gm.addToCache(msg)
	
	// 添加到传播队列
	select {
	case gm.propagationQueue <- msg:
	case <-gm.ctx.Done():
		return
	default:
		// 队列满了，丢弃消息
	}
}

// propagationWorker 传播工作器
func (gm *GossipManager) propagationWorker() {
	defer gm.wg.Done()
	
	for {
		select {
		case <-gm.ctx.Done():
			return
		case msg := <-gm.propagationQueue:
			if msg != nil {
				gm.processPropagationMessage(msg)
			}
		}
	}
}

// processPropagationMessage 处理传播消息
func (gm *GossipManager) processPropagationMessage(msg *GossipMessage) {
	// 检查TTL
	if msg.TTL <= 0 {
		return
	}
	
	// 获取健康节点
	healthyNodes := gm.cluster.GetHealthyNodes()
	if len(healthyNodes) == 0 {
		return
	}
	
	// 选择传播目标
	targets := gm.selectPropagationTargets(healthyNodes, msg)
	
	// 并发传播
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			gm.sendMessageToNode(msg, nodeID)
		}(target.ID)
	}
	
	// 等待传播完成
	wg.Wait()
	
	// 更新传播次数
	msg.Propagated++
	
	// 如果还有TTL，添加延迟后继续传播
	if msg.TTL > 1 {
		go func() {
			select {
			case <-gm.ctx.Done():
				return
			case <-time.After(gm.config.PropagationDelay):
				msg.TTL--
				gm.propagateMessage(msg)
			}
		}()
	}
}

// selectPropagationTargets 选择传播目标
func (gm *GossipManager) selectPropagationTargets(nodes []*Node, msg *GossipMessage) []*Node {
	// 过滤掉消息源和本节点
	var candidates []*Node
	for _, node := range nodes {
		if node.ID != msg.Source && node.ID != gm.cluster.GetLocalNode().ID {
			candidates = append(candidates, node)
		}
	}
	
	// 如果候选节点数量小于扇出数，返回所有候选节点
	if len(candidates) <= gm.config.FanOut {
		return candidates
	}
	
	// 随机选择FanOut个节点
	selected := make([]*Node, 0, gm.config.FanOut)
	indices := rand.Perm(len(candidates))
	
	for i := 0; i < gm.config.FanOut && i < len(indices); i++ {
		selected = append(selected, candidates[indices[i]])
	}
	
	return selected
}

// sendMessageToNode 发送消息到指定节点
func (gm *GossipManager) sendMessageToNode(msg *GossipMessage, nodeID string) {
	for retry := 0; retry < gm.config.RetryCount; retry++ {
		if err := gm.attemptSendMessage(msg, nodeID); err == nil {
			return
		}
		
		// 重试延迟
		if retry < gm.config.RetryCount-1 {
			time.Sleep(gm.config.RetryDelay)
		}
	}
}

// attemptSendMessage 尝试发送消息
func (gm *GossipManager) attemptSendMessage(msg *GossipMessage, nodeID string) error {
	// 连接到目标节点
	conn, err := gm.cluster.ConnectToNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to connect to node %s: %w", nodeID, err)
	}
	defer conn.Close()
	
	// 设置超时
	conn.SetWriteDeadline(time.Now().Add(gm.config.MessageTimeout))
	
	// 创建编解码器
	codec := protocol.NewCodec(conn)
	defer codec.Close()
	
	// 构造协议消息
	var protoMsg *protocol.Message
	switch msg.Type {
	case GossipMessageHeartbeat:
		protoMsg = protocol.NewMessageBuilder(protocol.MsgTypeNodeHeartbeat).
			WithPayload(&protocol.NodeHeartbeat{}).
			Build()
		protoMsg.Payload = msg.Payload
	case GossipMessageServiceSync:
		protoMsg = protocol.NewMessageBuilder(protocol.MsgTypeServiceSync).
			WithPayload(&protocol.ServiceSync{}).
			Build()
		protoMsg.Payload = msg.Payload
	default:
		return fmt.Errorf("unsupported message type: %s", msg.Type)
	}
	
	// 发送消息
	return codec.WriteMessage(protoMsg)
}

// isMessageProcessed 检查消息是否已处理
func (gm *GossipManager) isMessageProcessed(messageID string) bool {
	gm.cacheMu.RLock()
	defer gm.cacheMu.RUnlock()
	
	_, exists := gm.messageCache[messageID]
	return exists
}

// addToCache 添加到缓存
func (gm *GossipManager) addToCache(msg *GossipMessage) {
	gm.cacheMu.Lock()
	defer gm.cacheMu.Unlock()
	
	// 检查缓存大小
	if len(gm.messageCache) >= gm.config.CacheSize {
		// 删除最旧的消息
		gm.evictOldestMessage()
	}
	
	gm.messageCache[msg.ID] = msg
}

// evictOldestMessage 删除最旧的消息
func (gm *GossipManager) evictOldestMessage() {
	var oldestID string
	var oldestTime time.Time
	
	first := true
	for id, msg := range gm.messageCache {
		if first || msg.Timestamp.Before(oldestTime) {
			oldestID = id
			oldestTime = msg.Timestamp
			first = false
		}
	}
	
	if oldestID != "" {
		delete(gm.messageCache, oldestID)
	}
}

// cacheCleanupWorker 缓存清理工作器
func (gm *GossipManager) cacheCleanupWorker() {
	defer gm.wg.Done()
	
	ticker := time.NewTicker(gm.config.CleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-gm.ctx.Done():
			return
		case <-ticker.C:
			gm.cleanupExpiredMessages()
		}
	}
}

// cleanupExpiredMessages 清理过期消息
func (gm *GossipManager) cleanupExpiredMessages() {
	gm.cacheMu.Lock()
	defer gm.cacheMu.Unlock()
	
	now := time.Now()
	for id, msg := range gm.messageCache {
		if now.Sub(msg.Timestamp) > gm.config.CacheTTL {
			delete(gm.messageCache, id)
		}
	}
}

// GetStats 获取Gossip统计信息
func (gm *GossipManager) GetStats() *GossipStats {
	gm.cacheMu.RLock()
	defer gm.cacheMu.RUnlock()
	
	stats := &GossipStats{
		CacheSize:        len(gm.messageCache),
		MaxCacheSize:     gm.config.CacheSize,
		QueueSize:        len(gm.propagationQueue),
		MaxQueueSize:     cap(gm.propagationQueue),
		UpdateTime:       time.Now(),
		MessageTypeStats: make(map[string]int),
	}
	
	// 统计消息类型分布
	for _, msg := range gm.messageCache {
		stats.MessageTypeStats[msg.Type.String()]++
	}
	
	return stats
}

// GossipStats Gossip统计信息
type GossipStats struct {
	CacheSize        int               `json:"cache_size"`
	MaxCacheSize     int               `json:"max_cache_size"`
	QueueSize        int               `json:"queue_size"`
	MaxQueueSize     int               `json:"max_queue_size"`
	UpdateTime       time.Time         `json:"update_time"`
	MessageTypeStats map[string]int    `json:"message_type_stats"`
}

// DefaultGossipConfig 默认Gossip配置
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		FanOut:           3,
		PropagationDelay: 100 * time.Millisecond,
		MaxPropagations:  5,
		CacheSize:        1000,
		CacheTTL:         5 * time.Minute,
		CleanupInterval:  1 * time.Minute,
		RetryCount:       3,
		RetryDelay:       1 * time.Second,
		MessageTimeout:   5 * time.Second,
	}
}