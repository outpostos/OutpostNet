package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MessageType 消息类型常量
const (
	// Client -> Server 消息类型
	MsgTypeAuth             = "auth"
	MsgTypeRegisterWorkload = "register_workload"
	MsgTypeCreateService    = "create_service"
	MsgTypeQueryService     = "query_service"
	MsgTypeSTCPTunnelRequest = "stcp_tunnel_request"
	MsgTypeSUDPTunnelRequest = "sudp_tunnel_request"
	MsgTypeHeartbeat        = "heartbeat"
	
	// Server -> Client 消息类型
	MsgTypeAuthResponse      = "auth_response"
	MsgTypeServiceCreated    = "service_created"
	MsgTypeServiceInfo       = "service_info"
	MsgTypeSTCPTunnelResponse = "stcp_tunnel_response"
	MsgTypeSUDPTunnelResponse = "sudp_tunnel_response"
	MsgTypeError             = "error"
	MsgTypePong              = "pong"
	
	// 批量消息类型
	MsgTypeBatch = "batch"
	
	// 节点互联消息类型
	MsgTypeNodeHeartbeat = "node_heartbeat"
	MsgTypeServiceSync   = "service_sync"
	
	// Phase 2: 区域服务网格消息类型
	MsgTypeRegionServiceQuery        = "region_service_query"
	MsgTypeRegionServiceResponse     = "region_service_response"
	MsgTypeNodeHealthCheck           = "node_health_check"
	MsgTypeNodeHealthResponse        = "node_health_response"
	MsgTypeServiceFailureNotification = "service_failure_notification"
	MsgTypeServiceRecoveryNotification = "service_recovery_notification"
	MsgTypeRegionTopologyQuery       = "region_topology_query"
	MsgTypeRegionTopologyResponse    = "region_topology_response"
)

// 协议版本
const (
	ProtocolVersion = 1
)

// 消息头大小（4字节长度 + 1字节版本）
const MessageHeaderSize = 5

// Codec Protocol Buffers消息编解码器
type Codec struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	
	// 消息缓存池
	messagePool sync.Pool
	
	// 统计信息
	totalMessagesSent     int64
	totalMessagesReceived int64
	totalBytesSent        int64
	totalBytesReceived    int64
	
	// 性能优化配置
	bufferSize     int
	enableBatching bool
	batchTimeout   time.Duration
	
	// 批量消息缓存
	batchBuffer []*Message
	batchMutex  sync.Mutex
	batchTimer  *time.Timer
}

// NewCodec 创建新的编解码器
func NewCodec(conn net.Conn) *Codec {
	return NewCodecWithConfig(conn, &CodecConfig{
		BufferSize:     8192,
		EnableBatching: false,
		BatchTimeout:   10 * time.Millisecond,
	})
}

// CodecConfig 编解码器配置
type CodecConfig struct {
	BufferSize     int
	EnableBatching bool
	BatchTimeout   time.Duration
}

// NewCodecWithConfig 使用配置创建编解码器
func NewCodecWithConfig(conn net.Conn, config *CodecConfig) *Codec {
	codec := &Codec{
		conn:           conn,
		reader:         bufio.NewReaderSize(conn, config.BufferSize),
		writer:         bufio.NewWriterSize(conn, config.BufferSize),
		bufferSize:     config.BufferSize,
		enableBatching: config.EnableBatching,
		batchTimeout:   config.BatchTimeout,
		batchBuffer:    make([]*Message, 0, 10),
	}
	
	// 初始化消息池
	codec.messagePool = sync.Pool{
		New: func() interface{} {
			return &Message{}
		},
	}
	
	return codec
}

// WriteMessage 写入消息
func (c *Codec) WriteMessage(msg *Message) error {
	if c.enableBatching {
		return c.writeMessageBatched(msg)
	}
	return c.writeMessageDirect(msg)
}

// writeMessageDirect 直接写入消息
func (c *Codec) writeMessageDirect(msg *Message) error {
	// 序列化消息
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	
	// 写入消息头（长度 + 版本）
	header := make([]byte, MessageHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	header[4] = byte(ProtocolVersion)
	
	// 写入头部
	if _, err := c.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write message header: %w", err)
	}
	
	// 写入消息体
	if _, err := c.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write message body: %w", err)
	}
	
	// 刷新缓冲区
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	
	// 更新统计信息
	c.totalMessagesSent++
	c.totalBytesSent += int64(len(data) + MessageHeaderSize)
	
	return nil
}

// writeMessageBatched 批量写入消息
func (c *Codec) writeMessageBatched(msg *Message) error {
	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()
	
	// 添加到批量缓冲区
	c.batchBuffer = append(c.batchBuffer, msg)
	
	// 如果缓冲区满了，立即发送
	if len(c.batchBuffer) >= 10 {
		return c.flushBatch()
	}
	
	// 如果是第一个消息，启动定时器
	if len(c.batchBuffer) == 1 {
		c.batchTimer = time.AfterFunc(c.batchTimeout, func() {
			c.batchMutex.Lock()
			defer c.batchMutex.Unlock()
			c.flushBatch()
		})
	}
	
	return nil
}

// flushBatch 刷新批量消息
func (c *Codec) flushBatch() error {
	if len(c.batchBuffer) == 0 {
		return nil
	}
	
	// 停止定时器
	if c.batchTimer != nil {
		c.batchTimer.Stop()
		c.batchTimer = nil
	}
	
	// 创建批量消息
	batch := &BatchMessage{
		Messages: c.batchBuffer,
		Ordered:  false,
	}
	
	// 序列化批量消息
	batchData, err := proto.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch message: %w", err)
	}
	
	// 创建包装消息
	msg := &Message{
		Type:      MsgTypeBatch,
		Id:        generateMessageID(),
		Payload:   batchData,
		Timestamp: timestamppb.Now(),
		Version:   ProtocolVersion,
	}
	
	// 发送批量消息
	if err := c.writeMessageDirect(msg); err != nil {
		return err
	}
	
	// 清空缓冲区
	c.batchBuffer = c.batchBuffer[:0]
	
	return nil
}

// ReadMessage 读取消息
func (c *Codec) ReadMessage() (*Message, error) {
	// 读取消息头
	header := make([]byte, MessageHeaderSize)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return nil, fmt.Errorf("failed to read message header: %w", err)
	}
	
	// 解析消息长度和版本
	messageLen := binary.BigEndian.Uint32(header[0:4])
	version := header[4]
	
	// 检查版本兼容性
	if version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", version)
	}
	
	// 检查消息长度合理性
	if messageLen > uint32(c.bufferSize*10) {
		return nil, fmt.Errorf("message too large: %d bytes", messageLen)
	}
	
	// 读取消息体
	data := make([]byte, messageLen)
	if _, err := io.ReadFull(c.reader, data); err != nil {
		return nil, fmt.Errorf("failed to read message body: %w", err)
	}
	
	// 从池中获取消息对象
	msg := c.messagePool.Get().(*Message)
	msg.Reset()
	
	// 反序列化消息
	if err := proto.Unmarshal(data, msg); err != nil {
		c.messagePool.Put(msg)
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	
	// 更新统计信息
	c.totalMessagesReceived++
	c.totalBytesReceived += int64(messageLen + MessageHeaderSize)
	
	// 处理批量消息
	if msg.Type == MsgTypeBatch {
		return c.processBatchMessage(msg)
	}
	
	return msg, nil
}

// processBatchMessage 处理批量消息
func (c *Codec) processBatchMessage(msg *Message) (*Message, error) {
	batch := &BatchMessage{}
	if err := proto.Unmarshal(msg.Payload, batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal batch message: %w", err)
	}
	
	// 返回第一个消息（简化处理）
	if len(batch.Messages) > 0 {
		return batch.Messages[0], nil
	}
	
	return nil, fmt.Errorf("empty batch message")
}

// ReturnMessage 将消息返回到池中
func (c *Codec) ReturnMessage(msg *Message) {
	c.messagePool.Put(msg)
}

// Close 关闭编解码器
func (c *Codec) Close() error {
	// 刷新剩余的批量消息
	if c.enableBatching {
		c.batchMutex.Lock()
		c.flushBatch()
		c.batchMutex.Unlock()
	}
	
	// 关闭连接
	return c.conn.Close()
}

// GetStats 获取统计信息
func (c *Codec) GetStats() *CodecStats {
	return &CodecStats{
		TotalMessagesSent:     c.totalMessagesSent,
		TotalMessagesReceived: c.totalMessagesReceived,
		TotalBytesSent:        c.totalBytesSent,
		TotalBytesReceived:    c.totalBytesReceived,
	}
}

// CodecStats 编解码器统计信息
type CodecStats struct {
	TotalMessagesSent     int64
	TotalMessagesReceived int64
	TotalBytesSent        int64
	TotalBytesReceived    int64
}

// generateMessageID 生成消息ID
func generateMessageID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().UnixNano()%1000)
}

// 消息构建器，简化消息创建
type MessageBuilder struct {
	msg *Message
}

// NewMessageBuilder 创建消息构建器
func NewMessageBuilder(msgType string) *MessageBuilder {
	return &MessageBuilder{
		msg: &Message{
			Type:      msgType,
			Id:        generateMessageID(),
			Timestamp: timestamppb.Now(),
			Version:   ProtocolVersion,
		},
	}
}

// WithPayload 设置消息负载
func (b *MessageBuilder) WithPayload(payload proto.Message) *MessageBuilder {
	if data, err := proto.Marshal(payload); err == nil {
		b.msg.Payload = data
	}
	return b
}

// WithID 设置消息ID
func (b *MessageBuilder) WithID(id string) *MessageBuilder {
	b.msg.Id = id
	return b
}

// Build 构建消息
func (b *MessageBuilder) Build() *Message {
	return b.msg
}

// 消息类型转换器
type MessageConverter struct{}

// NewMessageConverter 创建消息转换器
func NewMessageConverter() *MessageConverter {
	return &MessageConverter{}
}

// ConvertAuthPayload 转换认证负载
func (c *MessageConverter) ConvertAuthPayload(token, mode string) *AuthPayload {
	return &AuthPayload{
		Token:          token,
		Mode:           mode,
		ClientVersion:  "1.0.0",
		Capabilities:   make(map[string]string),
	}
}

// ConvertAuthResponse 转换认证响应
func (c *MessageConverter) ConvertAuthResponse(success bool, message, clientID string) *AuthResponse {
	return &AuthResponse{
		Success:            success,
		Message:            message,
		ClientId:           clientID,
		ServerVersion:      "1.0.0",
		ServerCapabilities: make(map[string]string),
	}
}

// ConvertQueryServicePayload 转换服务查询负载
func (c *MessageConverter) ConvertQueryServicePayload(serviceName, namespace string, selector map[string]string) *QueryServicePayload {
	return &QueryServicePayload{
		ServiceName:      serviceName,
		ServiceNamespace: namespace,
		Selector:         selector,
		MaxEndpoints:     100,
		IncludeUnhealthy: false,
	}
}

// ConvertSTCPTunnelRequest 转换STCP隧道请求
func (c *MessageConverter) ConvertSTCPTunnelRequest(serviceName, targetAddr string) *STCPTunnelRequest {
	return &STCPTunnelRequest{
		ServiceName:    serviceName,
		TargetAddr:     targetAddr,
		Headers:        make(map[string]string),
		TimeoutSeconds: 30,
	}
}

// ConvertHeartbeat 转换心跳消息
func (c *MessageConverter) ConvertHeartbeat(status string) *HeartbeatPayload {
	return &HeartbeatPayload{
		Status:            status,
		Metrics:           make(map[string]string),
		ActiveConnections: 0,
		CpuUsage:          0,
		MemoryUsage:       0,
	}
}

// ConvertError 转换错误消息
func (c *MessageConverter) ConvertError(code int32, message, details string) *ErrorPayload {
	return &ErrorPayload{
		Code:     code,
		Message:  message,
		Details:  details,
		Metadata: make(map[string]string),
	}
}

// 高性能消息处理器
type MessageHandler struct {
	codec     *Codec
	converter *MessageConverter
	
	// 消息处理函数映射
	handlers map[string]func(*Message) error
	
	// 性能统计
	processingTimes map[string]time.Duration
	processingCount map[string]int64
	mutex           sync.RWMutex
}

// NewMessageHandler 创建消息处理器
func NewMessageHandler(codec *Codec) *MessageHandler {
	return &MessageHandler{
		codec:           codec,
		converter:       NewMessageConverter(),
		handlers:        make(map[string]func(*Message) error),
		processingTimes: make(map[string]time.Duration),
		processingCount: make(map[string]int64),
	}
}

// RegisterHandler 注册消息处理函数
func (h *MessageHandler) RegisterHandler(msgType string, handler func(*Message) error) {
	h.handlers[msgType] = handler
}

// ProcessMessage 处理消息
func (h *MessageHandler) ProcessMessage(msg *Message) error {
	start := time.Now()
	defer func() {
		h.mutex.Lock()
		h.processingTimes[msg.Type] += time.Since(start)
		h.processingCount[msg.Type]++
		h.mutex.Unlock()
	}()
	
	handler, exists := h.handlers[msg.Type]
	if !exists {
		return fmt.Errorf("no handler for message type: %s", msg.Type)
	}
	
	return handler(msg)
}

// GetProcessingStats 获取处理统计信息
func (h *MessageHandler) GetProcessingStats() map[string]*ProcessingStats {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	
	stats := make(map[string]*ProcessingStats)
	for msgType, totalTime := range h.processingTimes {
		count := h.processingCount[msgType]
		stats[msgType] = &ProcessingStats{
			MessageType:    msgType,
			TotalTime:      totalTime,
			Count:          count,
			AverageTime:    totalTime / time.Duration(count),
		}
	}
	
	return stats
}

// ProcessingStats 处理统计信息
type ProcessingStats struct {
	MessageType string
	TotalTime   time.Duration
	Count       int64
	AverageTime time.Duration
}

// 消息压缩器（可选功能）
type MessageCompressor struct {
	enabled bool
}

// NewMessageCompressor 创建消息压缩器
func NewMessageCompressor(enabled bool) *MessageCompressor {
	return &MessageCompressor{enabled: enabled}
}

// Compress 压缩消息
func (c *MessageCompressor) Compress(data []byte) ([]byte, error) {
	if !c.enabled {
		return data, nil
	}
	
	// 这里可以实现gzip或其他压缩算法
	// 为了简化，直接返回原数据
	return data, nil
}

// Decompress 解压缩消息
func (c *MessageCompressor) Decompress(data []byte) ([]byte, error) {
	if !c.enabled {
		return data, nil
	}
	
	// 这里可以实现对应的解压缩算法
	// 为了简化，直接返回原数据
	return data, nil
}