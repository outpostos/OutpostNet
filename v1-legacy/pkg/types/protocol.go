package types

import "time"

// MessageType 消息类型
type MessageType string

// Message 定义客户端-服务端通信协议
type Message struct {
	Type      MessageType `json:"type"`
	ID        string      `json:"id"`
	Payload   interface{} `json:"payload"`
	Timestamp time.Time   `json:"timestamp"`
}

// 消息类型常量
const (
	// Client -> Server 消息类型
	MsgTypeAuth             MessageType = "auth"
	MsgTypeRegisterWorkload MessageType = "register_workload"
	MsgTypeCreateService    MessageType = "create_service"     // Proxy 客户端创建服务
	MsgTypeQueryService     MessageType = "query_service"
	MsgTypeWorkConnRequest  MessageType = "work_conn_request"    // 工作连接请求
	MsgTypeWorkConnID       MessageType = "work_conn_id"         // 工作连接标识
	MsgTypeTunnelRequest    MessageType = "tunnel_request"       // TCP 隧道请求
	MsgTypeUDPTunnelRequest MessageType = "udp_tunnel_request"   // UDP 隧道请求
	MsgTypeTunnelData       MessageType = "tunnel_data"          // 隧道数据传输
	MsgTypeTunnelClose      MessageType = "tunnel_close"         // 隧道关闭
	MsgTypeSTCPTunnelRequest MessageType = "stcp_tunnel_request" // STCP 隧道请求
	MsgTypeSUDPTunnelRequest MessageType = "sudp_tunnel_request" // SUDP 隧道请求
	MsgTypeHeartbeat        MessageType = "heartbeat"
	MsgTypeWorkConnReady    MessageType = "work_conn_ready"      // Register Client 工作连接就绪
	MsgTypeReqWorkConn      MessageType = "req_work_conn"         // Server 请求 Register Client 创建工作连接
	MsgTypeNewWorkConn      MessageType = "new_work_conn"        // Register Client 创建新工作连接
	MsgTypeStartWorkConn    MessageType = "start_work_conn"      // Server 指示工作连接开始处理特定隧道
	MsgTypeHeartbeatResponse MessageType = "heartbeat_response"  // 心跳响应
	
	// Server -> Client 消息类型
	MsgTypeAuthResponse     MessageType = "auth_response"
	MsgTypeServiceCreated   MessageType = "service_created"    // 服务创建响应
	MsgTypeServiceInfo      MessageType = "service_info"
	MsgTypeWorkConnResponse MessageType = "work_conn_response"     // 工作连接响应
	MsgTypeReqWorkConnResponse MessageType = "req_work_conn_response" // 请求工作连接响应 (服务器中转模式)
	MsgTypeTunnelResponse   MessageType = "tunnel_response"        // TCP 隧道响应
	MsgTypeUDPTunnelResponse MessageType = "udp_tunnel_response"   // UDP 隧道响应
	MsgTypeSTCPTunnelResponse MessageType = "stcp_tunnel_response" // STCP 隧道响应
	MsgTypeSUDPTunnelResponse MessageType = "sudp_tunnel_response" // SUDP 隧道响应
	MsgTypeError            MessageType = "error"
	MsgTypePong             MessageType = "pong"
	MsgTypeWorkloadRegistered MessageType = "workload_registered" // 工作负载注册成功
)

// AuthPayload 认证消息负载
type AuthPayload struct {
	Token string `json:"token"`
	Mode  string `json:"mode"` // "register" 或 "proxy"
}

// AuthResponsePayload 认证响应负载
type AuthResponsePayload struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	ClientID string `json:"client_id"`
}

// RegisterWorkloadPayload 工作负载注册消息负载
type RegisterWorkloadPayload struct {
	Workload Workload `json:"workload"`
}

// QueryServicePayload 服务查询消息负载
type QueryServicePayload struct {
	ServiceName      string            `json:"service_name"`
	ServiceNamespace string            `json:"service_namespace"`
	Selector         map[string]string `json:"selector,omitempty"`
}

// ServiceInfoPayload 服务信息响应负载
type ServiceInfoPayload struct {
	Service   Service    `json:"service"`
	Endpoints []Endpoint `json:"endpoints"`
}

// ErrorPayload 错误消息负载
type ErrorPayload struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// CreateServicePayload 创建服务消息负载（Proxy 客户端发送）
type CreateServicePayload struct {
	Service Service `json:"service"`
}

// ServiceCreatedPayload 服务创建响应负载
type ServiceCreatedPayload struct {
	ServiceName      string `json:"service_name"`
	ServiceNamespace string `json:"service_namespace"`
	Status           string `json:"status"`
	Message          string `json:"message"`
}

// HeartbeatPayload 心跳消息负载
type HeartbeatPayload struct {
	Timestamp time.Time `json:"timestamp"`
	ClientID  string    `json:"client_id"`
	Status    string    `json:"status"`
}

// TunnelRequestPayload TCP 隧道请求负载
type TunnelRequestPayload struct {
	ServiceName      string `json:"service_name"`
	ServiceNamespace string `json:"service_namespace"`
}

// TunnelResponsePayload TCP 隧道响应负载
type TunnelResponsePayload struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	TunnelID string `json:"tunnel_id,omitempty"`
}

// UDPTunnelRequestPayload UDP 隧道请求负载
type UDPTunnelRequestPayload struct {
	ServiceName      string `json:"service_name"`
	ServiceNamespace string `json:"service_namespace"`
	ClientAddr       string `json:"client_addr"` // UDP 客户端地址
}

// UDPTunnelResponsePayload UDP 隧道响应负载
type UDPTunnelResponsePayload struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	TunnelID string `json:"tunnel_id,omitempty"`
}

// TunnelDataPayload 隧道数据传输负载
type TunnelDataPayload struct {
	TunnelID string `json:"tunnel_id"`
	Data     []byte `json:"data"`
}

// TunnelClosePayload 隧道关闭负载
type TunnelClosePayload struct {
	TunnelID string `json:"tunnel_id"`
	Reason   string `json:"reason,omitempty"`
}

// WorkConnRequestPayload 工作连接请求负载
type WorkConnRequestPayload struct {
	ServiceName      string `json:"service_name"`
	ServiceNamespace string `json:"service_namespace"`
}

// WorkConnResponsePayload 工作连接响应负载
type WorkConnResponsePayload struct {
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	WorkConnID string `json:"work_conn_id,omitempty"`
}

// WorkConnIDPayload 工作连接标识负载
type WorkConnIDPayload struct {
	WorkConnID string `json:"work_conn_id"`
}

// STCPTunnelRequestPayload STCP 隧道请求负载
type STCPTunnelRequestPayload struct {
	ServiceName string `json:"service_name"`
	TargetAddr  string `json:"target_addr"`
}

// STCPTunnelResponsePayload STCP 隧道响应负载
type STCPTunnelResponsePayload struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	TunnelID  string `json:"tunnel_id,omitempty"`
}

// SUDPTunnelRequestPayload SUDP 隧道请求负载
type SUDPTunnelRequestPayload struct {
	ServiceName string `json:"service_name"`
	TargetAddr  string `json:"target_addr"`
	LocalPort   int    `json:"local_port"`
}

// SUDPTunnelResponsePayload SUDP 隧道响应负载
type SUDPTunnelResponsePayload struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	TunnelID  string `json:"tunnel_id,omitempty"`
	ServerPort int   `json:"server_port,omitempty"` // 服务器分配的 UDP 端口
}

// WorkConnReadyPayload Register Client 工作连接就绪负载
type WorkConnReadyPayload struct {
	ConnectionID string         `json:"connection_id"`
	Workloads    []WorkloadInfo `json:"workloads"`
}

// WorkloadInfo 工作负载信息
type WorkloadInfo struct {
	ID       string            `json:"id"`
	Labels   map[string]string `json:"labels"`
	Protocol string            `json:"protocol"`
	Address  string            `json:"address"`
	Port     int               `json:"port"`
}

// ProxyRequestPayload Server 向 Register Client 发送的代理请求负载
type ProxyRequestPayload struct {
	SessionID   string `json:"session_id"`   // 会话ID
	WorkloadID  string `json:"workload_id"`  // 目标工作负载ID
	Protocol    string `json:"protocol"`     // 协议类型 (tcp/udp)
	RequestID   string `json:"request_id"`   // 请求ID
}

// ProxyResponsePayload Register Client 的代理响应负载
type ProxyResponsePayload struct {
	Success        bool   `json:"success"`
	Message        string `json:"message"`
	SessionID      string `json:"session_id"`
}

// ReqWorkConnPayload 请求工作连接负载 (服务器中转模式)
type ReqWorkConnPayload struct {
	Count     int    `json:"count,omitempty"`     // 请求的工作连接数量
	RequestID string `json:"request_id,omitempty"` // 请求ID用于关联
}

// NewWorkConnPayload 新工作连接负载
type NewWorkConnPayload struct {
	RunID       string `json:"run_id"`        // 客户端运行ID
	WorkConnID  string `json:"work_conn_id"`  // 工作连接ID
}

// StartWorkConnPayload 开始工作连接负载
type StartWorkConnPayload struct {
	ProxyName     string `json:"proxy_name"`      // 代理名称
	SrcAddr       string `json:"src_addr"`        // 源地址
	DstAddr       string `json:"dst_addr"`        // 目标地址
	Protocol      string `json:"protocol"`        // 协议类型
	WorkloadID    string `json:"workload_id"`     // 工作负载ID
}

