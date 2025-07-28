package types

import "time"

// Message 定义客户端-服务端通信协议
type Message struct {
	Type      string      `json:"type"`
	ID        string      `json:"id"`
	Payload   interface{} `json:"payload"`
	Timestamp time.Time   `json:"timestamp"`
}

// 消息类型常量
const (
	// Client -> Server 消息类型
	MsgTypeAuth             = "auth"
	MsgTypeRegisterWorkload = "register_workload"
	MsgTypeCreateService    = "create_service"     // Proxy 客户端创建服务
	MsgTypeQueryService     = "query_service"
	MsgTypeSTCPTunnelRequest = "stcp_tunnel_request" // STCP 隧道请求
	MsgTypeSUDPTunnelRequest = "sudp_tunnel_request" // SUDP 隧道请求
	MsgTypeHeartbeat        = "heartbeat"
	
	// Server -> Client 消息类型
	MsgTypeAuthResponse     = "auth_response"
	MsgTypeServiceCreated   = "service_created"    // 服务创建响应
	MsgTypeServiceInfo      = "service_info"
	MsgTypeSTCPTunnelResponse = "stcp_tunnel_response" // STCP 隧道响应
	MsgTypeSUDPTunnelResponse = "sudp_tunnel_response" // SUDP 隧道响应
	MsgTypeError            = "error"
	MsgTypePong             = "pong"
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
	Status string `json:"status"`
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
