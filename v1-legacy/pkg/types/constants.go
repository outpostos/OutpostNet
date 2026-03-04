package types

// ServiceKeel统一域名体系
const (
	// K8s兼容的核心资源
	CoreAPIVersion = "v1"

	// ServiceKeel统一域名体系
	ServiceKeelDomain     = "servicekeel.io"
	ServiceKeelAPIVersion = "servicekeel.io/v1"        // 主API版本
	NetworkAPIVersion     = "network.servicekeel.io/v1" // 网络组件API
	ProxyAPIVersion       = "proxy.servicekeel.io/v1"   // 代理组件API
)

// ServiceKeel CRD类型定义
const (
	// K8s原生资源类型 (保持兼容)
	ServiceKind   = "Service"
	EndpointsKind = "Endpoints"
	PodKind       = "Pod"

	// ServiceKeel扩展资源类型
	ProxyServiceKind  = "ProxyService"  // outpostnet专用的代理服务定义
	NetworkPolicyKind = "NetworkPolicy" // 网络策略
	LoadBalancerKind  = "LoadBalancer"  // 负载均衡器配置
	TunnelKind        = "Tunnel"        // 网络隧道配置
	HealthCheckKind   = "HealthCheck"   // 健康检查配置
)

// ProxyMode 代理模式
type ProxyMode string

const (
	ProxyModeTCP  ProxyMode = "tcp"
	ProxyModeUDP  ProxyMode = "udp"
	ProxyModeSTCP ProxyMode = "stcp" // 安全TCP
	ProxyModeSUDP ProxyMode = "sudp" // 安全UDP
)

// LoadBalancerType 负载均衡类型
type LoadBalancerType string

const (
	LoadBalancerRoundRobin         LoadBalancerType = "round_robin"
	LoadBalancerWeightedRoundRobin LoadBalancerType = "weighted_round_robin"
	LoadBalancerLeastConnections   LoadBalancerType = "least_connections"
	LoadBalancerRandom             LoadBalancerType = "random"
	LoadBalancerConsistentHash     LoadBalancerType = "consistent_hash"
)

// TunnelType 隧道类型
type TunnelType string

const (
	TunnelTypeSTCP TunnelType = "stcp"
	TunnelTypeSUDP TunnelType = "sudp"
)

// PolicyType 策略类型
type PolicyType string

const (
	PolicyTypeIngress PolicyType = "Ingress"
	PolicyTypeEgress  PolicyType = "Egress"
)