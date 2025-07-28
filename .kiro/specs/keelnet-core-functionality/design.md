# OutpostNet 边缘计算网络代理设计文档

## 概述

OutpostNet是一个专为边缘计算环境设计的轻量级网络代理，专注于解决**服务访问的能力**。系统采用简单可靠的设计理念，提供多协议网络代理、基本负载均衡和服务发现功能，确保边缘应用能够稳定访问所需的服务。

OutpostNet的核心价值是**服务映射和透明传输**，通过统一的代理接口简化边缘环境中的服务访问复杂度，让上层应用专注于业务逻辑而无需关心底层网络细节。项目遵循"简单、可靠、高效"的设计原则，避免复杂的治理功能，将复杂的服务管理逻辑交给上层系统处理。

## 架构

### 整体架构

OutpostNet采用经典的服务端-客户端架构，通过简单的代理模式实现服务访问能力：

```
┌─────────────────────────────────────────────────────────────┐
│                    OutpostNet 系统架构                          │
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │   Application   │    │  OutpostNet-client │                │
│  │                 │    │    (proxy)      │                │
│  │  localhost:8080 │◄──►│  localhost:8080 │                │
│  └─────────────────┘    └─────────────────┘                │
│                                   │                         │
│                                   ▼                         │
│                          ┌─────────────────┐                │
│                          │  OutpostNet-server │                │
│                          │                 │                │
│                          │ ┌─────────────┐ │                │
│                          │ │服务注册表   │ │                │
│                          │ │负载均衡器   │ │                │
│                          │ │健康检查器   │ │                │
│                          │ │连接管理器   │ │                │
│                          │ └─────────────┘ │                │
│                          └─────────────────┘                │
│                                   ▲                         │
│                                   │                         │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │  Backend Svc 1  │    │  OutpostNet-client │                │
│  │                 │◄──►│   (register)    │                │
│  │   127.0.0.1:80  │    │                 │                │
│  └─────────────────┘    └─────────────────┘                │
└─────────────────────────────────────────────────────────────┘
```

#### 核心设计原则

1. **简单可靠**：避免复杂的治理功能，专注于核心的服务访问能力
2. **透明传输**：提供统一的代理接口，隐藏底层网络复杂性
3. **边缘优化**：针对边缘环境的资源限制和网络不稳定性进行优化
4. **K8s兼容**：使用熟悉的Kubernetes概念降低学习成本

### 核心组件架构

#### OutpostNet-server 架构
```
┌─────────────────────────────────────────────────────────────┐
│                    OutpostNet-server                           │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  配置管理器      │  │  认证管理器      │  │  TLS管理器   │ │
│  │ - YAML解析      │  │ - 令牌验证      │  │ - 证书管理   │ │
│  │ - 配置验证      │  │ - 访问控制      │  │ - 加密通信   │ │
│  │ - 热重载        │  │ - 安全审计      │  │ - 双向认证   │ │
│  │ - Express Mode  │  │                 │  │ - Unix Socket│ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  服务注册表      │  │  负载均衡管理器  │  │  健康检查器  │ │
│  │ - K8s兼容API    │  │ - 轮询算法      │  │ - 基本检查   │ │
│  │ - 标签选择器    │  │ - 随机算法      │  │ - 故障转移   │ │
│  │ - 命名空间隔离  │  │ - 权重支持      │  │ - 自动恢复   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  连接管理器      │  │  协议处理器      │  │  监控管理器  │ │
│  │ - 连接池管理    │  │ - TCP透明代理   │  │ - 结构化日志 │ │
│  │ - 会话跟踪      │  │ - UDP数据转发   │  │ - 性能指标   │ │
│  │ - 资源清理      │  │ - STCP/SUDP隧道 │  │ - 健康端点   │ │
│  │ - Unix Socket   │  │ - 动态切换      │  │ - 状态API    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

#### OutpostNet-client 架构
```
┌─────────────────────────────────────────────────────────────┐
│                    OutpostNet-client                           │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  配置管理器      │  │  连接管理器      │  │  模式管理器  │ │
│  │ - YAML解析      │  │ - 服务器连接    │  │ - Register   │ │
│  │ - 配置验证      │  │ - 心跳保持      │  │ - Proxy      │ │
│  │ - 动态更新      │  │ - 断线重连      │  │ - 模式切换   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  Register模式    │  │  Proxy模式       │  │  协议处理器  │ │
│  │ - 服务注册      │  │ - 本地监听      │  │ - TCP代理    │ │
│  │ - 健康上报      │  │ - 服务发现      │  │ - UDP代理    │ │
│  │ - 标签管理      │  │ - 负载均衡      │  │ - 安全隧道   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 组件和接口

### 核心数据模型

基于现有代码实现，OutpostNet采用了简化的数据模型设计，专注于核心功能：

#### 服务和端点模型
```go
// Service 定义服务的逻辑抽象，类似 K8s Service
type Service struct {
    Metadata ServiceMetadata `yaml:"metadata" json:"metadata"`
    Spec     ServiceSpec     `yaml:"spec" json:"spec"`
}

// ServiceMetadata 服务元数据
type ServiceMetadata struct {
    Name      string            `yaml:"name" json:"name"`
    Namespace string            `yaml:"namespace" json:"namespace"`
    Labels    map[string]string `yaml:"labels" json:"labels"`
    
    // 地理位置信息（边缘环境适配）
    Site   string `yaml:"site,omitempty" json:"site,omitempty"`
    Zone   string `yaml:"zone,omitempty" json:"zone,omitempty"`
    Region string `yaml:"region,omitempty" json:"region,omitempty"`
}

// ServiceSpec 服务规格定义
type ServiceSpec struct {
    Selector     map[string]string `yaml:"selector" json:"selector"`
    Ports        []ServicePort     `yaml:"ports" json:"ports"`
    ProxyMode    string            `yaml:"proxy_mode" json:"proxy_mode"` // tcp/udp/stcp/sudp
    LoadBalancer string            `yaml:"load_balancer" json:"load_balancer"`
}

// Endpoint 定义具体的后端实例
type Endpoint struct {
    ID       string            `yaml:"id" json:"id"`
    IP       string            `yaml:"ip" json:"ip"`
    Port     int               `yaml:"port" json:"port"`
    Protocol string            `yaml:"protocol" json:"protocol"`
    Labels   map[string]string `yaml:"labels" json:"labels"`
    Status   EndpointStatus    `yaml:"status" json:"status"`
    LastSeen time.Time         `yaml:"last_seen" json:"last_seen"`
}

// EndpointStatus 端点状态
type EndpointStatus struct {
    Ready   bool   `yaml:"ready" json:"ready"`
    Reason  string `yaml:"reason,omitempty" json:"reason,omitempty"`
    Message string `yaml:"message,omitempty" json:"message,omitempty"`
}
```

#### 工作负载模型（K8s Pod风格）
```go
// Workload 定义工作负载实例，类似 K8s Pod
type Workload struct {
    Metadata WorkloadMetadata `yaml:"metadata" json:"metadata"`
    Spec     WorkloadSpec     `yaml:"spec" json:"spec"`
    Status   WorkloadStatus   `yaml:"status" json:"status"`
}

// WorkloadMetadata 工作负载元数据
type WorkloadMetadata struct {
    ID        string            `yaml:"id" json:"id"`
    Labels    map[string]string `yaml:"labels" json:"labels"`
    CreatedAt time.Time         `yaml:"created_at" json:"created_at"`
}

// WorkloadSpec 工作负载规格定义
type WorkloadSpec struct {
    LocalAddr string `yaml:"local_addr" json:"local_addr"`
    LocalPort int    `yaml:"local_port" json:"local_port"`
    Protocol  string `yaml:"protocol" json:"protocol"`
}

// WorkloadStatus 工作负载状态
type WorkloadStatus struct {
    Ready      bool      `yaml:"ready" json:"ready"`
    Reason     string    `yaml:"reason,omitempty" json:"reason,omitempty"`
    Message    string    `yaml:"message,omitempty" json:"message,omitempty"`
    LastSeen   time.Time `yaml:"last_seen" json:"last_seen"`
    RegisterIP string    `yaml:"register_ip" json:"register_ip"` // 注册时的客户端IP
}
```

#### 代理配置模型
```go
// Proxy 定义代理配置，客户端如何访问服务
type Proxy struct {
    Name             string            `yaml:"name" json:"name"`
    ServiceName      string            `yaml:"service_name" json:"service_name"`
    ServiceNamespace string            `yaml:"service_namespace" json:"service_namespace"`
    LocalPort        int               `yaml:"local_port" json:"local_port"`
    ProxyMode        string            `yaml:"proxy_mode" json:"proxy_mode"`
    Selector         map[string]string `yaml:"selector,omitempty" json:"selector,omitempty"`
    LoadBalancer     string            `yaml:"load_balancer,omitempty" json:"load_balancer,omitempty"`
}
```

### 服务注册表接口

```go
type ServiceRegistry interface {
    // 服务管理
    RegisterService(service *Service) error
    GetService(name, namespace string) (*Service, error)
    ListServices(namespace string) ([]*Service, error)
    
    // 工作负载管理 (K8s Pod 风格)
    RegisterWorkload(workload *Workload) error
    UnregisterWorkload(workloadID string) error
    GetWorkload(workloadID string) (*Workload, error)
    ListWorkloads(selector map[string]string) ([]*Workload, error)
    
    // 端点管理 (由 Controller 自动管理)
    GetEndpoints(serviceName, namespace string, selector map[string]string) ([]*Endpoint, error)
    RegisterEndpoint(serviceName, namespace string, endpoint *Endpoint) error
    UnregisterEndpoint(serviceName, namespace, endpointID string) error
    
    // Controller 使用的内部方法
    SyncEndpoints() error // 同步服务和工作负载，自动创建端点
}
```

### 负载均衡器接口

```go
type LoadBalancer interface {
    // SelectEndpoint 从可用端点中选择一个
    SelectEndpoint(endpoints []*Endpoint, key string) *Endpoint
    // UpdateStats 更新端点统计信息
    UpdateStats(endpoint *Endpoint, latency time.Duration, success bool)
    // Name 返回负载均衡器名称
    Name() string
}

type LoadBalancerManager interface {
    GetLoadBalancer(serviceName string, lbType LoadBalancerType) LoadBalancer
    RemoveLoadBalancer(serviceName string, lbType LoadBalancerType)
}

// 支持的负载均衡算法
const (
    RoundRobin         LoadBalancerType = "round_robin"
    WeightedRoundRobin LoadBalancerType = "weighted_round_robin"
    LeastConnections   LoadBalancerType = "least_connections"
    Random             LoadBalancerType = "random"
    ConsistentHash     LoadBalancerType = "consistent_hash"
)
```

### 健康检查器接口

```go
type HealthChecker interface {
    AddEndpoint(endpoint *Endpoint) error
    RemoveEndpoint(endpointID string) error
    GetHealthyEndpoints(endpoints []*Endpoint) []*Endpoint
    IsHealthy(endpointID string) bool
    Start(ctx context.Context)
}
```

### 协议处理器接口

OutpostNet支持四种协议模式：

1. **TCP** - 透明TCP代理
2. **UDP** - UDP数据包转发
3. **STCP** - 安全TCP隧道（加密）
4. **SUDP** - 安全UDP隧道（加密）

```go
// 协议处理在客户端和服务端分别实现
// 客户端：本地监听 -> 服务器查询 -> 目标端点连接
// 服务端：客户端连接 -> 负载均衡选择 -> 目标端点转发
```

## 数据模型

### 配置数据模型

#### 服务器配置
```yaml
# OutpostNet服务器配置
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "secret-token"
  unix_sock_path: "/tmp/OutpostNet.sock"  # 可选，启用Unix Socket
  tls:
    cert_file: "server.crt"
    key_file: "server.key"
    ca_file: "ca.crt"

# 预定义服务
services:
  - metadata:
      name: "web-service"
      namespace: "default"
      labels:
        app: "web"
        tier: "frontend"
    spec:
      selector:
        app: "web"
      ports:
        - name: "http"
          port: 80
          targetPort: 8080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "round_robin"

# K8s兼容API
k8s_api:
  enabled: true
  port: 8443

# 监控配置
monitoring:
  enabled: true
  port: 9090
  prometheus_enabled: true
```

#### 客户端配置
```yaml
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "secret-token"
  mode: "register"  # register | proxy
  conn_proto: "auto"  # tcp | unix | auto

# Register模式配置 - 注册为K8s Pod风格的工作负载
workloads:
  - metadata:
      labels:
        app: "web"
        version: "v1"
        zone: "east"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8080
      protocol: "tcp"

# Proxy模式配置
proxies:
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 8080
    proxy_mode: "tcp"
    selector:
      version: "v1"
    load_balancer: "round_robin"
```

### 通信协议

OutpostNet使用JSON消息协议进行客户端-服务端通信：

```go
// Message 定义客户端-服务端通信协议
type Message struct {
    Type      string      `json:"type"`
    ID        string      `json:"id"`
    Payload   interface{} `json:"payload"`
    Timestamp time.Time   `json:"timestamp"`
}

// 主要消息类型
const (
    // Client -> Server
    MsgTypeAuth             = "auth"
    MsgTypeRegisterWorkload = "register_workload"
    MsgTypeQueryService     = "query_service"
    MsgTypeSTCPTunnelRequest = "stcp_tunnel_request"
    MsgTypeSUDPTunnelRequest = "sudp_tunnel_request"
    MsgTypeHeartbeat        = "heartbeat"
    
    // Server -> Client
    MsgTypeAuthResponse     = "auth_response"
    MsgTypeServiceInfo      = "service_info"
    MsgTypeSTCPTunnelResponse = "stcp_tunnel_response"
    MsgTypeSUDPTunnelResponse = "sudp_tunnel_response"
    MsgTypeError            = "error"
    MsgTypePong             = "pong"
)
```

## 错误处理

### 错误分类和处理策略

#### 网络错误
- **连接拒绝**：标记端点为不健康，触发故障转移
- **超时**：重试机制，增加超时时间
- **DNS错误**：记录错误，使用缓存的IP地址

#### 配置错误
- **字段验证**：提供清晰的错误信息和建议
- **端口冲突**：检测并报告端口占用问题
- **服务名重复**：防止服务名冲突

#### 认证错误
- **无效令牌**：拒绝连接，记录安全事件
- **证书过期**：通知管理员，优雅关闭
- **访问拒绝**：记录访问尝试，返回权限拒绝

## 测试策略

### 单元测试策略

#### 核心组件测试
- **服务注册表测试**：验证服务注册、查询、端点管理
- **负载均衡器测试**：验证各种算法的分布均匀性
- **协议处理器测试**：验证TCP/UDP/STCP/SUDP代理功能

#### 集成测试策略
- **端到端测试**：完整的服务注册-发现-代理流程
- **故障转移测试**：验证健康检查和自动故障转移
- **性能测试**：验证高并发场景下的稳定性

### 边缘环境适配测试
- **资源限制测试**：在低资源环境下的性能表现
- **网络不稳定测试**：模拟网络中断和恢复场景
- **Unix Socket测试**：验证本地高性能通信

## 监控与可观测性

### 监控指标
- **连接数**：活跃连接数、总连接数
- **请求延迟**：P50、P95、P99延迟
- **错误率**：连接失败率、代理错误率
- **端点健康**：健康端点数、不健康端点数

### 日志记录
- **结构化日志**：JSON格式，包含请求ID和上下文
- **错误跟踪**：详细的错误堆栈和调试信息
- **性能日志**：关键操作的耗时统计

### 健康检查端点
- `/health` - 基本健康状态
- `/ready` - 就绪状态检查
- `/status` - 详细状态信息
- `/metrics` - Prometheus格式指标