# OutpostNet 边缘计算网络代理设计文档

## 概述

OutpostNet是一个专为边缘计算环境设计的轻量级网络代理，采用**动态服务发现**的设计理念，专注于解决**服务访问的能力**。系统通过简单可靠的端点注册和标签匹配机制，提供多协议网络代理、动态负载均衡和实时服务发现功能。

OutpostNet的核心价值是**端点聚合和透明传输**，通过统一的代理接口将具有相同标签的端点动态聚合为服务，让上层应用专注于业务逻辑而无需关心底层网络细节。项目遵循"简单、可靠、高效"的设计原则，避免复杂的预定义配置，通过工作负载驱动的方式实现服务的自然演进。

## 核心设计理念

### Service 是动态聚合概念

**Service 不是预定义的实体，而是 Proxy 通过 selector 动态聚合 Endpoints 的结果**

```
Service = SELECT * FROM endpoints WHERE labels MATCH selector
```

### 端点驱动的服务发现

```
传统模式 (管理员驱动):
管理员预定义服务 → 工作负载注册 → 代理使用服务

OutpostNet 模式 (工作负载驱动):  
工作负载注册端点 → 代理通过标签选择 → 服务动态形成
```

## 架构

### 整体架构

OutpostNet采用经典的服务端-客户端架构，通过动态端点聚合实现服务访问能力：

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
│                          │ │端点注册表   │ │                │
│                          │ │标签匹配器   │ │                │  
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

### 动态服务发现流程

```
步骤1: 端点注册
┌─────────────────────────────────────────────────────────────────┐
│ Register Client 向 Server 注册端点                               │
│                                                                 │
│ Endpoint:                                                       │
│ - name: "web-backend-1"                                         │
│ - ip: "127.0.0.1"                                               │
│ - port: 18000                                                   │
│ - labels: {app: "demo-web", version: "v1", zone: "local"}       │
│ - protocol: "tcp"                                               │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
步骤2: 代理请求聚合
┌─────────────────────────────────────────────────────────────────┐
│ Proxy Client 请求                                               │
│                                                                 │
│ - endpoint_selector: {app: "demo-web"}                          │
│ - load_balancer: "round_robin"                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
步骤3: 动态服务聚合
┌─────────────────────────────────────────────────────────────────┐
│ Server 动态聚合逻辑                                              │
│                                                                 │
│ 1. 根据 selector {app: "demo-web"} 查找匹配的 Endpoints          │
│    └── 找到: web-backend-1 (127.0.0.1:18000)                    │
│                                                                 │
│ 2. 形成虚拟 Service = [web-backend-1]                           │
│                                                                 │
│ 3. 应用负载均衡策略                                              │
│                                                                 │
│ 4. 建立代理连接                                                  │
└─────────────────────────────────────────────────────────────────┘
```

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
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  端点注册表      │  │  标签匹配引擎    │  │  健康检查器  │ │
│  │ - 端点存储      │  │ - 实时查询      │  │ - 基本检查   │ │
│  │ - 标签索引      │  │ - 动态聚合      │  │ - 故障转移   │ │
│  │ - 生命周期管理  │  │ - 负载均衡      │  │ - 自动恢复   │ │
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
│  │ - 端点注册      │  │ - 本地监听      │  │ - TCP代理    │ │
│  │ - 健康上报      │  │ - 标签查询      │  │ - UDP代理    │ │
│  │ - 标签管理      │  │ - 动态聚合      │  │ - 安全隧道   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 组件和接口

### 核心数据模型

#### 端点模型 (核心实体)
```go
// Endpoint 定义具体的后端实例 - 唯一的存储实体
type Endpoint struct {
    ID       string            `json:"id"`         // 端点唯一标识
    IP       string            `json:"ip"`         // 端点IP地址
    Port     int               `json:"port"`       // 端点端口
    Protocol string            `json:"protocol"`   // 协议类型
    Labels   map[string]string `json:"labels"`     // 标签集合 (用于匹配)
    Status   EndpointStatus    `json:"status"`     // 端点状态
    LastSeen time.Time         `json:"last_seen"`  // 最后心跳时间
}

// EndpointStatus 端点状态
type EndpointStatus struct {
    Ready   bool   `json:"ready"`              // 是否就绪
    Reason  string `json:"reason,omitempty"`   // 状态原因
    Message string `json:"message,omitempty"`  // 状态消息
}
```

#### 服务视图模型 (动态聚合结果)
```go
// ServiceView 服务视图 - 不是存储实体，而是查询结果
type ServiceView struct {
    Selector  map[string]string `json:"selector"`   // 选择器
    Endpoints []Endpoint        `json:"endpoints"`  // 匹配的端点列表
    Stats     ServiceStats      `json:"stats"`      // 实时统计
    CreatedAt time.Time         `json:"created_at"` // 聚合时间
}

// ServiceStats 服务统计
type ServiceStats struct {
    TotalEndpoints   int `json:"total_endpoints"`   // 总端点数
    ReadyEndpoints   int `json:"ready_endpoints"`   // 就绪端点数
    ActiveConnections int `json:"active_connections"` // 活跃连接数
}
```

#### 工作负载模型
```go
// Workload 定义工作负载实例，用于注册端点
type Workload struct {
    Metadata WorkloadMetadata `json:"metadata"`
    Spec     WorkloadSpec     `json:"spec"`
    Status   WorkloadStatus   `json:"status"`
}

// WorkloadMetadata 工作负载元数据
type WorkloadMetadata struct {
    Name      string            `json:"name"`       // 工作负载名称
    Labels    map[string]string `json:"labels"`     // 标签集合
    CreatedAt time.Time         `json:"created_at"` // 创建时间
}

// WorkloadSpec 工作负载规格定义
type WorkloadSpec struct {
    LocalAddr string `json:"local_addr"` // 本地地址
    LocalPort int    `json:"local_port"` // 本地端口
    Protocol  string `json:"protocol"`   // 协议类型
}

// WorkloadStatus 工作负载状态
type WorkloadStatus struct {
    Ready      bool      `json:"ready"`       // 是否就绪
    Reason     string    `json:"reason"`      // 状态原因
    Message    string    `json:"message"`     // 状态消息
    LastSeen   time.Time `json:"last_seen"`   // 最后心跳
    RegisterIP string    `json:"register_ip"` // 注册客户端IP
}
```

#### 代理配置模型
```go
// Proxy 定义代理配置，指定如何聚合端点
type Proxy struct {
    Name             string            `json:"name"`              // 代理名称
    LocalPort        int               `json:"local_port"`        // 本地监听端口
    ProxyMode        string            `json:"proxy_mode"`        // 代理模式
    EndpointSelector map[string]string `json:"endpoint_selector"` // 端点选择器
    LoadBalancer     string            `json:"load_balancer"`     // 负载均衡策略
    EndpointFilter   EndpointFilter    `json:"endpoint_filter"`   // 端点过滤器
}

// EndpointFilter 端点过滤器
type EndpointFilter struct {
    PreferLabels  map[string]string `json:"prefer_labels"`  // 优先选择的标签
    ExcludeLabels map[string]string `json:"exclude_labels"` // 排除的标签
}
```

### 端点注册表接口

```go
type EndpointRegistry interface {
    // 端点管理 (核心功能)
    RegisterEndpoint(endpoint *Endpoint) error
    UnregisterEndpoint(endpointID string) error
    GetEndpoint(endpointID string) (*Endpoint, error)
    ListEndpoints() ([]*Endpoint, error)
    
    // 动态查询 (Service聚合的核心)
    FindEndpointsBySelector(selector map[string]string) ([]*Endpoint, error)
    FindEndpointsByLabels(labels map[string]string) ([]*Endpoint, error)
    
    // 工作负载管理 (用于注册端点)
    RegisterWorkload(workload *Workload) error
    UnregisterWorkload(workloadID string) error
    GetWorkload(workloadID string) (*Workload, error)
    
    // 健康管理
    UpdateEndpointHealth(endpointID string, healthy bool, reason string) error
    GetHealthyEndpoints(selector map[string]string) ([]*Endpoint, error)
}
```

### 服务聚合器接口

```go
// ServiceAggregator 服务聚合器 - 动态创建服务视图
type ServiceAggregator interface {
    // 根据选择器聚合服务视图
    AggregateService(selector map[string]string) (*ServiceView, error)
    
    // 实时服务查询
    QueryService(selector map[string]string, filter *EndpointFilter) (*ServiceView, error)
    
    // 获取服务统计
    GetServiceStats(selector map[string]string) (*ServiceStats, error)
}
```

### 负载均衡器接口

```go
type LoadBalancer interface {
    // SelectEndpoint 从聚合的端点中选择一个
    SelectEndpoint(endpoints []*Endpoint, clientKey string) *Endpoint
    // UpdateStats 更新端点统计信息
    UpdateStats(endpoint *Endpoint, latency time.Duration, success bool)
    // Name 返回负载均衡器名称
    Name() string
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

### 协议处理器接口

OutpostNet支持四种协议模式：

1. **TCP** - 透明TCP代理
2. **UDP** - UDP数据包转发
3. **STCP** - 安全TCP隧道（加密）
4. **SUDP** - 安全UDP隧道（加密）

```go
// 协议处理在客户端和服务端分别实现
// 客户端：本地监听 -> 服务器查询 -> 动态端点聚合
// 服务端：客户端查询 -> 标签匹配 -> 负载均衡选择 -> 端点转发
```

## 数据模型

### 配置数据模型

#### 服务器配置 (简化 - 无需预定义服务)
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

# 动态服务发现配置
service_discovery:
  mode: "dynamic"                    # 动态模式
  auto_create_services: true         # 自动创建服务视图
  default_load_balancer: "round_robin"
  service_ttl: "24h"                 # 服务视图生存时间
  cleanup_interval: "1h"             # 清理间隔

# 服务默认策略
default_service_policy:
  load_balancer: "round_robin"
  health_check:
    enabled: true
    interval: "30s"
    timeout: "5s"
  session_affinity: false

# 监控配置
monitoring:
  enabled: true
  port: 9090
  prometheus_enabled: true
```

#### 客户端配置

##### Register模式 - 注册端点
```yaml
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "secret-token"
  mode: "register"
  conn_proto: "auto"  # tcp | unix | auto

# 工作负载配置 - 注册端点信息
workloads:
  - metadata:
      name: "web-backend-1"               # 端点名称
      labels:
        app: "demo-web"                   # 应用标签 (用于Service选择)
        version: "v1"                     # 版本标签
        node: "demo-register-node"        # 节点标签
        zone: "local"                     # 区域标签
        tier: "frontend"                  # 层级标签
        component: "web"                  # 组件标签
    spec:
      local_addr: "127.0.0.1"             # 端点IP地址
      local_port: 18000                   # 端点端口
      protocol: "tcp"                     # 协议类型
```

##### Proxy模式 - 聚合端点
```yaml
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "secret-token"
  mode: "proxy"

# 代理配置
proxies:
  - name: "demo-web-proxy"
    local_port: 19999                    # 本地代理端口
    proxy_mode: "tcp"                    # 代理模式
    
    # 端点选择器 - 告诉服务端我要什么样的端点
    endpoint_selector:
      app: "demo-web"                    # 选择 app=demo-web 的端点
    
    # 负载均衡策略
    load_balancer: "round_robin"
    
    # 端点过滤器 (可选)
    endpoint_filter:
      prefer_labels:                     # 优先选择的端点标签
        zone: "local"
        version: "v1"
      exclude_labels:                    # 排除的端点标签
        status: "maintenance"
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
    MsgTypeAuth                = "auth"
    MsgTypeRegisterWorkload    = "register_workload"
    MsgTypeQueryEndpoints      = "query_endpoints"        // 新：查询端点
    MsgTypeAggregateService    = "aggregate_service"      // 新：聚合服务
    MsgTypeSTCPTunnelRequest   = "stcp_tunnel_request"
    MsgTypeSUDPTunnelRequest   = "sudp_tunnel_request"
    MsgTypeHeartbeat           = "heartbeat"
    
    // Server -> Client
    MsgTypeAuthResponse        = "auth_response"
    MsgTypeEndpointList        = "endpoint_list"          // 新：端点列表
    MsgTypeServiceView         = "service_view"           // 新：服务视图
    MsgTypeSTCPTunnelResponse  = "stcp_tunnel_response"
    MsgTypeSUDPTunnelResponse  = "sudp_tunnel_response"
    MsgTypeError               = "error"
    MsgTypePong                = "pong"
)
```

## 动态服务聚合逻辑

### 核心聚合算法

```go
// 服务聚合的核心逻辑
func (s *Server) AggregateService(selector map[string]string, filter *EndpointFilter) (*ServiceView, error) {
    // 1. 根据选择器查找匹配的端点
    allEndpoints := s.endpointRegistry.FindEndpointsBySelector(selector)
    
    // 2. 应用过滤器
    filteredEndpoints := s.applyEndpointFilter(allEndpoints, filter)
    
    // 3. 健康检查过滤
    healthyEndpoints := s.healthChecker.GetHealthyEndpoints(filteredEndpoints)
    
    // 4. 创建服务视图
    serviceView := &ServiceView{
        Selector:  selector,
        Endpoints: healthyEndpoints,
        Stats:     s.calculateServiceStats(healthyEndpoints),
        CreatedAt: time.Now(),
    }
    
    return serviceView, nil
}

// 标签匹配逻辑
func (s *Server) matchesSelector(endpointLabels map[string]string, selector map[string]string) bool {
    for key, value := range selector {
        if endpointLabels[key] != value {
            return false
        }
    }
    return true
}
```

### 多视角服务示例

```go
// 同一批端点，不同选择器，形成不同的服务视图

// 端点注册表
endpoints := []Endpoint{
    {ID: "web-1", Labels: {app: "web", tier: "frontend", version: "v1"}},
    {ID: "web-2", Labels: {app: "web", tier: "frontend", version: "v2"}},
    {ID: "api-1", Labels: {app: "api", tier: "backend", version: "v1"}},
    {ID: "api-2", Labels: {app: "api", tier: "backend", version: "v2"}},
}

// 不同代理的不同服务视图
proxyA := Proxy{EndpointSelector: {app: "web"}}        // → [web-1, web-2]
proxyB := Proxy{EndpointSelector: {tier: "frontend"}}  // → [web-1, web-2]  
proxyC := Proxy{EndpointSelector: {version: "v1"}}     // → [web-1, api-1]
proxyD := Proxy{EndpointSelector: {app: "web", version: "v1"}} // → [web-1]
```

## 设计优势

### 1. **极简化设计**
- 服务端无需预定义任何服务
- 代理只需指定需要的端点标签
- 服务是端点的动态聚合结果

### 2. **工作负载驱动**
- 服务随工作负载自然演进
- 无需预先规划服务架构
- 支持微服务的快速迭代

### 3. **多视角服务**
- 同一批端点可以形成多个服务视角
- 不同代理可以有不同的聚合需求
- 灵活的标签匹配机制

### 4. **实时动态**
- 端点变化立即反映到服务视图
- 无需重启或重新配置
- 支持热插拔和弹性伸缩

## 监控与可观测性

### 监控指标
- **端点数量**：总端点数、健康端点数、各标签的端点分布
- **服务聚合**：活跃服务视图数、聚合查询频率
- **连接数**：活跃连接数、总连接数
- **请求延迟**：P50、P95、P99延迟
- **匹配效率**：标签匹配耗时、缓存命中率

### 日志记录
- **端点事件**：注册、注销、健康状态变化
- **服务聚合**：动态服务创建、标签匹配结果
- **连接跟踪**：代理连接建立、负载均衡决策
- **性能指标**：关键操作的耗时统计

### 健康检查端点
- `/health` - 基本健康状态
- `/ready` - 就绪状态检查
- `/endpoints` - 端点注册表状态
- `/services` - 动态服务视图
- `/metrics` - Prometheus格式指标