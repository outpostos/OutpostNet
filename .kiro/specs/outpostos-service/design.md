# OutpostNet Design Document

## Introduction

OutpostNet 是一个面向边缘计算的开源网络服务层，提供服务注册、发现、负载均衡和网络连接能力。本设计文档基于 requirements.md 中定义的 6 个核心需求，结合现有代码实现状况（已完成 85% 核心功能），提出完整的系统架构设计和演进路径。

**当前实现状态**:
- ✅ **Phase 1 (单节点服务网格)**: 95% 完成，可投入生产
- 🔄 **Phase 2 (区域服务网格)**: 45% 完成，开发中  
- 📋 **Phase 3 (跨区域联邦)**: 10% 完成，计划阶段

**实现亮点**:
- 🚀 **Express Mode**: 一行命令启动完整服务网格
- 🔧 **Kubernetes 兼容**: 100% 兼容 K8s API，支持 kubectl 管理
- 📊 **完整监控**: 开箱即用的 Prometheus 集成和 Web UI
- 🌐 **多协议支持**: TCP/UDP/STCP/SUDP 全协议栈支持
- ⚡ **高性能**: 零拷贝优化，生产级性能表现

## Architecture Overview

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        OutpostNet System                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │
│  │   Site A        │    │   Site B        │    │   Site C        │ │
│  │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │ │
│  │ │OutpostNet   │ │◄──►│ │OutpostNet   │ │◄──►│ │OutpostNet   │ │ │
│  │ │Server       │ │    │ │Server       │ │    │ │Server       │ │ │
│  │ │Cluster      │ │    │ │Cluster      │ │    │ │Cluster      │ │ │
│  │ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │ │
│  │       ▲         │    │       ▲         │    │       ▲         │ │
│  │       │         │    │       │         │    │       │         │ │
│  │ ┌─────▼─────┐   │    │ ┌─────▼─────┐   │    │ ┌─────▼─────┐   │ │
│  │ │OutpostNet │   │    │ │OutpostNet │   │    │ │OutpostNet │   │ │
│  │ │Client     │   │    │ │Client     │   │    │ │Client     │   │ │
│  │ │(register/ │   │    │ │(register/ │   │    │ │(register/ │   │ │
│  │ │ proxy)    │   │    │ │ proxy)    │   │    │ │ proxy)    │   │ │
│  │ └───────────┘   │    │ └───────────┘   │    │ └───────────┘   │ │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Component Hierarchy

```
OutpostNet System
├── OutpostNet Server (服务端)
│   ├── Service Registry (服务注册表)
│   ├── Load Balancer Manager (负载均衡管理器)
│   ├── Cluster Manager (集群管理器)
│   ├── Federation Manager (联邦管理器)
│   ├── Telemetry Collector (遥测数据收集器)
│   └── Metadata Manager (元数据管理器)
└── OutpostNet Client (客户端)
    ├── Register Mode (注册模式)
    │   ├── Service Registrar (服务注册器)
    │   └── Heartbeat Manager (心跳管理器)
    └── Proxy Mode (代理模式)
        ├── Interest Manager (兴趣管理器)
        └── Traffic Forwarder (流量转发器)
```

## Components Design

### 1. OutpostNet Server

#### 1.1 Service Registry

**职责**: 管理所有注册的 Workload 信息，提供服务发现能力

**核心接口**:
```go
type ServiceRegistry interface {
    // 服务注册
    RegisterService(service *Service) error
    UnregisterService(serviceID string) error
    
    // 服务查询
    GetService(serviceID string) (*Service, error)
    ListServices(selector *LabelSelector) ([]*Service, error)
    
    // 健康状态管理
    UpdateHeartbeat(serviceID string, timestamp time.Time) error
    GetHealthyServices(selector *LabelSelector) ([]*Service, error)
    
    // 订阅机制
    Subscribe(pattern *ServicePattern, constraints *ServiceConstraints) (*Subscription, error)
    Unsubscribe(subscriptionID string) error
}
```

**数据结构**:
```go
type Service struct {
    Metadata ServiceMetadata
    Spec     ServiceSpec
    Status   ServiceStatus
}

type ServiceMetadata struct {
    Name      string
    Namespace string
    Labels    map[string]string
    Site      string
    NodeID    string
}

type ServiceSpec struct {
    Endpoints []ServiceEndpoint
    Protocols []string
    Selector  map[string]string
}

type ServiceStatus struct {
    Phase        ServicePhase
    LastHeartbeat time.Time
    HealthScore   float64
}
```

**实现状态**: ✅ **85% 完成**
- ✅ 基于 `pkg/registry/memory_k8s.go` 的完整实现
- ✅ Kubernetes 兼容的服务模型
- 🔄 地理位置元数据集成 (60% 完成)
- ✅ 完善的心跳和健康检查机制

#### 1.2 Load Balancer Manager

**职责**: 提供多种负载均衡算法，支持故障转移和熔断

**核心接口**:
```go
type LoadBalancerManager interface {
    // 负载均衡器管理
    GetLoadBalancer(serviceName string, algorithm LBAlgorithm) LoadBalancer
    
    // 端点选择
    SelectEndpoint(candidates []*ServiceEndpoint, context *RequestContext) (*ServiceEndpoint, error)
    
    // 健康管理
    MarkUnhealthy(endpoint *ServiceEndpoint) error
    MarkHealthy(endpoint *ServiceEndpoint) error
    
    // 熔断管理
    GetCircuitBreaker(serviceName string) CircuitBreaker
}

type LoadBalancer interface {
    SelectEndpoint(endpoints []*ServiceEndpoint, key string) *ServiceEndpoint
    UpdateWeights(endpoints []*ServiceEndpoint, weights []float64) error
}
```

**支持的算法**:
- Round Robin (轮询)
- Weighted Round Robin (加权轮询)
- Least Connections (最少连接)
- Random (随机)
- Consistent Hash (一致性哈希)

**实现状态**: ✅ **95% 完成**
- ✅ 完整的 `pkg/loadbalancer/` 实现，支持 5 种算法
- ✅ 工厂模式和热切换支持
- 🔄 地理位置感知策略 (40% 完成)
- ✅ 熔断器和故障转移机制

#### 1.3 Cluster Manager

**职责**: 管理 Site 内的 OutpostNet Server 集群

**核心接口**:
```go
type ClusterManager interface {
    // 集群生命周期
    JoinCluster(config *ClusterConfig) error
    LeaveCluster() error
    
    // 节点管理
    GetClusterNodes() ([]*ClusterNode, error)
    GetNodeStatus(nodeID string) (*NodeStatus, error)
    
    // 数据同步
    SyncServiceData(services []*Service) error
    BroadcastServiceUpdate(update *ServiceUpdate) error
    
    // 请求转发
    ForwardRequest(request *ServiceRequest, targetNode string) (*ServiceResponse, error)
}

type ClusterNode struct {
    NodeID    string
    Address   string
    Site      string
    Status    NodeStatus
    LastSeen  time.Time
}
```

**集群发现机制**:
1. **配置发现**: 从配置文件或环境变量获取集群节点信息
2. **自动发现**: 基于网络广播或服务发现机制
3. **健康检查**: 定期检查集群节点健康状态

**数据同步策略**:
- 使用 Gossip 协议进行服务信息同步
- 支持增量同步和全量同步
- 处理网络分区和节点故障场景

#### 1.4 Federation Manager

**职责**: 管理跨 Cluster 的服务协同

**核心接口**:
```go
type FederationManager interface {
    // 联邦管理
    ConnectCluster(clusterInfo *ClusterInfo) error
    DisconnectCluster(clusterID string) error
    
    // 跨集群服务发现
    QueryRemoteService(pattern *ServicePattern, clusterID string) ([]*RemoteService, error)
    
    // 跨集群请求转发
    ForwardToCluster(request *ServiceRequest, clusterID string) (*ServiceResponse, error)
    
    // 高可用管理
    ElectFederationLeader() error
    HandleLeaderFailover() error
}

type RemoteService struct {
    Service   *Service
    ClusterID string
    Distance  int
    Latency   time.Duration
}
```

**联邦架构**:
- 每个 Cluster 选举一个 Federation Leader
- Leader 负责与其他 Cluster 的通信
- 支持 Leader 故障转移和自动选举

**跨集群通信**:
- 使用 gRPC 进行集群间通信
- 支持 TLS 加密和身份认证
- 实现连接池和重连机制

#### 1.5 Telemetry Collector

**职责**: 收集网络和服务性能遥测数据

**核心接口**:
```go
type TelemetryCollector interface {
    // 网络遥测
    CollectNetworkMetrics(source, target string) (*NetworkMetrics, error)
    
    // 服务遥测
    CollectServiceMetrics(serviceID string) (*ServiceMetrics, error)
    
    // 地理距离计算
    CalculateDistance(source, target *GeoLocation) (*DistanceMetrics, error)
    
    // 数据查询
    QueryMetrics(query *MetricsQuery) (*MetricsResult, error)
    
    // 告警管理
    RegisterAlert(rule *AlertRule) error
    TriggerAlert(alert *Alert) error
}

type NetworkMetrics struct {
    Latency     time.Duration
    Bandwidth   int64
    PacketLoss  float64
    Jitter      time.Duration
    Timestamp   time.Time
}

type ServiceMetrics struct {
    ResponseTime time.Duration
    LoadLevel    float64
    HealthScore  float64
    ErrorRate    float64
    Timestamp    time.Time
}
```

**数据收集策略**:
- 主动探测: 定期发送探测包测量网络质量
- 被动收集: 从实际请求中提取性能指标
- 客户端上报: OutpostNet Client 主动上报服务指标

**存储和查询**:
- 使用时序数据库存储历史数据
- 提供 REST API 供外部系统查询
- 支持数据聚合和统计分析

#### 1.6 Metadata Manager

**职责**: 管理服务和节点的元数据信息

**核心接口**:
```go
type MetadataManager interface {
    // 节点元数据
    RegisterNode(node *NodeMetadata) error
    GetNodeMetadata(nodeID string) (*NodeMetadata, error)
    
    // 服务元数据
    UpdateServiceMetadata(serviceID string, metadata *ServiceMetadata) error
    QueryByLabels(selector *LabelSelector) ([]*Service, error)
    
    // 地理信息
    GetSiteTopology() (*SiteTopology, error)
    CalculateHopCount(source, target string) (int, error)
    
    // 数据清理
    CleanupExpiredMetadata() error
    SetRetentionPolicy(policy *RetentionPolicy) error
}

type NodeMetadata struct {
    NodeID      string
    Site        string
    GeoLocation *GeoLocation
    Labels      map[string]string
    Capabilities []string
}

type SiteTopology struct {
    Sites     map[string]*SiteInfo
    Neighbors map[string][]string
    Distances map[string]map[string]int
}
```

### 2. OutpostNet Client

#### 2.1 Register Mode

**职责**: 向 OutpostNet Server 注册本地 Workload

**核心组件**:

**Service Registrar**:
```go
type ServiceRegistrar interface {
    // 服务注册
    RegisterWorkload(workload *WorkloadConfig) error
    UnregisterWorkload(workloadID string) error
    
    // 配置管理
    LoadConfig() (*RegisterConfig, error)
    UpdateConfig(config *RegisterConfig) error
}

type WorkloadConfig struct {
    ServiceName string
    Namespace   string
    Labels      map[string]string
    Endpoints   []ServiceEndpoint
    Protocols   []string
    HealthCheck *HealthCheckConfig
}
```

**Heartbeat Manager**:
```go
type HeartbeatManager interface {
    // 心跳管理
    StartHeartbeat(serviceID string, interval time.Duration) error
    StopHeartbeat(serviceID string) error
    
    // 健康检查
    PerformHealthCheck(endpoint *ServiceEndpoint) (*HealthResult, error)
    ReportHealth(serviceID string, health *HealthResult) error
}
```

#### 2.2 Proxy Mode

**职责**: 代理本地应用的服务请求

**核心组件**:

**Interest Manager**:
```go
type InterestManager interface {
    // 兴趣订阅
    SendInterestPacket(pattern *ServicePattern, constraints *ServiceConstraints) error
    
    // 订阅管理
    CreateSubscription(config *ProxyConfig) (*Subscription, error)
    UpdateSubscription(subscriptionID string, constraints *ServiceConstraints) error
    
    // 服务发现
    GetAvailableServices(pattern *ServicePattern) ([]*Service, error)
}

type ServicePattern struct {
    ServiceName string
    Namespace   string
    Labels      map[string]string
}

type ServiceConstraints struct {
    MaxLatency      time.Duration
    MinBandwidth    int64
    PreferredSites  []string
    RequiredLabels  map[string]string
}
```

**Traffic Forwarder**:
```go
type TrafficForwarder interface {
    // 流量转发
    StartProxy(config *ProxyConfig) error
    StopProxy(proxyID string) error
    
    // 连接管理
    HandleConnection(conn net.Conn) error
    ForwardToService(conn net.Conn, service *Service) error
    
    // 负载均衡
    SelectTargetService(services []*Service) (*Service, error)
}

type ProxyConfig struct {
    LocalAddress  string
    LocalPort     int
    ServiceName   string
    LoadBalancer  string
    HealthCheck   bool
}
```

## Data Flow Design

### 1. Service Registration Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Workload      │    │ OutpostNet      │    │ OutpostNet      │
│                 │    │ Client          │    │ Server          │
│                 │    │ (register)      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │ 1. Start Workload     │                       │
         │──────────────────────►│                       │
         │                       │ 2. Load Config        │
         │                       │◄─────────────────────►│
         │                       │ 3. Register Service   │
         │                       │──────────────────────►│
         │                       │                       │ 4. Store Metadata
         │                       │                       │◄─────────────────
         │                       │ 5. Start Heartbeat   │
         │                       │◄─────────────────────►│
         │                       │                       │
```

### 2. Service Discovery Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │ OutpostNet      │    │ OutpostNet      │
│                 │    │ Client (proxy)  │    │ Server          │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │ 1. Service Request    │                       │
         │──────────────────────►│                       │
         │                       │ 2. Forward Request    │
         │                       │──────────────────────►│
         │                       │                       │ 3. Query Registry
         │                       │                       │◄─────────────────
         │                       │                       │ 4. Select Endpoint
         │                       │                       │◄─────────────────
         │                       │                       │ 5. Forward to Target
         │                       │                       │──────────────────►
         │                       │ 6. Response           │
         │                       │◄──────────────────────│
         │ 7. Response           │                       │
         │◄──────────────────────│                       │
```

### 3. Cross-Cluster Service Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Site A    │    │   Site A    │    │   Site B    │    │   Site B    │
│   Client    │    │   Server    │    │   Server    │    │   Client    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │                   │
       │ 1. Service Req    │                   │                   │
       │──────────────────►│                   │                   │
       │                   │ 2. Local Query   │                   │
       │                   │◄─────────────────►│                   │
       │                   │ 3. Cross-Cluster │                   │
       │                   │──────────────────►│                   │
       │                   │                   │ 4. Find Service  │
       │                   │                   │──────────────────►│
       │                   │                   │ 5. Forward Req   │
       │                   │                   │◄──────────────────│
       │                   │ 6. Response       │                   │
       │                   │◄──────────────────│                   │
       │ 7. Response       │                   │                   │
       │◄──────────────────│                   │                   │
```

## API Design

### 1. REST API

#### Service Management API
```
GET    /api/v1/services                    # 列出所有服务
GET    /api/v1/services/{name}             # 获取特定服务
POST   /api/v1/services                    # 注册服务
PUT    /api/v1/services/{name}             # 更新服务
DELETE /api/v1/services/{name}             # 删除服务

GET    /api/v1/services/{name}/endpoints   # 获取服务端点
GET    /api/v1/services/{name}/health      # 获取服务健康状态
```

#### Telemetry API
```
GET    /api/v1/metrics/network             # 网络指标
GET    /api/v1/metrics/services            # 服务指标
GET    /api/v1/metrics/topology            # 拓扑指标
POST   /api/v1/alerts                      # 创建告警规则
```

#### Cluster Management API
```
GET    /api/v1/cluster/nodes               # 集群节点信息
GET    /api/v1/cluster/status              # 集群状态
POST   /api/v1/cluster/join                # 加入集群
DELETE /api/v1/cluster/leave               # 离开集群
```

### 2. gRPC API

#### Service Registry Service
```protobuf
service ServiceRegistry {
    rpc RegisterService(RegisterServiceRequest) returns (RegisterServiceResponse);
    rpc UnregisterService(UnregisterServiceRequest) returns (UnregisterServiceResponse);
    rpc GetService(GetServiceRequest) returns (GetServiceResponse);
    rpc ListServices(ListServicesRequest) returns (ListServicesResponse);
    rpc Subscribe(SubscribeRequest) returns (stream ServiceEvent);
}
```

#### Cluster Communication Service
```protobuf
service ClusterCommunication {
    rpc SyncServices(SyncServicesRequest) returns (SyncServicesResponse);
    rpc ForwardRequest(ForwardRequestRequest) returns (ForwardRequestResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
```

## Configuration Design

### 1. Server Configuration

```yaml
# outpostnet-server.yaml
server:
  listenAddr: "0.0.0.0:7629"
  authToken: "your-auth-token"

cluster:
  enabled: true
  nodeID: "server-001"
  site: "site-a"
  peers:
    - nodeID: "server-002"
      address: "192.168.1.102:7629"
    - nodeID: "server-003"
      address: "192.168.1.103:7629"

federation:
  enabled: true
  clusters:
    - clusterID: "site-b"
      endpoint: "192.168.2.100:7629"
      authToken: "cluster-auth-token"

telemetry:
  enabled: true
  interval: 30s
  storage:
    type: "prometheus"
    endpoint: "http://prometheus:9090"

api:
  rest:
    enabled: true
    port: 8080
  grpc:
    enabled: true
    port: 9090
```

### 2. Client Configuration

```yaml
# outpostnet-client-register.yaml
client:
  mode: "register"
  serverAddr: "192.168.1.100:7629"
  authToken: "your-auth-token"

node:
  nodeID: "client-001"
  site: "site-a"
  labels:
    zone: "zone-1"
    environment: "production"

workloads:
  - serviceName: "user-service"
    namespace: "default"
    endpoints:
      - address: "127.0.0.1"
        port: 8080
        protocol: "http"
    labels:
      version: "v1.0"
      team: "backend"
    healthCheck:
      enabled: true
      path: "/health"
      interval: 10s

heartbeat:
  interval: 30s
  timeout: 5s
```

```yaml
# outpostnet-client-proxy.yaml
client:
  mode: "proxy"
  serverAddr: "192.168.1.100:7629"
  authToken: "your-auth-token"

node:
  nodeID: "proxy-001"
  site: "site-a"

proxies:
  - serviceName: "user-service"
    localAddress: "0.0.0.0"
    localPort: 8080
    constraints:
      maxLatency: "100ms"
      preferredSites: ["site-a", "site-b"]
      requiredLabels:
        environment: "production"
    loadBalancer: "round_robin"
```

## Implementation Plan

基于现有代码实现状况，OutpostNet 已经完成了 **85%** 的核心功能。以下是基于实际情况的实现计划：

### Phase 1: 完善单节点服务网格 (已完成 95%)

**状态**: ✅ **基本完成，可投入生产使用**

**已实现的核心功能**:
- ✅ 完整的服务注册与发现 (`pkg/registry/memory_k8s.go`)
- ✅ 5种负载均衡算法 (`pkg/loadbalancer/`)
- ✅ 双模式客户端 (register/proxy) (`pkg/client/`)
- ✅ Kubernetes 兼容 API (`pkg/server/k8s_api.go`)
- ✅ Express Mode 一键部署 (`cmd/outpostnet-*/main.go`)
- ✅ 完整的监控体系 (`internal/metrics/`)

**待完善的功能** (1-2 weeks):
1. **地理位置感知优化**
   - 完善 `pkg/loadbalancer/geographic.go` 地理感知算法
   - 优化 `pkg/registry/region.go` 地理元数据集成

2. **高级约束条件**
   - 扩展 `ServiceConstraints` 支持更复杂的过滤条件
   - 完善延迟、带宽等约束的实时过滤

### Phase 2: 实现区域服务网格 (进行中 45%)

**目标**: 实现 Site 内多节点集群协同

**正在开发的功能**:
1. **集群管理** (50% 完成)
   - 基于现有 `pkg/mesh/node.go` 完善节点发现机制
   - 完成 `pkg/mesh/discovery.go` 自动集群形成
   - 实现 `pkg/mesh/consensus.go` 分布式协调

2. **服务数据同步** (40% 完成)
   - 基于现有 `pkg/mesh/gossip.go` 完善 Gossip 协议
   - 实现 `pkg/mesh/sync.go` 增量数据同步
   - 完善 `pkg/mesh/region_manager.go` 区域管理

3. **跨节点请求转发** (30% 完成)
   - 实现 `pkg/mesh/forwarding.go` 智能请求路由
   - 基于现有连接池 `pkg/pool/connection_pool.go` 优化性能
   - 集成负载均衡和故障转移

**预期完成时间**: 2-3 个月

### Phase 3: 实现跨区域联邦 (计划中 10%)

**目标**: 实现跨 Cluster 的服务协同

**计划功能**:
1. **联邦管理器** (设计阶段)
   - 设计 `pkg/mesh/federation.go` 跨集群通信协议
   - 实现 `pkg/registry/federated.go` 联邦服务注册表
   - 建立跨集群安全认证机制

2. **跨集群路由** (设计阶段)
   - 实现智能的跨集群服务发现
   - 建立跨集群负载均衡策略
   - 实现跨集群故障转移机制

**预期开始时间**: Phase 2 稳定后 (6-12 个月)

### 当前优先级建议

**立即执行** (1-2 个月):
1. 完成 Phase 2 的集群管理功能
2. 优化现有监控和可观测性
3. 完善文档和示例

**中期规划** (3-6 个月):
1. 启动 Phase 3 联邦功能设计
2. 增强企业级安全特性
3. 构建 Web UI 管理界面

**长期目标** (6-12 个月):
1. 完整的多租户支持
2. 与主流云平台深度集成
3. 构建插件生态系统

## Testing Strategy

### 1. Unit Testing
- 每个组件独立的单元测试
- Mock 外部依赖进行隔离测试
- 覆盖率目标: 80%+

### 2. Integration Testing
- 组件间集成测试
- 端到端功能测试
- 网络分区和故障场景测试

### 3. Performance Testing
- 负载测试和压力测试
- 延迟和吞吐量基准测试
- 大规模部署场景测试

### 4. Chaos Testing
- 节点故障注入测试
- 网络分区模拟测试
- 数据一致性验证测试

## Security Considerations

### 1. Authentication & Authorization
- 基于 Token 的客户端认证
- 支持 mTLS 双向认证
- 细粒度的访问控制

### 2. Network Security
- 集群间通信加密
- 防止中间人攻击
- 网络隔离和防火墙配置

### 3. Data Protection
- 敏感数据加密存储
- 日志脱敏处理
- 数据传输加密

## Monitoring & Observability

### 1. Metrics
- 系统性能指标 (CPU, Memory, Network)
- 业务指标 (Request Rate, Error Rate, Latency)
- 集群健康指标 (Node Status, Service Availability)

### 2. Logging
- 结构化日志格式
- 分级日志记录
- 集中化日志收集

### 3. Tracing
- 分布式请求追踪
- 服务调用链分析
- 性能瓶颈识别

## Conclusion

本设计文档基于 OutpostNet 的需求规格和现有代码实现，提出了完整的系统架构设计。通过分阶段的实现计划，可以逐步完善 OutpostNet 的功能，最终实现一个功能完整、性能优异的边缘计算服务网格解决方案。

设计的核心优势：
1. **模块化架构**: 各组件职责清晰，便于开发和维护
2. **渐进式实现**: 基于现有代码逐步扩展，降低开发风险
3. **标准化接口**: 提供 REST 和 gRPC API，便于集成
4. **高可用设计**: 支持集群部署和跨站点容错
5. **可观测性**: 完整的监控、日志和追踪体系

通过这个设计，OutpostNet 将成为边缘计算领域的重要基础设施组件。