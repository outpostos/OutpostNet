# Phase 2: OutpostNet 区域服务网格设计

## 🎯 Phase 2 目标

在保持Phase 1节点架构不变的基础上，为OutpostNet-server增加**服务发现和共享能力**，实现一个区域内多个节点组成的服务网格。

## 📋 设计原则

### 1. **完全兼容Phase 1**
- **部署架构不变**: 每个节点仍然是一个OutpostNet-server + 多个OutpostNet-client
- **配置格式不变**: 所有Phase 1的配置和API保持完全兼容
- **客户端无感知**: OutpostNet-client不需要任何改动

### 2. **仅增强server能力**
- **服务发现增强**: server间可以共享服务注册信息
- **跨节点路由**: 支持访问其他节点上的服务
- **故障隔离**: 单个节点故障不影响其他节点的独立运行

### 3. **渐进式部署**
- 支持单节点和多节点混合部署
- 可以逐个节点升级到Phase 2
- 向后兼容Phase 1的所有功能

## 🏗️ 架构设计

### 区域服务网格拓扑

```
Phase 2 架构：区域服务网格（保持节点内架构不变）
┌─────────────────────────────────────────────────────────────┐
│                        区域 (Region)                        │
│                                                             │
│     节点1                节点2                节点3          │
│ ┌─────────────┐      ┌─────────────┐      ┌─────────────┐   │
│ │OutpostNet-     │      │OutpostNet-     │      │OutpostNet-     │   │
│ │server       │◄────►│server       │◄────►│server       │   │
│ │             │      │             │      │             │   │
│ │本地服务注册表│      │本地服务注册表│      │本地服务注册表│   │
│ │+ 邻居服务缓存│      │+ 邻居服务缓存│      │+ 邻居服务缓存│   │
│ │             │      │             │      │             │   │
│ │ 负载均衡器   │      │ 负载均衡器   │      │ 负载均衡器   │   │
│ │ 连接管理     │      │ 连接管理     │      │ 连接管理     │   │
│ └─────────────┘      └─────────────┘      └─────────────┘   │
│        │                    │                    │         │
│  ┌─────┴─────┐         ┌────┴────┐         ┌────┴────┐    │
│  │  clients  │         │ clients │         │ clients │    │
│  │┌─────────┐│         │┌───────┐│         │┌───────┐│    │
│  ││client-A ││         ││client-C││         ││client-E││    │
│  │└─────────┘│         │└───────┘│         │└───────┘│    │
│  │┌─────────┐│         │┌───────┐│         │┌───────┐│    │
│  ││client-B ││         ││client-D││         ││client-F││    │
│  │└─────────┘│         │└───────┘│         │└───────┘│    │
│  └───────────┘         └─────────┘         └─────────┘    │
│                                                           │
│  服务发现范围:            服务发现范围:      服务发现范围:   │
│  - 本地: A, B            - 本地: C, D      - 本地: E, F   │
│  - 远程: C, D, E, F      - 远程: A, B, E, F - 远程: A, B, C, D │
└─────────────────────────────────────────────────────────────┘

特点:
1. 每个节点内部架构完全不变 (OutpostNet-server + clients)
2. server间增加服务共享协议，同步服务注册信息
3. 客户端无感知，仍然只连接本地server
4. 跨节点服务访问通过server间代理实现
```

### 核心组件设计

#### 1. **服务共享协议层**
```go
// 在现有 ServerConfig 基础上扩展（不改变原有结构）
type ServerConfig struct {
    // Phase 1 配置保持不变
    Server   ServerSpec    `yaml:"server"`
    Services []types.Service `yaml:"services,omitempty"`
    K8sAPI   K8sAPISpec    `yaml:"k8s_api,omitempty"`
    
    // Phase 2 新增：区域配置（可选）
    Region   *RegionConfig `yaml:"region,omitempty"`
}

// 新增区域配置
type RegionConfig struct {
    Enabled     bool        `yaml:"enabled"`         // 是否启用区域模式
    RegionID    string      `yaml:"region_id"`       // 区域标识
    NodeID      string      `yaml:"node_id"`         // 节点标识
    Neighbors   []NeighborNode `yaml:"neighbors"`    // 邻居节点列表
    
    // 服务共享配置
    ServiceShare ServiceShareConfig `yaml:"service_share"`
}

type NeighborNode struct {
    NodeID      string `yaml:"node_id"`
    Address     string `yaml:"address"`      // 邻居server地址
    Weight      int    `yaml:"weight"`       // 路由权重
    Zone        string `yaml:"zone,omitempty"` // 可用区
}

type ServiceShareConfig struct {
    SyncInterval    time.Duration `yaml:"sync_interval"`     // 同步间隔
    RetryCount      int          `yaml:"retry_count"`       // 重试次数
    HealthCheck     bool         `yaml:"health_check"`      // 是否启用健康检查
    CacheTimeout    time.Duration `yaml:"cache_timeout"`    // 缓存超时
}
```

#### 2. **增强的服务注册表**
```go
// 扩展现有的 K8sMemoryRegistry，不替换
type RegionRegistry struct {
    // 保持原有的本地注册表
    local    *K8sMemoryRegistry
    
    // 新增区域能力
    region   *RegionManager
    cache    *ServiceCache
    
    // 邻居节点服务缓存
    remoteServices map[string]*RemoteServiceInfo
    
    // 同步状态
    syncManager *ServiceSyncManager
}

// 服务注册保持不变，只增加同步
func (r *RegionRegistry) RegisterService(service *types.Service) error {
    // 1. 本地注册（保持Phase 1逻辑不变）
    if err := r.local.RegisterService(service); err != nil {
        return err
    }
    
    // 2. Phase 2新增：异步同步到邻居节点
    if r.region != nil && r.region.Enabled {
        go r.syncManager.SyncToNeighbors(service)
    }
    
    return nil
}

// 扩展服务发现：本地+远程
func (r *RegionRegistry) QueryService(name, namespace string, selector map[string]string) ([]*types.Endpoint, error) {
    // 1. 先查询本地服务（Phase 1逻辑）
    local, err := r.local.QueryService(name, namespace, selector)
    if err != nil {
        return nil, err
    }
    
    // 2. Phase 2新增：查询远程缓存
    var remote []*types.Endpoint
    if r.region != nil && r.region.Enabled {
        remote = r.queryRemoteService(name, namespace, selector)
    }
    
    // 3. 合并本地和远程结果
    return append(local, remote...), nil
}
```

#### 3. **跨节点路由层**
```go
// 新增跨节点代理组件
type CrossNodeProxy struct {
    localServer  *Server           // 本地server实例
    neighbors    map[string]*NeighborConnection // 邻居连接
    routeTable   *RouteTable       // 路由表
}

// 跨节点服务调用
func (p *CrossNodeProxy) ProxyToRemoteService(conn net.Conn, targetService string, targetNode string) error {
    // 1. 查找目标节点连接
    neighborConn, exists := p.neighbors[targetNode]
    if !exists {
        return fmt.Errorf("neighbor node %s not found", targetNode)
    }
    
    // 2. 建立到邻居节点的连接
    remoteConn, err := neighborConn.Connect()
    if err != nil {
        return err
    }
    
    // 3. 发送代理请求
    proxyReq := &ProxyRequest{
        ServiceName: targetService,
        Protocol:    "tcp",
    }
    
    // 4. 开始数据转发
    return p.startDataForward(conn, remoteConn)
}
```

#### 4. **配置兼容性保证**
```go
// 客户端配置完全不变
type ClientConfig struct {
    // Phase 1 配置保持100%兼容
    Client    ClientSpec       `yaml:"client"`
    Proxies   []types.Proxy    `yaml:"proxies,omitempty"`
    Workloads []ClientWorkload `yaml:"workloads,omitempty"`
    
    // 客户端完全无感知，不需要任何Phase 2配置
}

// 服务端配置向前兼容
type ServerConfig struct {
    // Phase 1 必需配置（保持不变）
    Server   ServerSpec      `yaml:"server"`
    Services []types.Service `yaml:"services,omitempty"`
    K8sAPI   K8sAPISpec      `yaml:"k8s_api,omitempty"`
    
    // Phase 2 可选配置（默认关闭）
    Region   *RegionConfig   `yaml:"region,omitempty"`
}

// 配置加载兼容性
func LoadServerConfig(configFile string) (*ServerConfig, error) {
    cfg, err := loadPhase1Config(configFile)
    if err != nil {
        return nil, err
    }
    
    // 如果没有region配置，直接返回Phase 1模式
    if cfg.Region == nil || !cfg.Region.Enabled {
        return cfg, nil
    }
    
    // 有region配置时，启用Phase 2功能
    return cfg, nil
}
```

## 🔧 实现路线图

### Phase 2.1: 服务共享协议 (2-3周)

#### 目标
- 实现server间的服务信息同步
- 保持Phase 1部署架构不变
- 确保向前兼容性

#### 关键任务
1. **区域配置扩展**
   - 扩展ServerConfig，添加可选的RegionConfig
   - 实现向前兼容的配置加载
   - 支持邻居节点的配置管理

2. **服务同步协议**
   - 设计server间的服务同步消息格式
   - 实现异步的服务信息广播
   - 添加服务缓存和过期机制

3. **增强服务注册表**
   - 扩展K8sMemoryRegistry，添加远程服务缓存
   - 实现本地+远程的服务查询
   - 保持原有API完全兼容

4. **健康检查和故障处理**
   - 实现邻居节点的健康检查
   - 处理网络分区和节点故障
   - 自动清理不可达服务

#### 验收标准
- ✅ Phase 1配置和部署方式完全兼容
- ✅ 支持2-3个节点的服务信息同步
- ✅ 服务注册和发现API保持不变
- ✅ 单节点故障不影响其他节点

### Phase 2.2: 跨节点路由 (2-3周)

#### 目标
- 实现跨节点的服务调用
- 支持透明的负载均衡
- 保持客户端完全无感知

#### 关键任务
1. **跨节点代理机制**
   - 实现server间的代理连接
   - 支持TCP/UDP/STCP/SUDP的跨节点转发
   - 处理连接建立和数据转发

2. **智能路由策略**
   - 本地优先路由（优先选择本地服务）
   - 负载均衡路由（在多个远程节点间分配）
   - 故障转移路由（自动切换到可用节点）

3. **连接池优化**
   - server间的连接复用
   - 动态连接管理
   - 连接健康检查

4. **性能优化**
   - 减少跨节点调用的延迟
   - 优化数据转发路径
   - 连接缓存和复用

#### 验收标准
- ✅ 支持所有协议的跨节点调用
- ✅ 本地服务优先，远程服务兜底
- ✅ 跨节点调用延迟 < 10ms额外开销
- ✅ 客户端完全无感知跨节点切换

### Phase 2.3: 运维和监控 (1-2周)

#### 目标
- 区域网格的监控和运维
- 故障诊断和问题排查
- Express Mode支持区域配置

#### 关键任务
1. **区域监控指标**
   - 节点间连接状态
   - 服务同步延迟
   - 跨节点调用统计

2. **Express Mode增强**
   - 支持区域配置的快速生成
   - 简化多节点部署脚本
   - 区域演示脚本

3. **故障诊断工具**
   - 节点连通性检查
   - 服务同步状态查询
   - 路由路径追踪

4. **管理工具**
   - 区域状态查看命令
   - 节点动态加入/退出
   - 配置验证工具

#### 验收标准
- ✅ 完整的区域监控面板
- ✅ Express Mode支持区域部署
- ✅ 故障自动诊断和修复建议
- ✅ 简化的运维操作

## 🚀 快速启动设计

### Express Mode 区域支持

为了保持用户体验的一致性，Phase 2 将为Express Mode增加区域配置支持：

```bash
# 启动区域内的第一个节点
./OutpostNet-server --express \
  --service web:80:8080 \
  --region-id edge-region-1 \
  --node-id node1

# 启动区域内的第二个节点（自动发现第一个节点）
./OutpostNet-server --express \
  --service api:8081:8081 \
  --region-id edge-region-1 \
  --node-id node2 \
  --neighbor node1:192.168.1.10:7629

# 启动区域内的第三个节点
./OutpostNet-server --express \
  --service db:3306:3306 \
  --region-id edge-region-1 \
  --node-id node3 \
  --neighbor node1:192.168.1.10:7629,node2:192.168.1.11:7629

# 客户端保持不变（完全无感知）
./OutpostNet-client --express --mode proxy --service web --local-port 8080
./OutpostNet-client --express --mode register --service cache --backend 127.0.0.1:6379
```

### 配置自动生成

Express Mode将自动生成区域配置：

```yaml
# 自动生成的服务端配置
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "OutpostNet-express-token"

services:
  - metadata:
      name: web
      namespace: default
    spec:
      ports:
        - port: 80
          targetPort: 8080

# Phase 2 新增：区域配置
region:
  enabled: true
  region_id: "edge-region-1"
  node_id: "node1"
  neighbors:
    - node_id: "node2"
      address: "192.168.1.11:7629"
    - node_id: "node3"
      address: "192.168.1.12:7629"
  service_share:
    sync_interval: "30s"
    health_check: true
```

### 自动化演示脚本

```bash
# 新增区域演示脚本
./scripts/region-demo.sh

# 该脚本将：
# 1. 启动3个节点的区域网格
# 2. 在不同节点注册不同服务
# 3. 演示跨节点的服务发现和调用
# 4. 模拟节点故障和恢复
# 5. 验证服务的高可用性
```

### 部署示例

#### 单节点升级到区域网格
```bash
# Phase 1: 单节点部署
./OutpostNet-server --express --service web:80:8080

# Phase 2: 零停机升级到区域模式
# 1. 更新配置文件，添加region配置
# 2. 重启服务（配置向前兼容）
# 3. 逐步添加其他节点

# 添加第二个节点
./OutpostNet-server --express --service api:8081:8081 \
  --region-id same-region --neighbor node1:ip:7629
```

#### 客户端无感知升级
```bash
# Phase 1和Phase 2客户端配置完全相同
./OutpostNet-client --express --mode proxy --service web --local-port 8080

# 客户端会自动享受：
# - 更高的服务可用性（多节点容错）
# - 更多的服务选择（区域内所有服务）
# - 透明的负载均衡（本地优先+远程兜底）
```

## 📊 技术选型

### 1. **服务同步协议**
**推荐**: 基于HTTP的轻量级协议
- **优势**: 简单易实现，易于调试
- **适用场景**: 区域内少量节点（2-5个）
- **兼容性**: 与现有架构无缝集成

**方案**: RESTful API + JSON格式
- **服务注册同步**: POST /api/v1/services/sync
- **服务查询**: GET /api/v1/services/discovery
- **健康检查**: GET /api/v1/health

### 2. **网络通信**
**推荐**: 复用现有的TCP协议
- **优势**: 与Phase 1完全兼容
- **实现**: 扩展现有的server处理逻辑
- **性能**: 无额外协议开销

**server间通信**: 基于现有的OutpostNet协议
- **认证**: 复用现有的auth_token机制
- **加密**: 支持TLS（可选）
- **连接复用**: 长连接+心跳检测

### 3. **数据存储**
**推荐**: 扩展现有的内存存储
- **本地服务**: 继续使用K8sMemoryRegistry
- **远程缓存**: 新增RemoteServiceCache
- **数据一致性**: 最终一致性模型

**无需外部依赖**:
- 不依赖etcd、Redis等外部组件
- 保持OutpostNet的轻量级特性
- 简化部署和运维

## 🔒 安全考虑

### 1. **节点间通信安全**
- 复用现有的auth_token认证机制
- 可选的TLS加密（生产环境推荐）
- IP白名单访问控制

### 2. **服务访问控制**
- 基于namespace的服务隔离
- 继承Phase 1的所有安全机制
- 跨节点访问审计日志

### 3. **配置安全**
- 敏感配置加密存储
- 配置文件权限控制
- 动态配置安全验证

## 📈 性能目标

### 1. **区域网格性能**
- 本地服务访问: 与Phase 1性能一致
- 跨节点服务发现: < 100ms
- 服务信息同步延迟: < 30s

### 2. **扩展性指标**
- 支持节点数量: 2-5个节点/区域
- 每节点服务数量: 与Phase 1一致
- 跨节点连接数: < 100并发连接

### 3. **可用性目标**
- 单节点故障恢复: < 30s
- 服务发现可用性: 99.5%
- 配置兼容性: 100%向前兼容

## 🧪 测试策略

### 1. **兼容性测试**
- Phase 1配置向前兼容性
- 客户端无感知升级测试
- API接口兼容性验证

### 2. **功能测试**
- 跨节点服务发现
- 服务信息同步
- 故障切换和恢复

### 3. **性能测试**
- 跨节点调用延迟
- 服务同步性能
- 多节点负载测试

### 4. **可靠性测试**
- 单节点故障测试
- 网络分区恢复
- 长时间运行稳定性

## 💡 实现建议

### 1. **开发优先级**
1. 扩展配置结构，保持兼容性
2. 实现服务信息同步协议
3. 增强服务注册表查询能力
4. 添加跨节点路由机制
5. 完善监控和运维工具

### 2. **风险控制**
- 严格保持Phase 1架构不变
- 所有新功能都可选择启用/禁用
- 充分的回归测试覆盖

### 3. **团队协作**
- server和client代码分离
- 清晰的配置扩展接口
- 定期兼容性验证

---

**Phase 2 将在保持现有架构稳定的基础上，为OutpostNet增加区域网格能力，为未来的跨区域扩展奠定基础。**