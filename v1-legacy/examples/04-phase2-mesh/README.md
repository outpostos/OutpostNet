# 示例4：Phase 2 区域网格

> 🌍 **学习分布式边缘网格**
> 
> 通过这个示例，你将学会 OutpostNet 的高级功能：跨区域服务网格、地理位置感知负载均衡、多节点服务发现等企业级特性。

## 📖 学习目标

通过这个示例，你将学会：
- 跨区域的分布式服务网格架构
- 地理位置感知的智能路由
- 多协议混合代理（TCP + STCP）
- 边缘计算场景的服务协调
- 故障转移和高可用性配置

## 🎯 应用场景

- **边缘计算** - CDN、边缘数据中心、5G边缘
- **多区域部署** - 全球化应用、灾备部署
- **物联网网关** - 工业物联网、智慧城市
- **混合云架构** - 云边协同、混合部署
- **服务网格** - 微服务网格、API网关集群

## 🏗️ 架构特性

### Phase 2 vs Phase 1 对比

| 特性 | Phase 1 (基础) | Phase 2 (高级) |
|------|----------------|----------------|
| **部署模式** | 单节点代理 | 多节点网格 |
| **服务发现** | 中心化注册 | 分布式发现 |
| **负载均衡** | 简单轮询 | 地理位置感知 |
| **故障处理** | 手动重连 | 自动故障转移 |
| **协议支持** | 单一协议 | 混合协议 |
| **适用场景** | 简单代理 | 企业级网格 |

## 📁 文件说明

```
04-phase2-mesh/
├── README.md                          # 本文档
├── phase2-region-mesh-config.yaml     # 区域网格服务端配置
├── phase2-multi-register-config.yaml  # 多服务注册配置
├── phase2-multi-proxy-config.yaml     # 多服务代理配置
└── run.sh                             # 一键运行脚本
```

## 🚀 快速开始

### 方法1：一键运行（推荐）
```bash
# 在 OutpostNet 根目录执行
make build
cd examples/04-phase2-mesh
./run.sh
```

### 方法2：手动运行（学习推荐）

#### 第1步：启动区域网格服务端
```bash
# 终端1
cd /path/to/OutpostNet
./build/outpostnet-server -config examples/04-phase2-mesh/phase2-region-mesh-config.yaml
```

#### 第2步：注册多个服务
```bash
# 终端2
./build/outpostnet-client -config examples/04-phase2-mesh/phase2-multi-register-config.yaml
```

#### 第3步：启动多服务代理
```bash
# 终端3
./build/outpostnet-client -config examples/04-phase2-mesh/phase2-multi-proxy-config.yaml
```

#### 第4步：测试网格功能
```bash
# 新终端 - 测试Web服务（TCP）
curl http://localhost:9080

# 测试API服务（STCP加密）
curl -k https://localhost:9081
```

## 🔧 工作原理

### 区域网格架构

```
                        OutpostNet Phase 2 区域网格
    ┌─────────────────────────────────────────────────────────────────┐
    │                     Region: East                                │
    │  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐│
    │  │   Node A    │◄────────┤   Hub Node  ├────────►│   Node B    ││
    │  │             │         │             │         │             ││
    │  │ Services:   │         │ Services:   │         │ Services:   ││
    │  │ - web-v1    │         │ - api-v2    │         │ - db-main   ││
    │  │ - cache     │         │ - gateway   │         │ - monitor   ││
    │  └─────────────┘         └─────────────┘         └─────────────┘│
    └─────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                     Region: West                                │
    │  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐│
    │  │   Node C    │◄────────┤   Hub Node  ├────────►│   Node D    ││
    │  │             │         │             │         │             ││
    │  │ Services:   │         │ Services:   │         │ Services:   ││
    │  │ - web-v2    │         │ - api-v1    │         │ - db-backup ││
    │  │ - analytics │         │ - lb        │         │ - storage   ││
    │  └─────────────┘         └─────────────┘         └─────────────┘│
    └─────────────────────────────────────────────────────────────────┘
```

### 智能路由决策

```
请求路由决策流程:
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────┐
│   客户端     │───▶│   代理节点   │───▶│   路由决策引擎   │───▶│  目标服务    │
│ (应用请求)   │    │ (地理位置A)  │    │                │    │ (最佳节点)   │
└─────────────┘    └──────────────┘    └─────────────────┘    └─────────────┘
                                                ▲
                                                │
                          ┌─────────────────────┴─────────────────────┐
                          │ 路由决策因子:                            │
                          │ 1. 地理距离 (延迟优化)                    │
                          │ 2. 服务健康状态                          │
                          │ 3. 负载均衡策略                          │
                          │ 4. 服务版本选择                          │
                          │ 5. 安全策略匹配                          │
                          └───────────────────────────────────────────┘
```

## 📋 配置详解

### 区域网格服务端配置 (phase2-region-mesh-config.yaml)
```yaml
server:
  listen_addr: "0.0.0.0:18000"
  auth_token: "phase2-mesh-token"

# Region 配置 - Phase 2 核心功能
region:
  enabled: true
  region_id: "region-east"           # 区域标识
  node_id: "node-a"                  # 节点标识
  neighbors:                         # 邻居节点配置
    - node_id: "node-b"
      address: "127.0.0.1:18001"
      region_id: "region-east"
    - node_id: "node-c"
      address: "127.0.0.1:18002"
      region_id: "region-west"
  
  # 地理位置信息
  geo_location:
    latitude: 39.9042
    longitude: 116.4074
    city: "Beijing"
    country: "CN"
    
  # 服务同步配置  
  service_sync:
    enabled: true
    sync_interval: "30s"
    max_hops: 3                      # 最大跳数
    
# 高级负载均衡
load_balancer:
  strategy: "geographic_aware"       # 地理位置感知
  health_check_enabled: true
  failover_enabled: true
  
# 多协议服务定义
services:
  # Web服务 - TCP透明代理
  - metadata:
      name: "web-service"
      namespace: "default"
      labels:
        app: "web"
        version: "v1"
        tier: "frontend"
    spec:
      selector:
        app: "web"
      ports:
        - name: "http"
          port: 80
          targetPort: 19080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "geographic_aware"
      
  # API服务 - STCP加密隧道
  - metadata:
      name: "api-service"  
      namespace: "default"
      labels:
        app: "api"
        version: "v2"
        tier: "backend"
    spec:
      selector:
        app: "api"
      ports:
        - name: "https"
          port: 443
          targetPort: 19081
          protocol: "stcp"
      proxy_mode: "stcp"
      encryption: true
      load_balancer: "least_connections"
```

### 多服务注册配置 (phase2-multi-register-config.yaml)
```yaml
client:
  server_addr: "127.0.0.1:18000"
  auth_token: "phase2-mesh-token"
  mode: "register"

# 区域信息
region:
  enabled: true
  region_id: "region-east"
  node_id: "backend-node-1"
  
  # 节点地理位置
  geo_location:
    latitude: 39.9042
    longitude: 116.4074
    city: "Beijing"
    zone: "east"

# 注册多个服务实例
workloads:
  # Web服务实例
  - metadata:
      labels:
        app: "web"
        version: "v1"
        tier: "frontend"
        node: "backend-node-1"
        zone: "east"
    spec:
      local_addr: "127.0.0.1"
      local_port: 19080
      protocol: "tcp"
      health_check:
        enabled: true
        path: "/health"
        interval: "10s"
        
  # API服务实例  
  - metadata:
      labels:
        app: "api"
        version: "v2"
        tier: "backend"
        node: "backend-node-1"
        zone: "east"
    spec:
      local_addr: "127.0.0.1"
      local_port: 19081
      protocol: "stcp"
      encryption: true
      health_check:
        enabled: true
        path: "/api/health"
        interval: "15s"
```

### 多服务代理配置 (phase2-multi-proxy-config.yaml)
```yaml
client:
  server_addr: "127.0.0.1:18000"
  auth_token: "phase2-mesh-token"
  mode: "proxy"

# 区域信息
region:
  enabled: true
  region_id: "region-east"
  node_id: "client-node-1"

# 多个代理配置
proxies:
  # Web服务代理 - TCP模式
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 9080
    proxy_mode: "tcp"
    
    # 服务选择策略
    selector:
      app: "web"
      version: "v1"                  # 只选择v1版本
    
    # 高级路由配置
    routing:
      strategy: "geographic_prefer"  # 优先选择地理位置近的
      fallback: "any_available"      # 备用策略
      max_latency: "100ms"           # 最大延迟阈值
      
  # API服务代理 - STCP加密模式
  - name: "api-proxy"
    service_name: "api-service"
    service_namespace: "default"
    local_port: 9081
    proxy_mode: "stcp"
    encryption: true
    
    # 服务选择策略
    selector:
      app: "api"
      tier: "backend"
    
    # 安全配置
    security:
      require_encryption: true
      min_tls_version: "1.3"
      
    # 高级路由配置
    routing:
      strategy: "least_latency"      # 最低延迟优先
      health_check: true
      timeout: "30s"
```

## 🌐 Phase 2 高级特性

### 1. 地理位置感知路由
```yaml
# 基于地理位置的智能路由
routing_policies:
  - name: "geo_aware"
    rules:
      - match:
          client_region: "asia-east"
        route_to:
          preferred_regions: ["asia-east", "asia-southeast"]
          max_latency: "50ms"
      - match:
          client_region: "us-west"
        route_to:
          preferred_regions: ["us-west", "us-central"]
          max_latency: "30ms"
```

### 2. 多协议混合支持
```yaml
# 同一服务支持多种协议
services:
  - name: "hybrid-service"
    protocols:
      - name: "web-interface"
        protocol: "tcp"             # Web界面用TCP
        port: 80
      - name: "api-interface"  
        protocol: "stcp"            # API用加密STCP
        port: 443
      - name: "metrics-interface"
        protocol: "udp"             # 监控数据用UDP
        port: 8125
```

### 3. 故障转移和高可用
```yaml
# 高可用配置
high_availability:
  enabled: true
  
  # 健康检查配置
  health_check:
    interval: "10s"
    timeout: "5s"
    retries: 3
    
  # 故障转移策略
  failover:
    strategy: "immediate"           # 立即切换
    backup_regions: ["region-west", "region-central"]
    
  # 服务冗余
  redundancy:
    min_replicas: 2                 # 最少2个副本
    max_replicas: 10                # 最多10个副本
    scale_threshold: 0.8            # 80%负载时扩容
```

## 🧪 测试和验证

### 1. 多协议测试
```bash
# 测试TCP Web服务
curl http://localhost:9080
curl -H "Accept: application/json" http://localhost:9080/api

# 测试STCP API服务
curl -k https://localhost:9081
curl -k -X POST https://localhost:9081/api/data -d '{"test":"data"}'
```

### 2. 地理路由测试
```bash
# 模拟不同地理位置的请求
curl -H "X-Client-Region: asia-east" http://localhost:9080
curl -H "X-Client-Region: us-west" http://localhost:9080

# 查看路由决策
curl http://localhost:18000/api/v1/routing/decisions
```

### 3. 故障转移测试
```bash
# 停止一个服务实例，测试自动故障转移
pkill -f "web-server-19080"

# 继续请求，应该自动切换到其他实例
for i in {1..10}; do curl http://localhost:9080; sleep 1; done
```

### 4. 网格状态监控
```bash
# 查看网格拓扑
curl http://localhost:18000/api/v1/mesh/topology

# 查看服务健康状态
curl http://localhost:18000/api/v1/services/health

# 查看路由统计
curl http://localhost:18000/api/v1/routing/stats
```

## 📊 监控和可观测性

### 1. 指标收集
```yaml
# 监控配置
monitoring:
  enabled: true
  
  # Prometheus指标
  prometheus:
    enabled: true
    port: 9090
    metrics:
      - "request_count"
      - "request_latency"
      - "service_health"
      - "geographic_routing"
      
  # 分布式追踪
  tracing:
    enabled: true
    jaeger_endpoint: "http://jaeger:14268/api/traces"
    
  # 日志聚合
  logging:
    level: "info"
    structured: true
    fields:
      - "region_id"
      - "node_id"
      - "service_name"
      - "routing_decision"
```

### 2. 仪表板
```bash
# 启动监控仪表板
./build/outpostnet-server -config phase2-region-mesh-config.yaml -dashboard

# 访问仪表板
open http://localhost:3000
```

## 🎯 生产部署建议

### 1. 多区域部署架构
```
Production Phase 2 Deployment:

Global Load Balancer
        │
    ┌───┴────┐
    │        │
Region-A   Region-B
    │        │
┌───┴───┐ ┌──┴────┐
Node-1  │ │ Node-3 │
Node-2  │ │ Node-4 │
└───────┘ └───────┘
```

### 2. 配置管理
```yaml
# 使用配置中心
config_source:
  type: "etcd"                      # 或consul、k8s configmap
  endpoints: ["etcd1:2379", "etcd2:2379"]
  watch: true                       # 配置热更新
  
# 证书管理
certificate_management:
  provider: "cert-manager"          # 或vault、acme
  auto_renewal: true
  renewal_threshold: "30d"
```

### 3. 安全加固
```yaml
# 生产安全配置
security:
  # mTLS认证
  mutual_tls:
    enabled: true
    cert_rotation: "24h"
    
  # 网络策略
  network_policies:
    - allow_regions: ["trusted-east", "trusted-west"]
    - deny_regions: ["untrusted-*"]
    
  # 访问控制
  rbac:
    enabled: true
    policies: "config/rbac-policies.yaml"
```

## 🎓 知识点总结

通过这个示例，你应该理解了：

1. **分布式网格架构**：
   - 多节点协调和服务发现
   - 地理位置感知的智能路由
   - 跨区域的服务网格

2. **高级功能特性**：
   - 混合协议支持（TCP + STCP + UDP）
   - 自动故障转移和高可用
   - 企业级监控和可观测性

3. **生产级特性**：
   - 配置热更新
   - 证书自动管理
   - 安全策略和访问控制

## ➡️ 完整学习路径

恭喜！你已经完成了 OutpostNet 的完整学习路径：

✅ [示例1：基础TCP代理](../01-basic-tcp/README.md) - 掌握基础概念  
✅ [示例2：UDP代理](../02-udp-proxy/README.md) - 理解无连接协议  
✅ [示例3：STCP加密隧道](../03-stcp-tunnel/README.md) - 学习安全传输  
✅ **示例4：Phase 2区域网格** - 掌握企业级功能

现在你可以：
1. **设计生产架构** - 规划多区域部署
2. **实施安全策略** - 配置加密和认证
3. **监控和运维** - 建立可观测性体系
4. **故障处理** - 实现高可用和灾备

## 💡 下一步行动

1. **评估现有架构** - 分析当前网络代理需求
2. **制定迁移计划** - 从传统方案迁移到OutpostNet  
3. **试点部署** - 在测试环境验证功能
4. **生产推广** - 逐步推广到生产环境
5. **监控优化** - 持续监控和性能优化

🎉 **恭喜你成为 OutpostNet 专家！**