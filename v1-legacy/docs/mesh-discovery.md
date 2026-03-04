# OutpostNet 集群节点发现功能

## 概述

OutpostNet 实现了灵活的集群节点发现机制，支持多种发现方法，能够自动发现、管理和监控集群中的节点状态。这是构建分布式服务网格的基础功能。

## 核心特性

### 多种发现方法
- **静态发现 (Static)**: 通过配置文件预定义节点列表
- **环境变量发现 (ENV)**: 从环境变量动态读取节点信息
- **DNS发现 (DNS)**: 通过DNS查询发现节点
- **多播发现 (Multicast)**: 通过UDP多播自动发现网络中的节点

### 节点生命周期管理
- **自动加入**: 新节点启动时自动加入集群
- **健康检查**: 定期检查节点连通性和状态
- **故障检测**: 自动检测失败节点并从集群中移除
- **状态同步**: 实时同步节点状态变化

### 事件驱动架构
- **节点事件**: 支持节点加入、离开、更新、失败等事件
- **事件监听**: 支持注册事件监听器响应节点变化
- **异步处理**: 事件处理采用异步模式，不阻塞主流程

## 架构设计

```
                    DiscoveryManager
                    ┌─────────────────┐
                    │   发现管理器     │
                    │                 │
                    │  ┌───────────┐  │
            ┌──────────│Static     │  │
            │       │  │Discovery  │  │
            │       │  └───────────┘  │
            │       │  ┌───────────┐  │    NodeEventListener
            │       │  │Env        │  │         Interface
            │       │  │Discovery  │  │    ┌─────────────┐
发现方法 ────┤       │  └───────────┘  ├───►│   事件监听   │
            │       │  ┌───────────┐  │    │  - Joined   │
            │       │  │DNS        │  │    │  - Left     │
            │       │  │Discovery  │  │    │  - Updated  │
            │       │  └───────────┘  │    │  - Failed   │
            │       │  ┌───────────┐  │    └─────────────┘
            └──────────│Multicast  │  │
                    │  │Discovery  │  │
                    │  └───────────┘  │
                    └─────────────────┘
```

## 配置说明

### 基本配置

```yaml
node:
  discovery:
    # 启用的发现方法
    methods: ["static", "env", "dns", "multicast"]
    
    # 发现间隔
    discovery_interval: "30s"
    
    # 节点超时时间
    node_timeout: "5m"
    
    # 健康检查间隔
    health_check_interval: "30s"
```

### 静态发现配置

```yaml
node:
  discovery:
    static_nodes:
      - id: "server-1"
        address: "192.168.1.100:7629"
        labels:
          zone: "east"
          role: "server"
        priority: 1
      
      - id: "server-2"
        address: "192.168.1.101:7629"
        labels:
          zone: "west"
          role: "server"
        priority: 2
```

### 环境变量发现配置

```yaml
node:
  discovery:
    env:
      enabled: true
      nodes_env_var: "OUTPOSTNET_NODES"
      cluster_env_var: "OUTPOSTNET_CLUSTER_NODES"
      separator: ","
```

环境变量格式：
```bash
# 简单节点列表
export OUTPOSTNET_NODES="192.168.1.10:7629,192.168.1.11:7629"

# JSON格式的集群节点
export OUTPOSTNET_CLUSTER_NODES='[
  {"id":"node-1","address":"192.168.1.10:7629","labels":{"zone":"east"}},
  {"id":"node-2","address":"192.168.1.11:7629","labels":{"zone":"west"}}
]'
```

### DNS发现配置

```yaml
node:
  discovery:
    dns:
      enabled: true
      domain: "outpostnet.local"
      servers: ["8.8.8.8:53"]
      interval: "60s"
      record_type: "A"
```

### 多播发现配置

```yaml
node:
  discovery:
    multicast:
      enabled: true
      group: "224.0.0.1"
      port: 7630
      interface: ""  # 默认接口
      ttl: 1
      interval: "30s"
```

## 使用示例

### 1. 启动支持发现的集群节点

```bash
# 启动第一个节点（种子节点）
./outpostnet-server -config examples/mesh-discovery-config.yaml

# 启动其他节点（会自动发现第一个节点）
export OUTPOSTNET_NODES="192.168.1.100:7629"
./outpostnet-server -config node-2-config.yaml
```

### 2. 编程接口使用

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/outpostos/OutpostNet/pkg/mesh"
)

func main() {
    // 创建本地节点
    config := mesh.DefaultNodeConfig()
    config.NodeID = "demo-node"
    config.Labels = map[string]string{
        "app": "demo",
        "env": "dev",
    }
    
    // 配置发现
    config.Discovery = &mesh.DiscoveryConfig{
        Methods: []string{"static", "env"},
        StaticNodes: []mesh.StaticNodeConfig{
            {
                ID:      "peer-1",
                Address: "192.168.1.100:7629",
                Labels:  map[string]string{"role": "server"},
            },
        },
        EnvConfig: mesh.EnvConfig{
            Enabled:     true,
            NodesEnvVar: "OUTPOSTNET_NODES",
            Separator:   ",",
        },
        DiscoveryInterval:   30 * time.Second,
        NodeTimeout:         5 * time.Minute,
        HealthCheckInterval: 30 * time.Second,
    }
    
    // 创建发现管理器
    localNode := &mesh.LocalNode{
        Node: &mesh.Node{
            ID:   config.NodeID,
            Addr: "127.0.0.1:7629",
            Labels: config.Labels,
        },
    }
    
    discoveryManager, err := mesh.NewDiscoveryManager(config.Discovery, localNode)
    if err != nil {
        log.Fatal(err)
    }
    
    // 注册事件监听器
    discoveryManager.AddEventListener(&NodeEventHandler{})
    
    // 启动发现
    if err := discoveryManager.Start(); err != nil {
        log.Fatal(err)
    }
    defer discoveryManager.Stop()
    
    // 运行应用逻辑
    select {}
}

// 自定义事件处理器
type NodeEventHandler struct{}

func (h *NodeEventHandler) OnNodeJoined(node *mesh.Node) {
    log.Printf("Node joined: %s (%s)", node.ID, node.Addr)
}

func (h *NodeEventHandler) OnNodeLeft(node *mesh.Node) {
    log.Printf("Node left: %s (%s)", node.ID, node.Addr)
}

func (h *NodeEventHandler) OnNodeUpdated(node *mesh.Node) {
    log.Printf("Node updated: %s (%s)", node.ID, node.Addr)
}

func (h *NodeEventHandler) OnNodeFailed(node *mesh.Node) {
    log.Printf("Node failed: %s (%s)", node.ID, node.Addr)
}
```

## 最佳实践

### 1. 选择合适的发现方法

- **静态发现**: 适用于稳定的生产环境，节点数量固定
- **环境变量发现**: 适用于容器化部署，通过环境变量注入节点信息
- **DNS发现**: 适用于有DNS基础设施的环境
- **多播发现**: 适用于同一网段的自动发现，常用于开发和测试

### 2. 配置合理的超时时间

```yaml
discovery_interval: "30s"      # 发现间隔不宜过短，避免网络负载
node_timeout: "5m"             # 节点超时应考虑网络延迟和负载
health_check_interval: "30s"   # 健康检查频率平衡及时性和性能
```

### 3. 使用标签进行节点分类

```yaml
static_nodes:
  - id: "web-server-1"
    address: "192.168.1.100:7629"
    labels:
      tier: "frontend"      # 应用层级
      zone: "east"          # 可用区
      role: "server"        # 节点角色
      priority: "high"      # 优先级
```

### 4. 安全考虑

- 在生产环境中启用TLS加密
- 使用防火墙限制多播发现的网络范围
- 定期更新认证token
- 监控异常的节点加入和离开行为

## 故障排除

### 常见问题

1. **节点无法被发现**
   - 检查网络连通性
   - 验证配置文件中的地址和端口
   - 确认防火墙设置

2. **环境变量发现失败**
   - 检查环境变量是否正确设置
   - 验证JSON格式是否正确
   - 确认分隔符配置

3. **节点频繁上下线**
   - 调整 `node_timeout` 和 `health_check_interval`
   - 检查网络稳定性
   - 监控系统资源使用情况

### 调试方法

启用详细日志：
```bash
export OUTPOSTNET_LOG_LEVEL=debug
./outpostnet-server -config config.yaml
```

查看节点状态：
```bash
# 通过API查看集群状态
curl http://localhost:9090/api/v1/cluster/nodes
```

## 后续开发

下一步将实现：
1. **服务数据同步** (Task 5.2) - 通过Gossip协议同步服务信息
2. **跨节点请求转发** (Task 5.3) - 支持请求在节点间转发
3. **地理位置感知** (Task 6) - 基于地理位置的负载均衡

## 相关文档

- [OutpostNet 架构设计](./architecture.md)
- [集群服务协同](./cluster-coordination.md)
- [负载均衡算法](./load-balancing.md)