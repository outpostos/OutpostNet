# OutpostNet Mesh Cluster 示例

这个示例展示了如何使用 OutpostNet 的最新集群功能，包括：
- 节点自动发现
- 服务数据同步
- 分布式服务网格

## 功能特性

### Phase 2 新功能
- **节点发现**: 支持静态、环境变量、DNS、多播等多种发现方法
- **服务同步**: 基于 Gossip 协议的分布式服务数据同步
- **集群管理**: 自动节点加入/离开、健康检查、故障恢复
- **事件驱动**: 完整的事件监听和处理机制

## 架构图

```
    集群节点 A                    集群节点 B                    集群节点 C
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│  OutpostNet     │          │  OutpostNet     │          │  OutpostNet     │
│  Server         │          │  Server         │          │  Server         │
│                 │          │                 │          │                 │
│ ┌─────────────┐ │          │ ┌─────────────┐ │          │ ┌─────────────┐ │
│ │   Node      │ │◄────────►│ │   Node      │ │◄────────►│ │   Node      │ │
│ │ Discovery   │ │          │ │ Discovery   │ │          │ │ Discovery   │ │
│ └─────────────┘ │          │ └─────────────┘ │          │ └─────────────┘ │
│ ┌─────────────┐ │          │ ┌─────────────┐ │          │ ┌─────────────┐ │
│ │  Service    │ │◄────────►│ │  Service    │ │◄────────►│ │  Service    │ │
│ │   Sync      │ │          │ │   Sync      │ │          │ │   Sync      │ │
│ └─────────────┘ │          │ └─────────────┘ │          │ └─────────────┘ │
│                 │          │                 │          │                 │
│   Services:     │          │   Services:     │          │   Services:     │
│   - web-api     │          │   - db-service  │          │   - cache-svc   │
│   - user-svc    │          │   - auth-svc    │          │   - log-svc     │
└─────────────────┘          └─────────────────┘          └─────────────────┘
         │                            │                            │
         │                            │                            │
    ┌─────────┐                 ┌─────────┐                 ┌─────────┐
    │Web Apps │                 │Database │                 │ Cache & │
    │Frontend │                 │Backend  │                 │ Logging │
    └─────────┘                 └─────────┘                 └─────────┘
```

## 快速开始

### 1. 启动集群节点

```bash
# 启动节点 A (种子节点)
./outpostnet-server -config cluster-node-a.yaml

# 启动节点 B
./outpostnet-server -config cluster-node-b.yaml  

# 启动节点 C
./outpostnet-server -config cluster-node-c.yaml
```

### 2. 注册后端服务

```bash
# 在节点 A 注册 Web API 服务
./outpostnet-client -config web-api-register.yaml

# 在节点 B 注册数据库服务  
./outpostnet-client -config database-register.yaml

# 在节点 C 注册缓存服务
./outpostnet-client -config cache-register.yaml
```

### 3. 创建服务代理

```bash
# 创建访问数据库的代理
./outpostnet-client -config database-proxy.yaml

# 创建访问缓存的代理
./outpostnet-client -config cache-proxy.yaml
```

### 4. 验证集群状态

```bash
# 查看集群节点状态
curl http://localhost:9090/api/v1/cluster/nodes

# 查看服务同步状态
curl http://localhost:9090/api/v1/sync/stats

# 查看发现的服务
curl http://localhost:9090/api/v1/services
```

## 配置文件说明

### 集群节点配置
- `cluster-node-a.yaml`: 种子节点配置
- `cluster-node-b.yaml`: 普通节点配置  
- `cluster-node-c.yaml`: 普通节点配置

### 服务注册配置
- `web-api-register.yaml`: Web API 服务注册
- `database-register.yaml`: 数据库服务注册
- `cache-register.yaml`: 缓存服务注册

### 代理配置
- `database-proxy.yaml`: 数据库访问代理
- `cache-proxy.yaml`: 缓存访问代理

## 高级特性

### 环境变量发现
```bash
# 通过环境变量发现其他节点
export OUTPOSTNET_NODES="192.168.1.100:7629,192.168.1.101:7629"
export OUTPOSTNET_CLUSTER_NODES='[
  {"id":"node-1","address":"192.168.1.100:7629","labels":{"zone":"east"}},
  {"id":"node-2","address":"192.168.1.101:7629","labels":{"zone":"west"}}
]'

./outpostnet-server -config cluster-env-discovery.yaml
```

### 多播发现
```bash
# 启用多播自动发现
./outpostnet-server -config cluster-multicast-discovery.yaml
```

### DNS 发现
```bash
# 通过 DNS 发现集群节点
./outpostnet-server -config cluster-dns-discovery.yaml
```

## 监控和调试

### 查看节点状态
```bash
# 查看本地节点信息
curl http://localhost:9090/api/v1/node/info

# 查看集群统计
curl http://localhost:9090/api/v1/cluster/stats
```

### 查看同步状态
```bash
# 查看同步条目
curl http://localhost:9090/api/v1/sync/entries

# 查看 Gossip 消息统计
curl http://localhost:9090/api/v1/sync/gossip/stats
```

### 调试日志
```bash
# 启用详细日志
export OUTPOSTNET_LOG_LEVEL=debug
export OUTPOSTNET_DISCOVERY_LOG=true
export OUTPOSTNET_SYNC_LOG=true

./outpostnet-server -config cluster-node-a.yaml
```

## 故障模拟

### 网络分区测试
```bash
# 模拟节点 B 网络分区
sudo iptables -A INPUT -s 192.168.1.101 -j DROP
sudo iptables -A OUTPUT -d 192.168.1.101 -j DROP

# 观察集群自动恢复
# ... 等待一段时间

# 恢复网络连接
sudo iptables -D INPUT -s 192.168.1.101 -j DROP  
sudo iptables -D OUTPUT -d 192.168.1.101 -j DROP
```

### 节点故障恢复
```bash
# 停止节点 C
pkill -f "cluster-node-c.yaml"

# 观察其他节点的故障检测
curl http://localhost:9090/api/v1/cluster/nodes

# 重启节点 C
./outpostnet-server -config cluster-node-c.yaml

# 观察节点重新加入集群
```

## 性能测试

### 服务同步延迟测试
```bash
# 运行同步延迟测试
./test-sync-latency.sh

# 运行网络分区恢复测试  
./test-partition-recovery.sh

# 运行大规模节点测试
./test-scale.sh
```

## 配置优化

### 高吞吐量场景
```yaml
node:
  discovery:
    discovery_interval: "10s"     # 加快发现
    health_check_interval: "15s"  # 加快健康检查
  sync:
    sync_interval: "15s"          # 加快同步
    gossip_interval: "5s"         # 加快 Gossip
    gossip_fanout: 5              # 增加传播扇出
```

### 低延迟场景
```yaml
node:
  sync:
    gossip_interval: "2s"         # 高频 Gossip
    conflict_resolution: "version_wins"  # 快速冲突解决
    enable_compression: false     # 禁用压缩减少CPU
```

### 大规模集群场景
```yaml
node:
  discovery:
    max_sync_entries: 50000       # 增加条目限制
  sync:
    gossip_fanout: 3              # 控制网络负载
    enable_compression: true      # 启用压缩节省带宽
    compression_threshold: 512    # 降低压缩阈值
```

## 相关文档

- [集群节点发现](../../docs/mesh-discovery.md)
- [服务数据同步](../../docs/service-sync.md)  
- [OutpostNet 架构](../../docs/architecture.md)
- [配置参考](../../docs/configuration.md)