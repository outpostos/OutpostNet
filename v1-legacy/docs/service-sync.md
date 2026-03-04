# OutpostNet 集群服务数据同步

## 概述

OutpostNet 实现了基于 Gossip 协议的分布式服务数据同步机制，确保集群中所有节点的服务信息保持一致。这是构建分布式服务网格的核心功能，支持服务的自动发现、故障恢复和负载均衡。

## 核心特性

### 分布式同步机制
- **Gossip 协议**: 采用流行病传播模型，确保信息在集群中快速传播
- **增量同步**: 只传播变化的数据，减少网络开销
- **全量同步**: 周期性全量同步，确保数据一致性
- **冲突解决**: 多种冲突解决策略，处理并发更新

### 数据管理
- **TTL 管理**: 自动清理过期数据，防止内存泄漏  
- **压缩传输**: 大数据包自动压缩，优化网络传输
- **版本控制**: 数据版本管理，支持有序更新
- **校验和验证**: 数据完整性检查，防止传输错误

### 容错机制
- **故障检测**: 自动检测节点故障和网络分区
- **自动重传**: 失败消息自动重试传输
- **多路径传播**: 通过多个节点传播，提高可靠性
- **分区恢复**: 网络分区恢复后自动同步

## 架构设计

```
                    ServiceSyncManager
                    ┌─────────────────────┐
                    │   同步管理器         │
                    │                     │
                    │  ┌───────────────┐  │
    本地服务 ────────────│  全量同步     │  │
                    │  │  (30s)       │  │
                    │  └───────────────┘  │
                    │  ┌───────────────┐  │     Gossip Messages
                    │  │  增量同步     │  ├────────────────┐
                    │  │  (检测变化)   │  │                │
                    │  └───────────────┘  │                │
                    │  ┌───────────────┐  │                ▼
                    │  │  Gossip传播   │  │         ┌─────────────┐
                    │  │  (10s)       │  │         │   远程节点   │
                    │  └───────────────┘  │         │             │
                    │  ┌───────────────┐  │         │ ┌─────────┐ │
                    │  │  数据清理     │  │         │ │ 接收处理 │ │
                    │  │  (5min)      │  │         │ │ 冲突解决 │ │
                    │  └───────────────┘  │         │ │ 本地应用 │ │
                    └─────────────────────┘         │ └─────────┘ │
                                                    └─────────────┘
```

## 同步数据模型

### SyncEntry 结构
```go
type SyncEntry struct {
    ID          string                 // 唯一标识
    Type        SyncEntryType          // 条目类型 (service/endpoint/node/config)
    NodeID      string                 // 来源节点ID
    Data        map[string]interface{} // 序列化数据
    Version     uint64                 // 版本号
    Timestamp   time.Time              // 时间戳
    TTL         time.Duration          // 生存时间
    Checksum    string                 // 校验和
}
```

### 数据类型
- **Service**: 服务定义数据
- **Endpoint**: 端点实例数据  
- **Node**: 节点状态数据
- **Config**: 配置变更数据

### 消息格式
```go
type SyncGossipMessage struct {
    MessageID   string                // 消息ID
    Type        SyncGossipMessageType // 消息类型
    SenderID    string                // 发送者ID
    Timestamp   time.Time             // 发送时间
    Entries     []*SyncEntry          // 同步条目
    Version     uint64                // 同步版本
    Compressed  bool                  // 是否压缩
}
```

## 配置说明

### 基本配置
```yaml
sync:
  # 同步间隔配置
  sync_interval: "30s"         # 全量同步间隔
  gossip_interval: "10s"       # Gossip传播间隔  
  
  # Gossip参数
  gossip_fanout: 3             # 每次传播目标节点数
  gossip_retransmit_mult: 4    # 重传倍数
  
  # 数据管理
  max_sync_entries: 10000      # 最大条目数
  entry_ttl: "10m"             # 条目TTL
  conflict_resolution: "timestamp_wins"  # 冲突解决策略
  
  # 压缩配置
  enable_compression: true      # 启用压缩
  compression_threshold: 1024   # 压缩阈值(字节)
```

### 冲突解决策略
- **timestamp_wins**: 时间戳优先（默认）
- **version_wins**: 版本号优先
- **node_priority**: 节点优先级决定

### 完整配置示例
```yaml
node:
  sync:
    sync_interval: "30s"
    gossip_interval: "10s" 
    gossip_fanout: 3
    gossip_retransmit_mult: 4
    max_sync_entries: 10000
    entry_ttl: "10m"
    conflict_resolution: "timestamp_wins"
    enable_compression: true
    compression_threshold: 1024
```

## 使用示例

### 1. 编程接口使用

```go
package main

import (
    "log"
    "time"
    
    "github.com/outpostos/OutpostNet/pkg/mesh"
    "github.com/outpostos/OutpostNet/pkg/types"
)

func main() {
    // 创建服务注册表
    registry := NewServiceRegistry()
    
    // 创建本地节点
    localNode := &mesh.LocalNode{
        Node: &mesh.Node{
            ID:   "node-1",
            Addr: "192.168.1.100:7629",
            Labels: map[string]string{
                "role": "server",
                "zone": "east",
            },
        },
    }
    
    // 创建集群
    cluster := &mesh.MeshCluster{}
    
    // 配置同步管理器
    syncConfig := &mesh.SyncConfig{
        SyncInterval:       30 * time.Second,
        GossipInterval:     10 * time.Second,
        GossipFanout:       3,
        MaxSyncEntries:     10000,
        ConflictResolution: "timestamp_wins",
    }
    
    // 创建同步管理器
    syncManager, err := mesh.NewServiceSyncManager(syncConfig, localNode, cluster, registry)
    if err != nil {
        log.Fatal(err)
    }
    
    // 添加事件监听器
    syncManager.AddListener(&ServiceSyncHandler{})
    
    // 启动同步
    if err := syncManager.Start(); err != nil {
        log.Fatal(err)
    }
    defer syncManager.Stop()
    
    // 运行应用逻辑
    select {}
}

// 自定义同步事件处理器
type ServiceSyncHandler struct{}

func (h *ServiceSyncHandler) OnServiceSynced(service *types.Service) {
    log.Printf("Service synced: %s/%s", service.Metadata.Namespace, service.Metadata.Name)
}

func (h *ServiceSyncHandler) OnEndpointSynced(endpoint *types.Endpoint) {
    log.Printf("Endpoint synced: %s", endpoint.ID)
}

func (h *ServiceSyncHandler) OnSyncConflict(local, remote *mesh.SyncEntry) {
    log.Printf("Sync conflict: %s vs %s", local.ID, remote.ID)
}

func (h *ServiceSyncHandler) OnSyncError(err error) {
    log.Printf("Sync error: %v", err)
}
```

### 2. 查看同步状态

```go
// 获取同步统计信息
stats := syncManager.GetSyncStats()
fmt.Printf("同步统计:\n")
fmt.Printf("  总条目数: %d\n", stats.TotalEntries)
fmt.Printf("  服务条目: %d\n", stats.ServiceEntries)
fmt.Printf("  端点条目: %d\n", stats.EndpointEntries)
fmt.Printf("  同步版本: %d\n", stats.SyncVersion)
```

### 3. 手动触发同步

```go
// 创建自定义同步条目
entry := &mesh.SyncEntry{
    ID:        "custom-entry",
    Type:      mesh.SyncEntryService,
    NodeID:    localNode.ID,
    Data:      serviceData,
    Version:   1,
    Timestamp: time.Now(),
    TTL:       10 * time.Minute,
}

// 处理同步消息
message := &mesh.SyncGossipMessage{
    MessageID: "msg-123",
    Type:      mesh.SyncGossipMessageSync,
    SenderID:  "remote-node",
    Entries:   []*mesh.SyncEntry{entry},
    Version:   1,
}

if err := syncManager.ProcessGossipMessage(message); err != nil {
    log.Printf("处理同步消息失败: %v", err)
}
```

## 工作流程

### 1. 初始化阶段
```
1. ServiceSyncManager 启动
2. 启动三个主要循环:
   - syncLoop: 定期全量/增量同步 (30s)
   - gossipLoop: Gossip 消息传播 (10s)  
   - cleanupLoop: 数据清理 (5min)
3. 注册事件监听器
4. 开始监听本地服务变化
```

### 2. 同步传播阶段
```
1. 检测本地服务/端点变化
2. 创建 SyncEntry 条目
3. 选择 Gossip 目标节点 (fanout=3)
4. 创建 SyncGossipMessage
5. 发送到目标节点
6. 目标节点接收并处理
7. 应用到本地注册表
8. 继续向其他节点传播
```

### 3. 冲突解决阶段
```
1. 接收到远程同步条目
2. 检查本地是否存在相同ID条目
3. 比较版本/时间戳
4. 根据冲突解决策略决定:
   - 接受远程条目 (更新本地)
   - 保留本地条目 (忽略远程)
   - 触发冲突事件 (需要手动处理)
5. 更新同步版本号
6. 通知事件监听器
```

### 4. 数据清理阶段
```
1. 检查条目TTL过期时间
2. 删除过期条目
3. 限制条目总数 (FIFO清理)
4. 压缩内存使用
5. 更新统计信息
```

## 性能优化

### 1. 网络优化
- **批量传输**: 将多个条目打包在一个消息中
- **压缩传输**: 大于1KB的消息自动压缩  
- **增量同步**: 只传输变化的数据
- **智能选择**: 优先选择网络延迟低的节点

### 2. 内存优化
- **TTL管理**: 自动清理过期数据
- **LRU淘汰**: 超出限制时淘汰最旧数据
- **引用共享**: 相同数据共享存储
- **垃圾回收**: 定期清理无用对象

### 3. CPU优化
- **异步处理**: 同步处理不阻塞主流程
- **批量操作**: 减少系统调用次数
- **缓存计算**: 缓存校验和等计算结果
- **并发控制**: 合理控制并发度

## 监控指标

### 关键指标
- **同步延迟**: 数据传播到所有节点的时间
- **冲突率**: 同步冲突发生的频率
- **传输效率**: 网络带宽利用率
- **内存使用**: 同步数据占用的内存
- **错误率**: 同步失败的比例

### 监控配置
```yaml
monitoring:
  sync_metrics:
    enabled: true
    interval: "30s"
    metrics:
      - "sync_entries_total"
      - "sync_conflicts_total" 
      - "gossip_messages_sent"
      - "gossip_messages_received"
      - "sync_latency_seconds"
```

## 故障排除

### 常见问题

1. **同步延迟过高**
   - 检查网络连通性和延迟
   - 调整 gossip_interval 和 gossip_fanout
   - 减少 entry_ttl 降低数据量

2. **频繁同步冲突**
   - 检查节点时间同步
   - 调整冲突解决策略
   - 增加 gossip_retransmit_mult

3. **内存使用过高**
   - 降低 max_sync_entries 限制
   - 减少 entry_ttl 时间
   - 启用压缩传输

### 调试方法

开启详细日志：
```bash
export OUTPOSTNET_SYNC_LOG_LEVEL=debug
./outpostnet-server -config config.yaml
```

查看同步状态：
```bash
# 通过API查看同步统计
curl http://localhost:9090/api/v1/sync/stats

# 查看同步条目
curl http://localhost:9090/api/v1/sync/entries
```

## 安全考虑

### 数据保护
- **消息签名**: 防止消息被篡改
- **节点认证**: 只接受可信节点的消息
- **传输加密**: 敏感数据加密传输
- **访问控制**: 限制同步数据的访问权限

### 攻击防护
- **速率限制**: 防止同步消息洪水攻击
- **消息去重**: 防止重放攻击
- **大小限制**: 限制消息大小防止DoS
- **源验证**: 验证消息来源合法性

## 后续开发

下一步将实现：
1. **跨节点请求转发** (Task 5.3) - 基于同步的服务信息进行请求路由
2. **地理位置感知负载均衡** (Task 6) - 结合地理信息优化服务选择
3. **跨集群联邦** (Task 7) - 扩展到跨集群的服务同步

## 相关文档

- [OutpostNet 架构设计](./architecture.md)
- [集群节点发现](./mesh-discovery.md)  
- [负载均衡算法](./load-balancing.md)
- [跨节点请求转发](./request-forwarding.md)