# Story S38: 跨集群服务发现

**Epic:** Epic 8 — 跨集群联邦
**Status:** Done

## 目标
实现按需的跨集群服务发现机制，补充 FederationManager 的定期批量同步，提供实时查询和缓存能力。

## 内容
- `ServiceCacheEntry` 结构体：ClusterID、Endpoints、CachedAt、TTL，`IsExpired()` 方法判断过期
- `DiscoveryConfig` 配置：CacheTTL(默认30s)、QueryTimeout(默认5s)、DialTimeout(默认5s)
- `DiscoveryMetrics` 计数器：QueriesSent、QueriesReceived、QueryErrors、CacheHits、CacheMisses
- `ServiceWatcher` 回调类型：`func(serviceName string, endpoints []FederatedEndpoint)`
- `ServiceDiscovery` 结构体：持有 FederationManager 引用、cache map（key="clusterID:serviceName"）、watchers map
- `Discover(ctx, serviceName)` 两级查找：
  1. 检查 FederationState 同步状态（FederationManager 定期同步的结果）
  2. 检查本地缓存
  3. Cache miss 时并行查询所有健康集群（每个集群一个 goroutine），缓存结果，通知 watchers
- `Watch(serviceName, watcher)`: 注册服务端点变更观察者
- `HandleServiceQuery(conn)`: 处理远程集群的入站查询，从 localManager 查找服务并响应
- `queryCluster()`: 通过 federation dialFederation 发送 fedMsgQuery 查询远程集群
- `Start(ctx)`: 启动缓存维护循环（按 CacheTTL 周期清理过期条目）
- `Stop()`: 优雅关闭
- `CacheSize()`: 返回缓存条目数（测试用）
- 发现线路协议（magic=0xBB, msgType=0x05）：查询格式 serviceNameLen(2B) + serviceName(变长)；响应复用 writeSyncPayload/readSyncPayload 格式
- 缓存管理：`getCached()` 遍历所有缓存条目匹配服务名、`setCache()` 写入、`evictExpired()` 定期清理

## 验收标准
- 11 个测试函数，覆盖以下场景：
- `TestServiceCacheEntry_Expiry`: 缓存条目过期判断
- `TestDiscoveryConfig_Defaults`: 默认配置值验证
- `TestServiceDiscovery_DiscoverFromFederationState`: 从 FederationState 同步数据发现服务
- `TestServiceDiscovery_DiscoverNotFound`: 服务不存在错误
- `TestServiceDiscovery_OnDemandQuery`: 按需查询远程集群
- `TestServiceDiscovery_CacheEviction`: 缓存过期清理
- `TestServiceDiscovery_Watch`: 观察者通知机制
- `TestServiceDiscovery_QueryWireProtocol`: 查询线路协议编解码
- `TestServiceDiscovery_ResponseWireProtocol`: 响应线路协议编解码
- `TestServiceDiscovery_ConcurrentDiscover`: 并发发现安全
- `TestServiceDiscovery_StartStop`: 启动与关闭

## 依赖
- `pkg/mesh.FederationManager`: 联邦管理器（dialFederation、State()）
- `pkg/mesh.FederationState`: 联邦状态查询
- `pkg/mesh.Manager`: 本地集群管理器（通过 FederationManager.localManager）
- `encoding/binary`: 线路协议编解码
- `sync/atomic`: 原子计数器
- `log/slog`: 结构化日志

## 代码位置
- `pkg/mesh/discovery.go`
- `pkg/mesh/discovery_test.go`
