# Story S32: 网络拓扑距离计算引擎

**Epic:** Epic 6 — 负载均衡引擎
**Status:** Done

## 目标
实现基于网络拓扑图的站点间距离计算，使用 BFS 最短路径算法，并提供带 TTL 的缓存机制。

## 内容
- `DistanceCalculator` 结构体：topology（邻接表 `map[string][]string`）、cache（`map[distancePair]entry`，带 TTL 默认 5 分钟）
- `distancePair` 内部类型：from/to 字符串对，作为缓存 key
- `entry` 内部类型：distance 值 + updatedAt 时间戳
- `NewDistanceCalculator(cacheTTL)`: 创建实例，cacheTTL=0 时默认 5 分钟
- `SetTopology(adjacency)`: 设置完整拓扑图，自动补全双向链接，清空缓存
- `AddLink(siteA, siteB)`: 添加单条双向链接，去重，清空全部缓存
- `Distance(from, to)`: BFS 最短路径计算跳数，同站点=0，空字符串=-1，不可达=-1；双检锁（Double-checked locking）缓存模式
- `EstimateLatency(from, to)`: 延迟估算模型 2ms base + hops * 10ms/hop，不可达返回 -1，同站点返回 0
- `ClearCache()`: 手动清除所有缓存
- `bfs(source, target)`: 内部 BFS 实现，需持有写锁调用，缓存双向结果
- `contains()`: 辅助函数，切片包含检查
- 线程安全：`sync.RWMutex` 保护 topology 和 cache

## 验收标准
- 19 个测试函数，覆盖以下场景：
- `TestNewDistanceCalculator`: 构造函数及默认 TTL
- `TestDistance_SameSite`: 同站点距离为 0
- `TestDistance_EmptySite`: 空字符串返回 -1
- `TestDistance_DirectNeighbors`: 直接相邻站点距离为 1
- `TestDistance_MultiHop`: 多跳路径计算正确性
- `TestDistance_NotConnected`: 不连通站点返回 -1
- `TestDistance_UnknownSite`: 拓扑中不存在的站点返回 -1
- `TestDistance_Caching`: 缓存命中验证
- `TestDistance_CacheExpiry`: 缓存 TTL 过期后重新计算
- `TestDistance_BidirectionalCache`: 双向缓存（A->B 同时缓存 B->A）
- `TestSetTopology_ClearsCache`: SetTopology 清空缓存
- `TestSetTopology_Bidirectional`: 自动双向链接补全
- `TestAddLink`: 添加链接功能
- `TestAddLink_NoDuplicates`: 重复添加不产生冗余
- `TestClearCache`: 手动清缓存
- `TestEstimateLatency`: 延迟估算公式验证
- `TestEstimateLatency_Unreachable`: 不可达返回 -1
- `TestDistance_ComplexTopology`: 6 节点复杂拓扑测试
- `TestDistance_ConcurrentAccess`: 并发安全（50 goroutine）

## 依赖
- `sync`: RWMutex 线程安全
- `time`: 缓存 TTL 管理

## 代码位置
- `pkg/telemetry/distance.go`
- `pkg/telemetry/distance_test.go`
