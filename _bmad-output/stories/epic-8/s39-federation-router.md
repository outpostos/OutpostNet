# Story S39: 联邦路由器

**Epic:** Epic 8 — 跨集群联邦
**Status:** Done

## 目标
实现统一的三层服务路由器，整合本地节点、集群内和跨集群联邦的服务查找路径。

## 内容
- `FederationRouter` 结构体：集成 Manager、FederationManager、ServiceDiscovery、LeaderElection 四个组件
- `RouterMetrics` 计数器：LocalHits、ClusterHits、FederationHits、RoutingErrors、TotalRequests（均为 atomic.Int64）
- `NewFederationRouter()`: 构造函数，接受四个组件（均可为 nil）
- `Route(ctx, serviceName)` 三层路由算法：
  - Tier 1（本地节点）：通过 Manager.IsLocal() 检查 + handler.ConnectLocalService() 连接
  - Tier 2（集群内节点）：通过 Manager.Forward() 转发到集群内其他节点
  - Tier 3（联邦集群）：通过 routeFederated() 转发到远程集群，受 LeaderElection 网关限制
  - 各层失败后逐级降级，全部失败返回 ErrServiceNotFound
- `routeFederated()` 联邦路由：
  - 如果配置了 LeaderElection 且当前节点非 Leader，直接返回 ErrServiceNotFound（仅 Leader 处理跨集群流量）
  - 先检查 FederationState 同步数据，有则通过 ForwardFederated 转发
  - 无则通过 ServiceDiscovery.Discover 按需发现，发现后通过 ForwardToCluster 转发
- `RouteInfo` 结构体：Tier("local"/"cluster"/"federation")、ClusterID、NodeID、Endpoints，用于诊断
- `Lookup(serviceName)` 诊断查询：返回所有层级的 RouteInfo 列表，不建立连接
  - 本地层：检查 IsLocal
  - 集群层：通过 LookupRemote 获取远程端点
  - 联邦层：通过 FederationState.LookupRemoteService 获取联邦端点

## 验收标准
- 7 个测试函数，覆盖以下场景：
- `TestFederationRouter_LocalHit`: 本地节点命中，Metrics.LocalHits 递增
- `TestFederationRouter_ClusterHit`: 集群内节点命中，Metrics.ClusterHits 递增
- `TestFederationRouter_FederationHit`: 联邦集群命中，Metrics.FederationHits 递增
- `TestFederationRouter_NotFound`: 全部层级未找到，返回 ErrServiceNotFound
- `TestFederationRouter_LeaderOnly`: 非 Leader 节点不进行联邦路由
- `TestFederationRouter_Lookup`: 诊断查询返回多层 RouteInfo
- `TestFederationRouter_NilComponents`: 组件为 nil 时安全降级

## 依赖
- `pkg/mesh.Manager`: 本地集群管理器
- `pkg/mesh.FederationManager`: 联邦管理器
- `pkg/mesh.ServiceDiscovery`: 按需服务发现
- `pkg/mesh.LeaderElection`: Leader 选举（网关控制）
- `log/slog`: 结构化日志
- `sync/atomic`: 原子计数器

## 代码位置
- `pkg/mesh/federation_router.go`
- `pkg/mesh/federation_router_test.go`
