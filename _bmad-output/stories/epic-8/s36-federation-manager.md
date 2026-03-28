# Story S36: 联邦管理器

**Epic:** Epic 8 — 跨集群联邦
**Status:** Done

## 目标
实现跨集群联邦管理，提供集群间健康检查、服务目录同步和请求转发功能。

## 内容
- `ClusterInfo` 结构体：ClusterID、Address、Status(NodeStatus)、LastSeen
- `RemoteClusterConfig` 结构体：ClusterID + Address
- `FederationConfig` 配置：LocalClusterID、ListenAddr、RemoteClusters、SyncInterval(默认60s)、HealthCheckInterval(默认15s)、DialTimeout(默认5s)、HealthTimeout(默认45s)
- `FederationMetrics` 计数器：SyncTotal、SyncErrors、ForwardTotal、ForwardErrors、ForwardActive、HealthCheckTotal、HealthCheckFails（均为 atomic.Int64）
- `FederationState` 结构体：线程安全的联邦拓扑状态 + 远程服务目录（`map[string]map[string][]ServiceEndpoint`，clusterID -> serviceName -> endpoints）
  - 方法：AddCluster、RemoveCluster（级联删除服务）、UpdateClusterStatus、GetCluster、HealthyClusters、SetRemoteServices、LookupRemoteService、AllRemoteServices、ClusterCount
- `FederatedEndpoint` 结构体：ClusterID + Address + ServiceEndpoint，关联服务端点与集群位置
- `FederationManager` 结构体：管理跨集群操作，持有 FederationState、localManager 引用、QueryHandler（供 ServiceDiscovery 设置）
- 联邦线路协议（magic=0xBB）：magic(1B) + msgType(1B)，消息类型：Sync(0x01)、Forward(0x02)、Ping(0x03)、Pong(0x04)、Query(0x05)
- `Start(ctx)`: 启动联邦监听器 -> 初始健康检查 -> 初始服务同步 -> 启动定期健康检查和同步循环
- `Stop()`: 优雅关闭
- 健康检查：`healthCheckAll()` 遍历所有集群执行 ping/pong，超过 HealthTimeout 标记为 Unhealthy；ping 携带本地 ClusterID
- 服务同步：`syncWithAllClusters()` 从 localManager 收集本地服务，与所有健康集群双向交换服务目录
- `ForwardToCluster(ctx, clusterID, serviceName)`: 向指定集群转发请求，验证集群健康状态
- `ForwardFederated(ctx, serviceName)`: 向任意拥有该服务的集群转发，按 clusterID 去重尝试
- `handleIncomingForward()`: 处理入站转发 — 先尝试本地服务，再尝试集群内转发
- `handleDirectConn()`: 独立联邦监听器的连接处理，验证 magic 字节后分发
- `handleFederationConn()`: 按消息类型分发到 Sync/Forward/Ping/Query 处理器
- `dialFederation()`: 连接远程集群，发送 magic + msgType 头

## 验收标准
- 12 个测试函数，覆盖以下场景：
- `TestFederationState_ClusterManagement`: 集群增删改查
- `TestFederationState_ServiceLookup`: 远程服务查找，排除不健康集群
- `TestFederationState_ConcurrentAccess`: 并发读写安全
- `TestFederationState_RemoveClusterClearsServices`: 删除集群级联清理服务
- `TestFederationManager_PingPong`: 健康检查 ping/pong 协议
- `TestFederationManager_ServiceSync`: 跨集群服务同步
- `TestFederationManager_CrossClusterForward`: 跨集群请求转发
- `TestFederationManager_ForwardFederated`: 自动选择集群转发
- `TestFederationManager_ForwardNotFound`: 服务不存在错误
- `TestFederationManager_HealthCheckTimeout`: 健康检查超时标记不健康
- `TestFederationConfig_Defaults`: 默认配置值验证
- `TestFederationState_AllRemoteServices`: 远程服务快照

## 依赖
- `pkg/mesh.Manager`: 本地集群管理器
- `pkg/mesh.ClusterState`: 集群状态（通过 Manager.State()）
- `pkg/mesh.Forwarder`: 复用线路协议辅助函数
- `encoding/binary`: 线路协议编解码
- `sync/atomic`: 原子计数器
- `net`: TCP 连接
- `log/slog`: 结构化日志

## 代码位置
- `pkg/mesh/federation.go`
- `pkg/mesh/federation_test.go`
