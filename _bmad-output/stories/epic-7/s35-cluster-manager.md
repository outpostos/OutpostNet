# Story S35: 集群管理器与服务同步

**Epic:** Epic 7 — 集群内服务网格
**Status:** Done

## 目标
实现集群管理器，编排 ClusterState 和 Forwarder，提供本地服务注册、定期服务同步和请求转发的统一入口。

## 内容
- `ClusterConfig` 配置：LocalNodeID、LocalAddr、Peers([]PeerConfig)、SyncInterval(默认30s)、DialTimeout(默认5s)、MaxForwardRetries(默认2)
- `PeerConfig` 结构体：NodeID + Address(host:port)
- `DefaultClusterConfig()`: 返回默认配置
- `Manager` 结构体：编排 ClusterState + Forwarder，维护 localServices map、done channel、WaitGroup
- `NewManager()`: 创建 Manager，注册配置中的 Peers 为健康节点，创建 Forwarder 并设置 SyncHandler
- `Start(ctx)`: 启动流程 — 启动 Forwarder 监听（等待最多 5s）-> 执行初始同步 -> 启动定期同步循环
- `RegisterLocalService(serviceName, protocol, localPort)`: 注册本地服务到 localServices map 和 ClusterState
- `UnregisterLocalService(serviceName)`: 反注册本地服务
- `Forward(ctx, serviceName)`: 委托给 Forwarder.Forward
- `IsLocal(serviceName)`: 委托给 ClusterState.IsLocal
- `State()`: 返回底层 ClusterState
- `ForwardingMetrics()`: 返回 Forwarder 的指标
- `Stop()`: 优雅关闭，关闭 done channel -> 关闭 syncListener -> 关闭 Forwarder -> 等待所有 goroutine
- `syncLoop(ctx)`: 定期同步循环，按 SyncInterval 触发
- `syncWithPeers()`: 与所有健康节点执行全量服务同步
- `syncWithPeer()`: 与单个节点交换服务列表
- 同步线路协议（magic=0xAA）：Count(2B) + [ServiceEntry x Count]，每个 ServiceEntry: NameLen(2B) + Name(变长) + Proto(1B) + Port(2B)
- `handleSyncConn()`: 处理入站同步连接，读取对端服务列表 -> 发送本地服务列表 -> 更新 ClusterState

## 验收标准
- 8 个测试函数，覆盖以下场景：
- `TestManager_NewManager`: 构造函数正确初始化
- `TestManager_RegisterUnregisterService`: 服务注册与反注册
- `TestManager_StartStop`: 启动与优雅关闭
- `TestManager_ForwardIntegration`: 端到端转发集成测试
- `TestManager_SyncBetweenPeers`: 双节点服务同步
- `TestManager_ForwardNotFound`: 转发不存在的服务
- `TestSyncPayload_RoundTrip`: 同步协议编解码往返
- `TestSyncPayload_Empty`: 空服务列表同步

## 依赖
- `pkg/mesh.ClusterState`: 集群状态
- `pkg/mesh.Forwarder`: 请求转发
- `encoding/binary`: 同步协议编解码
- `net`: TCP 连接
- `log/slog`: 结构化日志

## 代码位置
- `pkg/mesh/manager.go`
- `pkg/mesh/manager_test.go`
