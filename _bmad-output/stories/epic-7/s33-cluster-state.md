# Story S33: 集群状态管理

**Epic:** Epic 7 — 集群内服务网格
**Status:** Done

## 目标
实现集群拓扑与服务分布的线程安全状态管理，作为服务网格的核心数据结构。

## 内容
- `NodeStatus` 枚举：Unknown(0)、Healthy(1)、Unhealthy(2)，附带 `String()` 方法
- `NodeInfo` 结构体：NodeID、Address（host:port）、Status、LastSeen
- `ServiceEndpoint` 结构体：NodeID、ServiceName、Protocol（0=TCP, 1=UDP）、LocalPort(uint16)、Weight(默认1)、Healthy
- `ClusterState` 结构体：localNodeID + nodes map + services map，`sync.RWMutex` 保护并发访问
- `NewClusterState(localNodeID)`: 构造函数
- `LocalNodeID()`: 返回本地节点 ID
- `AddNode(info)`: 注册或更新节点
- `RemoveNode(nodeID)`: 移除节点并级联删除该节点的所有服务端点
- `AddService(ep)`: 注册服务端点，同节点同服务则更新，Weight <= 0 自动设为 1
- `RemoveService(serviceName, nodeID)`: 移除指定节点的服务端点，服务无端点时清理 key
- `LookupRemote(serviceName)`: 查找远程健康端点，排除本地节点、不健康端点、不健康节点
- `IsLocal(serviceName)`: 检查服务是否在本地节点注册
- `GetNode(nodeID)`: 获取节点信息
- `HealthyNodes()`: 返回所有健康节点列表
- `AllServices()`: 返回所有服务的深拷贝快照
- `NodeCount()`: 返回已知节点数量

## 验收标准
- 10 个测试函数，覆盖以下场景：
- `TestNewClusterState`: 构造函数及 localNodeID
- `TestClusterState_AddRemoveNodes`: 节点增删及 NodeCount
- `TestClusterState_AddRemoveServices`: 服务注册、更新、删除
- `TestClusterState_RemoveNode_CleansUpServices`: 删除节点级联清理服务
- `TestClusterState_LookupRemote`: 远程查找排除本地节点
- `TestClusterState_LookupRemote_ExcludesUnhealthyEndpoints`: 排除不健康端点和不健康节点
- `TestClusterState_IsLocal`: 本地服务检测
- `TestClusterState_HealthyNodes`: 健康节点过滤
- `TestClusterState_DefaultWeight`: Weight <= 0 自动设为 1
- `TestClusterState_ConcurrentAccess`: 并发读写安全

## 依赖
- `sync`: RWMutex 线程安全
- `time`: LastSeen 时间戳

## 代码位置
- `pkg/mesh/cluster.go`
- `pkg/mesh/cluster_test.go`
