# Story S37: Leader 选举

**Epic:** Epic 8 — 跨集群联邦
**Status:** Done

## 目标
实现基于优先级的 Leader 选举协议，用于集群内选举唯一的联邦网关节点。

## 内容
- `LeaderState` 枚举（int32）：Follower(0)、Candidate(1)、Leader(2)，附带 `String()` 方法
- `LeaderChangeCallback` 回调类型：`func(leaderID string, isLocal bool)`
- `ElectionConfig` 配置：NodeID、NodePriority(默认0)、Peers([]PeerConfig)、ElectionTimeout(默认5s)、HeartbeatInterval(默认10s)、LeaderTimeout(默认30s)、DialTimeout(默认3s)
- `ElectionMetrics` 计数器：ElectionsStarted、ElectionsWon、ElectionsLost、HeartbeatsSent、HeartbeatsRecv
- `LeaderElection` 结构体：维护 state、currentLeaderID、lastHeartbeat、callbacks 列表
- 选举算法：高优先级获胜；优先级相同时字典序较小的 NodeID 获胜
  - `startElection()`: 设为 Candidate -> 向所有 Peers 发送 Propose -> 无 Reject 则成为 Leader，有 Reject 则接受对方提供的 winnerID
  - `shouldAcceptProposal()`: 比较 proposerPriority 与本地 myPriority，相等时比较 ID 字典序
- 无 Peers 时自动成为 Leader
- Leader 发送定期心跳（`sendHeartbeats()`），Follower 检测心跳超时后触发重新选举
- `OnLeaderChange(cb)`: 注册 Leader 变更回调
- `IsLeader()`/`State()`/`CurrentLeader()`: 状态查询方法
- `HandleElectionMessage(conn)`: 处理入站选举消息，按类型分发到 Propose/Query/Leader 处理器
- 选举线路协议（magic=0xCC）：
  - Propose(0x01): magic + type + idLen(2B) + nodeID + priority(4B big-endian uint32)
  - Accept(0x02): type(1B)
  - Reject(0x03): type(1B) + idLen(2B) + winnerID
  - Query(0x04): type(1B)，响应 Leader(0x05) + idLen(2B) + leaderID
  - Heartbeat = Leader(0x05): magic + type + idLen(2B) + leaderID
- `monitorLoop()`: 按 HeartbeatInterval 周期运行 — Leader 发心跳，Follower 检查超时

## 验收标准
- 10 个测试函数，覆盖以下场景：
- `TestLeaderElection_SingleNode`: 无 Peers 自动成为 Leader
- `TestLeaderElection_Callback`: Leader 变更回调触发
- `TestLeaderElection_TwoNodes_HigherPriorityWins`: 高优先级节点获胜
- `TestLeaderElection_TwoNodes_TieBreakByID`: 优先级相同时 ID 小的获胜
- `TestLeaderElection_PeerUnreachable`: 对端不可达时自动成为 Leader
- `TestLeaderElection_StateString`: 状态枚举字符串表示
- `TestLeaderElection_DefaultConfig`: 默认配置值验证
- `TestLeaderElection_Metrics`: 选举指标计数正确性
- `TestLeaderElection_ShouldAcceptProposal`: 提案接受/拒绝逻辑
- `TestLeaderElection_ForwarderIntegration`: 与 Forwarder 监听器集成（magic=0xCC 路由）

## 依赖
- `pkg/mesh.PeerConfig`: 节点配置
- `pkg/mesh.Forwarder`: 共享监听器（ElectionHandler）
- `encoding/binary`: 线路协议编解码
- `sync/atomic`: 原子计数器
- `net`: TCP 连接
- `log/slog`: 结构化日志

## 代码位置
- `pkg/mesh/election.go`
- `pkg/mesh/election_test.go`
