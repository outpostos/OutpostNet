# Story S34: 跨节点服务请求转发

**Epic:** Epic 7 — 集群内服务网格
**Status:** Done

## 目标
实现集群内跨节点的 TCP 服务请求转发，包含复用监听器、线路协议和双向数据中继。

## 内容
- `LocalServiceHandler` 接口：`ConnectLocalService(serviceName string) (io.ReadWriteCloser, error)`
- `LocalServiceHandlerFunc` 适配器：将普通函数转换为 LocalServiceHandler 接口
- `ForwarderConfig` 配置：ListenAddr、DialTimeout(默认5s)、MaxRetries(默认2)
- `ForwardingMetrics` 计数器：ForwardTotal、ForwardErrors、ForwardActive、ForwardLatencyNs(累计纳秒)、IncomingTotal、IncomingErrors、IncomingActive（均为 atomic.Int64）
- `ConnHandler` 类型：`func(conn net.Conn)`，用于复用监听器分发
- `Forwarder` 结构体：核心转发器，维护 config、logger、cluster state、handler、listener、done channel、WaitGroup
- 复用监听器 `handleIncoming()`: 读取首字节魔术字节路由 — 0xAA=同步(SyncHandler)、0xBB=联邦(FederationHandler)、0xCC=选举(ElectionHandler)、其他=转发请求
- `ListenAndServe()`: 启动 TCP 监听循环，每个连接一个 goroutine
- `Forward(ctx, serviceName)`: 出站转发，round-robin 选择端点 + failover 重试（最多 MaxRetries+1 次），返回 trackedConn
- `dialAndForward()`: 连接对端并发送转发请求头，读取响应状态
- 线路协议 — 请求：NameLen(2B big-endian) + ServiceName(变长)；响应：Status(1B, 0=OK/1=NotFound/2=Error) + MsgLen(2B) + Message(变长)
- `biCopy(a, b)`: 双向数据中继，两个 goroutine 分别 io.Copy，任一方向结束即返回
- `trackedConn`: 包装 ReadWriteCloser，Close 时通过 sync.Once 调用 onClose 回调（减少活跃计数）
- 错误常量：ErrServiceNotFound、ErrNoHealthyEndpoint、ErrForwardingFailed、ErrForwarderClosed

## 验收标准
- 14 个测试函数，覆盖以下场景：
- `TestForwardHeader_RoundTrip`: 线路协议头编解码往返
- `TestForwardHeader_TooLong`: 超长服务名拒绝
- `TestForwardHeader_InvalidLength`: 无效长度检测
- `TestForwardResponse_RoundTrip`: 响应编解码往返
- `TestForwarder_EndToEnd`: 端到端转发集成测试
- `TestForwarder_ServiceNotFound`: 服务不存在错误处理
- `TestForwarder_ForwarderClosed`: 关闭后拒绝新请求
- `TestForwarder_RoundRobin`: 多端点轮询分配
- `TestForwarder_Failover`: 端点故障时自动重试
- `TestForwarder_IncomingServiceNotFound`: 入站请求服务不存在
- `TestForwarder_Metrics`: 性能计数器正确性
- `TestForwarder_ConcurrentForwards`: 并发转发安全
- `TestForwarder_ContextCancelled`: 上下文取消处理
- `TestForwarder_LargeDataTransfer`: 大数据传输正确性

## 依赖
- `pkg/mesh.ClusterState`: 集群状态查询
- `encoding/binary`: 线路协议编解码
- `sync/atomic`: 原子计数器
- `net`: TCP 连接管理
- `log/slog`: 结构化日志

## 代码位置
- `pkg/mesh/forwarding.go`
- `pkg/mesh/forwarding_test.go`
