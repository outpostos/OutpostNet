# OutpostNet 观测生产者（四层改造 票10-B-Net）

> 状态：R1 实施说明（票10-B-Net 落地，票11-D 按已合并现状校订）。契约是 go.mod 钉住的 OutpostRange
> `25a9bf2` 的 `proto/outpostrange/observation/v1alpha1/observation.proto`，生产者规则见 Range
> `docs/four-layer-producer-guide.md`。本仓库报告的 111/15、116/20 字段在 `563b72a`（10-B 时的基线）与
> `25a9bf2` 之间没有变化；其间新增的契约字段（10-D2 的 `PeerReplicationProgress` 10/11、
> `LetRuntimeStatus` 8/9，10-F 的 `COMPONENT_RANGE_LOADGEN`）都不属于 Net 进程的事实。
> 本文只写本仓库实际报告什么、从哪里取、哪些不报。落地位置与测试见第 8 节，升级顺序见第 7 节。

## 1. 谁暴露观测端点

| 进程 | 观测端点 | component | 报告的层（由 body 决定） |
| --- | --- | --- | --- |
| `outpostnet-service`（`internal/servicehost`） | 默认 `127.0.0.1:17961`，`--observation-addr` 覆盖，空串关闭 | `COMPONENT_OUTPOSTSERVICE` | Service：`service_access_changed`(116)；Net：本进程隧道的 `net_peer_changed`(111) |
| `outpostnet-agent`（`internal/agent`） | 默认 `127.0.0.1:17960`，`--observation-addr` 覆盖，空串关闭 | `COMPONENT_OUTPOSTNET` | Net：`net_peer_changed`(111)。旧组合宿主自带的 `imports` 入口**不报** Service 事实（Service 事实由 `outpostnet-service` 报告） |
| `outpostnet-server` / `outpostnet-sidecar` | 不暴露 | — | 节点内 Unix socket 路由中枢，不跨节点，不属于四层 Net 连接事实 |
| `outpostnet-controller` | 不暴露 | — | Kubernetes 控制面，不承载节点间连接 |

- 端点只监听回环地址（`localhost` 或 loopback IP），非回环地址在构造时拒绝。
- `node_id` 为空（例如只按 region 配置的旧 agent）时不启动观测：Range 的事件必须带稳定节点身份，
  不能用 region 或主机名冒充；记一条警告日志（`slog.Warn`），产品照常运行。
- 一个进程一条流 `(node_id, component)`；`outpostnet-agent` 与 `outpostnet-service` 同节点并存时是两条流，
  range-agent 需各配一个 `ObservationEndpoints` 项（`"Component": 4` / `"Component": 5`）。

HTTP 形态沿用 Range `docs/observation-api.md`：

- `GET /outpostrange/observation/v1alpha1/snapshot` → `Snapshot`（protobuf JSON）。
- `GET /outpostrange/observation/v1alpha1/events?after_seq=<n>` → SSE，每条 `data:` 是一个 `Event`。
  `after_seq` 早于保留窗口时回 `410 Gone`。range-agent（563b72a 至 25a9bf2）收到 410 后用同一 `after_seq` 重试，
  不会重新取快照（Range 侧已记账，见第 9 节）。
- `seq` 高 44 位为进程启动 epoch（`UnixMilli << 20`），低 20 位为 epoch 内序号。

## 2. Net 层报告的本地事实（`net_peer_changed` 111、快照字段 15）

来源：`internal/agent.TunnelManager` 管理的每个 `tunnel.ManagedClient`（本节点主动拨出的隧道）。

| 字段 | 取值 | 缺失规则 |
| --- | --- | --- |
| `peer_node_id` | 动态对端（对等目录 `node_id → addr`）的 `node_id` | 静态 `tunnels[]` 只有 region，不是节点身份 → 不填 |
| `peer_address` | `ManagedClient.ServerAddr()`（当前拨号地址） | 总是有 |
| `state` | `Connected`→`CONNECTED`；`Connecting`/`Reconnecting`→`CONNECTING`；`Disconnected`→`DISCONNECTED` | 总能读到客户端状态，故总填 |
| `last_error`、`last_error_time` | `ManagedClient` 最近一次拨号/认证失败或连接丢失的错误与节点时间（本票在 `pkg/tunnel` 新增只读 `Status()`） | 本进程尚未失败过 → 不填 |
| `rtt_ms` | **不报** | QUIC 平滑 RTT 每次采样都在变，按“只发变化”会刷屏；R1 不报，Range 显示 missing |

- 只报本节点拨出的会话。本节点作为隧道 server 被动接受的连接没有对端节点身份（认证只有共享 token），
  R1 不报；Range 从对端那一侧的 `net_peer_changed` 看这条链路。
- 对端从对等目录删除时，最后一次报 `DISCONNECTED`，之后从快照中移除。
- 采样：`ManagedClient` 状态回调与对等目录增删只做非阻塞通知（容量 1 的信号通道，满则合并），
  观测 goroutine 收到通知后读取全部隧道的只读状态，与上次报告逐项比较，不同才追加事件。
  回调里不加锁、不阻塞，隧道/重连路径不受观测影响。

### Net 复制进度（`net_replication_progress` 112、快照字段 16）

OutpostNet 自己的进程不运行 `pkg/state` 复制（没有 `GossipEngine`），因此 Net 进程**不报** 112/16。
`pkg/state` 只提供公共只读复制状态接口（第 4 节），由内嵌它的宿主在各自的流上报告：OutpostLet 报 112
（`scope = outpostlet.metadata`），OutpostMesh 报 114（`scope = outpostmesh.entities`）。Net 进程流上
Net 复制一栏在 Range 中显示 missing 是真实情况。

## 3. Service 层报告的本地事实（`service_access_changed` 116、快照字段 20）

来源：`outpostnet-service` 的每个稳定入口（`imports[]`）。每个应用连接到达入口时，入口按当前隧道状态打开一条流——
这就是“经入口的真实连接”。观测包装入口使用的 `StreamOpener` 与返回的流，只读结果，不改变转发。

| 字段 | 取值 |
| --- | --- |
| `service_name` | 稳定服务身份 `service.Target.String()`：动态为 `<service>@<node_id>`，静态 region 路径为 `<service> via region <region>` |
| `entry_address` | 入口实际绑定地址 |
| `state` | 见下表；入口从未有连接经过 → 不填（Range 显示 missing），不做主动探测 |
| `backend_node_id` | 动态目标的 `node_id`；静态 region 目标没有节点身份 → 不填；`NO_BACKEND` 时不填 |
| `backend_address` | 打开流时所用隧道客户端的 `ServerAddr()`（对端实例的隧道地址）；`NO_BACKEND` 时不填 |
| `previous_backend_node_id` | 仅当本事件报告后端节点切换时填。动态目标钉在 `service@node_id`，节点不会变，所以实际只有地址切换（实例替换），此字段在 R1 不会出现 |
| `last_error`、`last_error_time` | 最近一次经入口访问失败的错误与节点时间 |

状态判定（只依据真实连接结果）：

| 观测到的结果 | 报告 |
| --- | --- |
| 打开流时目标节点不在对等目录 / 没有该 region 的隧道（`agent.ErrNoTunnel`） | `NO_BACKEND`，清空 backend 字段 |
| 打开流失败（隧道正在重连时 `net.ErrClosed`、QUIC 打开失败等） | `UNREACHABLE` + `last_error`（backend 为尝试的那个实例） |
| 入口正在关闭（ctx 已取消）时打开失败 | 不报（不是测量结果） |
| 流打开后从对端收到第一个字节 | `REACHABLE` + 当时的 backend |
| 流打开后对端在发来任何字节前关闭（提供方节点拨本地后端失败时就是这样：`tunnel.Server` 直接关流） | `UNREACHABLE`，`last_error` 写明“对端在返回任何后端数据前关闭了流” |
| 本地应用先关闭连接、对端一直没回数据 | 不改变状态（没有可证明的结论） |

- 已知局限：一个从不回写数据就关闭连接的后端会被报告为 `UNREACHABLE`；隧道层没有更强的“后端已接受”信号。
- “只发变化”：`state`、`backend_*`、`last_error` 文本变化才发事件；同一错误反复出现只刷新快照里的
  `last_error_time`，不再发事件（应用高频重试时不刷屏）。
- 稳定入口可达不代表在途连接无损迁移，不在此声称（见 `docs/service-layer.md` 第 4 节）。

## 4. pkg/state 公共复制状态接口（阶段 2）

`GossipEngine.ReplicationStatus(ctx)` 返回只读快照：本地 `LocalStatus` + 每个当前 `PeerProvider` 对端的交换结果：
确认位置（`PeerProgressStore` 或引擎内存游标）、出向是否追平（确认位置 ≥ 本地位置，本地位置先读，结论只会更保守）、
按推送轮次的出向追平（`OutboundRoundCaughtUp`）及最近一次追平轮次的开始时间（`OutboundRoundTime`）、
最近一次完整应用的拉取的开始时间、入向是否追平（有成功拉取且其后没有失败交换）、最近成功时间、最近错误/时间/操作、
连续失败次数、退避截止时间。语义与票06 Mesh `GetReplicationStatus` 一致，Let/Mesh 据此映射到 Range
`ReplicationProgress`，不再各自包一层 recording syncer 复制实现。

- 出向两个口径（票 10-D1）：`OutboundCaughtUp` 是“此刻零积压”的瞬时事实。本地位置是 Lamport 式时钟，
  随连通域内所有节点的写入（应用对端条目时 max+1 合并）增长，而确认位置只在本节点推送轮次推进，
  所以持续写入下健康集群里它结构性为 false（10-C 中 `outpostlet.metadata` 稳态 lagging 的根因）。
  `OutboundRoundCaughtUp` 按推送轮次判定：每轮在收集变更前读本地位置，对端确认覆盖该位置即为追平；
  没有可推送变更的轮次（`SyncOnce` 跳过推送）也算追平，不会因无推送而永远落后；之后与该对端的交换
  失败即清除，退避跳过的轮次不算轮次。它在 `OutboundCaughtUp` 为 true 时必为 true。`OutboundRoundTime`
  是最近一次追平轮次的开始时间，失败后保留，可据此判断结论新鲜度与“落后起始时间”。两者都不参与墓碑 GC
  确认门（仍只看确认位置），`CaughtUp` 定义不变。
- 契约与映射（已合并）：Range 10-D2（PR #18，`8f31afb`，包含在 `25a9bf2` 中）在 `PeerReplicationProgress` 增加
  `outbound_round_caught_up`（10）与 `outbound_round_time`（11），语义与上面两个字段逐一对应；
  `outbound_caught_up` 仍按原口径（零积压）填写，不能用新口径代填。生产者映射由内嵌 `pkg/state` 的宿主完成：
  OutpostLet 10-D3（PR #67，`783a4b6`）在 112 `outpostlet.metadata` 每 peer 映射，OutpostMesh 10-D3（PR #50，
  `797c97d`）在 114 `outpostmesh.entities` 每 peer 映射；两者都只在测得时设置（尚无追平轮次时不填 `outbound_round_time`）。
- Net 自身生产者不投影 `ReplicationStatus`（含这两个字段）：Net 进程（`outpostnet-agent`、`outpostnet-service`）
  不构造 `GossipEngine`，没有可报告的复制状态；它报告的 111/15 `net_peers` 是隧道连接事实，与复制追平无关，
  把复制字段挂到 Net 流上会冒充一个不存在的复制者。
- 读数注意：没有本地写入时，被分区的 peer 也没有可推送的内容，`OutboundCaughtUp` 为 true，`OutboundRoundCaughtUp`
  随之为 true（10-D3-Let 真实装配测试实测）。“分区时为 false”的前提是存在对端未确认的本地写入；判断对端是否连通
  要结合 `InboundCaughtUp`、`LastError`/`LastErrorTime`（Range 投影即如此）。
- 并发安全：只在引擎锁内复制每 peer 状态，不持锁调用 Store。
- 不泄露凭据：`pkg/state` 的全部日志与导出 API 返回的错误只以“密码已遮蔽”的 peer 地址出现；任何 `Syncer` 的每 peer 错误在返回、记日志、进状态前去掉地址里的密码；`HTTPSyncer` 把 bearer token 与对端回显的 URL 密码替换为 `[REDACTED]`。脱敏收敛在 `pkg/state/redact.go`。
- `pkg/state` 不依赖 OutpostRange；映射到 proto 由宿主完成。

### OS time-source observations (V2-NET-1d)

Agents sample their shared `TimeReference` once a second using the existing
`MetricSample` event body. `net_time_quality` has value 1 and labels `quality`,
`source`, `source_state`; synchronized source observations additionally report
`net_time_source_uncertainty_seconds`, and a measured signed offset reports
`net_time_source_offset_seconds`. Unknown measurements are omitted. These samples
belong to the event timeline, not Snapshot fields. No Range contract upgrade is
needed. Public `TimeStatus.Source` and store/engine `TimeSource` provide the same
source facts for embedding hosts; see [source mapping and tests](os-time-source.md).

## 5. 快照与“只发变化”

- 观测状态由一个 `Reporter` 持有：每个键（Net：`peer_node_id` 或 `peer_address`；Service：`service_name`）一份
  当前已报告的值。更新与追加事件在同一把锁内完成，快照也在这把锁内读取当前全部值与事件日志的最后 `seq`，
  所以快照的 `seq` 精确等于“快照已包含的最后一条事件”，SSE 从 `after_seq=<snapshot.seq>` 之后只发变化。
- 快照字段 15（`net_peers`）与 20（`service_access`）携带全部当前值；没有的层（16–19）不填。
- 未测量的值不设置：`state`、`rtt_ms` 等 optional 字段只有测得才设置，不用 `UNSPECIFIED`/`0`/`false` 冒充。

## 6. 观测不得成为产品运行前提

- 观测 HTTP 服务在独立 goroutine 里启动；监听失败（端口占用等）或运行期出错只记 `slog.Error`，
  `outpostnet-agent` / `outpostnet-service` 继续运行（参考 OutpostLet `pkg/agent/agent.go` 与
  `TestObservationServiceListenFailureDoesNotStopAgent`）。
- 记录事实非阻塞：事件写入有界环形缓冲（默认 4096 条，`DefaultEventBufferSize`），满了丢最旧的并计数；
  每个订阅者另有一条 1024 条的通道，通道满（慢订阅者）时关闭该订阅，range-agent 用 `after_seq` 重连，
  不反压产品路径。
- 观测只读产品状态（隧道状态、入口打开流的结果），不写产品状态，不新增产品到 Range 的运行期网络依赖。
- 不实现故障注入。

## 7. 依赖、构建与升级顺序

- 依赖 `github.com/outpostos/OutpostRange`（私有仓，go.mod 钉在 `25a9bf2`，票11-D 对齐）的生成 proto 包，
  仅 `internal/observation`、`internal/agent`、`internal/servicehost` 与 `cmd/` 引用。
  `pkg/state`、`pkg/tunnel`、`pkg/service` 不得依赖 OutpostRange，由边界测试强制。
- 构建需要 `GOPRIVATE=github.com/outpostos GONOSUMDB=github.com/outpostos`，私有仓经 SSH
  （`git config url.git@github.com:.insteadOf https://github.com/`）。GitHub Actions 经只读部署密钥拉取（CI-01，
  `.github/workflows/ci.yml`，secret `OUTPOSTRANGE_DEPLOY_KEY`）。
- 升级顺序：**先升级 Range（collector、range-agent、judge）到不低于 go.mod 所钉的版本，再升级 Net 进程。**
  Range 用拒绝未知字段的 protobuf JSON 解码（Range `docs/four-layer-producer-guide.md` 第 1 节）：生产者若带着
  旧 Range 不认识的字段上报，旧 range-agent 解析失败并重连、旧 collector 回 400，观测停滞（产品不受影响）。
  Net 当前报告的字段旧到 `563b72a` 也认识，所以这条顺序对本仓库是约束而非已知故障。

## 8. 落地位置

| 内容 | 位置 |
| --- | --- |
| 公共复制状态接口 | `pkg/state/replication_status.go`（`GossipEngine.ReplicationStatus`、`PeerReplicationStatus`）；`gossip.go` 记录每 peer 结果；`redact.go` peer 地址与错误脱敏（日志、返回错误、状态共用） |
| 隧道只读状态 | `pkg/tunnel/lifecycle.go` `ManagedClient.Status()`（状态、地址、最近失败及时间） |
| Net 宿主读取面 | `internal/agent/tunnel_manager.go`：`PeerLinks`、`SetLinkObserver`、`RegionClient`、`ErrNoTunnel` |
| 观测生产者 | `internal/observation/`：`eventlog.go`（有界日志、seq epoch、慢订阅者丢弃）、`reporter.go`（快照）、`netpeers.go`（111/15 与采样）、`serviceaccess.go`（116/20，包装入口开流）、`server.go`（回环 HTTP/SSE） |
| 宿主接线 | `internal/agent/observation.go`（`StartObservation`，两个宿主共用）、`internal/agent/agent.go`（`EnableObservation`）、`internal/servicehost/observation.go` + `host.go` |
| 命令 | `cmd/outpostnet-agent`、`cmd/outpostnet-service` 的 `--observation-addr` |
| Range 契约解析（测试） | `internal/observation/observationtest`：按 range-agent 563b72a 的做法严格解码（拒绝未知字段）、按流校验 seq、生成 117 订阅快照事件。Range 的 `internal/rangeagent`、`internal/observationjson` 受 Go internal 规则限制不能从本模块导入，故按其源码复刻；票11-D 核对这两个目录在 `563b72a` 与 `25a9bf2` 之间只有测试文件变化，复刻仍有效 |
| 边界测试 | `pkg/service/boundary_test.go` `TestNetLibrariesDoNotDependOnOutpostRange` |

测试：

| 测试 | 证明 |
| --- | --- |
| `pkg/state` `TestReplicationStatus*` | 每 peer 交换结果、退避跳过不算拉取、冷启动后确认位置保留且无拉取、凭据不泄露、并发安全（`-race`） |
| `pkg/state` `TestOutboundRoundCaughtUp*` | 持续写入的健康双/三副本：`OutboundRoundCaughtUp` 稳态为 true 而 `OutboundCaughtUp` 为 false；分区时对断开 peer 为 false（`OutboundRoundTime` 停在分区前）、对连通 peer 为 true，恢复后全部为 true；无写入时两者都为 true；resync 清除产生无条目的 floor 位置后，跳过推送的轮次仍算追平 |
| `pkg/state` `TestGossipLogsAndErrorsNeverCarryPeerURLPassword`、`TestHTTPErrorBodyEchoingPeerURLPasswordIsRedacted`、`TestPeerAddressRedactionHelpers` | 带 `user:password@` 的 peer 经 GC horizon 拒收、resync（成功与失败）、退避、普通交换失败、HTTP 错误体回显密码各路径，捕获的 slog 输出与 `SyncOnce`/`AntiEntropyOnce`/`HTTPSyncer` 返回错误都不含密码 |
| `pkg/tunnel` `TestManagedClientStatusReportsLatestFailure` | 隧道最近失败可读、不含 token |
| `internal/servicehost` `TestObservationReportsNetAndServiceFactsThroughRangeContract` | 两个真实 Host（真实 QUIC、token、真实 TCP 应用）：订阅快照含已连通的 Net 链路与未测量的入口；REACHABLE、实例替换后 backend 地址切换、后端失效 UNREACHABLE、对端移出目录 DISCONNECTED + NO_BACKEND；全部事件经严格契约解析，晚到订阅者快照 seq 等于最后事件 |
| `internal/servicehost` `TestObservationPortTakenDoesNotStopHost`、`internal/agent` `TestObservationListenFailureDoesNotStopAgent` | 观测端口被占用时产品照常服务，只记日志 |
| `internal/agent` `TestAgentObservationReportsTunnelPeer`、`TestObservationWithoutNodeIDIsDisabledNotFatal` | 真实 agent + 真实隧道 server 的 Net 事实；无 node_id 时观测关闭不影响 agent |
| `internal/observation` 单元测试 | 环形缓冲与 410 gap、慢订阅者不阻塞、变化检测、入口结果判定、回环限制 |

## 9. 已知限制

- `rtt_ms`、被动接受的隧道连接、Net 复制进度（本仓进程不运行复制）不报。
- range-agent（563b72a 至 25a9bf2）遇到 `410 Gone` 时以同一 `after_seq` 重试，不重新取快照（`RunObservationEndpoint` 只在启动时取快照）；
  若订阅者落后超过保留窗口（4096 条）会一直拿到 410。属于 Range 侧行为，记录不改。
- “后端不回数据就关连接”会被判为 `UNREACHABLE`（第 3 节）。
- Net 链路的变化检测包含 `last_error_time`：分区期间每次重连失败都会刷新它，采样（1s）发现后发一条
  `net_peer_changed`，即每个对端每次退避各一条事件（退避 1s 起、翻倍、封顶 30s）。这是如实报告每次失败的时间，
  不做合并。
- 只在单机多进程/同进程多 Host 上验证，未在多台机器上验证。

## 10. 验证记录（322b90c，2026-10-03，单机）

- `go build ./...`、`go vet ./...`、`go test -count=1 ./...` 全部通过；`go test -race -count=1` 覆盖
  `pkg/state`、`pkg/tunnel`、`pkg/service`、`internal/observation/...`、`internal/agent`、`internal/servicehost` 通过；
  观测相关测试 `-race -count=5` 通过。
- `internal/observation` 单元测试在实现之后补写，用 4 个突变（入口自关不标记、去掉变化检测、追加时等待订阅者、
  移除对端不报断开）各自使对应测试失败，证明测试有效。
- 真实进程冒烟：两个 `outpostnet-service` 二进制（node-a 消费 `echo@node-b`、node-b 导出）、Range 563b72a 的
  `outpostrange serve` collector 与 `range-agent`（`ObservationEndpoints` 指向 node-a 的 `127.0.0.1:<port>`，`Component: 5`）。
  collector `/view` 的 `four_layers`：`net peer:node-b peer_link` 由快照得 `connected`；应用经入口连通后
  `service:echo@node-b access` 为 `reachable`、backend 轨道 `node-b`；停掉 node-b 的后端应用后再连一次为
  `unreachable`（`provider closed the stream before any backend data`）；停掉 node-b 进程后 Net 链路为
  `connecting`（`connection lost: ... server closing`，随后 `quic dial ...: timeout`）；coverage `net`、`service` 为 `reported`。
  另以占用的观测端口启动 node-b 二进制：`/readyz` 200，日志一条
  `observation endpoint unavailable; continuing without observation API ... address already in use`。
- 干净检出：`git clone` 本分支到临时目录，使用全新 `GOMODCACHE`（经 SSH 拉取私有 OutpostRange），
  `go build ./...` 与核心包测试通过，go.mod 无 `replace`。

## 11. Reliable delivery metrics (NET-10)

`pkg/delivery` exposes `DeliveryStatus()` in the same host-owned manner as
`pkg/state.ReplicationStatus`. It remains independent of the Range module.
`internal/observation.Reporter.UpdateDelivery(statuses...)` maps these local
facts into the existing **109 `MetricSample`** event body; it adds no proto
fields or exporter. A host embedding delivery calls this on its observation
sampling tick with its Inbox and Outbox snapshots. Net agent/service do not
construct delivery boxes, so they do not emit invented delivery facts. External
embedding hosts (including Mesh) own the equivalent mapping in their observation
producer; their runtime wiring is outside NET-10.

Every sample has `peer_node_id` and `box` (`inbox`/`outbox`) labels. Source node,
component, time and sequence come from the existing event log. No message ID,
mailbox, payload, endpoint address, token or error text becomes a metric label.

| Metric | Local fact |
| --- | --- |
| `delivery_pending_envelopes`, `delivery_pending_bytes` | Durable pending envelopes and JSON-encoded bytes |
| `delivery_pending_receipts`, `delivery_pending_receipt_bytes` | Receipts waiting for confirmation and encoded bytes |
| `delivery_oldest_pending_seconds` | Oldest known enqueue age, including receipts; zero when queue empty; omitted when pending legacy entries have no known timestamp |
| `delivery_send_success_total`, `delivery_send_failure_total` | Actual batch attempts; full or invalid/failed responses count as failures; skipped backoff polls do not count |
| `delivery_unknown_peer_total` | Destination missing from existing membership/identity mapping |
| `delivery_duplicate_total` | Receiver deduplication hits |
| `delivery_expired_total` | Expired arrivals plus locally committed expiry transitions |
| `delivery_rejected_total` | Invalid envelope/receipt admission results and committed Outbox rejections |
| `delivery_inbox_full_total`, `delivery_outbox_full_total` | Receiver admission refusals / local Send quota refusals |

Queue gauges survive box recovery; counters describe this process and reset on
reopen. Samples use the existing bounded, nonblocking timeline and its gap/slow
subscriber behavior. The current observation Snapshot contract has no generic
metric field: these samples are events, emitted on each host sampling tick,
not added to snapshot fields 15–20. Range must derive metric projections from
the timeline. `TestDeliveryMetricSamplesUseLocalFacts` verifies the real box
status → MetricSample mapping and secret-free proto JSON. Delivery HTTP tests
capture error/log redaction, including echoed tokens, credential-bearing peer
URLs, malformed responses, client failures and redirects.
