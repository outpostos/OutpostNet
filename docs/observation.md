# OutpostNet 观测生产者（四层改造 票10-B-Net）

> 状态：R1 实施说明。契约是 OutpostRange `563b72a` 的
> `proto/outpostrange/observation/v1alpha1/observation.proto`，生产者规则见 Range
> `docs/four-layer-producer-guide.md`。本文只写本仓库实际报告什么、从哪里取、哪些不报。
> 阶段 1 是设计说明；落地位置见第 8 节（阶段 2、3 完成后补全）。

## 1. 谁暴露观测端点

| 进程 | 观测端点 | component | 报告的层（由 body 决定） |
| --- | --- | --- | --- |
| `outpostnet-service`（`internal/servicehost`） | 默认 `127.0.0.1:17961`，`--observation-addr` 覆盖，空串关闭 | `COMPONENT_OUTPOSTSERVICE` | Service：`service_access_changed`(116)；Net：本进程隧道的 `net_peer_changed`(111) |
| `outpostnet-agent`（`internal/agent`） | 默认 `127.0.0.1:17960`，`--observation-addr` 覆盖，空串关闭 | `COMPONENT_OUTPOSTNET` | Net：`net_peer_changed`(111) |
| `outpostnet-server` / `outpostnet-sidecar` | 不暴露 | — | 节点内 Unix socket 路由中枢，不跨节点，不属于四层 Net 连接事实 |
| `outpostnet-controller` | 不暴露 | — | Kubernetes 控制面，不承载节点间连接 |

- 端点只监听回环地址（`localhost` 或 loopback IP），非回环地址在构造时拒绝。
- `node_id` 为空（例如只按 region 配置的旧 agent）时不启动观测：Range 的事件必须带稳定节点身份，
  不能用 region 或主机名冒充；记一条错误日志，产品照常运行。
- 一个进程一条流 `(node_id, component)`；`outpostnet-agent` 与 `outpostnet-service` 同节点并存时是两条流，
  range-agent 需各配一个 `ObservationEndpoints` 项（`"Component": 4` / `"Component": 5`）。

HTTP 形态沿用 Range `docs/observation-api.md`：

- `GET /outpostrange/observation/v1alpha1/snapshot` → `Snapshot`（protobuf JSON）。
- `GET /outpostrange/observation/v1alpha1/events?after_seq=<n>` → SSE，每条 `data:` 是一个 `Event`。
  `after_seq` 早于保留窗口时回 `410 Gone`（range-agent 会重新取快照）。
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
`pkg/state` 只提供公共只读复制状态接口（第 4 节），由内嵌它的 OutpostLet / OutpostMesh 在各自的流上报告
（例如 `scope = outpostlet.scheduling`）。Net 进程流上 Net 复制一栏在 Range 中显示 missing 是真实情况。

## 3. Service 层报告的本地事实（`service_access_changed` 116、快照字段 20）

来源：`outpostnet-service` 的每个稳定入口（`imports[]`）。每个应用连接到达入口时，入口按当前隧道状态打开一条流——
这就是“经入口的真实连接”。观测包装入口使用的 `StreamOpener` 与返回的流，只读结果，不改变转发。

| 字段 | 取值 |
| --- | --- |
| `service_name` | 稳定服务身份：`<service>@<node_id>`（动态）或 `<service>`（静态 region 路径，`Target.String()`） |
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
| 打开流失败（隧道正在重连、认证失败、QUIC 打开失败等） | `UNREACHABLE` + `last_error` |
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
最近一次完整应用的拉取的开始时间、入向是否追平（有成功拉取且其后没有失败交换）、最近成功时间、最近错误/时间/操作、
连续失败次数、退避截止时间。语义与票06 Mesh `GetReplicationStatus` 一致，Let/Mesh 据此映射到 Range
`ReplicationProgress`，不再各自包一层 recording syncer 复制实现。

- 并发安全：只在引擎锁内复制每 peer 状态，不持锁调用 Store。
- 不泄露 token：错误文本在记录时去掉 peer 地址中的 userinfo，并由 `HTTPSyncer` 把自己的 bearer token 替换为 `[REDACTED]`。
- `pkg/state` 不依赖 OutpostRange；映射到 proto 由宿主完成。

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
- 记录事实非阻塞：事件写入有界环形缓冲（默认 1024 条），满了丢最旧的并计数；订阅者通道满（慢订阅者）
  时关闭该订阅，range-agent 用 `after_seq` 重连，不反压产品路径。
- 观测只读产品状态（隧道状态、入口打开流的结果），不写产品状态，不新增产品到 Range 的运行期网络依赖。
- 不实现故障注入。

## 7. 依赖与构建

- 新增依赖 `github.com/outpostos/OutpostRange`（私有仓）的生成 proto 包，仅 `internal/observation`（及 `cmd/`）引用。
  `pkg/state`、`pkg/tunnel`、`pkg/service` 不得依赖 OutpostRange，由边界测试强制。
- 构建需要 `GOPRIVATE=github.com/outpostos GONOSUMDB=github.com/outpostos`，私有仓经 SSH
  （`git config url.git@github.com:.insteadOf https://github.com/`）。GitHub Actions 没有私有模块凭据时会拉取失败（CI-01）。

## 8. 落地位置

（阶段 2、3 完成后补全。）
