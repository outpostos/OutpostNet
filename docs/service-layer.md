# Service 层：稳定服务访问与 Net 通用通信的分离（四层改造 票09）

> 状态：R1 实施说明。描述的是本仓库当前代码事实与本票改动；未实现的能力明确标注“未实现”。
> 第 3–5 节的设计已在本票阶段 2（分离）与阶段 3（命令/样例/双节点测试）落地，代码与测试位置见第 6 节。
> 上位文档：工作空间 `docs/adr/0002-distributed-primitives-and-service-layers.md`、`CONTEXT.md`（身份寻址 `service@node_id`）。

## 1. 改造前现状核对（BASE 66b08e7）

| 位置 | 内容 | 归属判定 |
|------|------|----------|
| `pkg/tunnel/tunnel.go` `Listen`/`Dial`、`ServerConfig`/`ClientConfig` | QUIC 监听/拨号、TLS、keepalive | Net 通用连接 |
| `pkg/tunnel/auth.go` + `client.go dialAndAuth` + `server.go authenticateConn` | 连接级 token 认证（首条流握手，长度上限 1024） | Net 通用连接 |
| `pkg/tunnel/stream.go` | 流头 wire：`NameLen(2B)+Name`，relay 位 + HopCount；名字上限 255 | Net 通用流（名字只是流的目标标签，不承载业务语义） |
| `pkg/tunnel/lifecycle.go` `ManagedClient` | 自动重连、指数退避、`UpdateServerAddr` 换地址重拨、在途流计数与优雅关闭 | Net 通用重连 |
| `pkg/tunnel/server.go` `AddRoute/RemoveRoute` + `handleStream` | 按流头名字把流转发到本地 TCP 地址；无路由时走 fallback（relay） | 路由表机制属 Net；**“哪个业务服务导出到哪个本地地址”是 Service 决策** |
| `pkg/tunnel/client.go` `Client.ServeImport` | 早期单连接版本的本地监听导入 | 历史 API，票09 时保留兼容；票11-D 确认全组织无调用方后已删除（本地入口由 `pkg/service.Entry` 承担） |
| `internal/agent/tunnel_manager.go` `listenAndProxy`/`listenAndProxyDynamic`/`serveImport`/`serveDynamicImport` | 导入：本地稳定监听，每个连接打开一条服务流并双向转发 | **Service 领域（导入 / 稳定入口）**，与连接管理混在同一类型里 |
| `internal/agent/tunnel_manager.go` `startServer` 中 `for exports: srv.AddRoute` | 导出注册 | **Service 领域（导出）** |
| `internal/agent/tunnel_manager.go` 其余（`startClient`、`AddOrUpdatePeerClient`、`DynamicClient` 等） | 按 region / node_id 管理隧道客户端 | Net 宿主（连接管理） |
| `internal/agent/peer_directory.go` | `node_id → addr` 目录、角色仲裁、Unix socket 管理 API `PUT/GET /v1/peers` | Net 宿主的管理输入（复用，不改） |
| `internal/agent/relay.go` | 多跳流转发 | Net 通用流转发 |
| `internal/agent/config_watcher.go` `ReloadAgent` | 轮询配置文件；导出只做“新增名字/删除名字”，**同名换地址不生效** | 导出变更属 Service；该缺陷影响“换端口”场景 |
| `internal/server` + `internal/sidecar` + `pkg/protocol` | 节点内 Unix socket 路由中枢，sidecar 支持 TCP **与 UDP** | 节点内本地通信，不跨节点、不经 QUIC 隧道 |

结论：通用连接/流/重连已集中在 `pkg/tunnel`，且 `pkg/tunnel` 不依赖任何服务领域代码；混杂点在 `internal/agent.TunnelManager`，它同时承担“隧道连接管理”和“业务导入/导出”。

## 2. 未合并工作区 OutpostNet-t05（feat/t05-dns-proxy）对照（只读核对）

t05 未提交内容：`pkg/agentconfig/meshconfig.go`（`mesh.gateway_addr/dns/proxy/directory`）、`internal/agent/service_directory.go`（服务实例本地目录）、`internal/agent/mesh_subscriber.go`（订阅 OutpostMesh `StreamEntities` 维护目录，依赖 OutpostMesh gRPC 生成代码）、`internal/agent/directory_cache.go`（目录冷启动缓存）、`internal/meshdns/`（本地 DNS 把服务名解析到本地代理 IP），以及 `agentconfig.Config` 新增 `Mesh` 字段、go.mod 新依赖。

票09 的避让方式：

- **不实现 DNS**、不实现服务名解析；稳定入口是显式配置的本地 `listen_addr`。
- **不实现 Mesh 订阅目录**、不实现目录缓存、不新增任何目录复制/同步机制，不引入 OutpostMesh 依赖，不改 go.mod。
- **不修改 `pkg/agentconfig/agentconfig.go`**（t05 修改的同一文件），避免合并冲突；Service 配置复用 agentconfig 的既有类型。
- 不在 `internal/agent` 新增 `ServiceDirectory`/`ServiceInstance` 等与 t05 同名类型；Service 领域类型放在新包 `pkg/service`。
- 后续接线方式（留给后续票）：t05 的目录若要驱动 Service，只需实现 `pkg/service` 的 `StreamOpener`/目标解析，或把目录结果翻译成现有 `PUT /v1/peers` 输入；本票不做 Service→Mesh 目录接线。

## 3. 模块边界与 import 方向

```
cmd/outpostnet-service ──► internal/servicehost ──► pkg/service (Service 领域：Ref/Target/Entry/RegisterExports+ApplyExports/配置校验)
                                   │                     │
                                   ▼                     ▼
                           internal/agent ──────► pkg/agentconfig
                           (Net 宿主：TunnelManager/PeerDirectory/ManagementServer/ConfigWatcher)
                                   │
                                   ▼
                           pkg/tunnel, pkg/tunnel/tlsutil (Net 通用连接/流/重连)

cmd/outpostnet-agent ──► internal/agent（旧入口的组合宿主：Net 宿主 + 旧配置导入/导出组装，后者调用 pkg/service）
```

规则（1–3 由 `pkg/service/boundary_test.go` 解析源码强制，覆盖范围见下表；4 为约定）：

1. Net 通用包 `pkg/tunnel`、`pkg/tunnel/tlsutil`、`pkg/protocol`、`pkg/state`、`pkg/agentconfig` 的传递依赖中不得出现 `pkg/service`、`internal/servicehost`、`internal/agent`。
2. `pkg/service` 不依赖 `pkg/tunnel` 与任何 `internal/*`：它通过 `StreamOpener`（打开到某服务目标的双向流）和 `RouteTable`（`AddRoute/RemoveRoute`）两个小接口使用 Net，具体传输由宿主注入。
3. `internal/agent` 的流接口以传输术语表达：`TunnelManager.OpenPeerStream(ctx, nodeID, serviceName)` / `OpenRegionStream(ctx, region, serviceName)` 不携带任何 `pkg/service` 类型；Service 的 `Target` 由 `pkg/service` 侧适配器 `service.NetOpener` 映射到这两条传输路径（含流头约束校验）。由 `pkg/service/boundary_test.go` 的 `TestNetHostStreamAPIIsServiceAgnostic` 检查 `TunnelManager` 导出方法签名。
4. `internal/servicehost` 复用 `internal/agent` 的隧道管理、对等目录与管理 API，不复制它们。

**未完全拆开的部分（如实说明）**：`internal/agent` 同时是 Net 宿主和旧命令 `outpostnet-agent` 的**组合宿主**。为兼容旧配置（`imports`/`exports` 写在同一份 agent 配置里），它仍 import `pkg/service`，在包内组装旧配置的导入（`service.NewEntry`）与导出（`service.RegisterExports`、`ReloadAgent` 中的 `service.ApplyExports`）。也就是说 Net 宿主代码与旧入口的 Service 组装仍同在 `internal/agent` 一个包里，本票没有重写 `TunnelManager`。组合宿主的入口不报 Service 观测事实；把已有部署的入口迁到 `outpostnet-service` 的步骤见 [service-migration.md](service-migration.md)（票11x）。真正的通用层是 `pkg/tunnel`、`pkg/tunnel/tlsutil`（以及共享配置 `pkg/agentconfig`），它们不依赖 Service。

边界测试覆盖范围（`pkg/service/boundary_test.go`）：

| 测试 | 覆盖包 | 检查内容 |
|------|--------|----------|
| `TestNetCommonLayerDoesNotImportService` | `pkg/tunnel`、`pkg/tunnel/tlsutil`、`pkg/protocol`、`pkg/state`、`pkg/agentconfig` | 传递依赖中无 `pkg/service`、`internal/servicehost`、`internal/agent` |
| `TestServiceDomainDoesNotDependOnTransportOrHosts` | `pkg/service` | 传递依赖中无 `pkg/tunnel*` 与任何 `internal/*` |
| `TestNetHostStreamAPIIsServiceAgnostic` | `internal/agent`（仅 `TunnelManager` 导出方法签名） | 签名不使用 `pkg/service` 类型；**不**检查 `internal/agent` 的 import（组合宿主允许 import `pkg/service`） |
| `TestNetLibrariesDoNotDependOnOutpostRange`（票10-B） | `pkg/state`、`pkg/tunnel*`、`pkg/service`、`pkg/protocol`、`pkg/agentconfig` | 自身及模块内传递依赖不 import OutpostRange（观测契约只在 `internal/observation` 与宿主层使用，见 `docs/observation.md`） |

职责：

- **Net（pkg/tunnel + internal/agent 连接管理）**：连接、认证、流头、重连/换地址、在途流计数、relay。
- **Service（pkg/service + internal/servicehost + cmd/outpostnet-service）**：稳定服务身份 `service@node_id`、稳定入口（本地监听，每个新连接按当前隧道状态打开流）、导出表（新增/换地址/删除）、Service 配置校验。

## 4. 稳定入口与端点变化的语义

- 应用只认识稳定入口地址（例如 `127.0.0.1:18080`）和其背后的稳定服务身份（例如 `echo@node-b`）。
- 入口对**每个新连接**重新向 Net 宿主要流：动态路径按 `node_id` 查当前隧道客户端，该客户端的远端地址来自对等目录（`PUT /v1/peers`）。实例换节点地址/端口后，管理输入更新目录 → `ManagedClient.UpdateServerAddr` 关闭旧连接并重拨新地址 → 后续新连接到达新实例。
- 实例在同节点换本地端口：导出表把同名服务的路由改到新地址（`service.ApplyExports` 支持同名换址），后续新流到达新实例。
- **不承诺在途连接无损迁移**：
  - 隧道换地址时，旧 QUIC 连接被关闭，其上的在途流随之中断，应用侧旧 TCP 连接被关闭（现有 `UpdateServerAddr` 契约）。
  - 仅导出换本地端口时，路由只在流打开时查找，已建立的流继续连着旧实例直到任一端关闭。
- 宿主关闭（`outpostnet-service` 收到信号或启动中途出错）：先取消并等待全部入口——入口立即停止接受新连接，在途连接有 `ShutdownGrace`（默认 5s）宽限，之后由入口关闭；入口全部返回后才依次停止 health、管理 API 与隧道。`Run` 返回时不残留入口 goroutine，入口端口已释放。
- 认证、流头长度约束、优雅关闭（`StreamDone` 计数）与重连退避均沿用 `pkg/tunnel`，本票不改。

## 5. 协议支持（如实记录）

| 路径 | TCP | UDP |
|------|-----|-----|
| 跨节点静态路径（`tunnels` + `imports[].name/remote_region`，QUIC 流） | 支持 | **未实现**。`outpostnet-agent` 对 `protocol: udp` 只做配置校验，实际仍按 TCP 监听与转发（历史行为，保持兼容） |
| 跨节点动态路径（`imports[].service: name@node_id` + 对等目录，QUIC 流） | 支持 | **未实现**（同上） |
| `outpostnet-service` 稳定入口/导出 | 支持 | **拒绝配置**：校验阶段报错，避免把 UDP 静默当 TCP |
| 节点内 `outpostnet-server` + `outpostnet-sidecar`（Unix socket） | 支持 | 支持（仅节点内，不跨节点，不经 QUIC 隧道） |

跨节点 QUIC 上的 UDP（datagram 或流封装）在本仓库**未实现**；节点内 sidecar 的 UDP 能力不能视为跨节点 UDP。

## 6. 落地位置与验证

| 内容 | 位置 |
|------|------|
| Service 领域包 | `pkg/service/`：`target.go`（`Ref`/`Target`/`StreamOpener`）、`entry.go`（稳定入口）、`exports.go`（导出表）、`config.go`（Service 配置） |
| 边界测试 | `pkg/service/boundary_test.go`（源码 import 图，非 `_test.go` 文件） |
| Service 宿主 | `internal/servicehost/host.go`（复用 `agent.TunnelManager`/`PeerDirectory`/`ManagementServer`/`ConfigWatcher`） |
| 命令 | `cmd/outpostnet-service`（`make build-service`） |
| 旧入口改造 | `internal/agent/tunnel_manager.go`：静态/动态导入改为 `service.Entry`，`TunnelManager.OpenPeerStream`/`OpenRegionStream` 以传输术语开流，经 `service.NetOpener` 适配为 `service.StreamOpener`；导出走 `service.RegisterExports`；`config_watcher.go`：`ReloadAgent` 走 `service.ApplyExports`，`ConfigWatcher.SetLoader` |
| 双节点测试 | `internal/servicehost/host_test.go`：两套独立 Host（各自真实 QUIC 监听）+ 真实 TCP 应用 + token 认证；覆盖实例替换（换隧道端口，经 `PUT /v1/peers`）、同节点换端口（`ApplyExports` 与配置文件两种入口）、在途连接行为、错误 token |
| 可运行样例 | `examples/service/`（三个独立进程：node-a、node-b、node-b 替换实例） |

## 7. 已知限制

**V2-NET-3 更新（ADR-0008）**：配置 `tls.ca_file` 时，双向 TLS 以证书
URI SAN 认证节点，握手声明必须匹配证书；同组允许服务流。反向流无需 token，
动态拨号也校验目标节点身份。下述“自报身份/需要 token”只适用于未配置 CA 的
旧模式。签发、配置、时间无关校验与安全计数见 [节点证书](node-certificates.md)。


- 动态路径仍沿用对等目录的角色规则（`node_id` 字典序小的一方拨号），但**双方都能打开服务流**（P1-NET-5，ADR-0003 D5）：拨号方在 token 之后于同一认证流追加 `node_id` 声明（`[1B 版本=1][1B 长度 1–255][node_id]`），接受方回 `authOK` 后读取并把该连接登记为此 `node_id` 的反向通道；`TunnelManager.OpenPeerStream` 在本方不是拨号方时经该连接开流，拨号方用本节点隧道服务端的同一路由表处理反向流。同一 `node_id` 的新连接替换旧登记，连接关闭即注销；换地址（`PUT /v1/peers`）后在途反向流按现有契约随旧连接中断。
  - 声明是**自报的**，只在共享 token 认证成功后被采信，**不构成节点认证**；持有 token 的一方可冒用任意 `node_id`。节点级认证与授权按架构 v0.1 §3.9 另行处理。
  - 反向流需要配置 `auth.token`（无 token 时没有认证流，也就没有声明）；拨号方需要至少一个 `role: server` 隧道提供路由表，否则反向流被拒绝并记日志。
  - 兼容：旧拨号方不发声明，接受方按旧行为处理（不能反向开流）；旧接受方不读取声明字节，新拨号方的正向访问不受影响。
- `peers` 仅在启动时注入；运行期地址变化只通过 `PUT /v1/peers`（整表替换）。配置文件中 node_id/tunnels/imports/auth/tls/management/health_addr/peers 的变化需重启，watcher 只应用 exports 并对其余变化记录一条告警（`Host.sameRestartScope`）。比较对象是“重新套用同样命令行覆盖（`--node-id`、`--health-addr`）后的文件配置”与运行中配置，因此命令行覆盖本身不会导致每次 reload 误告警。
- 端点切换期间（重连退避初始 1s）新连接会被入口立即关闭，不排队等待；应用需自行重试。
- 不承诺在途连接无损迁移（见第 4 节）。
- 本票只在单机多进程/同进程多监听上验证；多台物理机/真实网络下未验证。
- `tunnel.ManagedClient.GracefulShutdown` 开始后不再接受新流（`OpenServiceStream`/`OpenRelayStream` 返回 `net.ErrClosed`），只等待已登记的在途流，超时后强制关闭。（票09b 已修复原 `sync.WaitGroup` 在等待期间被 `Add` 的竞态。）

## 8. 逻辑服务目录（P1-NET-2，ADR-0003 D2）

`internal/meshdirectory` 实现 `service.Directory`：订阅本节点 Mesh 网关 `GET /v1alpha1/entities:stream?filter.entityIdPrefix=urn:outpostos:`（SSE），用本地最小 JSON 结构体解码（**不**在 `go.mod` 引入 OutpostMesh，`gomod_test.go` 守护）。

- 快照在 `SNAPSHOT_COMPLETE` 时原子替换；`DELTA` 按实体更新，`DELETED`/`EXPIRED`/`is_live=false` 移除（`EXPIRED` 事件中 `isLive` 仍为 true，按事件类型判断）。
- 断线以 100ms 起、上限 5s 的指数退避重连并携带最后确认的 `resumeCursor`；游标不可用时 Mesh 回落为新快照，按首个快照处理。
- V2-NET-2：同一 SSE 流按组件分派服务登记与节点租约。新式登记按本地单调钟上的租约序号前进、incarnation 匹配判定；过渡期旧式登记继续按 `expiry_time` 判定。详见 [租约目录说明与验证状态](v2-net2-directory-leases.md)。
- 冷启动缓存 `<data-dir>/service-directory.json`：快照完成时与之后至多每 5s 写一次（临时文件 + fsync + rename）；启动时载入并标记 `Stale=true`，首个快照完成后清除；断线期间 `Stale=true`，缓存租约视为刚看到，最多再信任一个 `LeaseTTL`，旧式实例按 TTL 剔除；损坏的缓存告警后忽略。V2 缓存保存已校验的写者与 incarnation，拒绝缺少写者信息的 V1 缓存并等待新快照。
- Mesh 的权重在端点上，实例取其首个 TCP 端点的权重。
- 契约样本：`internal/meshdirectory/testdata/*.sse` 为真实网关（OutpostMesh f0ab178）录制的 SSE 输出；`-tags integration` 的 `TestRealMeshGateway` 对真实二进制验证（`OUTPOSTMESH_GATEWAY_BIN`）。
- 本票只提供目录；入口与导出接入见 P1-NET-3 / P1-NET-4。

## 9. 逻辑入口（P1-NET-3，ADR-0003 D3）

`imports[].service` 不含 `@` 时为逻辑入口（如 `service: echo`），需要 `mesh.gateway_url`。入口用 `service.LogicalOpener` 代替 `NetOpener`：

- 每个新连接：`Directory.Instances` → `Select`（先过滤后排序，见 `pkg/service/directory.go`）→ 依次尝试。本节点候选直拨 `127.0.0.1:<port>`；远端候选经 `TunnelManager.OpenConfirmedPeerStream`（拨号方或接受方连接均可，见 §7）。单次尝试上限 3s。
- **判定点**：远端候选以“确认字节”为准。开流时流头 `NameLen` 置 bit 14，提供方 `tunnel.Server` 在拨通本地后端后先回 1 字节（`0` 已连接 / `1` 无路由 / `2` 后端拨号失败），之后才开始转发；带确认的流不走中继兜底。入口在 `OpenStream` 返回前不读取应用连接的任何字节，所以确认前的任何失败都换下一个候选，确认后的失败只关闭连接，不重发。
- 被拒绝的流（无路由、后端拨号失败或流头读取失败）会完整结束收发两方向、释放 QUIC 流额度；同一对等连接在后端恢复后仍可开新流。
- 无候选时立即关闭连接并计数：`no_candidate`（目录新鲜但无可用实例）、`all_unreachable`（有候选但都失败）、`directory_stale`（无可用实例且目录未同步或已断线）。目录 stale 时仍照常选择，成功计入 `ResolvedFromStale` 并记日志。计数见 `Host.LogicalStats`；观测事实 `ServiceAccessChanged` 的 backend 为实际选中的节点与地址（本地为 `127.0.0.1:<port>`，远端为 `<node_id>/<service>`），`no_candidate` / `directory_stale` 记为 `NO_BACKEND`，`all_unreachable` 记为 `UNREACHABLE`。实例 ID 与尝试次数只进日志，不在观测契约中。
- 兼容：只有逻辑入口使用确认；旧版提供方不认识 bit 14，会把流当成超长名称关闭，入口记为不可达并换下一个候选。逻辑服务要求承载节点与消费节点都升级。`name@node` 与静态入口不变。

## 10. 目录驱动导出（P1-NET-4，ADR-0003 D4）

配置 `mesh.gateway_url` 后，`outpostnet-service` 按目录自动维护本节点的导出路由（`internal/servicehost/direxports.go`），远端逻辑入口无需本节点写静态 `exports`：

- 对目录中每个服务，取 `node_id == 本节点` 的实例，按 `service.Select` 的过滤与排序取首个，导出 `<service_name> → 127.0.0.1:<port>`。订阅目录变更即时对账，另每 1s 兜底对账一次。
- 实例变为 `DRAINING`/`NOT_READY`、`is_live=false`、节点租约失效或旧式 TTL 到期时路由移除；端口变化时路由原地改址，已建立的流保持原后端（路由只在开流时查找）。
- 与静态 `exports` 同名时静态生效，并按服务名每分钟至多告警一次；静态导出移除后目录导出接管，目录登记也消失后路由删除，不残留。
- 目录 stale（Mesh 不可达）时继续使用最后的目录内容，实例继续按节点租约或旧式 TTL 失效。
- 路由表 = 静态导出 ∪ 目录导出（静态优先），`Host.ApplyExports` 与配置文件热更新只改静态部分；`Host.ExportRoutes` 返回当前生效的路由。

