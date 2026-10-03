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
| `pkg/tunnel/client.go` `Client.ServeImport` | 早期单连接版本的本地监听导入 | 历史 API，保留兼容；新代码不使用 |
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

**未完全拆开的部分（如实说明）**：`internal/agent` 同时是 Net 宿主和旧命令 `outpostnet-agent` 的**组合宿主**。为兼容旧配置（`imports`/`exports` 写在同一份 agent 配置里），它仍 import `pkg/service`，在包内组装旧配置的导入（`service.NewEntry`）与导出（`service.RegisterExports`、`ReloadAgent` 中的 `service.ApplyExports`）。也就是说 Net 宿主代码与旧入口的 Service 组装仍同在 `internal/agent` 一个包里，本票没有重写 `TunnelManager`。真正的通用层是 `pkg/tunnel`、`pkg/tunnel/tlsutil`（以及共享配置 `pkg/agentconfig`），它们不依赖 Service。

边界测试覆盖范围（`pkg/service/boundary_test.go`）：

| 测试 | 覆盖包 | 检查内容 |
|------|--------|----------|
| `TestNetCommonLayerDoesNotImportService` | `pkg/tunnel`、`pkg/tunnel/tlsutil`、`pkg/protocol`、`pkg/state`、`pkg/agentconfig` | 传递依赖中无 `pkg/service`、`internal/servicehost`、`internal/agent` |
| `TestServiceDomainDoesNotDependOnTransportOrHosts` | `pkg/service` | 传递依赖中无 `pkg/tunnel*` 与任何 `internal/*` |
| `TestNetHostStreamAPIIsServiceAgnostic` | `internal/agent`（仅 `TunnelManager` 导出方法签名） | 签名不使用 `pkg/service` 类型；**不**检查 `internal/agent` 的 import（组合宿主允许 import `pkg/service`） |

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

- 动态路径沿用对等目录的角色规则：`node_id` 字典序小的一方拨号，且只有拨号方能打开服务流，因此消费方节点的 `node_id` 必须小于提供方；反向访问需要静态路径或后续改造。
- `peers` 仅在启动时注入；运行期地址变化只通过 `PUT /v1/peers`（整表替换）。配置文件中 node_id/tunnels/imports/auth/tls/management/health_addr/peers 的变化需重启，watcher 只应用 exports 并对其余变化记录一条告警（`Host.sameRestartScope`）。比较对象是“重新套用同样命令行覆盖（`--node-id`、`--health-addr`）后的文件配置”与运行中配置，因此命令行覆盖本身不会导致每次 reload 误告警。
- 端点切换期间（重连退避初始 1s）新连接会被入口立即关闭，不排队等待；应用需自行重试。
- 不承诺在途连接无损迁移（见第 4 节）。
- 本票只在单机多进程/同进程多监听上验证；多台物理机/真实网络下未验证。
- `tunnel.ManagedClient.GracefulShutdown` 开始后不再接受新流（`OpenServiceStream`/`OpenRelayStream` 返回 `net.ErrClosed`），只等待已登记的在途流，超时后强制关闭。（票09b 已修复原 `sync.WaitGroup` 在等待期间被 `Add` 的竞态。）
