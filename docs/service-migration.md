# 从 agent 组合宿主迁移到 outpostnet-service（四层改造 票11x）

> 状态：R1 运维说明。本文针对已经在用 `outpostnet-agent` 承担稳定入口的部署：入口和导出写在 agent 配置的
> `imports`/`exports` 里（组合宿主，见 [service-layer.md](service-layer.md) 第 3 节）。本文说明怎样把这部分迁到
> 独立的 `outpostnet-service`，包括配置对照、兼容期行为和回退方法。
> 示例配置在 [`examples/migration/`](../examples/migration/)，由 `pkg/service/migration_example_test.go` 用真实加载器校验。
> 写明“未在实机验证”的内容只有代码和单测依据。

## 1. 为什么迁移，什么时候可以不迁

- 组合宿主的入口不报 Service 事实（[observation.md](observation.md) 第 1 节）。只要入口还在 agent 里，Range 的
  Service 层就一直显示 missing。11-C 滚动升级全程就是这种情况（Range `docs/artifacts/four-layer-11-2026-10-03/11c/test-report.md` G7）。
- 稳定入口、导出表和 Service 配置校验归 `outpostnet-service`（`pkg/service` + `internal/servicehost`）。
  `outpostnet-agent` 只负责 Net 职责：隧道、对等目录、relay、metrics。
- 组合形态仍然兼容：同一份 agent 配置换成新二进制可以继续运行（CHANGELOG 2.0 Compatibility，11-C 实证）。
  如果不需要 Range 的 Service 层，可以继续使用组合形态。

## 2. 目标形态

迁移后同一节点上有两个进程。它们各自建立 QUIC 隧道，彼此不共享连接，所以地址和 socket 都不能重用：

| 项目 | `outpostnet-agent`（只保留 Net 职责） | `outpostnet-service` |
| --- | --- | --- |
| 隧道监听 | 原端口，例如 `5252` | **新端口**，例如 `5253`。QUIC 走 UDP，节点间防火墙要放行该 UDP 端口 |
| `imports` / `exports` | 清空 | 从 agent 原样搬过来 |
| 管理 socket | `/run/outpostnet/outpostnet.sock`（OutpostLet 继续往这里推对等目录） | `/run/outpostnet/service.sock`（默认值） |
| 对等目录来源 | OutpostLet `PUT /v1/peers` | 配置里的 `peers`（每次启动注入），之后可以 `PUT /v1/peers`。见第 5 节 |
| health | `:8082` | `:8083`（默认值），或者绑回环 |
| metrics | `:9090` | 无 |
| 观测端点 | `127.0.0.1:17960`，component 4，报 111 | `127.0.0.1:17961`，component 5，报 116 与本进程隧道的 111 |

这一形态在 11-B 三节点实机上跑通过：Range `docs/artifacts/four-layer-11-2026-10-03/11b/deploy/products-up.sh`
的 `net_agent_config` / `service_config`，快照见同目录 `13-product-snapshots-raw.jsonl.txt`。
示例：迁移前是 [`node1.agent-combined.yaml`](../examples/migration/node1.agent-combined.yaml)，迁移后是
[`node1.agent-net.yaml`](../examples/migration/node1.agent-net.yaml) 加
[`node1.service.yaml`](../examples/migration/node1.service.yaml)。

如果节点上的 agent 除了组合入口没有别的用途（没有 `relays`、没有静态 `tunnels`、不需要 `:9090` metrics），
迁移完成后可以停掉 agent，只留 `outpostnet-service`。这时对等目录的来源要改，见第 5 节方式 B。

## 3. 配置对照（agent → service）

`outpostnet-service` 使用 agent 配置的 schema，并增加 `peers`。差异见下表（完整说明见 [configuration.md](configuration.md)
的 outpostnet-service 一节）：

| agent 配置项 | 迁移到 service 时的处理 |
| --- | --- |
| `node_id` | **必填**，保持不变。只配了 `region`、没有 `node_id` 的 agent 不能直接迁移，要先确定节点身份 |
| `tunnels[] role: server` | 换成新端口（第 2 节）。导出只在本进程的 server 监听上注册 |
| `tunnels[] role: client` + `remote_region` | 只有静态入口（`imports[].name` + `remote_region`）需要。service 启动时会检查每个静态入口都有同 `remote_region` 的 client 隧道，没有就启动失败（agent 对这种配置不报错，入口只是一直不监听） |
| `exports[]` | 原样搬过来。`protocol` 必须是 `tcp` |
| `imports[]` | 原样搬过来，`listen_addr` 不变，应用不用改。`protocol` 必须是 `tcp`。同一 `listen_addr` 不能出现两次 |
| `protocol: udp` | service 直接拒绝。agent 一直把它当 TCP 转发（跨节点 UDP 未实现），所以改成 `tcp` 后实际行为不变 |
| `relays[]` | service 拒绝。relay 留在 agent 上 |
| `auth.token`、`tls` | 原样复制，所有节点的 service 用同一 token |
| `management.socket_path` | 改成和 agent 不同的路径（默认 `/run/outpostnet/service.sock`） |
| `health_addr` | 改成和 agent 不同的地址（默认 `:8083`） |
| `metrics`、`persistence` | 可以保留，service 接受但不使用 |
| `region` | 接受，service 不使用 |
| 新增 `peers[]` | 写对端 `outpostnet-service` 的隧道地址（`<对端 IP>:5253`），**不是**对端 agent 的端口（5252）。service 的入口只能到达它所拨的那个监听上注册的导出 |
| 未知字段 | service 拒绝（agent 忽略）。拼错的字段会在迁移时暴露出来 |

命令行：service 只有 `--config`（必填）、`--node-id`、`--health-addr`、`--watch-interval`、`--observation-addr`。
agent 的 `--region`、`--metrics-addr`、`--persistence-dir` 在 service 上不存在，传了会报错。

两个规则不变：

- 动态路径（`service: <name>@<node_id>`）沿用对等目录的角色规则：`node_id` 字典序小的一方拨号，并且只有拨号方能开流。
  所以消费方节点的 `node_id` 必须小于提供方。agent 和 service 都是这样，迁移不改变哪些入口能用。
- 配置热更新：service 只就地应用 `exports` 的变化（`--watch-interval`，默认 5s），其他变化都要重启。
  `outpostnet-agent` 命令**不监视配置文件**，任何配置变化（包括清空 `imports`/`exports`）都要重启 agent。

## 4. 迁移步骤（逐节点，入口不改地址）

前提：

1. 所有节点的 Net 二进制已经是包含 `outpostnet-service` 的版本。可以先用新二进制和原配置替换 agent（11-C 实证）。
   旧版本节点没有 `outpostnet-service`，不能作为迁移后入口的提供方。
2. Range 已经先升级，并且 range-agent 给每个节点加上 service 的观测端点，例如
   `-observation outpostservice,5,http://127.0.0.1:17961`（11-B 用法），或者在 `ObservationEndpoints` 里加 `"Component": 5`。
3. 节点间防火墙已放行 service 隧道的 UDP 端口。

整个过程分三个阶段。各阶段之间不需要所有节点同时操作，但**阶段 1 必须在所有节点完成后才能开始阶段 2**，
**阶段 2 必须在所有节点完成后才能开始阶段 3**。原因见第 6 节。

### 阶段 0：备份（每个节点）

```bash
cp /etc/outpostnet/agent.yaml /etc/outpostnet/agent.yaml.combined      # 回退用
curl -s --unix-socket /run/outpostnet/outpostnet.sock http://unix/v1/peers > /var/tmp/outpostnet-peers.json
```

`GET /v1/peers` 的输出可以原样 `PUT` 回去，多出的 `status` 字段会被忽略
（`internal/agent/management_restore_test.go`）。agent 重启后可以用这份文件恢复对等目录（见阶段 2）。

### 阶段 1：在所有节点启动 outpostnet-service，只带导出

service 配置：新的隧道端口、`peers`、`exports`（从 agent 复制），先**不写** `imports`。
agent 不动，这时两边都导出同一个本地服务，指向同一个 `local_addr`，互不影响。

验证：

```bash
curl -s http://127.0.0.1:8083/readyz                                     # ok
curl -s --unix-socket /run/outpostnet/service.sock http://unix/v1/peers   # 拨号方节点上对端为 connected
```

应用流量这时还全部走 agent。

### 阶段 2：逐节点把入口从 agent 移到 service

对每个有 `imports` 的节点执行：

1. 从 agent 配置删除 `imports`（保留 `exports`），重启 agent。旧入口地址这时被释放。
2. 恢复 agent 的对等目录：`curl -s -X PUT -H 'Content-Type: application/json' --unix-socket /run/outpostnet/outpostnet.sock http://unix/v1/peers -d @/var/tmp/outpostnet-peers.json`。
   agent 的对等目录只在内存里，重启后是空的。OutpostLet（截至 Let main `b7991b6`）只在自身启动和成员变化时推送，
   不会因为 agent 重启而重推（11-C G1，实测中断 179–198s）。这一步是为了让 agent 的隧道和 111 观测立即恢复。
   迁移后 agent 已经没有入口，所以这一步不影响应用流量。
3. 把原 `imports` 写进 service 配置，重启 service。入口在原 `listen_addr` 上重新监听。
4. 验证入口：`curl` 原入口地址；`curl -s http://127.0.0.1:17961/outpostrange/observation/v1alpha1/snapshot`
   中 `serviceAccess` 列出这些入口。

第 1 步重启 agent 时，agent 隧道上已建立的连接都会被关闭，包括其他尚未迁移的节点经本节点 agent 导出的连接。
这些连接需要应用重试，新连接在 agent 就绪后恢复。

第 1 步到第 3 步之间，该节点的入口不可用，新连接会被拒绝。时长约等于 agent 和 service 各重启一次的时间。
11-C 实测 agent 就绪用时 0.5–0.6s，service 的就绪用时没有在实机测过。顺序不能颠倒：
service 启动时如果入口地址被占用，会直接启动失败（[configuration.md](configuration.md)）。
如果不能接受这个间隙，可以给 service 入口换一个新 `listen_addr`，先启动 service，再让应用切到新地址，
最后删除 agent 的 `imports`。代价是应用配置要改。

### 阶段 3：所有节点完成阶段 2 后，清理 agent 的导出

从 agent 配置删除 `exports`，重启 agent，然后按阶段 2 第 2 步恢复对等目录。
结果和 [`node1.agent-net.yaml`](../examples/migration/node1.agent-net.yaml) 一致。

完成后的检查：

- Range 中每个节点的 Service 层是 reported（来自 component 5 的 116），不再是 missing。
- `GET /v1/peers` 在两个 socket 上都正常。agent 上是 OutpostLet 推送的表，service 上是 `peers` 种子或者第 5 节方式 B 的推送。

## 5. service 的对等目录由谁提供

对等目录 `node_id → 隧道地址` 是 Net 宿主的管理输入。迁移后真正用它解析动态入口的是 service。
OutpostLet 的 `outpostnet-publisher` 只往一个 socket 推送（`outpostNet.socketPath`），每个成员只公布一个
`outpostNet.tunnelPort`。因此有两种方式：

- **方式 A：静态种子（11-B 实机验证过）。** Let 继续推送给 agent，service 用配置里的 `peers`。
  service 每次启动都会重新注入 `peers`，所以 service 重启后不会出现 G1 那种空目录。
  代价是成员或地址变化不会自动同步：要么改 `peers` 并重启 service，要么 `PUT /v1/peers`（运行期 PUT 的内容不持久化，
  service 重启后回到配置里的种子）。
- **方式 B：Let 改推 service。** 把 Let 的 `outpostNet.socketPath` 改成 `/run/outpostnet/service.sock`，
  `outpostNet.tunnelPort` 改成 service 的隧道端口（所有节点一致）。这样 service 的目录跟随 Let 成员视图变化，
  Let 的整表 `PUT` 会覆盖启动种子。agent 就不再收到对等目录。如果 agent 也不承担 relay、静态隧道或 metrics，可以停掉。
  这条路径用的是和 agent 相同的管理 API（`internal/servicehost/host_test.go` 用 `PUT /v1/peers` 做实例替换），
  **但没有在实机验证**。另外，改 Let 配置需要重启 Let。按 Let 官方 unit 停止 outpostlet 会连带停掉本节点的工作负载容器
  （11-C G3），重启前先确认 Let 侧的处理方式。

对 Outpost OS 节点来说，成员视图的权威来源是 Let，所以长期形态应该是方式 B。
方式 A 适合静态部署，也可以作为迁移过渡阶段使用。

## 6. 兼容期行为

迁移过程中会出现三种情况：有的节点已迁移，有的没有；同一节点上 agent 和 service 同时导出。

- **入口在 service 上的节点**拨的是对端 service 的隧道端口，只能到达对端 service 注册的导出。
  所以所有提供方都要先完成阶段 1，消费方才能进入阶段 2。
- **入口还在 agent 上的节点**拨的是对端 agent 的端口，依赖对端 agent 的导出。
  所以所有消费方都要先完成阶段 2，提供方才能进入阶段 3。
- 同一服务在 agent 和 service 上同时导出时，两条路径各自独立，指向同一个本地实例，没有冲突。
- 观测：agent（component 4）和 service（component 5）是两条独立的流。迁移期间 Service 层只覆盖已迁到 service 的入口，
  还在 agent 上的入口在 Range 中仍是 missing。
- 协议：agent 和 service 使用同一套 `pkg/tunnel` 隧道协议和 token 认证，迁移不改变线上协议。

## 7. 回退

任一阶段都可以回到组合形态：

1. 停止 `outpostnet-service`，释放入口地址和隧道端口。
2. 恢复 `agent.yaml.combined`，重启 agent。
3. 恢复 agent 对等目录：用阶段 0 保存的文件 `PUT /v1/peers`；或者等 Let 下一次推送或重启 Let。
   如果跳过这一步，agent 的动态入口会一直不可用，直到 Let 重推（11-C G1）。
4. 如果用了第 5 节方式 B，同时把 Let 的 `outpostNet.socketPath` / `tunnelPort` 改回 agent 的值并重启 Let。

回退后 Range 的 Service 层回到 missing。阶段 3 之前 agent 一直保留导出，所以在阶段 1、2 中回退本节点时，
其他节点仍可经 agent 路径访问本节点的导出；agent 重启期间已建立的连接会被关闭，之后的新连接正常。
阶段 3 之后回退，要先在提供方节点恢复 agent 的 `exports`，消费方节点才能回到 agent 入口（顺序与第 6 节相反）。

## 8. 未覆盖与待验证

- 整个迁移流程（阶段 1–3 与回退）没有在 VM 上完整跑过。实机证据只有 11-B 的目标形态和 11-C 的组合形态升级。
  待实机复验：阶段 2 每个节点的入口中断时长；方式 B（Let 推送 service）；回退第 3 步恢复对等目录后动态入口是否立即可用。
- agent 重启后对等目录为空（11-C G1）是产品行为，本文只给出运维恢复方法，没有修复它。
- 入口不承诺在途连接无损迁移（[service-layer.md](service-layer.md) 第 4 节）。阶段 2 中被关闭的连接需要应用自己重试。
