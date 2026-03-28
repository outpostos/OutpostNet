# OutpostNet v2 - Epic & Story Overview

**Based on:** [PRD](./prd.md)
**Date:** 2026-03-02 (Epic 1-5)
**Status:** Epics 1-5 Done

---

## Epic Dependency Graph

```
Epic 1: 单节点 Pod 互通（手动配置）   ← 证明核心机制可行
    │
    ▼
Epic 2: Controller + Webhook（自动化） ← 把 Epic 1 的手动过程自动化
    │
    ▼
Epic 3: QUIC Tunnel Core              ← 跨区域的基础
    │
    ▼
Epic 4: 跨区域服务访问                  ← 端到端
    │
    ▼
Epic 5: Relay 中转 (Growth)            ← 不可直连区域中转
```

---

## Epic 1: 单节点 Pod 互通（手动配置）

**目标：** Server + Sidecar 架构，实现同一边缘节点上 Pod 间 TCP/UDP 服务互访
**状态：** Done
**交付物：** `cmd/outpostnet-server`, `cmd/outpostnet-sidecar`, `pkg/protocol`, `pkg/config`

| Story | 标题 | 文件 |
|-------|------|------|
| S1 | 项目脚手架与构建系统 | [s01-project-scaffold.md](./stories/epic-1/s01-project-scaffold.md) |
| S2 | Server-Sidecar 通信协议与连接管理 | [s02-protocol-connection.md](./stories/epic-1/s02-protocol-connection.md) |
| S3 | TCP 服务导出与导入 | [s03-tcp-export-import.md](./stories/epic-1/s03-tcp-export-import.md) |
| S4 | UDP 服务导出与导入 | [s04-udp-export-import.md](./stories/epic-1/s04-udp-export-import.md) |
| S5 | 配置文件格式与加载 | [s05-config-file.md](./stories/epic-1/s05-config-file.md) |
| S6 | K8s 部署清单与端到端验证 | [s06-k8s-deployment.md](./stories/epic-1/s06-k8s-deployment.md) |

```
S1 → S2 → S3 (可与 S4 并行) → S5 → S6
          └→ S4 ──────────────┘
```

---

## Epic 2: Controller + Webhook（自动化）

**目标：** 将 Epic 1 的手动配置过程自动化
**状态：** Done
**交付物：** `cmd/outpostnet-controller`, `pkg/annotations`, `internal/controller/`

| Story | 标题 | 文件 |
|-------|------|------|
| S7 | Controller 项目脚手架 | [s07-controller-scaffold.md](./stories/epic-2/s07-controller-scaffold.md) |
| S8 | 注解规范与解析库 | [s08-annotation-spec.md](./stories/epic-2/s08-annotation-spec.md) |
| S9 | Mutating Webhook — Sidecar 自动注入 | [s09-mutating-webhook.md](./stories/epic-2/s09-mutating-webhook.md) |
| S10 | ConfigMap 自动生成 | [s10-configmap-reconciler.md](./stories/epic-2/s10-configmap-reconciler.md) |
| S11 | EndpointSlice 与 Service 管理 | [s11-endpointslice.md](./stories/epic-2/s11-endpointslice.md) |
| S12 | K8s E2E 集成验证 | [s12-k8s-e2e.md](./stories/epic-2/s12-k8s-e2e.md) |

```
S7 → S8 → S9 → S10 → S11 → S12
```

---

## Epic 3: QUIC Tunnel Core

**目标：** 构建 QUIC 隧道库，Service 级 Stream 多路复用
**状态：** Done
**交付物：** `pkg/tunnel/`

| Story | 标题 | 文件 |
|-------|------|------|
| S13 | QUIC 隧道库脚手架与 TLS 工具 | [s13-quic-scaffold.md](./stories/epic-3/s13-quic-scaffold.md) |
| S14 | Tunnel Server — 接受连接与 Stream 路由 | [s14-tunnel-server.md](./stories/epic-3/s14-tunnel-server.md) |
| S15 | Tunnel Client — 拨号与 Stream 转发 | [s15-tunnel-client.md](./stories/epic-3/s15-tunnel-client.md) |
| S16 | Service-Stream 多路复用 | [s16-service-stream-mux.md](./stories/epic-3/s16-service-stream-mux.md) |
| S17 | 连接生命周期 — Keepalive、重连、优雅关闭 | [s17-connection-lifecycle.md](./stories/epic-3/s17-connection-lifecycle.md) |
| S18 | 吞吐基准测试与集成测试 | [s18-benchmarks.md](./stories/epic-3/s18-benchmarks.md) |

```
S13 → S14 → S15 → S16 → S17 → S18
```

---

## Epic 4: 跨区域服务访问 (Layer 2)

**目标：** Agent + Controller 集成，跨区域端到端
**状态：** Done
**交付物：** `cmd/outpostnet-agent`, `internal/agent/`, `pkg/agentconfig/`

| Story | 标题 | 文件 |
|-------|------|------|
| S19 | Agent 脚手架与配置格式 | [s19-agent-scaffold.md](./stories/epic-4/s19-agent-scaffold.md) |
| S20 | Agent 隧道管理 — QUIC 隧道建立与桥接 | [s20-agent-tunnel.md](./stories/epic-4/s20-agent-tunnel.md) |
| S21 | 配置持久化与断网恢复 | [s21-config-persistence.md](./stories/epic-4/s21-config-persistence.md) |
| S22 | Controller 跨区域隧道配置生成 | [s22-controller-tunnel-config.md](./stories/epic-4/s22-controller-tunnel-config.md) |
| S23 | Prometheus 指标与可观测性 | [s23-prometheus-metrics.md](./stories/epic-4/s23-prometheus-metrics.md) |
| S24 | 跨区域端到端 E2E 测试 | [s24-cross-region-e2e.md](./stories/epic-4/s24-cross-region-e2e.md) |

```
S19 → S20 → S21 → S22 → S24
             └→ S23 ────┘
```

---

## Epic 5: Relay 中转 (Growth)

**目标：** 不可直连区域通过第三方中转
**状态：** Done
**交付物：** `internal/agent/relay*`, `pkg/agentconfig` (relay extensions)

| Story | 标题 | 文件 |
|-------|------|------|
| S25 | Relay 配置模型 | [s25-relay-config.md](./stories/epic-5/s25-relay-config.md) |
| S26 | Agent Relay 转发 | [s26-agent-relay-forwarding.md](./stories/epic-5/s26-agent-relay-forwarding.md) |
| S27 | Controller Relay 路径计算 | [s27-controller-relay-path.md](./stories/epic-5/s27-controller-relay-path.md) |
| S28 | Relay 指标与可观测性 | [s28-relay-metrics.md](./stories/epic-5/s28-relay-metrics.md) |
| S29 | Relay 端到端 E2E 测试 | [s29-relay-e2e.md](./stories/epic-5/s29-relay-e2e.md) |

```
S25 → S26 → S27 → S29
             └→ S28 ┘
```

---

## Component-Epic Mapping

| 代码组件 | Epic |
|---------|------|
| `cmd/outpostnet-server` | Epic 1 |
| `cmd/outpostnet-sidecar` | Epic 1 |
| `cmd/outpostnet-controller` | Epic 2 |
| `cmd/outpostnet-agent` | Epic 4 |
| `pkg/protocol` | Epic 1 |
| `pkg/config` | Epic 1 |
| `pkg/annotations` | Epic 2 |
| `pkg/tunnel` | Epic 3 |
| `pkg/agentconfig` | Epic 4, 5 |
| `pkg/version` | Epic 1 |
| `internal/server` | Epic 1 |
| `internal/sidecar` | Epic 1 |
| `internal/controller` | Epic 2, 4, 5 |
| `internal/agent` | Epic 4, 5 |
