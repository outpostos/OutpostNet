# OutpostNet v2 - Epic & Story Breakdown

**Based on:** [PRD](./prd.md)
**Date:** 2026-03-02

---

## Epic Overview

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
Epic 5: Relay 中转 (Growth)
```

---

## Epic 1: 单节点 Pod 互通（手动配置）

**目标：** Server + Sidecar 架构，实现同一边缘节点上 Pod 间 TCP/UDP 服务互访

### Architecture

```
Pod A                              Pod B
┌────────────────┐                ┌────────────────┐
│ App (:8080)    │                │ App (:3306)    │
│       ↕ TCP    │                │       ↕ TCP    │
│ Sidecar        │                │ Sidecar        │
│  导出: :8080   │                │  导出: :3306   │
│  导入: :3306   │                │  导入: :8080   │
└───────┬────────┘                └───────┬────────┘
        │ Unix Socket                     │ Unix Socket
        ▼                                 ▼
┌──────────────────────────────────────────────────┐
│            OutpostNet Server (节点级)              │
│            /var/run/outpostnet/server.sock        │
│                                                  │
│  - 接收 Sidecar 服务注册（导出）                    │
│  - 处理 Sidecar 服务请求（导入）                    │
│  - 路由：导入方 → Server → 导出方                   │
└──────────────────────────────────────────────────┘
```

**关键技术约束：**
- Sidecar ↔ Server 通信使用 **Unix domain socket**，不使用 TCP/IP
- App ↔ Sidecar 通信使用 TCP（localhost），App 无需改动
- 支持 TCP + UDP 两种协议

**数据流：**
```
App A 访问 svc-b:3306
  → Sidecar A 本地监听 :3306（导入）
    → 通过 Unix socket 转发到 Server
      → Server 路由到 Sidecar B（导出 svc-b）
        → Sidecar B 通过 Unix socket 接收
          → 转发到 App B :3306
```

### Components

| 组件 | 说明 |
|------|------|
| `cmd/outpostnet-server` | 节点级 Server，Unix socket 监听，服务注册与路由 |
| `cmd/outpostnet-sidecar` | Pod 级 Sidecar，导出本地服务 + 导入外部服务 |
| `pkg/proxy` | TCP/UDP 代理核心逻辑 |
| `pkg/protocol` | Server-Sidecar 通信协议 |

### Stories

---

#### S1: 项目脚手架与构建系统

**内容：**
- Go module 初始化（go.mod）
- 目录结构：
  ```
  cmd/outpostnet-server/main.go
  cmd/outpostnet-sidecar/main.go
  pkg/proxy/
  pkg/protocol/
  pkg/config/
  ```
- Makefile：build-server、build-sidecar、test、fmt、vet
- 基础 main.go（启动 → 打印版本 → 退出）

**依赖：** 无

**验收：** `make build` 编译出 `outpostnet-server` 和 `outpostnet-sidecar` 二进制文件

---

#### S2: Server-Sidecar 通信协议与连接管理

**内容：**
- 定义消息协议（消息类型、帧格式）：
  - `Register` — Sidecar 注册导出服务
  - `Connect` — Sidecar 请求导入服务
  - `Data` — 数据转发帧
  - `Heartbeat` — 保活
- Server 创建 Unix socket（`/var/run/outpostnet/server.sock`），接受 Sidecar 连接
- Sidecar 通过 Unix socket 连接 Server，完成注册握手
- 心跳保活机制
- 连接断开检测与日志

**依赖：** S1

**验收：** Sidecar 通过 Unix socket 连上 Server，注册成功，心跳正常，断开有日志

---

#### S3: TCP 服务导出与导入（端到端）

**内容：**
- **导出侧：** Sidecar 向 Server 注册 TCP 服务（服务名 + 本地端口）
- **导入侧：** Sidecar 在 localhost 监听端口，接受 App 连接，通过 Unix socket 转发到 Server
- **Server 路由：** 匹配导入请求到对应导出 Sidecar，双向数据转发
- **导出侧转发：** Server 通过 Unix socket 将数据发给导出 Sidecar，Sidecar 连接到本地 App

**数据流：**
```
App A (:9999) → Sidecar A (导入, localhost:3306)
  → Unix socket → Server (路由: db-service → Sidecar B)
    → Unix socket → Sidecar B (导出, → localhost:3306)
      → App B (:3306)
```

**依赖：** S2

**验收：** 两个进程模拟 Pod，通过 Server 中转完成 TCP 通信，`nc` 测试通过

---

#### S4: UDP 服务导出与导入（端到端）

**内容：**
- 扩展 S3 的机制支持 UDP 协议
- UDP 会话管理（基于源地址的伪连接跟踪）
- Server 侧 UDP 数据报转发
- 导出/导入双向 UDP 数据流

**依赖：** S2（可与 S3 并行开发）

**验收：** UDP 服务通过 Server 中转通信正常，`nc -u` 测试通过

---

#### S5: 配置文件格式与加载

**内容：**
- 定义 YAML 配置文件格式
- Server 配置：
  ```yaml
  # server-config.yaml
  listen: "/var/run/outpostnet/server.sock"
  log_level: "info"
  ```
- Sidecar 配置：
  ```yaml
  # sidecar-config.yaml
  server: "/var/run/outpostnet/server.sock"
  exports:
    - name: "web-service"
      local_port: 8080
      protocol: "tcp"
  imports:
    - name: "db-service"
      listen_port: 3306
      protocol: "tcp"
  ```
- 配置加载与校验
- 用配置文件替代 S3/S4 中的硬编码参数

**依赖：** S3, S4

**验收：** Server 和 Sidecar 从 YAML 配置文件启动，行为与 S3/S4 一致

---

#### S6: K8s 部署清单与端到端验证

**内容：**
- Server 部署为 DaemonSet（每节点一个），挂载 hostPath 共享 Unix socket
- 应用 Pod + Sidecar 容器部署 YAML 示例
  - Pod 中包含 App 容器 + Sidecar 容器
  - 通过共享 Volume 挂载 Unix socket
- Sidecar 配置通过 ConfigMap 挂载
- 端到端测试脚本

**依赖：** S5

**验收：** 在 K8s 边缘节点上部署 Server + 两个 Pod（含 Sidecar），Pod A 通过服务名访问 Pod B 的 TCP/UDP 服务

---

### Story Dependency Graph

```
S1: 项目脚手架
  │
  ▼
S2: 协议与连接 (Unix socket)
  │
  ├──────────┐
  ▼          ▼
S3: TCP    S4: UDP    ← 可并行
  │          │
  └────┬─────┘
       ▼
S5: 配置文件
       │
       ▼
S6: K8s 部署
```

---

## Epic 2: Controller + Webhook（自动化）

**目标：** 将 Epic 1 的手动配置过程自动化

**覆盖 PRD：** FR-1, FR-3, FR-4, SC-5

**交付物：** `cmd/outpostnet-controller`

**验收：** Pod 添加注解后，Sidecar 自动注入，Service DNS 可达

*Stories TBD — 在 Epic 1 完成后细化*

---

## Epic 3: QUIC Tunnel Core

**目标：** 构建 QUIC 隧道库，Service 级 Stream 多路复用

**覆盖 PRD：** FR-2, FR-6(部分), NFR-3

**交付物：** `pkg/tunnel`

**验收：** 单元测试 + 基准测试

*Stories TBD*

---

## Epic 4: 跨区域服务访问 (Layer 2)

**目标：** Agent + Controller 集成，跨区域端到端

**覆盖 PRD：** FR-5, FR-6, FR-7, SC-1~SC-4, UJ-2/UJ-3/UJ-5, NFR-1/NFR-2/NFR-4/NFR-5

**交付物：** `cmd/outpostnet-agent` + Controller 扩展 + Helm Chart

**验收：** 两区域端到端：注解 → 隧道 → 跨区域 Service DNS 可达

*Stories TBD*

---

## Epic 5: Relay 中转 (Growth)

**目标：** 不可直连区域通过第三方中转

**覆盖 PRD：** FR-8, UJ-4

*Stories TBD*
