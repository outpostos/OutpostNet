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

### Architecture

```
用户操作：在 Pod/Deployment 上添加 OutpostNet 注解
                    │
                    ▼
┌──────────────────────────────────────────────────┐
│         OutpostNet Controller (Deployment)        │
│                                                  │
│  1. Mutating Webhook                             │
│     - 拦截 Pod 创建请求                            │
│     - 解析注解 → 注入 Sidecar 容器 + Volume        │
│                                                  │
│  2. Pod Reconciler                               │
│     - 监听带注解的 Pod                              │
│     - 自动生成 Sidecar ConfigMap                   │
│     - 清理已删除 Pod 的 ConfigMap                   │
│                                                  │
│  3. EndpointSlice Reconciler                     │
│     - 为导出的服务创建 K8s Service                  │
│     - 管理 EndpointSlice → CoreDNS 可解析          │
└──────────────────────────────────────────────────┘
```

**注解格式：**
```yaml
metadata:
  annotations:
    outpostnet.io/inject: "true"
    outpostnet.io/exports: "web-service:8080:tcp,metrics:9090:tcp"
    outpostnet.io/imports: "db-service:3306:tcp,cache:6379:tcp"
```

**自动化数据流：**
```
1. 用户创建带注解的 Pod
   → Webhook 拦截，注入 Sidecar 容器 + socket volume
2. Pod 调度到节点
   → Pod Reconciler 检测，生成 ConfigMap（含 exports/imports）
3. Sidecar 启动，读取 ConfigMap，连接节点上的 Server
   → 注册导出服务 / 监听导入端口
4. EndpointSlice Reconciler 检测导出服务
   → 创建 K8s Service + EndpointSlice
   → CoreDNS 可解析服务名
```

### Components

| 组件 | 说明 |
|------|------|
| `cmd/outpostnet-controller` | Controller Manager，包含 Webhook + Reconcilers |
| `pkg/annotations` | 注解解析与验证库 |
| `internal/controller/webhook` | Mutating Webhook 实现 |
| `internal/controller/reconciler` | Pod + EndpointSlice Reconciler |

### Stories

---

#### S7: Controller 项目脚手架

**内容：**
- 添加 `controller-runtime` 和 `client-go` 依赖
- `cmd/outpostnet-controller/main.go` + `cmd/outpostnet-controller/app/controller.go`
  - Cobra CLI + controller-runtime Manager 启动
  - 健康检查端点 (/healthz, /readyz)
- RBAC 清单：
  - ServiceAccount, ClusterRole, ClusterRoleBinding
  - 权限：Pods (get/list/watch), ConfigMaps (get/list/watch/create/update/delete), Services (create/update/delete), EndpointSlices (create/update/delete)
- Controller Deployment 清单
- Makefile 增加 `build-controller` 目标
- Dockerfile

**依赖：** S6 (Epic 1)

**验收：** `make build-controller` 编译成功，Controller 进程启动后健康检查端点可达

---

#### S8: 注解规范与解析库

**内容：**
- 定义注解格式：
  ```
  outpostnet.io/inject: "true"                           # 是否注入 Sidecar
  outpostnet.io/exports: "name:port:protocol,..."        # 导出服务列表
  outpostnet.io/imports: "name:listenPort:protocol,..."  # 导入服务列表
  outpostnet.io/sidecar-id: "custom-id"                  # 可选：自定义 Sidecar ID
  ```
- `pkg/annotations/` 包：
  - `Parse(pod)` → 解析 Pod 注解为结构化数据
  - `HasOutpostNet(pod)` → 是否标记了 OutpostNet 注解
  - `Validate()` → 校验注解格式
- 完整单元测试（正常值、边界值、非法值）

**依赖：** S7

**验收：** 注解解析库能正确解析各种注解格式，单元测试全部通过

---

#### S9: Mutating Webhook — Sidecar 自动注入

**内容：**
- Webhook Server 启动（TLS 证书通过 controller-runtime 内置 cert 管理或挂载 Secret）
- MutatingWebhookConfiguration 清单
- Pod 创建时拦截逻辑：
  1. 检查 `outpostnet.io/inject: "true"` 注解
  2. 解析 exports/imports 注解
  3. 注入 Sidecar 容器（image、args、volumeMounts）
  4. 注入共享 Volume（hostPath: `/var/run/outpostnet`）
  5. 注入 ConfigMap Volume（由 S10 的 Reconciler 创建）
  6. 添加 label `outpostnet.io/injected: "true"` 标记已注入
- 幂等性：已注入的 Pod 不重复注入
- 单元测试：mock admission request

**依赖：** S8

**验收：** 创建带注解的 Pod 时，Webhook 自动注入 Sidecar 容器和必要的 Volume

---

#### S10: ConfigMap 自动生成

**内容：**
- Pod Reconciler：
  1. Watch 带 `outpostnet.io/injected: "true"` label 的 Pod
  2. Pod 创建/更新时，从注解生成 Sidecar YAML 配置
  3. 创建 ConfigMap `outpostnet-sidecar-{pod-name}`，内容为 sidecar.yaml
  4. ConfigMap 设置 OwnerReference → Pod 删除时自动 GC
- 配置生成逻辑：
  ```yaml
  server: "/var/run/outpostnet/server.sock"
  sidecar_id: "{pod-name}"
  heartbeat_interval: "10s"
  exports: [从注解解析]
  imports: [从注解解析]
  ```
- 处理 Pod 重建场景（ConfigMap 已存在时更新）

**依赖：** S9

**验收：** 带注解的 Pod 创建后，对应的 ConfigMap 自动生成；Pod 删除后 ConfigMap 自动清理

---

#### S11: EndpointSlice 与 Service 管理

**内容：**
- EndpointSlice Reconciler：
  1. Watch 带导出注解的 Pod
  2. 为每个唯一的导出服务名创建 headless Service（`{service-name}.{namespace}`）
  3. 创建 EndpointSlice，端点指向 Pod IP + Sidecar 导入端口
  4. Pod 就绪/删除时更新 EndpointSlice
- Service 标签：`app.kubernetes.io/managed-by: outpostnet-controller`
- CoreDNS 集成验证：`nslookup {service-name}.{namespace}.svc.cluster.local`
- 处理多 Pod 导出同一服务的场景（聚合端点）

**依赖：** S10

**验收：** 导出服务的 Pod 创建后，K8s Service 和 EndpointSlice 自动创建；其他 Pod 通过 Service DNS 名称可解析到正确端点

---

#### S12: K8s E2E 集成验证

**内容：**
- 部署 Controller 到集群
- 测试场景：
  1. 创建带注解的 echo-server Deployment → 验证 Sidecar 注入
  2. 验证 ConfigMap 自动生成
  3. 创建带注解的 echo-client Deployment → 验证 Sidecar 注入
  4. 验证 echo-client 通过 Service DNS 访问 echo-server
  5. 删除 Pod → 验证 ConfigMap 和 EndpointSlice 清理
- E2E 测试脚本 `hack/e2e-controller.sh`
- 示例 Deployment YAML（取代 Epic 1 手动 Pod YAML）

**依赖：** S11

**验收：** 用户只需在 Deployment 上添加 OutpostNet 注解，无需手动编写 ConfigMap 或 Sidecar 容器定义，服务通过 DNS 名称可达

---

### Story Dependency Graph

```
S7: Controller 脚手架
  │
  ▼
S8: 注解规范与解析
  │
  ▼
S9: Webhook Sidecar 注入
  │
  ▼
S10: ConfigMap 自动生成
  │
  ▼
S11: EndpointSlice 管理
  │
  ▼
S12: E2E 集成验证
```

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
