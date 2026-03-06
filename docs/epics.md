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

**覆盖 PRD：** FR-2, FR-6(部分), NFR-3, AC-4

**交付物：** `pkg/tunnel`

**验收：** 单元测试 + 基准测试（≥100 Mbps 吞吐，10 Service 多路复用 ≤10% 退化）

### Architecture

```
区域 A (边缘)                                    区域 B (云端/其他边缘)
┌──────────────────────┐                        ┌──────────────────────┐
│  Sidecar (import)    │                        │  Sidecar (export)    │
│  localhost:3306 ◄────┤                        ├────► App :3306       │
│                      │                        │                      │
│  Tunnel Client       │                        │  Tunnel Server       │
│  ┌─────────────────┐ │     QUIC Connection    │ ┌─────────────────┐ │
│  │ Stream 1: svc-a ├─┼────────────────────────┼─┤ Stream 1: svc-a │ │
│  │ Stream 2: svc-b ├─┼────────────────────────┼─┤ Stream 2: svc-b │ │
│  │ Stream 3: svc-c ├─┼────────────────────────┼─┤ Stream 3: svc-c │ │
│  │ ...              │ │   Single QUIC Conn     │ │ ...              │ │
│  └─────────────────┘ │   (TLS 1.3 built-in)   │ └─────────────────┘ │
└──────────────────────┘                        └──────────────────────┘
```

**核心设计原则：**
- **单连接多路复用：** 一条 QUIC 连接承载所有 Service，每个 Service 映射到独立 QUIC Stream
- **无队头阻塞：** QUIC Stream 互相独立，一个 Service 丢包不影响其他
- **内置加密：** QUIC 内置 TLS 1.3，无需额外加密层
- **连接迁移：** 边缘设备 IP 变化时无缝切换
- **0-RTT 重连：** 断线后快速恢复

**Stream 协议（QUIC Stream 内）：**
```
新 Stream 打开时，Client 发送 ServiceHeader:
┌──────────────┬──────────────────┐
│ NameLen (2B) │ ServiceName (var)│
└──────────────┴──────────────────┘

Server 解析 ServiceHeader → 路由到对应本地服务
之后 Stream 上的数据为透明的双向字节流（L4 转发）
```

**与 Epic 1 的关系：**
- Epic 1 的 `pkg/protocol` 是节点内 Unix socket 通信协议
- Epic 3 的 `pkg/tunnel` 是跨节点 QUIC 隧道协议
- 两者在 Epic 4 中通过 Agent 集成：Agent 读取配置 → 建立 QUIC 隧道 → 连接本地 Server

### Components

| 组件 | 说明 |
|------|------|
| `pkg/tunnel` | QUIC 隧道核心库 |
| `pkg/tunnel/tlsutil` | TLS 证书工具（生成自签名证书、加载证书） |
| `pkg/tunnel/server.go` | Tunnel Server — 接受 QUIC 连接，路由 Stream 到本地服务 |
| `pkg/tunnel/client.go` | Tunnel Client — 拨号 QUIC，为每个导入 Service 打开 Stream |
| `pkg/tunnel/stream.go` | Stream 多路复用管理，ServiceHeader 编解码 |
| `pkg/tunnel/lifecycle.go` | 连接生命周期：keepalive、自动重连、优雅关闭 |

### Stories

---

#### S13: QUIC 隧道库脚手架与 TLS 工具

**内容：**
- 添加 `quic-go` 依赖
- `pkg/tunnel/tlsutil/` — TLS 证书工具：
  - `GenerateSelfSignedCert()` — 生成自签名 TLS 证书（开发/测试用）
  - `LoadTLSConfig(certFile, keyFile)` — 从文件加载证书
  - `InsecureTLSConfig()` — 跳过验证的客户端配置（测试用）
- `pkg/tunnel/` — 基础类型定义：
  - `ServerConfig` — 监听地址、TLS 配置、keepalive 参数
  - `ClientConfig` — 远端地址、TLS 配置、重连参数
  - `ServiceRoute` — 服务名 → 本地地址映射
- 基础 QUIC listener 和 dialer（仅建立连接，不传输数据）
- 单元测试：证书生成、连接建立/关闭

**依赖：** S12 (Epic 2)

**验收：** `go test ./pkg/tunnel/...` 通过，QUIC 连接可建立和关闭

---

#### S14: Tunnel Server — 接受连接与 Stream 路由

**内容：**
- `pkg/tunnel/server.go` — Tunnel Server 实现：
  - `NewServer(cfg ServerConfig)` — 创建 Server 实例
  - `ListenAndServe(ctx)` — 监听 QUIC 连接
  - `handleConnection(conn)` — 接受新连接，循环接受 Stream
  - `handleStream(stream)` — 读取 ServiceHeader，路由到对应本地服务
  - `AddRoute(serviceName, localAddr)` — 注册服务路由
  - `RemoveRoute(serviceName)` — 移除服务路由
- Stream 路由逻辑：
  1. 接受 QUIC Stream
  2. 读取 ServiceHeader（ServiceName）
  3. 查找本地服务地址
  4. 建立 TCP 连接到本地服务
  5. 双向数据转发（Stream ↔ TCP）
- 单元测试：mock QUIC connection，验证 Stream 路由

**依赖：** S13

**验收：** Tunnel Server 可接受连接，正确路由 Stream 到本地 TCP 服务

---

#### S15: Tunnel Client — 拨号与 Stream 转发

**内容：**
- `pkg/tunnel/client.go` — Tunnel Client 实现：
  - `NewClient(cfg ClientConfig)` — 创建 Client 实例
  - `Connect(ctx)` — 拨号 QUIC 连接
  - `OpenServiceStream(serviceName)` — 打开一个新 Stream，发送 ServiceHeader
  - `ServeImport(ctx, serviceName, listenAddr)` — 在本地地址监听，每个 TCP 连接打开一个 Stream
- 数据流（每个导入连接）：
  1. 本地 App 连接到 listenAddr
  2. Client 打开 QUIC Stream，发送 ServiceHeader
  3. 双向转发：App TCP ↔ QUIC Stream
- `pkg/tunnel/stream.go` — ServiceHeader 编解码：
  - `WriteServiceHeader(stream, serviceName)` — 写入 ServiceHeader
  - `ReadServiceHeader(stream)` — 读取 ServiceHeader
- 单元测试：端到端测试（Client → Server → echo service）

**依赖：** S14

**验收：** Client 拨号 Server，打开 Stream，数据通过隧道双向传输

---

#### S16: Service-Stream 多路复用

**内容：**
- 多 Service 并发 Stream 管理：
  - 同一 QUIC 连接上同时打开多个 Stream（每个连接对应一个 Service 实例）
  - 并发安全的 Stream 计数和跟踪
  - Stream 生命周期管理（打开、转发、关闭、错误处理）
- 无队头阻塞验证测试：
  - Stream A 大量数据传输时，Stream B 的延迟不受影响
  - 一个 Stream 失败不影响其他 Stream
- 并发测试：
  - 10 个 Service 同时通过 1 条连接传输
  - 数据无串扰验证（FR-2 测试标准）
  - 高并发 Stream 打开/关闭

**依赖：** S15

**验收：** 1 条 QUIC 连接同时传输 10 个 Service，数据无串扰，Stream 互不阻塞

---

#### S17: 连接生命周期 — Keepalive、重连、优雅关闭

**内容：**
- `pkg/tunnel/lifecycle.go` — 连接生命周期管理：
  - **Keepalive：** 利用 QUIC 内置 keepalive + 应用层心跳（可选）
  - **自动重连：** 连接断开后指数退避重连（初始 1s，最大 30s）
  - **0-RTT 重连：** 启用 QUIC 0-RTT，断线后快速恢复
  - **连接迁移：** 利用 QUIC 连接迁移特性，IP 变化时不断连
  - **优雅关闭：** 收到 shutdown 信号后，等待进行中的 Stream 完成再关闭
- Client 包装层 `ManagedClient`：
  - 自动管理连接生命周期
  - 连接断开时自动重连
  - 重连后恢复所有 Import listener
  - 连接状态回调（Connected/Disconnected/Reconnecting）
- 单元测试：
  - 模拟连接中断 → 验证自动重连
  - 优雅关闭 → 验证进行中 Stream 完成
  - 重连延迟不超过 10 秒（FR-6 标准）

**依赖：** S16

**验收：** 隧道断开后 10 秒内自动重连；优雅关闭完成所有进行中 Stream

---

#### S18: 吞吐基准测试与集成测试

**内容：**
- `pkg/tunnel/tunnel_bench_test.go` — 基准测试：
  - 单 Stream 吞吐量测试（目标：≥100 Mbps）
  - 10 Service 多路复用吞吐测试（目标：退化 ≤10%）
  - Stream 打开/关闭延迟测试
  - 连接建立延迟测试
- `pkg/tunnel/tunnel_integration_test.go` — 集成测试：
  - 端到端：Client → QUIC Tunnel → Server → TCP echo service → 验证数据一致性
  - 多 Service 并发：10 个 Service 同时通信
  - 大数据传输：传输 100MB 数据验证完整性
  - 重连场景：断开 → 重连 → 恢复通信
  - 优雅关闭：shutdown 期间不丢数据
- Makefile 增加 `bench-tunnel` 目标

**依赖：** S17

**验收：**
- 单 Stream 吞吐 ≥100 Mbps（千兆网络）
- 10 Service 多路复用退化 ≤10%
- 所有集成测试通过

---

### Story Dependency Graph

```
S13: QUIC 隧道库脚手架 + TLS
  │
  ▼
S14: Tunnel Server（Stream 路由）
  │
  ▼
S15: Tunnel Client（Stream 转发）
  │
  ▼
S16: Service-Stream 多路复用
  │
  ▼
S17: 连接生命周期（Keepalive/重连/关闭）
  │
  ▼
S18: 吞吐基准测试 + 集成测试
```

---

## Epic 4: 跨区域服务访问 (Layer 2)

**目标：** Agent + Controller 集成，跨区域端到端

**覆盖 PRD：** FR-5, FR-6, FR-7, SC-1~SC-4, UJ-2/UJ-3/UJ-5, NFR-1/NFR-2/NFR-4/NFR-5

**交付物：** `cmd/outpostnet-agent` + Controller 扩展 + Helm Chart

**验收：** 两区域端到端：注解 → 隧道 → 跨区域 Service DNS 可达

### Architecture

```
区域 A (边缘)                                    区域 B (云端)
┌──────────────────────────────────────┐        ┌──────────────────────────────────────┐
│  Pod (app + sidecar)                 │        │  Pod (app + sidecar)                 │
│  annotations:                        │        │  annotations:                        │
│    outpostnet.io/exports: svc-a:80   │        │    outpostnet.io/exports: svc-b:3306 │
│    outpostnet.io/region: edge-a      │        │    outpostnet.io/region: cloud       │
│           │                          │        │           │                          │
│           ▼                          │        │           ▼                          │
│  OutpostNet Server (DaemonSet)       │        │  OutpostNet Server (DaemonSet)       │
│  /var/run/outpostnet/server.sock     │        │  /var/run/outpostnet/server.sock     │
│           │                          │        │           │                          │
│           ▼                          │        │           ▼                          │
│  OutpostNet Agent (DaemonSet)        │        │  OutpostNet Agent (DaemonSet)        │
│  ┌─────────────────────────────┐     │        │  ┌─────────────────────────────┐     │
│  │ Tunnel Client → svc-b      │─────┼────────┼──│ Tunnel Server ← streams     │     │
│  │ Tunnel Server ← streams    │◄────┼────────┼──│ Tunnel Client → svc-a       │     │
│  └─────────────────────────────┘     │        │  └─────────────────────────────┘     │
└──────────────────────────────────────┘        └──────────────────────────────────────┘
                    ▲                                            ▲
                    │         Controller (云端)                   │
                    │    ┌──────────────────────────┐            │
                    └────│  Region Reconciler        │────────────┘
                         │  - 检测跨区域注解          │
                         │  - 生成 Agent ConfigMap    │
                         │  - 管理跨区域 Service/EP   │
                         └──────────────────────────┘
```

**跨区域数据流：**
```
Region A App 访问 svc-b (Region B 的服务):
  1. App → Sidecar A (import svc-b, localhost:3306)
  2. Sidecar A → Server A (Unix socket)
  3. Server A → Agent A (本地注册的 svc-b 由 Agent tunnel-client 导出)
  4. Agent A Tunnel Client → QUIC Stream → Agent B Tunnel Server
  5. Agent B → Server B (Unix socket, 转发到 svc-b 导出方)
  6. Server B → Sidecar B → App B (:3306)
```

**区域标识：**
```yaml
# Node label 标识区域
metadata:
  labels:
    outpostnet.io/region: "edge-a"

# Pod 注解声明跨区域服务
metadata:
  annotations:
    outpostnet.io/inject: "true"
    outpostnet.io/region: "edge-a"
    outpostnet.io/exports: "my-api:8080:tcp"
    outpostnet.io/imports: "cloud-db:3306:tcp@cloud"  # @region 表示跨区域导入
```

**Agent 配置格式（Controller 生成）：**
```yaml
region: "edge-a"
tunnels:
  - remote_region: "cloud"
    remote_addr: "cloud-gateway.example.com:4242"
    role: "client"  # 或 "server"
    tls:
      insecure: false
      ca_cert: "/etc/outpostnet/ca.pem"
exports:
  - name: "my-api"
    local_addr: "127.0.0.1:8080"
    protocol: "tcp"
imports:
  - name: "cloud-db"
    listen_addr: "127.0.0.1:3306"
    remote_region: "cloud"
    protocol: "tcp"
persistence:
  dir: "/var/lib/outpostnet"
metrics:
  addr: ":9090"
```

### Components

| 组件 | 说明 |
|------|------|
| `cmd/outpostnet-agent` | 边缘节点 Agent，管理 QUIC 隧道，桥接本地 Server |
| `internal/agent` | Agent 核心逻辑：隧道管理、配置监听、持久化 |
| `internal/controller/reconciler/region_reconciler.go` | 跨区域 Service 关系检测与配置生成 |
| `pkg/agentconfig` | Agent 配置格式定义与加载 |
| `deploy/agent-daemonset.yaml` | Agent DaemonSet 部署清单 |

### Stories

---

#### S19: Agent 脚手架与配置格式

**内容：**
- `cmd/outpostnet-agent/main.go` + `cmd/outpostnet-agent/app/agent.go`
  - Cobra CLI，flags: --config, --region, --metrics-addr, --persistence-dir
  - 健康检查端点 (/healthz, /readyz)
  - 信号处理 (SIGTERM → graceful shutdown)
- `pkg/agentconfig/` — Agent 配置定义：
  - `AgentConfig` 结构体：region, tunnels, exports, imports, persistence, metrics
  - `LoadConfig(path)` — 从 YAML 加载
  - `ValidateConfig()` — 校验
- Agent DaemonSet 部署清单 + RBAC
- Dockerfile
- Makefile 增加 `build-agent` 目标

**依赖：** S18 (Epic 3)

**验收：** `make build-agent` 编译成功，Agent 进程启动后健康检查可达

---

#### S20: Agent 隧道管理 — QUIC 隧道建立与桥接

**内容：**
- `internal/agent/tunnel_manager.go` — 隧道管理器：
  - 根据配置创建 Tunnel Server 或 Tunnel Client
  - Server 模式：接受远程连接，路由 Stream 到本地服务
  - Client 模式：连接远端，为导入服务打开 Stream
  - 使用 `pkg/tunnel.ManagedClient` 实现自动重连
- `internal/agent/bridge.go` — 本地桥接：
  - Agent 向本地 OutpostNet Server 注册导出服务（通过 Unix socket）
  - Agent 作为代理：远程隧道 ↔ 本地 Server
- Agent main 集成：启动 → 加载配置 → 建立隧道 → 注册服务
- 优雅关闭：SIGTERM → 等待 Stream 完成 → 关闭隧道
- 单元测试：隧道建立、桥接逻辑

**依赖：** S19

**验收：** Agent 启动后根据配置建立 QUIC 隧道，跨进程 TCP 通信通过隧道可达

---

#### S21: 配置持久化与断网恢复

**内容：**
- `internal/agent/persistence.go` — 配置持久化：
  - 收到新配置时写入本地文件 (`/var/lib/outpostnet/config.yaml`)
  - Agent 重启时优先从本地文件加载
  - 配置版本号跟踪，避免回退
- `internal/agent/config_watcher.go` — ConfigMap 监听：
  - Watch K8s ConfigMap 变更
  - 配置变更时热更新隧道（新增/删除隧道，不影响已有连接）
  - 云端不可达时使用本地持久化配置
- 测试场景：
  - Agent 重启 → 从本地配置恢复
  - 断网 → 使用最后有效配置继续运行

**依赖：** S20

**验收：** Agent 重启后 5 秒内恢复隧道；断网后使用本地配置正常运行 (FR-7, NFR-4)

---

#### S22: Controller 扩展 — 跨区域隧道配置生成

**内容：**
- 跨区域注解扩展：
  - `outpostnet.io/region: "<region>"` — 标识 Pod 所在区域
  - `outpostnet.io/imports: "svc:port:proto@region"` — `@region` 表示跨区域导入
- `internal/controller/reconciler/region_reconciler.go` — Region Reconciler：
  - Watch 带区域注解的 Pod
  - 检测跨区域服务关系（A.imports 引用了 B.exports）
  - 为每个区域生成 Agent ConfigMap（隧道配置）
  - 为跨区域导入的服务创建本地 Service + EndpointSlice（指向 Agent 代理端口）
- Agent ConfigMap 生成逻辑：
  ```yaml
  # 生成到 edge-a 区域的 Agent ConfigMap
  region: edge-a
  tunnels:
    - remote_region: cloud
      remote_addr: <cloud-agent-service>:4242
      role: client
  imports:
    - name: cloud-db
      listen_addr: "0.0.0.0:13306"
      remote_region: cloud
  ```
- Controller main 注册新 Reconciler

**依赖：** S21

**验收：** 带跨区域注解的 Pod 创建后，Controller 自动生成 Agent ConfigMap 和代理 Service

---

#### S23: Prometheus 指标与可观测性

**内容：**
- Agent `/metrics` 端点（Prometheus 格式）：
  - `outpostnet_tunnel_state` — 隧道连接状态 (gauge: 0=disconnected, 1=connected)
  - `outpostnet_active_streams` — 当前活跃 Stream 数 (gauge)
  - `outpostnet_bytes_sent_total` — 发送字节数 (counter)
  - `outpostnet_bytes_received_total` — 接收字节数 (counter)
  - `outpostnet_reconnect_total` — 重连次数 (counter)
  - `outpostnet_stream_latency_seconds` — Stream 建立延迟 (histogram)
  - `outpostnet_config_reload_total` — 配置重载次数 (counter)
- `internal/agent/metrics.go` — 指标收集与注册
- Agent DaemonSet 添加 Prometheus annotation
- 单元测试：验证指标注册和更新

**依赖：** S20

**验收：** `curl agent:9090/metrics` 返回 Prometheus 格式指标，包含所有关键指标 (NFR-5)

---

#### S24: 跨区域端到端 E2E 测试

**内容：**
- 部署清单：
  - Agent DaemonSet（每个区域）
  - 示例 Deployment（跨区域注解）
  - TLS 证书 Secret（测试用自签名证书）
- `hack/e2e-cross-region.sh` — E2E 测试脚本：
  1. 部署两个 "区域"（使用 namespace 模拟：`region-a`, `region-b`）
  2. 在 region-a 部署导出服务（echo-server）
  3. 在 region-b 部署导入服务（echo-client, 引用 region-a 的 echo-server）
  4. Controller 生成 Agent 配置
  5. Agent 建立 QUIC 隧道
  6. 验证跨区域 TCP 连通性
  7. 验证 Service DNS 可达
  8. 断网测试：停止 Controller → 验证已建立隧道继续工作
  9. 清理
- SC-1~SC-4 验证：
  - SC-1: 注解后 30s 内服务可达
  - SC-2: 单隧道多路复用
  - SC-3: 断网后通信不中断
  - SC-4: iptables diff 为空

**依赖：** S22, S23

**验收：** 两区域端到端通信：添加注解 → 隧道自动建立 → Service DNS 可达 → 断网后不中断

---

### Story Dependency Graph

```
S19: Agent 脚手架
  │
  ▼
S20: Agent 隧道管理
  │
  ├──────────────────┐
  ▼                  ▼
S21: 配置持久化     S23: Prometheus 指标
  │
  ▼
S22: Controller 跨区域扩展
  │
  └──────────────────┐
                     ▼
                   S24: 跨区域 E2E 测试
```

---

## Epic 5: Relay 中转 (Growth)

**目标：** 不可直连区域通过第三方中转

**覆盖 PRD：** FR-8, UJ-4

**交付物：** Relay 转发逻辑 + Controller 路径计算 + 3 区域 E2E 验证

**验收：** Region A 通过 Region C 中转访问 Region B 的服务，延迟增长 ≤ 2x relay RTT

### Architecture

```
区域 A (边缘)              区域 C (中继)              区域 B (云端)
┌──────────────┐          ┌──────────────────┐        ┌──────────────┐
│  App (import │          │  Agent C         │        │  App (export │
│  svc-b)      │          │                  │        │  svc-b:3306) │
│      │       │          │  Tunnel Server   │        │      │       │
│  Agent A     │          │  ┌────────────┐  │        │  Agent B     │
│  ┌────────┐  │   QUIC   │  │ Relay Rule │  │  QUIC  │  ┌────────┐ │
│  │ Client ├──┼──────────┼──│ svc-b → B  ├──┼────────┼──│ Server │ │
│  └────────┘  │          │  └────────────┘  │        │  └────────┘ │
└──────────────┘          └──────────────────┘        └──────────────┘

Data flow:
  App A → Agent A (import proxy) → QUIC Stream → Agent C (relay) → QUIC Stream → Agent B → svc-b
```

**Relay 核心设计：**
- Relay 是角色而非专用节点，任何可暴露端口的区域都可充当 Relay
- Agent 的 Tunnel Server 收到 Stream 后，如果目标服务有 relay 规则，转发到目标区域的 Tunnel Client
- Stream 级别转发：读 ServiceHeader → 查 relay 规则 → 打开下游 Stream → 双向转发
- 配置驱动：Controller 计算可达路径，生成含 relay 规则的 Agent ConfigMap

**Relay 配置格式（Controller 生成）：**
```yaml
# Region C (relay) 的 Agent 配置
region: "relay-c"
tunnels:
  - remote_region: "edge-a"
    role: server
    listen_addr: "0.0.0.0:4242"
  - remote_region: "cloud-b"
    role: client
    remote_addr: "cloud-b-gateway:4242"
relays:
  - service: "svc-b"
    from_region: "edge-a"
    to_region: "cloud-b"
    max_hops: 3
```

### Components

| 组件 | 说明 |
|------|------|
| `pkg/agentconfig` | 扩展 RelayConfig 配置 |
| `internal/agent/relay.go` | Relay 转发逻辑：Stream 级别中继 |
| `internal/controller/reconciler/region_reconciler.go` | 扩展路径计算，生成 relay 配置 |
| `internal/agent/metrics.go` | 扩展 relay 相关指标 |

### Stories

---

#### S25: Relay 配置模型

**内容：**
- `pkg/agentconfig/` — 扩展配置格式：
  - 新增 `RelayConfig` 结构体：
    ```go
    type RelayConfig struct {
        Service    string `yaml:"service"`      // 被中继的服务名
        FromRegion string `yaml:"from_region"`  // 请求来源区域
        ToRegion   string `yaml:"to_region"`    // 转发目标区域
        MaxHops    int    `yaml:"max_hops"`     // 最大跳数（防环）
    }
    ```
  - `Config.Relays []RelayConfig` — Agent 的 relay 规则列表
- 配置验证：
  - `from_region` ≠ `to_region`（不能自己转自己）
  - `to_region` 必须对应一个已配置的 tunnel（client 角色）
  - `max_hops` 默认 3，最大 5
  - 检测循环引用
- YAML 序列化/反序列化
- 单元测试：解析、验证、边界值

**依赖：** S24 (Epic 4)

**验收：** Agent 可解析含 relay 规则的配置，验证通过，测试覆盖正常和异常场景

---

#### S26: Agent Relay 转发

**内容：**
- `internal/agent/relay.go` — Relay 转发器：
  - `RelayManager` — 管理 relay 规则和转发逻辑
  - 注册 relay 规则到 tunnel server 的 stream handler
  - Stream 处理流程：
    1. 读取 ServiceHeader（服务名）
    2. 查找 relay 规则（匹配 service + from_region）
    3. 在目标区域的 tunnel client 上打开新 Stream
    4. 写入 ServiceHeader 到下游 Stream
    5. 双向数据转发（上游 Stream ↔ 下游 Stream）
  - Hop 计数：在 ServiceHeader 后附加 1 字节 hop count
    - 每次 relay +1，超过 max_hops 时拒绝
  - 错误处理：下游不可达时关闭上游 Stream 并记录日志
- `internal/agent/tunnel_manager.go` — 集成 relay：
  - Server 收到 stream 时，先查本地路由，再查 relay 规则
  - 提供 `GetClient(region)` 方法供 relay 使用
- `internal/agent/agent.go` — 集成 RelayManager 到 Agent 生命周期
- 单元测试：3 个进程模拟 A→C→B relay

**依赖：** S25

**验收：** 3 进程测试：Region A 通过 Region C relay 访问 Region B 的 TCP 服务

---

#### S27: Controller Relay 路径计算

**内容：**
- `internal/controller/reconciler/region_reconciler.go` — 扩展 Region Reconciler：
  - 区域可达性矩阵：
    - 输入：每个区域的 tunnel 配置（已知连接）
    - 输出：哪些区域对可以直连
  - 路径计算算法（BFS 最短路径）：
    - 对每个需要跨区域导入的服务：
      1. 如果源区域和目标区域可直连：生成直连配置
      2. 否则：BFS 找最短 relay 路径
      3. 为路径中的每个 relay 节点生成 relay 规则
  - Relay ConfigMap 生成：
    - 为 relay 区域的 Agent 生成包含 relays 字段的配置
    - 确保 relay 区域有到源和目标区域的 tunnel 配置
  - 区域连通性注解（可选）：
    - `outpostnet.io/reachable: "region-a,region-b"` — 声明可达区域
- 单元测试：
  - 直连场景（不需要 relay）
  - 单跳 relay（A→C→B）
  - 多跳 relay（A→C→D→B）
  - 不可达场景（无路径）

**依赖：** S26

**验收：** Controller 自动检测不可直连的区域对，计算 relay 路径，生成正确的 Agent ConfigMap

---

#### S28: Relay 指标与可观测性

**内容：**
- `internal/agent/metrics.go` — 扩展指标：
  - `outpostnet_relay_streams_total` — relay 处理的 stream 总数 (counter, labels: from_region, to_region, service)
  - `outpostnet_relay_bytes_total` — relay 转发的字节数 (counter, labels: direction, from_region, to_region)
  - `outpostnet_relay_active_streams` — 当前活跃的 relay stream 数 (gauge)
  - `outpostnet_relay_latency_seconds` — relay stream 建立延迟 (histogram)
  - `outpostnet_relay_errors_total` — relay 错误数 (counter, labels: error_type)
- `internal/agent/relay.go` — 集成指标收集
- 健康检查扩展：
  - `/readyz` 报告 relay 路径健康状态
  - relay 下游不可达时标记降级
- 单元测试：验证指标注册和更新

**依赖：** S26

**验收：** `curl agent:9090/metrics` 返回 relay 相关指标，relay stream 计数和字节数准确

---

#### S29: Relay 端到端 E2E 测试

**内容：**
- `internal/agent/relay_test.go` — Go 集成测试：
  - 3 个 Agent 进程：A（client）、C（relay）、B（server）
  - Sub-tests：
    1. `SC-1_RelayConnectivity` — A 通过 C 访问 B 的 echo 服务
    2. `SC-2_RelayMultiStream` — 10 并发 stream 通过 relay
    3. `SC-3_RelayLargeData` — 256KB 数据通过 relay
    4. `SC-4_RelayHopLimit` — 超过 max_hops 时拒绝
    5. `SC-5_DirectAndRelay` — 同时有直连和 relay 路径，各自独立工作
- `hack/e2e-relay.sh` — K8s E2E 测试脚本：
  - 3 namespace 模拟 3 个区域
  - 部署 echo server + 3 个 Agent
  - 验证 relay 连通性、指标、健康检查
- Makefile 增加 `e2e-relay` 目标
- FR-8 验证：延迟增长 ≤ 2x relay RTT

**依赖：** S27, S28

**验收：** 3 区域端到端：A→C→B relay 通信成功，指标准确，hop limit 生效

---

### Story Dependency Graph

```
S25: Relay 配置模型
  │
  ▼
S26: Agent Relay 转发
  │
  ├──────────────────┐
  ▼                  ▼
S27: Controller     S28: Relay 指标
 路径计算
  │                  │
  └──────────────────┘
           │
           ▼
         S29: Relay E2E 测试
```
