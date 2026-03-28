# Product Requirements Document - OutpostNet v2

**Author:** John (PM) + Product Owner
**Date:** 2026-03-01
**Status:** Approved
**Approved Date:** 2026-03-29

---

## Executive Summary

### Problem Statement

Kubernetes 的服务网络模型建立在一个核心假设上：**IP 地址是廉价的，集群内网络是扁平的**。CNI 插件（Calico、Flannel 等）负责让所有 Pod 互通，每个 Pod 有独立 IP，Service 通过 ClusterIP/iptables 做路由。

在边缘计算场景中，这个假设不成立：
- 节点分散在不同物理位置，跨越不同网络
- 节点之间可能隔着 NAT、防火墙
- 不可能像数据中心一样构建扁平网络
- IP 地址不廉价，不能随意分配
- 网络拓扑多样，有的节点可达，有的不可达

### Product Definition

OutpostNet 是 OutpostOS 边缘操作系统的**网络可达性层**，定位为边缘计算场景下的**特殊 CNI**。它在非扁平网络环境下，为 Kubernetes 上层能力（Service、kube-proxy、CoreDNS）提供底层网络可达性。

### Core Principle: 分层不混

OutpostNet 只做一件事：**让原本不可达的服务变得可达。**

```
┌─────────────────────────────────────────────┐
│  K8s 原生能力（不重新造）                      │
│  Service / kube-proxy / CoreDNS / Annotation │
│  负载均衡 / 服务治理 / 流量策略                 │
└───────────────────┬─────────────────────────┘
                    │ 依赖底层可达性
┌───────────────────▼─────────────────────────┐
│  OutpostNet（只做这一层）                      │
│  在非扁平网络环境下提供服务间网络可达性           │
│  = 边缘场景的 CNI                             │
└─────────────────────────────────────────────┘
```

### Differentiator

| 对标方案 | OutpostNet 的差异 |
|---------|------------------|
| **FRP** | FRP 以端口为单位映射，导致端口爆炸。OutpostNet 以 Service 为单位，单隧道多路复用 |
| **EdgeMesh** | EdgeMesh 需要修改 iptables，侵入性高。OutpostNet 完全在 K8s API 层面解决，不碰底层网络栈 |
| **旧版 OutpostNet** | 旧版重复造轮子（自建注册中心、协议、服务发现）。新版复用 K8s 原生能力，只做网络可达性 |

### Target Users

- **OutpostOS 平台运维人员** — 负责边缘节点部署和网络配置
- **边缘应用开发者** — 需要服务间通信，不想关心底层网络细节

---

## Success Criteria

| ID | Criteria | Measurement |
|----|----------|-------------|
| SC-1 | 边缘节点上通过 Service 注解声明的服务，在 30 秒内对其他授权节点可达 | 端到端连通性测试 |
| SC-2 | 单条隧道连接可承载 N 个 Service 的流量，无需为每个 Service 分配独立端口 | 隧道上同时传输 10+ Service 流量的集成测试 |
| SC-3 | 云端配置下发完成后，即使断网，已建立的服务通信不中断 | 断网场景测试：配置下发 → 建立通信 → 断开云端 → 验证通信持续 |
| SC-4 | 不修改 iptables，不侵入宿主机网络栈 | 代码审查 + 部署前后 iptables diff 为空 |
| SC-5 | 用户只需在 Service/Pod 上添加注解即可完成配置，无需编写额外配置文件 | 用户操作步骤不超过：添加注解 → 服务可达 |

---

## Product Scope

### Network Topology Model

```
                        云 (Cloud)
                    ┌──────────────┐
                    │  云端服务      │  ← 也是一个"区域"
                    └──────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
     节点组 A (LAN)    节点组 B (LAN)    节点组 C (LAN)
    ┌───────────┐    ┌───────────┐    ┌───────────┐
    │ Node1     │    │ Node4     │    │ Node6     │
    │ Node2     │    │ Node5     │    │ Node7     │
    │ Node3     │    │           │    │           │
    └───────────┘    └───────────┘    └───────────┘
```

**关键定义：**
- **节点组 (Node Group)**：同一局域网内的一组边缘节点，组内 IP 互通
- **区域 (Region)**：一个节点组构成一个区域。云端也是一个区域，不享有特殊地位
- **中转 (Relay)**：一种角色，任何能对外暴露端口的区域都可以承担，不限于云端

### Control Plane / Data Plane Separation

```
┌──────────────────────────────────────────────┐
│             控制面 (Control Plane)              │
│                                              │
│  位于云端（或任何有管理能力的区域）                │
│  职责：                                       │
│  - 定义服务关系和网络拓扑                        │
│  - 生成数据面配置                               │
│  - 下发配置到边缘节点                            │
│                                              │
│  配置下发完成后，控制面可以离线                    │
└──────────────────┬───────────────────────────┘
                   │ 配置下发（一次性）
                   ▼
┌──────────────────────────────────────────────┐
│             数据面 (Data Plane)                │
│                                              │
│  运行在每个边缘节点上                            │
│  职责：                                       │
│  - 根据配置建立隧道连接                          │
│  - 多路复用 Service 流量                        │
│  - 自主运行，无需实时联系控制面                    │
│                                              │
│  拓扑不变 → 断网后通信继续                       │
└──────────────────────────────────────────────┘
```

### Service Access Layers

**Layer 1: 单节点内服务互访**
- 同一边缘节点上的所有服务能够互相访问
- OutpostNet 作为本地 CNI 提供可达性

**Layer 2: 跨区域服务访问**
- 区域 A 的服务访问区域 B 的服务
- 边缘访问云端服务 / 云端访问边缘服务
- 均通过数据面隧道实现
- Layer 2 和 Layer 3（之前旧版的分类）本质没有区别，都是区域到区域的通信
- 当两个区域不可直连时，通过一个双方都能到达的区域做中转（中转是角色，不是固定实体）

### MVP Scope

MVP 聚焦于建立完整的端到端通信链路：

| 能力 | 包含 | 不包含 |
|------|------|--------|
| 配置方式 | K8s Service/Pod 注解 | CLI 工具、Web UI |
| 隧道协议 | QUIC 隧道（内置多路复用 + TLS 1.3） | TCP 回退模式 |
| 网络拓扑 | 两个区域直连 | 多跳中转 |
| 服务发现 | K8s 原生 EndpointSlice | 自建服务注册中心 |
| Sidecar | Webhook 自动注入 | 手动配置 |
| 断网支持 | 配置持久化，断网后维持通信 | 断网后动态重配置 |

### Growth Scope

| 能力 | 说明 |
|------|------|
| UDP 服务代理 | 支持 UDP 协议的服务（QUIC 底层已是 UDP） |
| TCP 回退 | 当 UDP 被防火墙封锁时，回退到 TCP + yamux 多路复用 |
| 多跳中转 | 通过中间区域中转到达目标区域 |
| 中转角色 | 任何区域可配置为中转节点 |
| 配置热更新 | 不断开现有连接的前提下更新配置 |

### Vision Scope

| 能力 | 说明 |
|------|------|
| 自动拓扑发现 | 自动检测区域间可达性 |
| 智能路由 | 根据延迟、带宽选择最优路径 |
| 零配置模式 | 节点加入后自动接入网络 |

---

## User Journeys

### UJ-1: 边缘应用开发者 — 让边缘服务在节点内可访问

**前置条件：** 边缘节点已部署 OutpostNet 组件

**步骤：**
1. 开发者在 Pod 或 Service 上添加 OutpostNet 注解，声明需要暴露的端口
2. OutpostNet Controller（Webhook）检测到注解，自动注入 Sidecar
3. 同一节点上的其他服务通过标准 K8s Service 名称即可访问该服务

**验收标准：**
- 开发者只需添加一个注解
- 无需修改应用代码或网络配置
- 服务在 30 秒内可达

### UJ-2: 平台运维 — 将边缘服务暴露到云端

**前置条件：** 边缘节点和云端均已部署 OutpostNet 组件，且云端 Controller 可达

**步骤：**
1. 运维人员在云端通过 Service 注解声明：需要访问边缘区域 A 的某个服务
2. OutpostNet Controller 生成隧道配置并下发到边缘节点
3. 边缘节点的 OutpostNet Agent 根据配置建立到云端的隧道
4. 云端通过标准 K8s Service 名称即可访问边缘服务

**验收标准：**
- 隧道自动建立，无需手动配置端口映射
- 单条隧道承载多个 Service 流量
- 云端通过 Service DNS 名称透明访问

### UJ-3: 平台运维 — 边缘访问云端服务

**前置条件：** 同 UJ-2

**步骤：**
1. 运维人员在云端通过注解声明：边缘区域 A 需要访问云端某个服务
2. OutpostNet Controller 生成配置并下发
3. 边缘节点通过标准 K8s Service 名称访问云端服务

**验收标准：**
- 边缘应用通过 Service 名称透明访问云端服务
- 与访问本地服务的方式一致

### UJ-4: 平台运维 — 跨节点组服务访问

**前置条件：** 节点组 A 和 B 均已部署 OutpostNet，且存在可达的中转区域

**步骤：**
1. 运维人员在云端配置：节点组 A 的服务需要访问节点组 B 的服务
2. Controller 确定路由路径（直连或经中转）
3. 生成配置下发到相关节点
4. 隧道建立，服务可达

**验收标准：**
- 跨节点组服务访问对应用透明
- 通过 Service 名称访问，与本地服务一致

### UJ-5: 断网恢复 — 云端不可达时维持通信

**前置条件：** 已完成配置下发，服务通信正常

**步骤：**
1. 云端与边缘之间网络中断
2. 已建立的隧道和服务通信不受影响
3. 网络恢复后，新的配置变更可以继续下发

**验收标准：**
- 断网后现有服务通信不中断
- 无需人工干预恢复

---

## Architecture Constraints

### AC-1: 不修改 iptables

OutpostNet 不修改宿主机 iptables 规则。所有流量拦截和路由通过 K8s 原生机制（EndpointSlice、Sidecar 代理）实现。

### AC-2: 复用 K8s 原生能力

不重新实现以下 K8s 已有能力：
- Service 抽象和路由 → 使用 kube-proxy
- 服务名解析 → 使用 CoreDNS
- 负载均衡 → 使用 kube-proxy / 上层组件
- 服务治理 → 上层组件职责

### AC-3: 注解驱动配置

所有用户侧配置通过 K8s 标准 Annotation 完成，不引入额外的配置文件格式。

### AC-4: 基于 QUIC 的隧道协议

不使用 FRP 作为数据面。基于 QUIC 协议（quic-go）自研隧道层：
- QUIC Stream 作为多路复用基础，每个 Service 映射到独立 Stream
- 无队头阻塞 (HOL blocking)：一个 Service 的丢包不影响其他 Service
- 内置 TLS 1.3 加密，无需额外加密层
- 连接迁移：边缘设备 IP 变化时不断连
- 0-RTT 重连：断线后快速恢复

### AC-5: 配置即控制面

控制面本质是配置下发，不是运行时服务：
- 配置下发完成后，控制面可以离线
- 数据面根据本地持久化的配置自主运行
- 网络拓扑不变时，断网不影响现有通信

---

## Functional Requirements

### FR-1: 注解驱动的服务暴露

用户在 Pod 或 Service 上添加 OutpostNet 注解后，OutpostNet 自动完成：
- 检测被注解的资源
- 注入 Sidecar（通过 Mutating Webhook）
- 生成相应的隧道配置
- 建立隧道连接使服务可达

**测试标准：** 添加注解后 30 秒内，目标服务对授权方可达

### FR-2: 单隧道多路复用

单条 QUIC 连接可同时承载多个 Service 的流量：
- 每个 Service 映射到独立的 QUIC Stream，天然隔离
- 新增 Service 不需要新建隧道连接
- 隧道连接数与 Service 数量解耦

**测试标准：** 1 条隧道同时传输 10 个不同 Service 的请求/响应，数据无串扰

### FR-3: 自动 Sidecar 注入

通过 K8s Mutating Webhook，在 Pod 创建时：
- 检测 OutpostNet 注解
- 自动注入隧道客户端 Sidecar 容器
- 生成并挂载隧道配置 ConfigMap
- 无需修改用户的 Deployment/Pod 定义

**测试标准：** 带注解的 Pod 创建后自动包含 Sidecar 容器，原始 Pod spec 不变

### FR-4: EndpointSlice 服务发现集成

OutpostNet Controller 创建和管理 EndpointSlice 资源：
- 将隧道代理端点注册为 K8s EndpointSlice
- 使 K8s 原生 Service 路由能正确将流量导向隧道代理
- 确保 CoreDNS 解析结果指向可用的隧道端点

**测试标准：** 通过 `kubectl get endpointslice` 可看到由 OutpostNet 管理的端点，且 Service DNS 解析正确

### FR-5: 隧道配置生成与下发

Controller 根据注解和服务关系自动生成隧道配置：
- 确定源区域和目标区域
- 确定隧道路径（直连或中转）
- 生成数据面 Agent 可消费的配置
- 通过 K8s ConfigMap 下发到目标节点

**测试标准：** 添加服务注解后，相关节点的 ConfigMap 在 10 秒内更新

### FR-6: 数据面隧道建立与维护

边缘节点上的 OutpostNet Agent：
- 读取本地配置建立到目标区域的 QUIC 隧道
- 支持连接保活（Keepalive）
- 支持断线自动重连
- 支持优雅关闭

**测试标准：** 隧道断开后 10 秒内自动重连；Agent 收到 SIGTERM 后完成所有进行中请求再退出

### FR-7: 配置本地持久化

Agent 将接收到的配置持久化到本地存储：
- 配置变更时更新本地副本
- Agent 重启时从本地配置恢复
- 云端不可达时使用最后一次有效配置继续运行

**测试标准：** Agent 重启后 5 秒内恢复所有隧道连接；断网后使用本地配置正常运行

### FR-8: 中转路由支持

当两个区域不能直连时，支持通过第三方区域中转：
- 中转是一种角色，不限于云端
- 任何能对外暴露端口的区域可以作为中转
- 中转配置由控制面指定

**测试标准：** 区域 A 通过区域 C 中转访问区域 B 的服务，延迟增加不超过中转链路 RTT 的 2 倍

---

## Non-Functional Requirements

### NFR-1: 资源占用

OutpostNet Agent（数据面）在边缘节点上的资源占用：
- 内存：空闲时不超过 30MB，承载 10 个 Service 时不超过 100MB
- CPU：空闲时不超过 1% 单核，数据转发时不超过 5% 单核
- 测量方法：通过容器 cgroup 指标采集

### NFR-2: 隧道建立延迟

从配置下发到隧道可用的时间：
- 直连隧道：不超过 5 秒（不含网络 RTT）
- 中转隧道：不超过 10 秒（不含网络 RTT）
- 测量方法：从 ConfigMap 更新事件到第一个数据包可通过的时间

### NFR-3: 数据面吞吐

单条隧道连接的数据吞吐能力：
- 不低于 100 Mbps（在千兆网络环境下）
- 多路复用 10 个 Service 时吞吐下降不超过 10%
- 测量方法：iperf3 通过隧道测试

### NFR-4: 断网恢复

- 已建立隧道在控制面断网后继续工作，持续时间无限制（只要数据面网络正常）
- Agent 重启后从本地配置恢复，不依赖控制面在线
- 测量方法：断网场景自动化测试

### NFR-5: 可观测性

- Agent 暴露 Prometheus 指标端点
- 关键指标：隧道状态、活跃连接数、吞吐量、重连次数、延迟
- 测量方法：Prometheus scrape 验证

---

## Technical Reference

### Reference Implementation: EdgeWize Transport

OutpostNet v2 的架构借鉴 EdgeWize Transport（`/data/github_bak/downloads/edgewize-io/transport`）的以下模式：

| 模式 | 来源 | 在 OutpostNet 中的应用 |
|------|------|----------------------|
| 注解驱动配置 | `tunnel.edgewize.io/proxy-ports` | OutpostNet 注解定义待暴露服务 |
| Webhook Sidecar 注入 | `sidecar_webhook.go` | 自动注入隧道客户端到 Pod |
| EndpointSlice 管理 | `pod_tunnel_controller.go` | 将隧道端点注册为 K8s EndpointSlice |
| Controller 模式 | `controller-runtime` | Reconcile Loop 监听资源变化 |

### Key Difference from EdgeWize Transport

| 维度 | EdgeWize Transport | OutpostNet v2 |
|------|-------------------|---------------|
| 数据面 | FRP（端口映射） | QUIC 隧道（Service 级 Stream 复用） |
| 端口使用 | 每 Service 一个端口 | 单 QUIC 连接多 Stream |
| 隧道配置 | FRP toml 文件 | Service 级声明式配置 |
| 加密 | 无 | QUIC 内置 TLS 1.3 |
| HOL blocking | 有（TCP） | 无（QUIC Stream 独立） |
| 离线支持 | 无明确设计 | 配置持久化，断网后自主运行 |

### Legacy Code Reference

旧版 OutpostNet 代码位于 `v1-legacy/` 目录，仅作参考：
- `v1-legacy/pkg/server/` — 旧版 Server 中转架构
- `v1-legacy/pkg/client/` — 旧版 Client 实现
- `v1-legacy/pkg/protocol/` — 旧版自定义协议（可参考 Protobuf 定义）

---

## Out of Scope

以下明确不属于 OutpostNet 的职责：

- **负载均衡** — K8s kube-proxy 或上层组件负责
- **服务治理** — 流量策略、熔断、限流等由上层组件负责
- **服务注册中心** — 使用 K8s 原生 API（Service、EndpointSlice）
- **DNS 服务** — 使用 CoreDNS
- **证书管理 / mTLS** — 上层安全组件负责
- **应用层协议理解** — OutpostNet 只做 L4 转发，不理解 HTTP/gRPC 等

---

## Glossary

| Term | Definition |
|------|-----------|
| **节点组 (Node Group)** | 同一局域网内的一组边缘节点，组内 IP 互通 |
| **区域 (Region)** | 一个节点组构成一个区域。云端也是一个区域 |
| **中转 (Relay)** | 一种角色，帮助不可直连的两个区域转发流量 |
| **控制面 (Control Plane)** | 配置管理和下发，本质是配置数据 |
| **数据面 (Data Plane)** | 运行在边缘节点上的 Agent，负责实际流量转发 |
| **多路复用 (Multiplexing)** | 单条隧道连接承载多个 Service 的流量 |
