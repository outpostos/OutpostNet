# 架构概览

## 核心概念

OutpostNet 的核心是 `outpostnet-agent`——一个二进制，通过配置区分 server 和 client 角色，在两台或多台主机之间建立 QUIC 加密隧道。

这个模型与 frp 类似：frp 需要 `frps`（服务端）+ `frpc`（客户端），OutpostNet 把两者合并为一个 `outpostnet-agent`，通过 `role: server` / `role: client` 区分。

```
主机 A (server 角色)                主机 B (client 角色)
┌──────────────────┐                ┌──────────────────┐
│  App (:8080)     │  QUIC Tunnel   │     App (:9080)  │
│   │              │  (TLS 1.3)     │              │   │
│   ▼              │                │              ▼   │
│  outpostnet-agent ◄══════════════► outpostnet-agent  │
│  (export)        │                │        (import)  │
└──────────────────┘                └──────────────────┘
```

---

## 数据流

### 场景一：两台主机，一个服务（最常用）

```
App-B (主机 B, :9080) → agent-B (client) ──── QUIC ────► agent-A (server) → App-A (主机 A, :8080)
```

主机 A 的 agent 导出本地 8080 端口的服务，主机 B 的 agent 导入并监听在本地 9080 端口。用户在主机 B 访问 `localhost:9080`，流量经 QUIC 隧道透明转发到主机 A 的 8080。

### 场景二：多跳中继（三区域）

```
区域 A ──── QUIC ────► 区域 B (中继) ──── QUIC ────► 区域 C
agent-A                agent-B                       agent-C
(export)               (relay)                       (import)
```

区域 A 无法直连区域 C，通过区域 B 的 agent 做中继。agent-B 配置 `relays` 规则，自动将来自 A 的流量转发给 C。最多支持 5 跳。

### 场景三：多服务、多区域

一个 agent 可以同时配置：
- 多个 tunnel（连接到不同区域）
- 多个 export（导出多个本地服务）
- 多个 import（从不同区域导入服务）

```yaml
region: site-b
tunnels:
  - remote_region: site-a
    role: client
    remote_addr: 10.0.1.1:4242
  - remote_region: site-c
    role: server
    listen_addr: 0.0.0.0:4243
exports:
  - name: api
    local_addr: 127.0.0.1:8080
    protocol: tcp
imports:
  - name: database
    listen_addr: 127.0.0.1:5432
    remote_region: site-a
    protocol: tcp
```

---

## 协议

### QUIC 隧道（agent ↔ agent）

- 基于 QUIC（quic-go 实现），内置 TLS 1.3 加密
- 原生多流复用，每个 QUIC 连接承载多个服务流
- 可选 token 认证（两端配置相同 `auth.token`）
- 网络变化时自动恢复（QUIC 协议特性）

---

## 关键设计决策

| 决策 | 原因 |
|------|------|
| 一个二进制，两个角色 | 简化部署，减少用户认知成本（对标 frp 但更简单） |
| QUIC 作为隧道协议 | 原生多路复用、内置 TLS、移动网络下抖动恢复能力强 |
| 配置持久化 | 边缘节点网络不稳定，agent 重启后自动恢复隧道 |
| Token 认证而非证书 | 降低部署门槛，无需管理 PKI 基础设施 |

---

## 进阶：单主机多应用架构

> 以下内容适用于 **同一台主机上有多个应用** 需要共享隧道的场景。基本的跨主机隧道不需要这些组件。

对于单主机多应用，OutpostNet 提供额外两个组件：

- `outpostnet-server` — 本机路由中心（Unix socket），管理多个 sidecar 的服务注册
- `outpostnet-sidecar` — 每个应用旁运行一个，连接到 server，负责该应用的服务导出/导入

```
同一主机内：
┌────────────────────────────────────────────┐
│  App-1 → sidecar-1 ┐                      │
│                     ├─► server ──► agent ──┼──► QUIC 隧道
│  App-2 → sidecar-2 ┘   (路由)    (隧道)   │
└────────────────────────────────────────────┘
```

### 组件职责

**outpostnet-server**
- 通过 Unix domain socket 接受 sidecar 连接（同机通信，性能更高）
- 维护服务注册表：哪个 sidecar 导出了哪些服务
- 将连接请求路由到对应的 sidecar
- 心跳检测并清理失效连接

**outpostnet-sidecar**
- 连接本机 server，注册本地服务（export）或监听本地端口导入远程服务（import）
- 同时支持 TCP 和 UDP
- 每个应用独立配置、独立进程，互不影响

### 内部协议（server ↔ sidecar）

使用基于 Unix socket 的自定义二进制协议：
- 帧格式：长度前缀 + 消息类型 + Payload
- 消息类型：Handshake / Register / Connect / OpenStream / Data / StreamClose / Heartbeat
- 流模型：32-bit stream ID，控制流（ID=0）用于握手和注册
- 缓冲区：每个流 32KB

详见 [配置参考](configuration.md) 中的 server 和 sidecar 配置说明。
