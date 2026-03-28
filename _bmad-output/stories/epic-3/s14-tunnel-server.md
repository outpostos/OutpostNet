# Story S14: Tunnel Server

**Epic:** Epic 3 — QUIC Tunnel Core
**Status:** Done

## 目标
实现 Tunnel Server，接受 QUIC 连接并路由 Stream 到本地服务。

## 内容
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

## 验收标准
- Tunnel Server 可接受连接，正确路由 Stream 到本地 TCP 服务

## 依赖
- S13

## 代码位置
- `pkg/tunnel/server.go`
- `pkg/tunnel/`
