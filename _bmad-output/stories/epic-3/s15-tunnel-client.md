# Story S15: Tunnel Client

**Epic:** Epic 3 — QUIC Tunnel Core
**Status:** Done

## 目标
实现 Tunnel Client，拨号 QUIC 连接并为每个导入服务打开 Stream 进行双向转发。

## 内容
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

## 验收标准
- Client 拨号 Server，打开 Stream，数据通过隧道双向传输

## 依赖
- S14

## 代码位置
- `pkg/tunnel/client.go`
- `pkg/tunnel/stream.go`
- `pkg/tunnel/`
