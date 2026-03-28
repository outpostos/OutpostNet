# Story S16: Service-Stream 多路复用

**Epic:** Epic 3 — QUIC Tunnel Core
**Status:** Done

## 目标
实现多 Service 并发 Stream 管理，验证无队头阻塞和数据隔离。

## 内容
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

## 验收标准
- 1 条 QUIC 连接同时传输 10 个 Service，数据无串扰，Stream 互不阻塞

## 依赖
- S15

## 代码位置
- `pkg/tunnel/`
