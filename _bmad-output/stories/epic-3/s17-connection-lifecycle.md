# Story S17: 连接生命周期

**Epic:** Epic 3 — QUIC Tunnel Core
**Status:** Done

## 目标
实现连接生命周期管理，包括 Keepalive、自动重连、0-RTT 重连、连接迁移和优雅关闭。

## 内容
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

## 验收标准
- 隧道断开后 10 秒内自动重连
- 优雅关闭完成所有进行中 Stream

## 依赖
- S16

## 代码位置
- `pkg/tunnel/lifecycle.go`
- `pkg/tunnel/`
