# Story S18: 吞吐基准测试与集成测试

**Epic:** Epic 3 — QUIC Tunnel Core
**Status:** Done

## 目标
通过基准测试验证吞吐性能，通过集成测试验证端到端数据完整性和故障恢复。

## 内容
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

## 验收标准
- 单 Stream 吞吐 ≥100 Mbps（千兆网络）
- 10 Service 多路复用退化 ≤10%
- 所有集成测试通过

## 依赖
- S17

## 代码位置
- `pkg/tunnel/tunnel_bench_test.go`
- `pkg/tunnel/tunnel_integration_test.go`
- `pkg/tunnel/`
