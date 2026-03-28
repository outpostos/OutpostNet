# Story S29: Relay 端到端 E2E 测试

**Epic:** Epic 5 — Relay 中转 (Growth)
**Status:** Done

## 目标
通过 3 区域端到端测试验证 relay 中转的完整链路，包括连通性、并发、大数据、hop 限制和混合路径。

## 内容
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

## 验收标准
- 3 区域端到端：A→C→B relay 通信成功，指标准确，hop limit 生效

## 依赖
- S27, S28

## 代码位置
- `internal/agent/relay_test.go`
- `internal/agent/relay.go`
- `internal/controller/reconciler/`
- `pkg/agentconfig/`
