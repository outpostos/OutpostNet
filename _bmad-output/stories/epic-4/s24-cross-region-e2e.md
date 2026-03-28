# Story S24: 跨区域端到端 E2E 测试

**Epic:** Epic 4 — 跨区域服务访问 (Layer 2)
**Status:** Done

## 目标
通过两区域端到端测试验证完整的跨区域服务访问链路，包括注解驱动、隧道建立、DNS 可达和断网恢复。

## 内容
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

## 验收标准
- 两区域端到端通信：添加注解 → 隧道自动建立 → Service DNS 可达 → 断网后不中断

## 依赖
- S22, S23

## 代码位置
- `cmd/outpostnet-agent/`
- `internal/agent/`
- `internal/controller/reconciler/`
- `pkg/agentconfig/`
