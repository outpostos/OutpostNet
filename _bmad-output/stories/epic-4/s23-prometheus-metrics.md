# Story S23: Prometheus 指标与可观测性

**Epic:** Epic 4 — 跨区域服务访问 (Layer 2)
**Status:** Done

## 目标
为 Agent 添加 Prometheus 格式的指标端点，覆盖隧道状态、流量和连接生命周期关键指标。

## 内容
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

## 验收标准
- `curl agent:9090/metrics` 返回 Prometheus 格式指标，包含所有关键指标 (NFR-5)

## 依赖
- S20

## 代码位置
- `internal/agent/metrics.go`
- `cmd/outpostnet-agent/`
- `pkg/agentconfig/`
