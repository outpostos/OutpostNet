# Story S28: Relay 指标与可观测性

**Epic:** Epic 5 — Relay 中转 (Growth)
**Status:** Done

## 目标
扩展 Agent 指标，新增 relay 相关的 Prometheus 指标和健康检查。

## 内容
- `internal/agent/metrics.go` — 扩展指标：
  - `outpostnet_relay_streams_total` — relay 处理的 stream 总数 (counter, labels: from_region, to_region, service)
  - `outpostnet_relay_bytes_total` — relay 转发的字节数 (counter, labels: direction, from_region, to_region)
  - `outpostnet_relay_active_streams` — 当前活跃的 relay stream 数 (gauge)
  - `outpostnet_relay_latency_seconds` — relay stream 建立延迟 (histogram)
  - `outpostnet_relay_errors_total` — relay 错误数 (counter, labels: error_type)
- `internal/agent/relay.go` — 集成指标收集
- 健康检查扩展：
  - `/readyz` 报告 relay 路径健康状态
  - relay 下游不可达时标记降级
- 单元测试：验证指标注册和更新

## 验收标准
- `curl agent:9090/metrics` 返回 relay 相关指标，relay stream 计数和字节数准确

## 依赖
- S26

## 代码位置
- `internal/agent/metrics.go`
- `internal/agent/relay.go`
