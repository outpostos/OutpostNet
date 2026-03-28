# Story S26: Agent Relay 转发

**Epic:** Epic 5 — Relay 中转 (Growth)
**Status:** Done

## 目标
实现 Agent 的 Relay 转发器，支持 Stream 级别的跨区域中继转发。

## 内容
- `internal/agent/relay.go` — Relay 转发器：
  - `RelayManager` — 管理 relay 规则和转发逻辑
  - 注册 relay 规则到 tunnel server 的 stream handler
  - Stream 处理流程：
    1. 读取 ServiceHeader（服务名）
    2. 查找 relay 规则（匹配 service + from_region）
    3. 在目标区域的 tunnel client 上打开新 Stream
    4. 写入 ServiceHeader 到下游 Stream
    5. 双向数据转发（上游 Stream ↔ 下游 Stream）
  - Hop 计数：在 ServiceHeader 后附加 1 字节 hop count
    - 每次 relay +1，超过 max_hops 时拒绝
  - 错误处理：下游不可达时关闭上游 Stream 并记录日志
- `internal/agent/tunnel_manager.go` — 集成 relay：
  - Server 收到 stream 时，先查本地路由，再查 relay 规则
  - 提供 `GetClient(region)` 方法供 relay 使用
- `internal/agent/agent.go` — 集成 RelayManager 到 Agent 生命周期
- 单元测试：3 个进程模拟 A→C→B relay

## 验收标准
- 3 进程测试：Region A 通过 Region C relay 访问 Region B 的 TCP 服务

## 依赖
- S25

## 代码位置
- `internal/agent/relay.go`
- `internal/agent/tunnel_manager.go`
- `internal/agent/agent.go`
