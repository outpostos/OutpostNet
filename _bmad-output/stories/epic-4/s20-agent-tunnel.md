# Story S20: Agent 隧道管理

**Epic:** Epic 4 — 跨区域服务访问 (Layer 2)
**Status:** Done

## 目标
实现 Agent 隧道管理器，根据配置建立 QUIC 隧道并桥接本地 OutpostNet Server。

## 内容
- `internal/agent/tunnel_manager.go` — 隧道管理器：
  - 根据配置创建 Tunnel Server 或 Tunnel Client
  - Server 模式：接受远程连接，路由 Stream 到本地服务
  - Client 模式：连接远端，为导入服务打开 Stream
  - 使用 `pkg/tunnel.ManagedClient` 实现自动重连
- `internal/agent/bridge.go` — 本地桥接：
  - Agent 向本地 OutpostNet Server 注册导出服务（通过 Unix socket）
  - Agent 作为代理：远程隧道 ↔ 本地 Server
- Agent main 集成：启动 → 加载配置 → 建立隧道 → 注册服务
- 优雅关闭：SIGTERM → 等待 Stream 完成 → 关闭隧道
- 单元测试：隧道建立、桥接逻辑

## 验收标准
- Agent 启动后根据配置建立 QUIC 隧道
- 跨进程 TCP 通信通过隧道可达

## 依赖
- S19

## 代码位置
- `internal/agent/tunnel_manager.go`
- `internal/agent/bridge.go`
- `cmd/outpostnet-agent/`
