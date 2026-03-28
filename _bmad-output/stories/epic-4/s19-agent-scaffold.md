# Story S19: Agent 脚手架与配置格式

**Epic:** Epic 4 — 跨区域服务访问 (Layer 2)
**Status:** Done

## 目标
搭建 Agent 二进制入口、配置格式定义、部署清单和构建流程。

## 内容
- `cmd/outpostnet-agent/main.go` + `cmd/outpostnet-agent/app/agent.go`
  - Cobra CLI，flags: --config, --region, --metrics-addr, --persistence-dir
  - 健康检查端点 (/healthz, /readyz)
  - 信号处理 (SIGTERM → graceful shutdown)
- `pkg/agentconfig/` — Agent 配置定义：
  - `AgentConfig` 结构体：region, tunnels, exports, imports, persistence, metrics
  - `LoadConfig(path)` — 从 YAML 加载
  - `ValidateConfig()` — 校验
- Agent DaemonSet 部署清单 + RBAC
- Dockerfile
- Makefile 增加 `build-agent` 目标

## 验收标准
- `make build-agent` 编译成功
- Agent 进程启动后健康检查可达

## 依赖
- S18 (Epic 3)

## 代码位置
- `cmd/outpostnet-agent/`
- `internal/agent/`
- `pkg/agentconfig/`
