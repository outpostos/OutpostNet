# Story S21: 配置持久化与断网恢复

**Epic:** Epic 4 — 跨区域服务访问 (Layer 2)
**Status:** Done

## 目标
实现配置持久化和 ConfigMap 监听，确保 Agent 重启和断网场景下的自动恢复。

## 内容
- `internal/agent/persistence.go` — 配置持久化：
  - 收到新配置时写入本地文件 (`/var/lib/outpostnet/config.yaml`)
  - Agent 重启时优先从本地文件加载
  - 配置版本号跟踪，避免回退
- `internal/agent/config_watcher.go` — ConfigMap 监听：
  - Watch K8s ConfigMap 变更
  - 配置变更时热更新隧道（新增/删除隧道，不影响已有连接）
  - 云端不可达时使用本地持久化配置
- 测试场景：
  - Agent 重启 → 从本地配置恢复
  - 断网 → 使用最后有效配置继续运行

## 验收标准
- Agent 重启后 5 秒内恢复隧道
- 断网后使用本地配置正常运行 (FR-7, NFR-4)

## 依赖
- S20

## 代码位置
- `internal/agent/persistence.go`
- `internal/agent/config_watcher.go`
- `pkg/agentconfig/`
