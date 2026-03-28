# Story S2: Server-Sidecar 通信协议与连接管理

**Epic:** Epic 1 — 单节点 Pod 互通（手动配置）
**Status:** Done

## 目标
定义 Server-Sidecar 间的消息协议，实现 Unix socket 连接管理和心跳保活。

## 内容
- 定义消息协议（消息类型、帧格式）：
  - `Register` — Sidecar 注册导出服务
  - `Connect` — Sidecar 请求导入服务
  - `Data` — 数据转发帧
  - `Heartbeat` — 保活
- Server 创建 Unix socket（`/var/run/outpostnet/server.sock`），接受 Sidecar 连接
- Sidecar 通过 Unix socket 连接 Server，完成注册握手
- 心跳保活机制
- 连接断开检测与日志

## 验收标准
- Sidecar 通过 Unix socket 连上 Server，注册成功，心跳正常，断开有日志

## 依赖
- S1: 项目脚手架与构建系统

## 代码位置
- `pkg/protocol/`
- `cmd/outpostnet-server/`
- `cmd/outpostnet-sidecar/`
