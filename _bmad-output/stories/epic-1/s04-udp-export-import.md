# Story S4: UDP 服务导出与导入

**Epic:** Epic 1 — 单节点 Pod 互通（手动配置）
**Status:** Done

## 目标
扩展代理机制以支持 UDP 协议，实现 UDP 服务的导出与导入。

## 内容
- 扩展 S3 的机制支持 UDP 协议
- UDP 会话管理（基于源地址的伪连接跟踪）
- Server 侧 UDP 数据报转发
- 导出/导入双向 UDP 数据流

## 验收标准
- UDP 服务通过 Server 中转通信正常，`nc -u` 测试通过

## 依赖
- S2: Server-Sidecar 通信协议与连接管理（可与 S3 并行开发）

## 代码位置
- `pkg/proxy/`
- `pkg/protocol/`
- `cmd/outpostnet-server/`
- `cmd/outpostnet-sidecar/`
