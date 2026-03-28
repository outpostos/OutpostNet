# Story S3: TCP 服务导出与导入

**Epic:** Epic 1 — 单节点 Pod 互通（手动配置）
**Status:** Done

## 目标
实现 TCP 服务的导出与导入，通过 Server 中转完成端到端 TCP 通信。

## 内容
- **导出侧：** Sidecar 向 Server 注册 TCP 服务（服务名 + 本地端口）
- **导入侧：** Sidecar 在 localhost 监听端口，接受 App 连接，通过 Unix socket 转发到 Server
- **Server 路由：** 匹配导入请求到对应导出 Sidecar，双向数据转发
- **导出侧转发：** Server 通过 Unix socket 将数据发给导出 Sidecar，Sidecar 连接到本地 App

**数据流：**
```
App A (:9999) → Sidecar A (导入, localhost:3306)
  → Unix socket → Server (路由: db-service → Sidecar B)
    → Unix socket → Sidecar B (导出, → localhost:3306)
      → App B (:3306)
```

## 验收标准
- 两个进程模拟 Pod，通过 Server 中转完成 TCP 通信，`nc` 测试通过

## 依赖
- S2: Server-Sidecar 通信协议与连接管理

## 代码位置
- `pkg/proxy/`
- `pkg/protocol/`
- `cmd/outpostnet-server/`
- `cmd/outpostnet-sidecar/`
