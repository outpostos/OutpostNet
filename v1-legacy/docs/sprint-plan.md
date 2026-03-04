# Sprint Plan - Epic 1: 单节点 Pod 互通（手动配置）

**Sprint Goal:** 实现同一边缘节点上 Pod 间 TCP/UDP 服务互访
**Status:** in-progress
**Date:** 2026-03-03

---

## Stories

| ID | Story | Status | Dependencies |
|----|-------|--------|-------------|
| S1 | 项目脚手架与构建系统 | in-progress | - |
| S2 | Server-Sidecar 通信协议与连接管理 | backlog | S1 |
| S3 | TCP 服务导出与导入（端到端） | backlog | S2 |
| S4 | UDP 服务导出与导入（端到端） | backlog | S2 |
| S5 | 配置文件格式与加载 | backlog | S3, S4 |
| S6 | K8s 部署清单与端到端验证 | backlog | S5 |

## Dependency Graph

```
S1: 项目脚手架
  │
  ▼
S2: 协议与连接 (Unix socket)
  │
  ├──────────┐
  ▼          ▼
S3: TCP    S4: UDP    ← 可并行
  │          │
  └────┬─────┘
       ▼
S5: 配置文件
       │
       ▼
S6: K8s 部署
```

## Acceptance Criteria

- **S1:** `make build` 编译出 `outpostnet-server` 和 `outpostnet-sidecar` 二进制文件
- **S2:** Sidecar 通过 Unix socket 连上 Server，注册成功，心跳正常，断开有日志
- **S3:** 两个进程模拟 Pod，通过 Server 中转完成 TCP 通信，`nc` 测试通过
- **S4:** UDP 服务通过 Server 中转通信正常，`nc -u` 测试通过
- **S5:** Server 和 Sidecar 从 YAML 配置文件启动，行为与 S3/S4 一致
- **S6:** K8s 边缘节点上部署 Server + 两个 Pod，Pod A 通过服务名访问 Pod B
