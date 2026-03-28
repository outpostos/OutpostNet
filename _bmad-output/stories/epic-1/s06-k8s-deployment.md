# Story S6: K8s 部署清单与端到端验证

**Epic:** Epic 1 — 单节点 Pod 互通（手动配置）
**Status:** Done

## 目标
编写 K8s 部署清单，在真实 K8s 环境中完成 Server + Sidecar 端到端验证。

## 内容
- Server 部署为 DaemonSet（每节点一个），挂载 hostPath 共享 Unix socket
- 应用 Pod + Sidecar 容器部署 YAML 示例
  - Pod 中包含 App 容器 + Sidecar 容器
  - 通过共享 Volume 挂载 Unix socket
- Sidecar 配置通过 ConfigMap 挂载
- 端到端测试脚本

## 验收标准
- 在 K8s 边缘节点上部署 Server + 两个 Pod（含 Sidecar），Pod A 通过服务名访问 Pod B 的 TCP/UDP 服务

## 依赖
- S5: 配置文件格式与加载

## 代码位置
- `cmd/outpostnet-server/`
- `cmd/outpostnet-sidecar/`
- `pkg/config/`
- `pkg/proxy/`
