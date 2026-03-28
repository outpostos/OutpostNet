# Story S7: Controller 项目脚手架

**Epic:** Epic 2 — Controller + Webhook（自动化）
**Status:** Done

## 目标
搭建 Controller 项目框架，包含 controller-runtime Manager 启动和健康检查端点。

## 内容
- 添加 `controller-runtime` 和 `client-go` 依赖
- `cmd/outpostnet-controller/main.go` + `cmd/outpostnet-controller/app/controller.go`
  - Cobra CLI + controller-runtime Manager 启动
  - 健康检查端点 (/healthz, /readyz)
- RBAC 清单：
  - ServiceAccount, ClusterRole, ClusterRoleBinding
  - 权限：Pods (get/list/watch), ConfigMaps (get/list/watch/create/update/delete), Services (create/update/delete), EndpointSlices (create/update/delete)
- Controller Deployment 清单
- Makefile 增加 `build-controller` 目标
- Dockerfile

## 验收标准
- `make build-controller` 编译成功，Controller 进程启动后健康检查端点可达

## 依赖
- S6: K8s 部署清单与端到端验证 (Epic 1)

## 代码位置
- `cmd/outpostnet-controller/`
- `cmd/outpostnet-controller/app/controller.go`
- `Makefile`
