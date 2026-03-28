# Story S12: K8s E2E 集成验证

**Epic:** Epic 2 — Controller + Webhook（自动化）
**Status:** Done

## 目标
在真实 K8s 集群中部署 Controller 并完成端到端自动化验证。

## 内容
- 部署 Controller 到集群
- 测试场景：
  1. 创建带注解的 echo-server Deployment → 验证 Sidecar 注入
  2. 验证 ConfigMap 自动生成
  3. 创建带注解的 echo-client Deployment → 验证 Sidecar 注入
  4. 验证 echo-client 通过 Service DNS 访问 echo-server
  5. 删除 Pod → 验证 ConfigMap 和 EndpointSlice 清理
- E2E 测试脚本 `hack/e2e-controller.sh`
- 示例 Deployment YAML（取代 Epic 1 手动 Pod YAML）

## 验收标准
- 用户只需在 Deployment 上添加 OutpostNet 注解，无需手动编写 ConfigMap 或 Sidecar 容器定义，服务通过 DNS 名称可达

## 依赖
- S11: EndpointSlice 与 Service 管理

## 代码位置
- `cmd/outpostnet-controller/`
- `internal/controller/webhook/`
- `internal/controller/reconciler/`
- `pkg/annotations/`
