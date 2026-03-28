# Story S11: EndpointSlice 与 Service 管理

**Epic:** Epic 2 — Controller + Webhook（自动化）
**Status:** Done

## 目标
实现 EndpointSlice Reconciler，为导出服务自动创建 K8s Service 和 EndpointSlice，使服务通过 CoreDNS 可解析。

## 内容
- EndpointSlice Reconciler：
  1. Watch 带导出注解的 Pod
  2. 为每个唯一的导出服务名创建 headless Service（`{service-name}.{namespace}`）
  3. 创建 EndpointSlice，端点指向 Pod IP + Sidecar 导入端口
  4. Pod 就绪/删除时更新 EndpointSlice
- Service 标签：`app.kubernetes.io/managed-by: outpostnet-controller`
- CoreDNS 集成验证：`nslookup {service-name}.{namespace}.svc.cluster.local`
- 处理多 Pod 导出同一服务的场景（聚合端点）

## 验收标准
- 导出服务的 Pod 创建后，K8s Service 和 EndpointSlice 自动创建；其他 Pod 通过 Service DNS 名称可解析到正确端点

## 依赖
- S10: ConfigMap 自动生成

## 代码位置
- `internal/controller/reconciler/`
- `pkg/annotations/`
