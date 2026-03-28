# Story S10: ConfigMap 自动生成

**Epic:** Epic 2 — Controller + Webhook（自动化）
**Status:** Done

## 目标
实现 Pod Reconciler，自动为带注解的 Pod 生成 Sidecar 配置 ConfigMap。

## 内容
- Pod Reconciler：
  1. Watch 带 `outpostnet.io/injected: "true"` label 的 Pod
  2. Pod 创建/更新时，从注解生成 Sidecar YAML 配置
  3. 创建 ConfigMap `outpostnet-sidecar-{pod-name}`，内容为 sidecar.yaml
  4. ConfigMap 设置 OwnerReference → Pod 删除时自动 GC
- 配置生成逻辑：
  ```yaml
  server: "/var/run/outpostnet/server.sock"
  sidecar_id: "{pod-name}"
  heartbeat_interval: "10s"
  exports: [从注解解析]
  imports: [从注解解析]
  ```
- 处理 Pod 重建场景（ConfigMap 已存在时更新）

## 验收标准
- 带注解的 Pod 创建后，对应的 ConfigMap 自动生成；Pod 删除后 ConfigMap 自动清理

## 依赖
- S9: Mutating Webhook

## 代码位置
- `internal/controller/reconciler/`
- `pkg/annotations/`
- `pkg/config/`
