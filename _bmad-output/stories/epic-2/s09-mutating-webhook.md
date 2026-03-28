# Story S9: Mutating Webhook

**Epic:** Epic 2 — Controller + Webhook（自动化）
**Status:** Done

## 目标
实现 Mutating Webhook，在 Pod 创建时自动注入 Sidecar 容器和必要的 Volume。

## 内容
- Webhook Server 启动（TLS 证书通过 controller-runtime 内置 cert 管理或挂载 Secret）
- MutatingWebhookConfiguration 清单
- Pod 创建时拦截逻辑：
  1. 检查 `outpostnet.io/inject: "true"` 注解
  2. 解析 exports/imports 注解
  3. 注入 Sidecar 容器（image、args、volumeMounts）
  4. 注入共享 Volume（hostPath: `/var/run/outpostnet`）
  5. 注入 ConfigMap Volume（由 S10 的 Reconciler 创建）
  6. 添加 label `outpostnet.io/injected: "true"` 标记已注入
- 幂等性：已注入的 Pod 不重复注入
- 单元测试：mock admission request

## 验收标准
- 创建带注解的 Pod 时，Webhook 自动注入 Sidecar 容器和必要的 Volume

## 依赖
- S8: 注解规范与解析库

## 代码位置
- `internal/controller/webhook/`
- `pkg/annotations/`
- `cmd/outpostnet-controller/`
