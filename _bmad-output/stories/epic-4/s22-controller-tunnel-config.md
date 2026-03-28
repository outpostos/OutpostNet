# Story S22: Controller 跨区域隧道配置生成

**Epic:** Epic 4 — 跨区域服务访问 (Layer 2)
**Status:** Done

## 目标
扩展 Controller 的 Region Reconciler，检测跨区域服务关系并自动生成 Agent ConfigMap。

## 内容
- 跨区域注解扩展：
  - `outpostnet.io/region: "<region>"` — 标识 Pod 所在区域
  - `outpostnet.io/imports: "svc:port:proto@region"` — `@region` 表示跨区域导入
- `internal/controller/reconciler/region_reconciler.go` — Region Reconciler：
  - Watch 带区域注解的 Pod
  - 检测跨区域服务关系（A.imports 引用了 B.exports）
  - 为每个区域生成 Agent ConfigMap（隧道配置）
  - 为跨区域导入的服务创建本地 Service + EndpointSlice（指向 Agent 代理端口）
- Agent ConfigMap 生成逻辑：
  ```yaml
  # 生成到 edge-a 区域的 Agent ConfigMap
  region: edge-a
  tunnels:
    - remote_region: cloud
      remote_addr: <cloud-agent-service>:4242
      role: client
  imports:
    - name: cloud-db
      listen_addr: "0.0.0.0:13306"
      remote_region: cloud
  ```
- Controller main 注册新 Reconciler

## 验收标准
- 带跨区域注解的 Pod 创建后，Controller 自动生成 Agent ConfigMap 和代理 Service

## 依赖
- S21

## 代码位置
- `internal/controller/reconciler/region_reconciler.go`
- `cmd/outpostnet-agent/`
- `pkg/agentconfig/`
