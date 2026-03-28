# Story S27: Controller Relay 路径计算

**Epic:** Epic 5 — Relay 中转 (Growth)
**Status:** Done

## 目标
扩展 Controller 的 Region Reconciler，实现区域可达性矩阵和 BFS 最短路径计算，自动生成 relay 配置。

## 内容
- `internal/controller/reconciler/region_reconciler.go` — 扩展 Region Reconciler：
  - 区域可达性矩阵：
    - 输入：每个区域的 tunnel 配置（已知连接）
    - 输出：哪些区域对可以直连
  - 路径计算算法（BFS 最短路径）：
    - 对每个需要跨区域导入的服务：
      1. 如果源区域和目标区域可直连：生成直连配置
      2. 否则：BFS 找最短 relay 路径
      3. 为路径中的每个 relay 节点生成 relay 规则
  - Relay ConfigMap 生成：
    - 为 relay 区域的 Agent 生成包含 relays 字段的配置
    - 确保 relay 区域有到源和目标区域的 tunnel 配置
  - 区域连通性注解（可选）：
    - `outpostnet.io/reachable: "region-a,region-b"` — 声明可达区域
- 单元测试：
  - 直连场景（不需要 relay）
  - 单跳 relay（A→C→B）
  - 多跳 relay（A→C→D→B）
  - 不可达场景（无路径）

## 验收标准
- Controller 自动检测不可直连的区域对，计算 relay 路径，生成正确的 Agent ConfigMap

## 依赖
- S26

## 代码位置
- `internal/controller/reconciler/region_reconciler.go`
