# Story S8: 注解规范与解析库

**Epic:** Epic 2 — Controller + Webhook（自动化）
**Status:** Done

## 目标
定义 OutpostNet 注解格式规范，实现注解解析与验证库。

## 内容
- 定义注解格式：
  ```
  outpostnet.io/inject: "true"                           # 是否注入 Sidecar
  outpostnet.io/exports: "name:port:protocol,..."        # 导出服务列表
  outpostnet.io/imports: "name:listenPort:protocol,..."  # 导入服务列表
  outpostnet.io/sidecar-id: "custom-id"                  # 可选：自定义 Sidecar ID
  ```
- `pkg/annotations/` 包：
  - `Parse(pod)` → 解析 Pod 注解为结构化数据
  - `HasOutpostNet(pod)` → 是否标记了 OutpostNet 注解
  - `Validate()` → 校验注解格式
- 完整单元测试（正常值、边界值、非法值）

## 验收标准
- 注解解析库能正确解析各种注解格式，单元测试全部通过

## 依赖
- S7: Controller 项目脚手架

## 代码位置
- `pkg/annotations/`
