# Story S5: 配置文件格式与加载

**Epic:** Epic 1 — 单节点 Pod 互通（手动配置）
**Status:** Done

## 目标
定义 YAML 配置文件格式，实现配置加载与校验，替代硬编码参数。

## 内容
- 定义 YAML 配置文件格式
- Server 配置：
  ```yaml
  # server-config.yaml
  listen: "/var/run/outpostnet/server.sock"
  log_level: "info"
  ```
- Sidecar 配置：
  ```yaml
  # sidecar-config.yaml
  server: "/var/run/outpostnet/server.sock"
  exports:
    - name: "web-service"
      local_port: 8080
      protocol: "tcp"
  imports:
    - name: "db-service"
      listen_port: 3306
      protocol: "tcp"
  ```
- 配置加载与校验
- 用配置文件替代 S3/S4 中的硬编码参数

## 验收标准
- Server 和 Sidecar 从 YAML 配置文件启动，行为与 S3/S4 一致

## 依赖
- S3: TCP 服务导出与导入
- S4: UDP 服务导出与导入

## 代码位置
- `pkg/config/`
- `cmd/outpostnet-server/`
- `cmd/outpostnet-sidecar/`
