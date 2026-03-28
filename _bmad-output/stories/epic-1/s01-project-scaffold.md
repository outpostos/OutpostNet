# Story S1: 项目脚手架与构建系统

**Epic:** Epic 1 — 单节点 Pod 互通（手动配置）
**Status:** Done

## 目标
初始化 Go 项目结构和构建系统，为后续开发奠定基础。

## 内容
- Go module 初始化（go.mod）
- 目录结构：
  ```
  cmd/outpostnet-server/main.go
  cmd/outpostnet-sidecar/main.go
  pkg/proxy/
  pkg/protocol/
  pkg/config/
  ```
- Makefile：build-server、build-sidecar、test、fmt、vet
- 基础 main.go（启动 → 打印版本 → 退出）

## 验收标准
- `make build` 编译出 `outpostnet-server` 和 `outpostnet-sidecar` 二进制文件

## 依赖
无

## 代码位置
- `cmd/outpostnet-server/main.go`
- `cmd/outpostnet-sidecar/main.go`
- `pkg/proxy/`
- `pkg/protocol/`
- `pkg/config/`
- `Makefile`
- `go.mod`
