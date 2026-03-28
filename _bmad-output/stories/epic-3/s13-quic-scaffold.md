# Story S13: QUIC 隧道库脚手架与 TLS 工具

**Epic:** Epic 3 — QUIC Tunnel Core
**Status:** Done

## 目标
添加 QUIC 依赖，构建 TLS 证书工具和隧道基础类型定义，实现基础 QUIC 连接建立。

## 内容
- 添加 `quic-go` 依赖
- `pkg/tunnel/tlsutil/` — TLS 证书工具：
  - `GenerateSelfSignedCert()` — 生成自签名 TLS 证书（开发/测试用）
  - `LoadTLSConfig(certFile, keyFile)` — 从文件加载证书
  - `InsecureTLSConfig()` — 跳过验证的客户端配置（测试用）
- `pkg/tunnel/` — 基础类型定义：
  - `ServerConfig` — 监听地址、TLS 配置、keepalive 参数
  - `ClientConfig` — 远端地址、TLS 配置、重连参数
  - `ServiceRoute` — 服务名 → 本地地址映射
- 基础 QUIC listener 和 dialer（仅建立连接，不传输数据）
- 单元测试：证书生成、连接建立/关闭

## 验收标准
- `go test ./pkg/tunnel/...` 通过
- QUIC 连接可建立和关闭

## 依赖
- S12 (Epic 2)

## 代码位置
- `pkg/tunnel/`
- `pkg/tunnel/tlsutil/`
