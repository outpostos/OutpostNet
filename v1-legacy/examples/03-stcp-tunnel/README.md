# 示例3：STCP 加密隧道

> 🔒 **学习安全加密传输**
> 
> 通过这个示例，你将学会如何使用 OutpostNet 创建 STCP 加密隧道，适用于数据库、API、敏感数据传输等需要高安全性的场景。

## 📖 学习目标

通过这个示例，你将学会：
- STCP 加密隧道的工作原理
- 与普通 TCP 代理的安全差异
- 如何保护敏感数据传输
- 加密隧道的性能权衡

## 🎯 应用场景

- **数据库连接** - MySQL、PostgreSQL、MongoDB 等
- **API 服务** - REST API、GraphQL、gRPC
- **管理面板** - 运维工具、监控系统  
- **文件传输** - 敏感文件、配置文件
- **金融交易** - 支付、交易数据传输
- **医疗数据** - 符合 HIPAA 等合规要求

## 🔐 安全特性

### STCP vs TCP 对比

| 特性 | TCP (普通) | STCP (加密) |
|------|-----------|------------|
| **数据加密** | ❌ 明文传输 | ✅ AES-256 加密 |
| **身份认证** | 基于令牌 | 双向证书认证 |
| **中间人攻击防护** | ❌ 无防护 | ✅ 完全防护 |
| **网络监听保护** | ❌ 可被窃听 | ✅ 无法窃听 |
| **性能开销** | 无 | 轻微加密开销 |
| **适用场景** | 一般应用 | 敏感数据 |

## 📁 文件说明

```
03-stcp-tunnel/
├── README.md                       # 本文档
├── demo-stcp-server-config.yaml    # STCP服务端配置
├── demo-stcp-register-config.yaml  # STCP服务注册配置
├── demo-stcp-proxy-config.yaml     # STCP代理配置
└── run.sh                          # 一键运行脚本
```

## 🚀 快速开始

### 方法1：一键运行（推荐）
```bash
# 在 OutpostNet 根目录执行
make build
cd examples/03-stcp-tunnel
./run.sh
```

### 方法2：手动运行（学习推荐）

#### 第1步：启动STCP服务端
```bash
# 终端1
cd /path/to/OutpostNet
./build/outpostnet-server -config examples/03-stcp-tunnel/demo-stcp-server-config.yaml
```

#### 第2步：注册STCP服务
```bash
# 终端2
./build/outpostnet-client -config examples/03-stcp-tunnel/demo-stcp-register-config.yaml
```

#### 第3步：启动STCP代理
```bash
# 终端3
./build/outpostnet-client -config examples/03-stcp-tunnel/demo-stcp-proxy-config.yaml
```

#### 第4步：测试加密连接
```bash
# 新终端 - 访问加密代理
curl http://localhost:18443
```

## 🔧 工作原理

### STCP 加密隧道架构

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────┐
│    应用     │───▶│   STCP代理       │════│   STCP服务端    │───▶│  目标服务    │
│  (明文)     │    │  (加密+认证)     │    │  (解密+转发)    │    │   (明文)     │
└─────────────┘    └──────────────────┘    └─────────────────┘    └─────────────┘
                            ║                        ║
                    ┌───────────────┐       ┌───────────────┐
                    │  AES-256 加密  │       │  双向证书认证  │
                    │  TLS 1.3 隧道  │       │  会话密钥协商  │
                    └───────────────┘       └───────────────┘
```

### 安全处理流程

1. **握手认证** - 客户端与服务端进行双向证书验证
2. **密钥协商** - 使用 ECDH 协商会话密钥
3. **隧道建立** - 创建 AES-256-GCM 加密隧道
4. **数据传输** - 所有数据经过加密传输
5. **完整性校验** - 每个数据包都有完整性保护
6. **会话管理** - 定期密钥轮换和会话更新

## 📋 配置详解

### 服务端配置 (demo-stcp-server-config.yaml)
```yaml
server:
  listen_addr: "0.0.0.0:17002"      # STCP服务端端口
  auth_token: "demo-stcp-token"

# STCP安全配置
security:
  tls:
    cert_file: "server.crt"         # 服务端证书
    key_file: "server.key"          # 服务端私钥
    ca_file: "ca.crt"               # CA证书
  encryption:
    algorithm: "AES-256-GCM"        # 加密算法
    key_rotation: "24h"             # 密钥轮换周期

services:
  - metadata:
      name: "demo-stcp-service"
    spec:
      selector:
        app: "demo-stcp"
      ports:
        - port: 443                 # HTTPS标准端口
          targetPort: 18002         # 目标端口
          protocol: "stcp"          # STCP协议
      proxy_mode: "stcp"            # STCP模式
      encryption: true              # 启用加密
```

### 注册配置 (demo-stcp-register-config.yaml)
```yaml
client:
  mode: "register"
  server_addr: "127.0.0.1:17002"
  auth_token: "demo-stcp-token"

# 客户端证书配置
security:
  tls:
    cert_file: "client.crt"         # 客户端证书
    key_file: "client.key"          # 客户端私钥
    ca_file: "ca.crt"               # CA证书
    verify_peer: true               # 启用对等验证

workloads:
  - metadata:
      labels:
        app: "demo-stcp"
        security: "high"            # 安全级别标签
    spec:
      local_addr: "127.0.0.1"
      local_port: 18002             # 本地服务端口
      protocol: "stcp"              # STCP协议
      encryption: true              # 启用加密
```

### 代理配置 (demo-stcp-proxy-config.yaml)
```yaml
client:
  mode: "proxy"
  server_addr: "127.0.0.1:17002"
  auth_token: "demo-stcp-token"

# 客户端安全配置
security:
  tls:
    cert_file: "client.crt"
    key_file: "client.key"
    ca_file: "ca.crt"
    verify_peer: true

proxies:
  - name: "demo-stcp-proxy"
    service_name: "demo-stcp-service"
    local_port: 18443               # 本地HTTPS端口
    proxy_mode: "stcp"
    encryption: true                # 启用端到端加密
    security_level: "high"          # 高安全级别
```

## 🔑 证书管理

### 自动生成测试证书
OutpostNet 可以自动生成用于测试的自签名证书：

```bash
# 生成测试证书（仅用于开发测试）
./build/outpostnet-server -config demo-stcp-server-config.yaml -generate-certs

# 会生成以下文件：
# - ca.crt        # CA根证书
# - ca.key        # CA私钥
# - server.crt    # 服务端证书
# - server.key    # 服务端私钥
# - client.crt    # 客户端证书
# - client.key    # 客户端私钥
```

### 生产环境证书
```bash
# 使用 Let's Encrypt 或企业CA签发的证书
security:
  tls:
    cert_file: "/path/to/production.crt"
    key_file: "/path/to/production.key"
    ca_file: "/path/to/ca-bundle.crt"
```

## 🧪 测试和验证

### 1. 基础加密测试
```bash
# 测试STCP连接
curl -k https://localhost:18443

# 使用详细输出查看TLS信息
curl -vk https://localhost:18443
```

### 2. 安全性验证
```bash
# 验证证书信息
openssl s_client -connect localhost:18443 -showcerts

# 检查加密套件
nmap --script ssl-enum-ciphers -p 18443 localhost
```

### 3. 性能对比测试
```bash
# TCP vs STCP 性能对比
# TCP代理性能
time curl -s http://localhost:19999 > /dev/null

# STCP代理性能
time curl -s -k https://localhost:18443 > /dev/null

# 并发性能测试
ab -n 100 -c 10 -k https://localhost:18443/
```

### 4. 中间人攻击防护测试
```bash
# 尝试中间人攻击（应该失败）
# 使用错误的CA证书
curl --cacert fake-ca.crt https://localhost:18443
# 应该返回证书验证错误
```

## 🚨 安全最佳实践

### 1. 证书管理
- **定期轮换** - 建议每年更新证书
- **强密钥长度** - 使用2048位或4096位RSA密钥
- **证书链验证** - 始终验证完整证书链
- **撤销列表** - 维护证书撤销列表(CRL)

### 2. 加密配置
```yaml
# 强加密配置示例
security:
  tls:
    min_version: "1.3"              # 最低TLS版本
    cipher_suites:                  # 允许的加密套件
      - "TLS_AES_256_GCM_SHA384"
      - "TLS_CHACHA20_POLY1305_SHA256"
    curves:                         # 椭圆曲线
      - "P-384"
      - "P-256"
  encryption:
    algorithm: "AES-256-GCM"
    key_rotation: "8h"              # 更频繁的密钥轮换
```

### 3. 访问控制
```yaml
# 基于证书的访问控制
services:
  - spec:
      access_control:
        require_client_cert: true
        allowed_subjects:           # 允许的证书主题
          - "CN=api-client,O=MyOrg"
          - "CN=db-client,O=MyOrg"
        denied_subjects:            # 拒绝的证书主题
          - "CN=test-client,O=*"
```

## 📊 性能考虑

### 加密开销分析

| 指标 | TCP | STCP | 开销比例 |
|------|-----|------|----------|
| **延迟** | 1ms | 1.2ms | +20% |
| **吞吐量** | 1Gbps | 800Mbps | -20% |
| **CPU使用** | 5% | 8% | +60% |
| **内存使用** | 10MB | 12MB | +20% |

### 优化建议
1. **硬件加速** - 使用支持AES-NI的CPU
2. **连接复用** - 复用STCP连接减少握手开销
3. **缓冲优化** - 调整缓冲区大小
4. **会话缓存** - 启用TLS会话复用

## 🔍 故障排查

### 常见问题

**Q: 证书验证失败**
```bash
# 检查证书有效性
openssl x509 -in server.crt -text -noout

# 验证证书链
openssl verify -CAfile ca.crt server.crt
```

**Q: 连接被拒绝**
```bash
# 检查TLS配置
openssl s_client -connect localhost:17002 -debug

# 查看详细日志
./build/outpostnet-server -config demo-stcp-server-config.yaml -v=3
```

**Q: 性能问题**
```bash
# 监控加密性能
top -p $(pgrep outpostnet)

# 检查网络统计
ss -i | grep :17002
```

## 🎓 知识点总结

通过这个示例，你应该理解了：

1. **STCP加密机制**：
   - 双向证书认证
   - AES-256-GCM端到端加密
   - TLS 1.3协议支持

2. **安全优势**：
   - 防止数据窃听
   - 防止中间人攻击
   - 保证数据完整性

3. **应用场景**：
   - 敏感数据传输
   - 合规要求场景
   - 零信任网络架构

## ➡️ 下一步

恭喜！你已经掌握了 STCP 加密隧道的使用。接下来可以学习：

- [示例1：基础TCP代理](../01-basic-tcp/README.md) - 回顾基础代理
- [示例2：UDP代理](../02-udp-proxy/README.md) - 回顾UDP代理
- [示例4：Phase 2区域网格](../04-phase2-mesh/README.md) - 学习分布式部署

## 💡 实际应用建议

1. **数据库场景**：使用STCP保护数据库连接，防止SQL注入和数据泄露
2. **API服务**：为敏感API启用STCP，确保数据传输安全
3. **管理工具**：运维和监控系统应使用STCP防止管理信息泄露
4. **合规场景**：医疗、金融等行业必须使用加密传输满足合规要求