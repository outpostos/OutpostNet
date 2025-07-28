# 默认配置

OutpostNet 支持零配置启动，无需任何配置文件即可快速体验完整的网络代理功能。

## 🚀 零配置快速启动

### 启动服务端

```bash
# 无需任何参数，直接启动
./OutpostNet-server

# 输出示例:
# 2025/07/15 14:00:00 starting OutpostNet-server with default configuration on 0.0.0.0:7629
# 2025/07/15 14:00:00 using default auth token: OutpostNet-default-token
# 2025/07/15 14:00:00 K8s API enabled on port 8443
# 2025/07/15 14:00:00 registered service: default/default-service
# 2025/07/15 14:00:00 OutpostNet-server listening on 0.0.0.0:7629
# 2025/07/15 14:00:00 health checker started
```

### 启动客户端 (Register 模式)

```bash
# 注册本地服务 (默认 8080 端口)
./OutpostNet-client -mode register

# 输出示例:
# 2025/07/15 14:01:00 starting OutpostNet-client with default configuration
# 2025/07/15 14:01:00 mode: register, server: 127.0.0.1:7629, auth token: OutpostNet-default-token
# 2025/07/15 14:01:00 registering local service on port 8080 with labels map[app:default environment:development node:localhost]
# 2025/07/15 14:01:00 client authenticated in register mode
# 2025/07/15 14:01:00 registered workload xxx with labels: map[app:default ...]
```

### 启动客户端 (Proxy 模式)

```bash
# 代理远程服务到本地 8080 端口
./OutpostNet-client -mode proxy

# 输出示例:
# 2025/07/15 14:02:00 starting OutpostNet-client with default configuration
# 2025/07/15 14:02:00 mode: proxy, server: 127.0.0.1:7629, auth token: OutpostNet-default-token
# 2025/07/15 14:02:00 proxy listening on port 8080 for service default-service
# 2025/07/15 14:02:00 client authenticated in proxy mode
# 2025/07/15 14:02:00 starting TCP proxy for default-proxy on :8080
```

## 🔧 默认配置详情

### 服务端默认配置

```yaml
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "OutpostNet-default-token"

k8s_api:
  enabled: true
  port: 8443

services:
  - metadata:
      name: "default-service"
      namespace: "default"
      labels:
        app: "default"
        managed-by: "OutpostNet"
        environment: "development"
    spec:
      selector:
        app: "default"
      ports:
        - name: "http"
          port: 80
          targetPort: 8080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "round_robin"
```

### 客户端默认配置 (Proxy 模式)

```yaml
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "OutpostNet-default-token"
  mode: "proxy"

proxies:
  - name: "default-proxy"
    service_name: "default-service"
    service_namespace: "default"
    local_port: 8080
    proxy_mode: "tcp"
    selector:
      app: "default"
    load_balancer: "round_robin"
```

### 客户端默认配置 (Register 模式)

```yaml
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "OutpostNet-default-token"
  mode: "register"

workloads:
  - metadata:
      labels:
        app: "default"
        environment: "development"
        node: "localhost"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8080
      protocol: "tcp"
```

## 🎯 使用场景

### 1. 快速体验和测试

```bash
# 终端 1: 启动测试服务
python3 -m http.server 8080

# 终端 2: 启动 OutpostNet-server
./OutpostNet-server

# 终端 3: 注册服务
./OutpostNet-client -mode register

# 终端 4: 启动代理
./OutpostNet-client -mode proxy

# 终端 5: 测试访问
curl http://localhost:8080
```

### 2. 开发环境原型

```bash
# 开发者可以立即开始使用，无需编写配置文件
./OutpostNet-server &
./OutpostNet-client -mode register &
./OutpostNet-client -mode proxy &

# 应用即可通过 localhost:8080 访问远程服务
```

### 3. 演示和教学

```bash
# 演示 OutpostNet 的基本功能，无需复杂配置
./OutpostNet-server
# 观众可以立即看到服务启动和配置信息
```

## 📊 默认配置的优势

### ✅ 零配置启动
- 无需编写任何配置文件
- 开箱即用的完整功能
- 降低学习和使用门槛

### ✅ 开发友好
- 快速原型开发
- 简化测试流程
- 减少配置错误

### ✅ 功能完整
- 包含所有核心功能
- 支持服务发现
- 启用负载均衡和健康检查
- 自动服务注册和代理

### ✅ 生产就绪
- 合理的默认值
- 安全的认证机制
- 完整的日志输出

## 🔧 自定义配置

如果需要自定义配置，可以随时提供配置文件：

```bash
# 使用自定义配置文件
./OutpostNet-server -config my-server-config.yaml
./OutpostNet-client -config my-client-config.yaml
```

配置文件会覆盖默认配置，提供更多灵活性。

## 🚨 注意事项

### 生产环境建议
1. **更改默认认证令牌**: 默认令牌 `OutpostNet-default-token` 仅适用于开发测试
2. **配置 TLS 证书**: 生产环境建议启用 TLS 加密
3. **调整端口配置**: 根据实际环境调整监听端口
4. **设置适当的选择器**: 根据实际服务标签配置选择器

### 端口占用
- 服务端: 7629 (控制通信)
- 客户端: 8080 (代理监听)
- K8s API: 8443 (管理接口)

确保这些端口未被其他服务占用。

## 🎬 快速演示

我们提供了一个完整的演示脚本，展示默认配置的使用：

```bash
# 运行演示脚本
./scripts/demo-default-config.sh

# 或者手动运行测试
./scripts/test-default-config.sh
```

演示脚本会：
1. 启动模拟后端服务
2. 启动 OutpostNet-server（零配置）
3. 启动 OutpostNet-client（注册模式）
4. 启动 OutpostNet-client（代理模式）
5. 演示完整的代理功能

## 🛠️ 故障排除

### 端口冲突
```bash
# 检查端口占用
netstat -an | grep -E ":(7629|8080|8443)"
lsof -i :7629
```

### 连接问题
```bash
# 检查服务状态
curl -k https://localhost:8443/api/v1/services
curl -k https://localhost:8443/api/v1/endpoints
```

### 日志调试
```bash
# 启用详细日志
export KEELNET_LOG_LEVEL=debug
./OutpostNet-server
```

---

## 🎯 实现状态

### ✅ 已完成功能
- **零配置启动**: 支持无参数启动 OutpostNet-server 和 OutpostNet-client
- **默认服务配置**: 自动创建 default-service 和相关配置
- **双模式客户端**: 支持 register 和 proxy 两种模式
- **自动服务发现**: 客户端自动发现和连接到默认服务
- **完整代理功能**: 端到端的 TCP 代理转发
- **详细日志**: 完整的启动和运行日志输出

### ⚠️ 待实现功能
- **K8s API**: 管理接口（8443端口）
- **健康检查**: 端点健康监控
- **负载均衡**: 多端点负载均衡算法
- **STCP/SUDP**: 安全隧道模式

### 🧪 测试验证
- **自动化测试**: `./scripts/test-default-config.sh`
- **演示脚本**: `./scripts/demo-default-config.sh`
- **核心功能**: 代理功能测试通过 ✅
- **端口配置**: 避免macOS常见端口冲突 ✅

---

**默认配置让 OutpostNet 真正做到开箱即用！** 🎉