# 快速开始指南

## 📋 当前实现状态

> **重要说明**: OutpostNet 目前处于 **Phase 1 - 节点服务网格阶段**。这是一个功能完整的单节点服务网格，为区域网格扩展奠定基础。
> 
> - **Phase 1** (当前): 节点服务网格 - 单节点一个OutpostNet-server + 节点上多个OutpostNet-client
> - **Phase 2** (规划中): 区域服务网格 - 一个区域内多个节点组成服务网格，实现区域内通信
> - **Phase 3** (待讨论): 跨区域服务网格 - 完全去中心化的分布式服务网格
>
> 详见: [架构演进文档](architecture.md) | [当前实现说明](current-implementation.md)

## 🚀 30秒极速体验 (Express Mode)

**最快速的体验方式 - 一键演示：**

```bash
# 克隆项目并进入目录
git clone https://github.com/imneov/servicekeel.git
cd servicekeel/OutpostNet

# 构建并运行完整演示
make build && ./scripts/quick-demo.sh
```

这个自动化脚本会：
- ✅ 自动构建 OutpostNet
- ✅ 启动后端服务和完整代理环境  
- ✅ 演示负载均衡和故障转移
- ✅ 验证 K8s API 兼容性
- ✅ 提供详细的测试说明

---

## 💡 Express Mode 快速命令 (设计中)

**未来的极简使用方式：**

```bash
# 服务端一行命令启动
./OutpostNet-server --express --service web:80:8080 --token demo

# 客户端一行命令注册
./OutpostNet-client register --express --service web --backend 127.0.0.1:8080 --token demo

# 客户端一行命令代理
./OutpostNet-client proxy --express --service web --local-port 8080 --token demo
```

> 📋 **注意**: Express Mode 是为了简化体验，底层依然使用标准的 Kubernetes Service/Endpoint 配置，确保完全兼容 K8s 生态。

---

## 🔧 5分钟手动体验

如果您想了解详细配置，这个指南将帮助您在 5 分钟内手动搭建一个完整的 OutpostNet 代理环境。

### 📋 准备工作

#### 环境要求
- **操作系统**: Linux, macOS, 或 Windows
- **Go 版本**: 1.23.0 或更高 (仅构建时需要)
- **网络**: 确保端口 7629, 8080, 8081, 8082 可用

#### 快速检查
```bash
# 检查 Go 版本
go version

# 检查端口可用性
netstat -an | grep -E ":(7629|8080|8081|8082)"
```

### 🏗️ 步骤 1: 构建 OutpostNet

```bash
# 1. 克隆项目
git clone https://github.com/imneov/servicekeel.git
cd servicekeel/OutpostNet

# 2. 构建二进制文件
make build
# 或者手动构建
# go build -o build/OutpostNet-server ./cmd/OutpostNet-server
# go build -o build/OutpostNet-client ./cmd/OutpostNet-client

# 3. 验证构建结果
ls -la build/
./build/OutpostNet-server --version
./build/OutpostNet-client --version
```

### 🖥️ 步骤 2: 启动后端服务

我们将启动两个简单的 HTTP 服务作为后端：

```bash
# 终端 1: 启动后端服务 1
python3 -m http.server 8081 --directory /tmp &
echo "后端服务 1 启动在端口 8081"

# 终端 2: 启动后端服务 2  
python3 -m http.server 8082 --directory /tmp &
echo "后端服务 2 启动在端口 8082"

# 验证后端服务
curl http://localhost:8081
curl http://localhost:8082
```

### ⚙️ 步骤 3: 配置 OutpostNet-server

创建服务端配置文件：

```bash
cat > server-config.yaml << 'EOF'
# OutpostNet 服务端配置
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "demo-secret-token"

# K8s API 支持 (可选)
k8s_api:
  enabled: true
  port: 8443

# 预定义服务
services:
  - metadata:
      name: "demo-web-service"
      namespace: "default"
      labels:
        app: "demo-web"
        environment: "development"
    spec:
      selector:
        app: "demo-web"  # 匹配工作负载标签
      ports:
        - name: "http"
          port: 80
          targetPort: 8080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "round_robin"
EOF
```

### 🚀 步骤 4: 启动 OutpostNet-server

```bash
# 启动服务端 (新终端)
./build/OutpostNet-server -config server-config.yaml

# 你应该看到类似输出:
# 2025/07/15 14:00:00 registered service: default/demo-web-service  
# 2025/07/15 14:00:00 OutpostNet-server listening on 0.0.0.0:7629
# 2025/07/15 14:00:00 health checker started
# 2025/07/15 14:00:00 starting K8s API on port 8443
```

### 📝 步骤 5: 注册后端服务 (Register 模式)

为两个后端服务创建注册配置：

```bash
# 注册配置 1
cat > register-config-1.yaml << 'EOF'
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "demo-secret-token"
  mode: "register"

# 工作负载注册 (K8s Pod 风格)
workloads:
  - metadata:
      labels:
        app: "demo-web"      # 匹配服务选择器
        version: "v1"
        instance: "backend-1"
        node: "local-node-1"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8081       # 对应后端服务 1
      protocol: "tcp"
EOF

# 注册配置 2
cat > register-config-2.yaml << 'EOF'
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "demo-secret-token"
  mode: "register"

workloads:
  - metadata:
      labels:
        app: "demo-web"      # 匹配服务选择器
        version: "v1"
        instance: "backend-2"
        node: "local-node-2"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8082       # 对应后端服务 2
      protocol: "tcp"
EOF
```

启动注册客户端：

```bash
# 终端 3: 注册后端服务 1
./build/OutpostNet-client -config register-config-1.yaml &

# 终端 4: 注册后端服务 2
./build/OutpostNet-client -config register-config-2.yaml &

# 观察服务端日志，应该看到:
# 2025/07/15 14:01:00 client xxx authenticated in register mode
# 2025/07/15 14:01:00 registered workload xxx with labels: map[app:demo-web ...]
# 2025/07/15 14:01:00 added endpoint xxx to health checker
```

### 🔗 步骤 6: 创建代理客户端 (Proxy 模式)

创建代理配置：

```bash
cat > proxy-config.yaml << 'EOF'
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "demo-secret-token"
  mode: "proxy"

# 代理配置
proxies:
  - name: "demo-web-proxy"
    service_name: "demo-web-service"
    service_namespace: "default"
    local_port: 8080
    proxy_mode: "tcp"
    selector:
      app: "demo-web"
    load_balancer: "round_robin"
EOF
```

启动代理客户端：

```bash
# 终端 5: 启动代理客户端
./build/OutpostNet-client -config proxy-config.yaml

# 应该看到:
# 2025/07/15 14:02:00 client xxx authenticated in proxy mode
# 2025/07/15 14:02:00 starting TCP proxy for demo-web-proxy on :8080
```

### 🧪 步骤 7: 测试负载均衡

现在通过代理访问服务，观察负载均衡效果：

```bash
# 多次访问，观察负载均衡
for i in {1..10}; do
  echo "=== 请求 $i ==="
  curl -s http://localhost:8080 | head -5
  echo "---"
  sleep 1
done
```

你应该看到请求被轮询分配到两个不同的后端服务。

### 📊 步骤 8: 验证健康检查

模拟服务故障，测试健康检查和故障转移：

```bash
# 1. 查看当前健康状态
curl -s http://localhost:8443/api/v1/endpoints | python3 -m json.tool

# 2. 停止一个后端服务
kill $(lsof -t -i:8081)

# 3. 等待 30 秒让健康检查生效
sleep 30

# 4. 再次测试访问
for i in {1..5}; do
  echo "=== 故障转移测试 $i ==="
  curl -s http://localhost:8080 | head -3
  echo "---"
done

# 5. 重新启动服务测试恢复
python3 -m http.server 8081 --directory /tmp &
sleep 30

# 6. 验证服务恢复
for i in {1..5}; do
  echo "=== 恢复测试 $i ==="  
  curl -s http://localhost:8080 | head -3
  echo "---"
done
```

### 🔍 步骤 9: 使用 kubectl 管理 (可选)

如果启用了 K8s API，可以使用 kubectl 管理：

```bash
# 查看服务
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify get services

# 查看端点
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify get endpoints

# 描述服务详情
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify describe service demo-web-service
```

## 🎯 高级功能演示

### STCP 加密隧道模式

演示安全的加密隧道：

```bash
# 1. 创建 STCP 服务配置
cat > stcp-server-config.yaml << 'EOF'
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "demo-secret-token"

services:
  - metadata:
      name: "secure-service"
      namespace: "default"
    spec:
      selector:
        app: "secure-web"
      proxy_mode: "stcp"  # 加密隧道模式
      load_balancer: "round_robin"
EOF

# 2. STCP 注册配置
cat > stcp-register-config.yaml << 'EOF'
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "demo-secret-token"
  mode: "register"

workloads:
  - metadata:
      labels:
        app: "secure-web"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8081
      protocol: "stcp"
EOF

# 3. STCP 代理配置
cat > stcp-proxy-config.yaml << 'EOF'
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "demo-secret-token"
  mode: "proxy"

proxies:
  - name: "secure-proxy"
    service_name: "secure-service"
    service_namespace: "default"
    local_port: 9443
    proxy_mode: "stcp"  # 加密隧道模式
EOF

# 4. 测试 STCP 隧道
# 启动对应的服务端、注册客户端和代理客户端
# 然后通过 https://localhost:9443 访问加密服务
```

### SUDP 加密 UDP 隧道

演示加密的 UDP 传输：

```bash
# 1. 启动 UDP 测试服务
nc -u -l 8083 &  # UDP 回显服务

# 2. SUDP 配置类似 STCP，但协议改为 "sudp"
# 3. 测试 UDP 数据通过加密隧道传输
echo "Hello UDP" | nc -u localhost 9053
```

## 🔧 故障排除

### 常见问题

#### 1. 连接被拒绝
```bash
# 检查服务是否启动
ps aux | grep OutpostNet
netstat -tlnp | grep 7629

# 检查防火墙
sudo ufw status  # Ubuntu
sudo firewall-cmd --list-ports  # CentOS
```

#### 2. 认证失败
```bash
# 确认 auth_token 一致
grep auth_token server-config.yaml proxy-config.yaml register-config-*.yaml
```

#### 3. 服务发现失败
```bash
# 检查标签匹配
# 服务的 selector 必须匹配工作负载的 labels
```

#### 4. 负载均衡不工作
```bash
# 检查端点健康状态
curl http://localhost:8443/api/v1/endpoints

# 查看健康检查日志
tail -f OutpostNet-server.log | grep "health"
```

### 日志调试

```bash
# 启用详细日志
export KEELNET_LOG_LEVEL=debug
./build/OutpostNet-server -config server-config.yaml

# 分析连接流程
tail -f OutpostNet-server.log | grep -E "(auth|proxy|endpoint)"
```

## 🎉 完成！

恭喜！您已经成功搭建了一个完整的 OutpostNet 代理环境，包括：

- ✅ **多协议支持**: TCP 透明代理正常工作
- ✅ **负载均衡**: 请求在多个后端间轮询分配
- ✅ **健康检查**: 自动故障检测和转移
- ✅ **服务发现**: K8s 风格的动态服务发现
- ✅ **kubectl 管理**: 云原生管理接口

### 下一步

- 📖 阅读 [配置详解](./configuration.md) 了解高级配置选项
- 🏗️ 查看 [架构设计](./architecture.md) 理解技术原理  
- 🎯 参考 [最佳实践](./deployment.md) 进行生产环境部署
- 🔧 探索 [协议模式](./proxy-modes.md) 学习 STCP/SUDP 加密隧道

---

**OutpostNet: 让网络代理变得简单而强大** 🚀