# Express Mode 设计文档

## 核心原则

**Express Mode = K8s配置自动生成器 + 快速启动命令**

- 保持与Kubernetes Service/Endpoint完全兼容
- Express Mode只是简化用户输入，底层依然是标准K8s配置
- 生成的配置文件完全符合K8s规范

## 命令设计

### OutpostNet-server Express Mode

```bash
# 基础Express启动
./OutpostNet-server --express

# 自定义端口和认证
./OutpostNet-server --express \
  --port 7000 \
  --token demo-secret

# 预定义服务（自动生成Service配置）
./OutpostNet-server --express \
  --service web-service:80:8080 \
  --service api-service:3000:3000 \
  --token demo-secret

# 等价的完整命令
./OutpostNet-server --express \
  --port 7000 \
  --token demo-secret \
  --service web-service:80:8080 \
  --service db-service:3306:3306 \
  --lb round_robin \
  --health-check 30s
```

### OutpostNet-client Express Mode

```bash
# Register模式 Express
./OutpostNet-client register --express \
  --server 127.0.0.1:7000 \
  --token demo-secret \
  --service web-service \
  --backend 127.0.0.1:8080 \
  --labels app=web,version=v1

# Proxy模式 Express  
./OutpostNet-client proxy --express \
  --server 127.0.0.1:7000 \
  --token demo-secret \
  --service web-service \
  --local-port 8080

# 组合Express（同时register和proxy）
./OutpostNet-client --express \
  --server 127.0.0.1:7000 \
  --token demo-secret \
  --provide web-service:127.0.0.1:8080 \
  --consume api-service:3000
```

## 自动生成的K8s配置

### Server端自动生成

当用户执行：
```bash
./OutpostNet-server --express --service web-service:80:8080 --token demo-secret
```

自动生成标准的OutpostNet配置：
```yaml
# 自动生成: /tmp/OutpostNet-express-server.yaml
server:
  listen_addr: "0.0.0.0:7000"
  auth_token: "demo-secret"

k8s_api:
  enabled: true
  port: 8443

# 标准的K8s Service定义
services:
  - apiVersion: v1
    kind: Service
    metadata:
      name: web-service
      namespace: default
      labels:
        OutpostNet.io/express-mode: "true"
        OutpostNet.io/created-by: "express"
      annotations:
        OutpostNet.io/created-at: "2025-01-17T10:00:00Z"
    spec:
      type: ClusterIP
      selector:
        app: web-service  # 自动生成选择器
      ports:
      - name: http
        port: 80
        targetPort: 8080
        protocol: TCP
      proxy_mode: tcp
      load_balancer: round_robin
```

### Client端自动生成

当用户执行：
```bash
./OutpostNet-client register --express \
  --server 127.0.0.1:7000 \
  --token demo-secret \
  --service web-service \
  --backend 127.0.0.1:8080 \
  --labels app=web,version=v1
```

自动生成标准的OutpostNet配置：
```yaml
# 自动生成: /tmp/OutpostNet-express-register.yaml
client:
  server_addr: "127.0.0.1:7000"
  auth_token: "demo-secret"
  mode: "register"

# 标准的K8s Workload定义（扩展概念）
workloads:
  - apiVersion: OutpostNet.io/v1
    kind: Workload
    metadata:
      name: web-service-workload-1
      namespace: default
      labels:
        app: web-service  # 匹配Service的selector
        version: v1
        OutpostNet.io/express-mode: "true"
        OutpostNet.io/node-id: "auto-generated-node-1"
      annotations:
        OutpostNet.io/local-address: "127.0.0.1"
        OutpostNet.io/local-port: "8080"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8080
      protocol: "tcp"
```

当用户执行：
```bash
./OutpostNet-client proxy --express \
  --server 127.0.0.1:7000 \
  --token demo-secret \
  --service web-service \
  --local-port 8080
```

自动生成标准的OutpostNet配置：
```yaml
# 自动生成: /tmp/OutpostNet-express-proxy.yaml
client:
  server_addr: "127.0.0.1:7000"
  auth_token: "demo-secret"
  mode: "proxy"

# 标准的代理配置
proxies:
  - name: web-service-proxy
    service_name: web-service
    service_namespace: default
    local_port: 8080
    proxy_mode: tcp
    selector:
      app: web-service  # 默认选择器
    load_balancer: round_robin
```

## 配置文件路径策略

### 临时配置（Express Mode）
```bash
# Express模式使用临时配置文件
/tmp/OutpostNet-express-server-{timestamp}.yaml
/tmp/OutpostNet-express-register-{timestamp}.yaml  
/tmp/OutpostNet-express-proxy-{timestamp}.yaml

# 用户可以选择保存为永久配置
./OutpostNet-server --express --save-config server-config.yaml
```

### 配置显示和保存
```bash
# 显示生成的配置（不启动）
./OutpostNet-server --express --service web:80:8080 --show-config

# 保存配置到文件
./OutpostNet-server --express --service web:80:8080 --save-config my-server.yaml

# 使用已保存的配置
./OutpostNet-server -config my-server.yaml
```

## Express Mode 与标准模式的平滑过渡

### 从Express到标准配置
```bash
# 1. 开始使用Express Mode
./OutpostNet-server --express --service web:80:8080 --save-config server.yaml

# 2. 查看和编辑生成的标准配置
cat server.yaml
vi server.yaml  # 添加更多高级配置

# 3. 使用标准配置启动
./OutpostNet-server -config server.yaml

# 4. 继续使用标准K8s配置工作流
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify get services
```

### kubectl兼容性验证
```bash
# Express Mode生成的配置完全兼容kubectl
./OutpostNet-server --express --service web:80:8080

# 使用kubectl查看Express创建的资源
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify get services
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify describe service web-service

# 通过kubectl创建的资源也能被OutpostNet使用
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify apply -f my-service.yaml
```

## 完整Express工作流示例

### 5分钟完整演示
```bash
# 1. 启动Express Server (自动生成web-service配置)
./OutpostNet-server --express --service web-service:80:8080 --token demo &

# 2. 启动后端服务
python3 -m http.server 8081 &
python3 -m http.server 8082 &

# 3. 注册后端服务1
./OutpostNet-client register --express \
  --server 127.0.0.1:7000 --token demo \
  --service web-service --backend 127.0.0.1:8081 \
  --labels app=web-service,instance=backend-1 &

# 4. 注册后端服务2  
./OutpostNet-client register --express \
  --server 127.0.0.1:7000 --token demo \
  --service web-service --backend 127.0.0.1:8082 \
  --labels app=web-service,instance=backend-2 &

# 5. 启动代理客户端
./OutpostNet-client proxy --express \
  --server 127.0.0.1:7000 --token demo \
  --service web-service --local-port 8080 &

# 6. 测试负载均衡
for i in {1..10}; do curl http://localhost:8080; done

# 7. 使用kubectl验证（标准K8s接口）
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify get services
kubectl --server=https://localhost:8443 --insecure-skip-tls-verify get endpoints
```

## Express Mode 实现要点

### 1. 配置生成逻辑
- Express参数 → 标准K8s YAML → OutpostNet内部处理
- 保持100% K8s兼容性
- 支持配置导出和编辑

### 2. 参数设计原则
- 最少必需参数（--server, --token, --service）
- 智能默认值（端口、负载均衡算法等）
- 渐进式复杂度（可选的高级参数）

### 3. 错误处理
- 参数验证和智能提示
- 配置冲突检测
- 优雅的错误信息

### 4. 向后兼容
- 原有的 -config 参数依然有效
- Express生成的配置可以用 -config 加载
- 支持混合使用模式

## 核心优势

1. **降低门槛**: 一行命令即可启动，无需学习YAML
2. **保持标准**: 底层完全是K8s标准配置，无vendor lock-in  
3. **平滑过渡**: Express → 标准配置 → kubectl管理
4. **配置透明**: 用户随时可以查看和导出生成的配置
5. **工具兼容**: 生成的配置完全兼容kubectl和K8s生态

Express Mode让OutpostNet同时做到"简单易用"和"标准兼容"，是对"开发者友好"的最佳实现。