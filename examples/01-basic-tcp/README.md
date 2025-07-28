# 示例1：基础 TCP 代理

> 🎯 **最简单的 OutpostNet 使用方式**
> 
> 通过这个示例，你将学会如何使用 OutpostNet 创建基础的 TCP 透明代理。

## 📖 学习目标

通过这个示例，你将学会：
- OutpostNet 的基本工作原理
- 如何配置服务端、注册端和代理端
- 如何实现透明的 TCP 代理转发

## 🎯 应用场景

- 内网服务的外网访问
- 负载均衡和服务发现
- 服务网格的基础组件
- 替代传统的端口转发

## 📁 文件说明

```
01-basic-tcp/
├── README.md                    # 本文档
├── demo-server-config.yaml      # 服务端配置
├── demo-register-config.yaml    # 服务注册配置
├── demo-proxy-config.yaml       # 代理客户端配置
└── run.sh                       # 一键运行脚本
```

## 🚀 快速开始

### 方法1：一键运行（推荐）
```bash
# 在 OutpostNet 根目录执行
make build
cd examples/01-basic-tcp
./run.sh
```

### 方法2：手动运行（学习推荐）

#### 第1步：启动服务端
```bash
# 终端1
cd /path/to/OutpostNet
./build/outpostnet-server -config examples/01-basic-tcp/demo-server-config.yaml
```

#### 第2步：注册服务
```bash  
# 终端2
./build/outpostnet-client -config examples/01-basic-tcp/demo-register-config.yaml
```

#### 第3步：启动代理
```bash
# 终端3  
./build/outpostnet-client -config examples/01-basic-tcp/demo-proxy-config.yaml
```

#### 第4步：测试连接
```bash
# 新终端
curl http://localhost:19999
# 或用浏览器访问 http://localhost:19999
```

## 🔧 工作原理

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────┐
│  你的应用    │───▶│ OutpostNet代理   │───▶│ OutpostNet服务端 │───▶│  目标服务    │
│             │    │  (localhost:19999)│    │ (服务发现+转发)  │    │ (localhost:18000)│
└─────────────┘    └──────────────────┘    └─────────────────┘    └─────────────┘
```

### 详细流程

1. **服务端启动** - 监听端口17000，等待客户端连接
2. **服务注册** - register客户端向服务端注册本地的18000端口服务
3. **代理启动** - proxy客户端在本地19999端口创建代理
4. **请求转发** - 访问19999的请求通过服务端转发到18000端口

## 📋 配置详解

### 服务端配置 (demo-server-config.yaml)
```yaml
server:
  listen_addr: "0.0.0.0:17000"    # 服务端监听地址
  auth_token: "demo-test-token"    # 认证令牌

services:
  - metadata:
      name: "demo-web-service"     # 服务名称
      namespace: "default"        # 命名空间
    spec:
      selector:
        app: "demo-web"           # 服务选择器
      proxy_mode: "tcp"           # 代理模式
      load_balancer: "round_robin" # 负载均衡算法
```

### 注册配置 (demo-register-config.yaml)
```yaml
client:
  mode: "register"               # 注册模式
  server_addr: "127.0.0.1:17000" # 服务端地址
  auth_token: "demo-test-token"   # 认证令牌

workloads:
  - metadata:
      labels:
        app: "demo-web"           # 服务标签（匹配服务端selector）
    spec:
      local_addr: "127.0.0.1"     # 本地服务地址
      local_port: 18000           # 本地服务端口
```

### 代理配置 (demo-proxy-config.yaml)
```yaml
client:
  mode: "proxy"                  # 代理模式
  server_addr: "127.0.0.1:17000" # 服务端地址

proxies:
  - name: "demo-web-proxy"       # 代理名称
    service_name: "demo-web-service" # 目标服务名
    local_port: 19999            # 本地监听端口
    proxy_mode: "tcp"            # 代理模式
```

## 🧪 测试和验证

### 1. 检查服务状态
```bash
# 查看服务端日志
tail -f /tmp/outpostnet-server.log

# 查看注册的服务
curl http://localhost:17000/api/v1/services
```

### 2. 测试代理功能
```bash
# 基本连接测试
curl -i http://localhost:19999

# 性能测试
ab -n 100 -c 10 http://localhost:19999/

# 并发测试
for i in {1..5}; do curl http://localhost:19999 & done; wait
```

### 3. 常见问题排查
```bash
# 检查端口占用
netstat -tlnp | grep -E '17000|18000|19999'

# 检查进程状态  
ps aux | grep outpostnet

# 查看详细日志
./build/outpostnet-server -config demo-server-config.yaml -v=2
```

## 🎓 知识点总结

通过这个示例，你应该理解了：

1. **OutpostNet的三个核心组件**：
   - Server：服务发现和路由中心
   - Register Client：服务提供方，注册本地服务
   - Proxy Client：服务消费方，创建本地代理

2. **服务发现机制**：
   - 通过标签(labels)和选择器(selector)匹配服务
   - 支持动态服务注册和发现

3. **配置模式**：
   - 声明式配置，类似 Kubernetes Service
   - 统一的认证和安全机制

## ➡️ 下一步

恭喜！你已经掌握了 OutpostNet 的基础用法。接下来可以学习：

- [示例2：UDP 代理](../02-udp-proxy/README.md) - 学习 UDP 协议代理
- [示例3：STCP 加密隧道](../03-stcp-tunnel/README.md) - 学习安全加密传输
- [示例4：Phase 2 区域网格](../04-phase2-mesh/README.md) - 学习高级分布式功能

## ❓ 常见问题

**Q: 为什么需要三个组件？**
A: 这是分布式代理的标准架构 - Server负责协调，Register暴露服务，Proxy消费服务。

**Q: 可以只使用两个组件吗？**  
A: 可以，但会失去服务发现和负载均衡的能力，不推荐。

**Q: 端口可以自定义吗？**
A: 完全可以，修改配置文件中的端口号即可。

**Q: 支持多个服务实例吗？**
A: 支持！启动多个register客户端，服务端会自动负载均衡。