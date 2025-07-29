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

### 动态服务发现架构
```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────┐
│  你的应用    │───▶│ OutpostNet代理   │───▶│ OutpostNet服务端 │───▶│  目标服务    │
│             │    │  (localhost:19999)│    │   (端点聚合)    │    │ (localhost:18000)│
└─────────────┘    └──────────────────┘    └─────────────────┘    └─────────────┘
```

### Service 是动态聚合概念

**核心理念：Service = Proxy 通过 selector 动态聚合 Endpoints 的结果**

```
步骤1: 端点注册
Register Client 注册端点：
- IP: 127.0.0.1:18000  
- Labels: {app: "demo-web", version: "v1", zone: "local"}

步骤2: 代理请求聚合  
Proxy Client 请求：
- endpoint_selector: {app: "demo-web"}
- load_balancer: "round_robin"

步骤3: 服务端动态聚合
Server 实时匹配：
1. 查找匹配 {app: "demo-web"} 的端点
2. 形成虚拟 Service = [127.0.0.1:18000]  
3. 应用负载均衡策略
4. 建立代理连接
```

### 详细流程

1. **服务端启动** - 监听端口17000，无需预定义任何服务
2. **端点注册** - register客户端注册端点到服务端，带上标签信息
3. **代理启动** - proxy客户端指定需要的端点标签选择器
4. **动态聚合** - 服务端实时匹配端点，形成虚拟服务
5. **请求转发** - 访问19999的请求动态转发到匹配的端点

## 📋 配置详解

### 服务端配置 (demo-server-config.yaml) - 简化设计
```yaml
server:
  listen_addr: "0.0.0.0:17000"
  auth_token: "demo-test-token"

# 动态服务发现配置 - 无需预定义服务！
service_discovery:
  mode: "dynamic"                    # 动态模式
  auto_create_services: true         # 自动创建服务
  default_load_balancer: "round_robin"
  service_ttl: "24h"                 # 服务生存时间
  cleanup_interval: "1h"             # 清理间隔
```

### 注册配置 (demo-register-config.yaml) - 注册端点
```yaml
client:
  mode: "register"               # 注册模式
  server_addr: "127.0.0.1:17000"
  auth_token: "demo-test-token"

workloads:
  - metadata:
      name: "web-backend-1"               # 端点名称
      labels:
        app: "demo-web"                   # 应用标签 (用于匹配)
        version: "v1"                     # 版本标签
        node: "demo-register-node"        # 节点标签
        zone: "local"                     # 区域标签
        tier: "frontend"                  # 层级标签
    spec:
      local_addr: "127.0.0.1"             # 端点IP地址
      local_port: 18000                   # 端点端口
      protocol: "tcp"                     # 协议类型
```

### 代理配置 (demo-proxy-config.yaml) - 端点选择器
```yaml
client:
  mode: "proxy"
  server_addr: "127.0.0.1:17000"
  auth_token: "demo-test-token"

proxies:
  - name: "demo-web-proxy"
    local_port: 19999                    # 本地代理端口
    proxy_mode: "tcp"                    # 代理模式
    
    # 端点选择器 - 告诉服务端我要什么样的端点
    endpoint_selector:
      app: "demo-web"                    # 选择 app=demo-web 的端点
    
    # 负载均衡策略 (可选)
    load_balancer: "round_robin"
    
    # 端点过滤器 (可选)
    endpoint_filter:
      prefer_labels:                     # 优先选择的端点标签
        zone: "local"
        version: "v1"
      exclude_labels:                    # 排除的端点标签
        status: "maintenance"
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

1. **OutpostNet的核心设计理念**：
   - **Service = 动态聚合概念**：不是预定义实体，而是端点的聚合结果
   - **端点驱动**：工作负载注册端点，代理通过标签选择
   - **实时匹配**：服务端实时查询和聚合端点

2. **三个核心组件的新角色**：
   - **Server**：端点注册表 + 标签匹配引擎 + 动态聚合器
   - **Register Client**：端点注册者，提供带标签的端点信息
   - **Proxy Client**：端点聚合者，通过选择器获取端点列表

3. **动态服务发现机制**：
   - **标签匹配**：`endpoint.labels` 匹配 `proxy.endpoint_selector`
   - **实时聚合**：无需预定义服务，按需聚合端点
   - **多视角服务**：同一批端点可以形成多个不同的服务视角

4. **配置模式的变化**：
   - **服务端简化**：无需预定义服务，只需配置聚合策略
   - **端点丰富**：register配置包含丰富的标签信息
   - **代理灵活**：proxy通过选择器定义需要的端点特征

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