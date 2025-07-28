# 示例2：UDP 代理

> 🎯 **学习 UDP 协议代理**
> 
> 通过这个示例，你将学会如何使用 OutpostNet 创建 UDP 代理，适用于 DNS、游戏服务器、实时通信等场景。

## 📖 学习目标

通过这个示例，你将学会：
- UDP 代理与 TCP 代理的区别
- 如何处理无连接的 UDP 协议
- UDP 会话管理和超时控制
- 适用于 UDP 的场景和最佳实践

## 🎯 应用场景

- **DNS 服务代理** - 域名解析服务
- **游戏服务器** - 实时游戏通信  
- **视频流媒体** - 实时音视频传输
- **日志收集** - Syslog、GELF 等协议
- **监控数据** - StatsD、Graphite 等

## 📁 文件说明

```
02-udp-proxy/
├── README.md                      # 本文档
├── demo-udp-server-config.yaml    # UDP服务端配置
├── demo-udp-register-config.yaml  # UDP服务注册配置  
├── demo-udp-proxy-config.yaml     # UDP代理配置
└── run.sh                         # 一键运行脚本
```

## 🚀 快速开始

### 方法1：一键运行（推荐）
```bash
# 在 OutpostNet 根目录执行
make build
cd examples/02-udp-proxy
./run.sh
```

### 方法2：手动运行（学习推荐）

#### 第1步：启动服务端
```bash
# 终端1
cd /path/to/OutpostNet
./build/outpostnet-server -config examples/02-udp-proxy/demo-udp-server-config.yaml
```

#### 第2步：注册UDP服务
```bash
# 终端2
./build/outpostnet-client -config examples/02-udp-proxy/demo-udp-register-config.yaml
```

#### 第3步：启动UDP代理
```bash
# 终端3
./build/outpostnet-client -config examples/02-udp-proxy/demo-udp-proxy-config.yaml
```

#### 第4步：测试UDP代理
```bash
# 新终端 - 发送UDP数据包
echo "Hello UDP World!" | nc -u localhost 15353
```

## 🔧 工作原理

### UDP vs TCP 的区别

| 特性 | TCP | UDP |
|------|-----|-----|
| **连接方式** | 面向连接 | 无连接 |
| **可靠性** | 可靠传输 | 不保证可靠性 |
| **会话管理** | 自动管理 | 需要超时控制 |
| **适用场景** | Web、数据库 | DNS、游戏、流媒体 |

### OutpostNet UDP 代理流程

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────┐
│  UDP客户端   │───▶│ OutpostNet代理   │───▶│ OutpostNet服务端 │───▶│  UDP服务端   │
│             │◀───│   (会话管理)     │◀───│  (负载均衡)     │◀───│             │
└─────────────┘    └──────────────────┘    └─────────────────┘    └─────────────┘
```

#### 详细处理步骤

1. **接收UDP包** - 代理监听本地UDP端口
2. **会话识别** - 根据客户端IP+端口创建会话
3. **服务发现** - 通过服务端找到目标UDP服务
4. **数据转发** - 将UDP包转发到目标服务
5. **响应处理** - 将服务端响应返回给客户端
6. **会话超时** - 自动清理inactive会话

## 📋 配置详解

### 服务端配置 (demo-udp-server-config.yaml)
```yaml
server:
  listen_addr: "0.0.0.0:17001"      # UDP服务端端口
  auth_token: "demo-udp-token"

services:
  - metadata:
      name: "demo-udp-service"       # UDP服务名
    spec:
      selector:
        app: "demo-udp"             # 服务选择器
      ports:
        - port: 53                  # 服务端口（DNS标准端口）
          targetPort: 18001         # 目标端口
          protocol: "udp"           # UDP协议
      proxy_mode: "udp"             # UDP代理模式
      session_timeout: "30s"        # UDP会话超时
```

### 注册配置 (demo-udp-register-config.yaml)
```yaml
client:
  mode: "register"
  server_addr: "127.0.0.1:17001"

workloads:
  - metadata:
      labels:
        app: "demo-udp"             # 匹配服务选择器
    spec:
      local_addr: "127.0.0.1"
      local_port: 18001             # 本地UDP服务端口
      protocol: "udp"               # UDP协议
```

### 代理配置 (demo-udp-proxy-config.yaml)
```yaml
client:
  mode: "proxy"
  server_addr: "127.0.0.1:17001"

proxies:
  - name: "demo-udp-proxy"
    service_name: "demo-udp-service"
    local_port: 15353               # 本地UDP代理端口
    proxy_mode: "udp"
    session_timeout: "30s"          # 会话超时配置
```

## 🧪 测试和验证

### 1. 基础UDP测试
```bash
# 发送简单消息
echo "test message" | nc -u localhost 15353

# 发送JSON数据
echo '{"type":"test","data":"hello"}' | nc -u localhost 15353

# 使用netcat保持连接
nc -u localhost 15353
# 然后输入消息按回车发送
```

### 2. 模拟DNS查询
```bash
# 安装dig工具（如果没有）
# macOS: brew install bind
# Ubuntu: apt install dnsutils

# 通过代理查询DNS（如果后端是DNS服务）
dig @localhost -p 15353 example.com
```

### 3. 并发测试
```bash
# 并发发送UDP包
for i in {1..10}; do
    echo "Message $i" | nc -u localhost 15353 &
done
wait
```

### 4. 会话测试
```bash
# 测试会话超时
echo "session test 1" | nc -u localhost 15353
sleep 35  # 超过session_timeout
echo "session test 2" | nc -u localhost 15353
```

## 🎭 UDP特有功能

### 1. 会话管理
OutpostNet 为无连接的UDP协议提供会话管理：
- 基于客户端IP+端口标识会话
- 自动超时清理inactive会话
- 支持会话粘性（同一客户端路由到同一后端）

### 2. 负载均衡
支持多种UDP负载均衡策略：
- **轮询（Round Robin）** - 依次分发到不同后端
- **一致性哈希** - 相同客户端总是路由到相同后端
- **最少连接** - 路由到会话数最少的后端

### 3. 超时控制
```yaml
# 在服务端配置中设置
services:
  - spec:
      session_timeout: "30s"       # 会话超时
      idle_timeout: "5s"           # 空闲超时
      max_sessions: 1000           # 最大会话数
```

## 🚨 注意事项

### 1. UDP的局限性
- **不保证送达** - 网络丢包不会重传
- **无序到达** - 数据包可能乱序
- **重复数据** - 可能收到重复包

### 2. 防火墙考虑
```bash
# 确保UDP端口开放
# Linux firewall
sudo ufw allow 15353/udp
sudo ufw allow 17001/udp
sudo ufw allow 18001/udp

# macOS firewall
sudo pfctl -f /etc/pf.conf
```

### 3. 性能优化
- 调整缓冲区大小
- 合理设置超时时间
- 监控会话数量

## 🔍 故障排查

### 常见问题

**Q: UDP代理没有响应**
```bash
# 检查UDP端口监听
netstat -ulnp | grep -E '15353|17001|18001'

# 测试本地UDP服务
echo "direct test" | nc -u localhost 18001
```

**Q: 会话超时过快**
```bash
# 增加会话超时时间
session_timeout: "60s"  # 改为60秒
```

**Q: 性能问题**
```bash
# 查看会话统计
curl http://localhost:17001/metrics | grep udp_sessions

# 监控UDP流量
sudo tcpdump -i any -n udp port 15353
```

## 🎓 知识点总结

通过这个示例，你应该理解了：

1. **UDP代理特点**：
   - 无连接协议需要会话管理
   - 超时控制的重要性
   - 与TCP代理的配置差异

2. **会话管理机制**：
   - 基于客户端IP+端口的会话标识
   - 自动超时清理
   - 会话粘性支持

3. **适用场景**：
   - DNS服务代理
   - 实时游戏通信
   - 监控数据收集

## ➡️ 下一步

恭喜！你已经掌握了 UDP 代理的使用。接下来可以学习：

- [示例1：基础TCP代理](../01-basic-tcp/README.md) - 回顾TCP代理基础
- [示例3：STCP加密隧道](../03-stcp-tunnel/README.md) - 学习安全加密传输  
- [示例4：Phase 2区域网格](../04-phase2-mesh/README.md) - 学习高级分布式功能

## 💡 实际应用建议

1. **DNS代理场景**：设置较短的超时时间（5-10秒）
2. **游戏服务器**：设置较长的超时时间（60-300秒）
3. **日志收集**：考虑使用批量处理优化性能
4. **监控数据**：实现会话粘性确保数据一致性