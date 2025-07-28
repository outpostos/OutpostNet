# OutpostNet 使用示例

> 🎯 **循序渐进学习 OutpostNet**
> 
> 这个目录包含了 4 个独立的示例，每个示例都专注于一个特定的功能，帮助你从基础到高级全面掌握 OutpostNet。

## 🚀 5分钟快速开始

```bash
# 1. 构建项目
make build

# 2. 选择一个示例开始
cd examples/01-basic-tcp
./run.sh
```

就这么简单！每个示例都有完整的文档和一键运行脚本。

## 📚 学习路径

### 🎮 初学者路径 (5-10分钟/个)

按顺序学习这些示例，每个都建立在前一个的基础上：

| 示例 | 学习内容 | 时间 | 难度 |
|------|----------|------|------|
| **[01-basic-tcp](01-basic-tcp/)** | TCP透明代理、服务发现 | 5分钟 | ⭐ |
| **[02-udp-proxy](02-udp-proxy/)** | UDP代理、会话管理 | 8分钟 | ⭐⭐ |
| **[03-stcp-tunnel](03-stcp-tunnel/)** | 加密隧道、安全传输 | 10分钟 | ⭐⭐⭐ |
| **[04-phase2-mesh](04-phase2-mesh/)** | 分布式网格、企业级功能 | 15分钟 | ⭐⭐⭐⭐ |

### 🎯 快速选择指南

**如果你想要...**

- **快速了解 OutpostNet** → 从 [01-basic-tcp](01-basic-tcp/) 开始
- **代理DNS或游戏服务** → 直接看 [02-udp-proxy](02-udp-proxy/)  
- **保护敏感数据传输** → 直接看 [03-stcp-tunnel](03-stcp-tunnel/)
- **构建企业级服务网格** → 直接看 [04-phase2-mesh](04-phase2-mesh/)

## 📂 目录结构

```
examples/
├── README.md                          # 本文档
├── 01-basic-tcp/                      # 🎮 基础TCP代理
│   ├── README.md                      #   详细文档和教程
│   ├── demo-server-config.yaml        #   服务端配置
│   ├── demo-register-config.yaml      #   服务注册配置
│   ├── demo-proxy-config.yaml         #   代理配置
│   └── run.sh                         #   一键运行脚本
├── 02-udp-proxy/                      # 🚀 UDP代理
│   ├── README.md
│   ├── demo-udp-*-config.yaml
│   └── run.sh
├── 03-stcp-tunnel/                    # 🔒 加密隧道
│   ├── README.md
│   ├── demo-stcp-*-config.yaml
│   └── run.sh
├── 04-phase2-mesh/                    # 🌍 分布式网格
│   ├── README.md
│   ├── phase2-*-config.yaml
│   └── run.sh
├── configs/                           # 📁 原始配置文件库
├── tests/                             # 🧪 测试脚本
├── tools/                             # 🔧 测试工具
└── kubectl/                           # ☸️ Kubernetes集成
```

## 💡 OutpostNet 核心概念

### 3个核心组件

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ outpostnet- │    │  outpostnet-     │    │ outpostnet-     │
│   server    │    │ client(register) │    │ client(proxy)   │
│             │    │                  │    │                 │
│ 服务发现中心 │◄───┤   服务提供方     │    │   服务消费方     │
│ 路由和协调   │    │   注册本地服务   │    │   创建本地代理   │
└─────────────┘    └──────────────────┘    └─────────────────┘
```

### 4种代理模式

| 模式 | 用途 | 特点 | 示例场景 |
|------|------|------|----------|
| **TCP** | 透明代理 | 无加密，高性能 | Web服务、负载均衡 |
| **UDP** | 数据包转发 | 无连接，会话管理 | DNS、游戏、监控 |
| **STCP** | 加密隧道 | 端到端加密 | 数据库、API、敏感数据 |
| **混合模式** | 多协议支持 | 智能路由 | 企业服务网格 |

## 🧪 测试所有示例

### 并行测试（高级用户）
```bash
# 在不同终端中并行运行所有示例
cd examples/01-basic-tcp && ./run.sh &    # 终端1
cd examples/02-udp-proxy && ./run.sh &    # 终端2  
cd examples/03-stcp-tunnel && ./run.sh &  # 终端3
cd examples/04-phase2-mesh && ./run.sh &  # 终端4

# 等待所有示例启动
sleep 10

# 测试所有代理
curl http://localhost:19999  # TCP代理
echo "test" | nc -u localhost:15353  # UDP代理
curl -k https://localhost:18443  # STCP代理
curl http://localhost:9080  # Phase 2网格
```

### 顺序测试（推荐）
```bash
# 依次运行每个示例，学习完成后按Ctrl+C停止
for example in 01-basic-tcp 02-udp-proxy 03-stcp-tunnel 04-phase2-mesh; do
    echo "=== 开始示例: $example ==="
    cd examples/$example
    ./run.sh
    cd ../..
done
```

## 🔧 故障排查

### 常见问题

**Q: 端口被占用**
```bash
# 查看端口占用
netstat -tlnp | grep -E '17000|18000|19999'

# 清理残留进程
pkill -f outpostnet
```

**Q: 连接失败**
```bash
# 检查服务状态
ps aux | grep outpostnet

# 查看日志
tail -f /tmp/outpostnet-*.log
```

**Q: 配置错误**
```bash
# 验证配置文件
./build/outpostnet-server -config examples/01-basic-tcp/demo-server-config.yaml -validate-config
```

### 获取帮助

- 📖 查看各示例的详细README文档
- 🔧 运行 `make help` 查看构建选项
- 🐛 在 GitHub 提交问题
- 💬 查看主项目文档

## 🎯 完成学习后

当你完成所有示例后，你将掌握：

✅ **基础代理技能** - TCP/UDP/STCP代理配置和使用  
✅ **安全传输** - 加密隧道和证书管理  
✅ **服务网格** - 分布式服务发现和路由  
✅ **企业级部署** - 高可用、监控、故障转移  

**下一步行动：**
1. 在你的项目中集成 OutpostNet
2. 根据需求定制配置文件
3. 部署到生产环境
4. 建立监控和运维体系

---

🎉 **开始你的 OutpostNet 学习之旅吧！**

建议从 [01-basic-tcp](01-basic-tcp/) 开始，只需要5分钟就能看到效果。