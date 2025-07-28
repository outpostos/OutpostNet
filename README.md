# OutpostNet

🚀 **Cloud-native network proxy for edge computing environments**

OutpostNet 是 OutpostOS 项目的核心网络代理组件，为边缘计算环境提供轻量级的 TCP/UDP 隧道功能，采用云原生设计理念，支持分布式服务发现和负载均衡。

> 🎯 **Getting Started?** Jump to our [Examples Directory](examples/) for hands-on configuration examples and demos!

## ✨ 特性

### 🎯 核心功能
- **服务端 + 客户端架构** - 完整的代理服务端和客户端，可替换 frp/ngrok
- **多协议支持** - 支持 tcp、udp、stcp、sudp 四种代理模式
- **云原生概念** - 引入 Kubernetes Service、Endpoint、Label 等概念
- **配置驱动** - 通过 YAML 配置文件定义代理规则
- **多协议隧道** - 支持 TCP、UDP、STCP(加密)、SUDP(加密UDP) 四种代理模式

### 🔧 技术特性
- **轻量级设计** - 最小化资源消耗，适合边缘环境部署
- **健康感知** - 自动检测端点健康状态，智能路由
- **高级负载均衡** - 支持 5 种算法：轮询、加权轮询、最少连接、随机、一致性哈希
- **标签选择器** - 基于 Kubernetes 风格的标签选择器进行服务发现
- **线程安全** - 完全并发安全的架构设计
- **可扩展性** - 插件化的负载均衡和服务注册表架构

## 📦 安装

### 从源码构建

**环境要求:**
- Go 1.23.0 或更高版本
- Git

**构建步骤:**

```bash
# 克隆代码
git clone https://github.com/outpostos/OutpostNet.git
cd OutpostNet

# 构建服务端和客户端
make build

# 验证安装
./build/outpostnet-server --version
./build/outpostnet-client --version
```

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行集成测试
go test -run TestServerStartup -v
go test -run TestPhase1Complete -v
```

## 🚀 快速开始

> 💡 **Recommended**: Check out our [Examples Directory](examples/) for ready-to-use configurations and complete demos!

### 选项1: 使用示例配置 (推荐)

```bash
# 构建二进制文件
make build

# 运行完整演示
./examples/quick-start/demo.sh

# 或查看所有可用示例
make examples
```

### 选项2: 手动配置

#### 1. 启动 OutpostNet-server

创建服务端配置文件 `server-config.yaml` (或使用 `examples/configs/demo-server-config.yaml`):

```yaml
# 服务端配置
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "your-secret-token"

# 预定义服务
services:
  - metadata:
      name: "web-service"
      namespace: "default"
      labels:
        app: "web"
        version: "v1"
    spec:
      selector:
        app: "web"
      ports:
        - name: "http"
          port: 80
          targetPort: 8080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "round_robin"
```

启动服务端:

```bash
./OutpostNet-server -config server-config.yaml
```

### 2. 注册后端工作负载 (Register 模式) 

创建工作负载注册配置 `client-register-config.yaml`:

```yaml
# 客户端配置 - 注册模式 (K8s Pod 风格)
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "your-secret-token"
  mode: "register"

# 注册工作负载实例 (不指定服务名，只提供标签)
# Controller 会根据服务的 selector 自动发现并分配
workloads:
  - metadata:
      labels:
        app: "web"              # 服务发现标签
        version: "v1"           # 版本标签
        tier: "frontend"        # 层级标签
        zone: "east"            # 区域标签
        node: "edge-node-1"     # 节点标签
    spec:
      local_addr: "127.0.0.1"
      local_port: 8080
      protocol: "tcp"
```

启动注册客户端:

```bash
# 确保本地有服务运行在 8080 端口
python3 -m http.server 8080 &

# 启动注册客户端
./OutpostNet-client -config client-register-config.yaml
```

### 3. 代理访问服务 (Proxy 模式)

创建代理配置 `client-proxy-config.yaml`:

```yaml
# 客户端配置 - 代理模式
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "your-secret-token"
  mode: "proxy"

# 代理配置
proxies:
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 8080
    proxy_mode: "tcp"
    selector:
      app: "web"
    load_balancer: "round_robin"
```

启动代理客户端:

```bash
./OutpostNet-client -config client-proxy-config.yaml
```

### 4. 测试访问

```bash
# 通过代理访问远程服务
curl http://localhost:8080
```

## 📖 详细使用指南

### 配置文件详解

#### 服务端配置

```yaml
server:
  listen_addr: "0.0.0.0:7629"    # 监听地址
  auth_token: "secret-token"     # 认证令牌
  tls:                           # TLS 配置 (可选)
    cert_file: "server.crt"
    key_file: "server.key"
    ca_file: "ca.crt"

services:
  - metadata:
      name: "service-name"        # 服务名称
      namespace: "default"       # 命名空间
      labels:                    # 标签
        app: "web"
        tier: "frontend"
    spec:
      selector:                  # 端点选择器
        app: "web"
      ports:
        - name: "http"           # 端口名称
          port: 80               # 服务端口
          targetPort: 8080       # 目标端口
          protocol: "tcp"        # 协议类型
      proxy_mode: "tcp"          # 代理模式: tcp/udp/stcp/sudp
      load_balancer: "round_robin" # 负载均衡算法
```

#### 客户端配置

**Register 模式 (服务提供方):**

```yaml
client:
  server_addr: "server:7629"
  auth_token: "secret-token"
  mode: "register"

services:
  - name: "web-service"
    namespace: "default"
    local_addr: "127.0.0.1"      # 本地服务地址
    local_port: 8080             # 本地服务端口
    labels:
      app: "web"
      version: "v1"
      zone: "east"
    protocol: "tcp"
```

**Proxy 模式 (服务消费方):**

```yaml
client:
  server_addr: "server:7629"
  auth_token: "secret-token"
  mode: "proxy"

proxies:
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 8080             # 本地监听端口
    proxy_mode: "tcp"
    selector:                    # 端点过滤
      version: "v1"
      zone: "east"
    load_balancer: "round_robin"
```

### 负载均衡算法

OutpostNet 支持 5 种高级负载均衡算法:

| 算法 | 描述 | 适用场景 |
|------|------|----------|
| `round_robin` | 轮询算法 (默认) | 通用场景，负载均衡分布 |
| `weighted_round_robin` | 加权轮询 | 不同性能服务器，支持权重配置 |
| `least_connections` | 最少连接数 | 有状态服务，连接保持 |
| `random` | 随机选择 | 简单场景，无状态服务 |
| `consistent_hash` | 一致性哈希 | 需要会话亲和性的服务 |

### 代理模式对比

| 模式 | 用途 | 安全性 | 性能 | 适用场景 |
|------|------|--------|------|----------|
| `tcp` | 透明 TCP 代理 | 明文传输 | 高 | 内网服务访问 |
| `udp` | 透明 UDP 代理 | 明文传输 | 高 | UDP 服务代理 |
| `stcp` | 安全 TCP 隧道 | 加密传输 | 中 | 敏感数据传输 |
| `sudp` | 安全 UDP 隧道 | 加密传输 | 中 | 加密 UDP 通信 |

## 🏗️ 架构设计

### 核心概念 (K8s 风格)

OutpostNet 完全采用 Kubernetes 的设计哲学，主要概念包含:

- **Service**: 定义服务的访问策略和选择器，类似 K8s Service
- **Workload**: 具体的工作负载实例，类似 K8s Pod
- **Endpoint**: 由 Controller 根据 Service selector 自动发现 Workload 生成
- **Proxy**: 代理配置，定义如何访问 Service
- **Label**: 用于服务发现和路由的元数据标签

### 🔄 K8s 风格的服务发现流程

与传统的手动服务注册不同，OutpostNet 采用 Kubernetes 的声明式服务发现：

#### 1. 管理员定义服务模板 (Service)
```yaml
# server-config.yaml - 管理员预定义服务
services:
  - metadata:
      name: "web-service"
      namespace: "default"
    spec:
      selector:
        app: "web"        # 自动发现带有 app=web 标签的工作负载
        version: "v1"     # 可选：只选择 v1 版本
      proxy_mode: "tcp"
      load_balancer: "round_robin"
```

#### 2. 工作负载只注册自己 (Workload)
```yaml
# client-register-config.yaml - 工作负载只注册实例和标签
workloads:
  - metadata:
      labels:
        app: "web"        # 提供标签，不指定服务名
        version: "v1"
        node: "edge-1"
    spec:
      local_addr: "127.0.0.1"
      local_port: 8080
```

#### 3. Controller 自动服务发现
```
OutpostNet-server 中的 Controller 自动：
1. 扫描所有注册的工作负载实例
2. 根据服务的 selector 匹配工作负载标签  
3. 自动为匹配的服务创建/更新 Endpoint
4. 实现真正的解耦和动态发现
```

### ✨ 这样设计的优势

| 特性 | 传统方式 | K8s 风格 |
|------|----------|----------|
| **服务注册** | 工作负载指定服务名 | 工作负载只提供标签 |
| **服务发现** | 手动配置映射 | Controller 自动发现 |
| **解耦程度** | 紧耦合 | 完全解耦 |
| **动态性** | 静态配置 | 标签驱动的动态发现 |
| **扩展性** | 需要修改配置 | 自动扩容/缩容 |
| **容错性** | 单点故障 | 分布式自愈 |

### 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │  OutpostNet-client │    │  OutpostNet-server │
│                 │    │    (proxy)      │    │                 │
│  localhost:8080 │◄──►│  localhost:8080 │◄──►│   0.0.0.0:8080  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │ Service Registry│
                                               │ Load Balancer   │
                                               │ Endpoint Mgmt   │
                                               └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Backend Svc 1  │    │  OutpostNet-client │    │  Backend Svc N  │
│                 │◄──►│   (register)    │    │                 │
│   127.0.0.1:80  │    │                 │    │   127.0.0.1:80  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 工作流程

1. **服务注册**: Register 模式的客户端向 server 注册本地服务
2. **服务发现**: Proxy 模式的客户端查询可用的服务端点
3. **负载均衡**: Server 根据算法选择最优端点
4. **连接建立**: 建立从 proxy 到 register 的隧道连接
5. **数据转发**: 透明转发应用数据到后端服务

## 🧪 开发和测试

### 运行开发环境

```bash
# 启动服务端 (终端1)
go run ./cmd/OutpostNet-server -config examples/server-config.yaml

# 启动后端服务 (终端2)
python3 -m http.server 8080

# 启动注册客户端 (终端3)
go run ./cmd/OutpostNet-client -config examples/client-register-config.yaml

# 启动代理客户端 (终端4)
go run ./cmd/OutpostNet-client -config examples/client-proxy-config.yaml
```

### 测试命令

```bash
# 单元测试
go test ./pkg/config/...
go test ./pkg/registry/...
go test ./pkg/loadbalancer/...

# 集成测试
go test -run TestServerStartup -v
go test -run TestPhase1Complete -v

# 性能测试
go test -bench=. ./pkg/loadbalancer/...
```

### 调试技巧

```bash
# 启用详细日志
export KEELNET_LOG_LEVEL=debug

# 检查配置文件
./OutpostNet-server -config server-config.yaml -validate-only

# 查看服务注册状态
curl http://localhost:8443/api/services
curl http://localhost:8443/api/endpoints
```

## 🚧 开发状态

### Phase 1 - 核心代理功能 ✅ (已完成)
- [x] **项目架构和配置模型** - 完整的 YAML 配置系统
- [x] **TCP/UDP 透明代理** - 标准 TCP 和 UDP 代理功能
- [x] **STCP 加密隧道** - 点对点安全隧道，支持证书认证
- [x] **SUDP 加密UDP隧道** - UDP over TCP 加密传输
- [x] **K8s 风格服务发现** - Service/Workload/Endpoint 模型
- [x] **高级负载均衡** - 5种算法：轮询、加权、最少连接、随机、一致性哈希
- [x] **健康检查机制** - 自动端点健康监控和故障转移
- [x] **完整的测试覆盖率** - 单元测试、集成测试、端到端测试

### Phase 2 - 分级服务发现 🚧 (规划中)
- [ ] **分级路由策略** - 本节点→跨节点→跨集群→跨云边路由
- [ ] **分布式服务发现** - Gossip 协议的去中心化服务注册
- [ ] **边缘节点通信** - Unix Socket 和跨集群通信支持
- [ ] **故障自愈机制** - 自动故障检测和服务迁移

### Phase 3 - 产品完整性 📋 (未来版本)
- [ ] **边缘网关功能** - HTTP/HTTPS ingress 支持
- [ ] **性能监控指标** - Prometheus 指标导出
- [ ] **管理界面** - Web UI 和 CLI 工具
- [ ] **企业级功能** - RBAC、审计日志、配额管理

## 🤝 贡献指南

欢迎贡献代码! 请遵循以下流程:

1. Fork 项目
2. 创建特性分支: `git checkout -b feature/amazing-feature`
3. 提交更改: `git commit -m 'Add amazing feature'`
4. 推送分支: `git push origin feature/amazing-feature`
5. 提交 Pull Request

### 代码规范

- 遵循 Go 官方代码规范
- 添加必要的单元测试
- 更新相关文档
- 确保所有测试通过

## 📝 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙋‍♂️ 支持

- 📖 查看 [CLAUDE.md](CLAUDE.md) 了解详细设计文档
- 🐛 报告问题: [GitHub Issues](https://github.com/imneov/servicekeel/issues)
- 💬 讨论交流: [GitHub Discussions](https://github.com/imneov/servicekeel/discussions)
- 📧 联系邮箱: [your-email@example.com](mailto:your-email@example.com)

---

**OutpostNet** - 让边缘计算网络更简单、更安全、更高效 🚀