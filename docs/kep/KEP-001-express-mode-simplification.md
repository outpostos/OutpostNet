# KEP-001: Express Mode - 简化 OutpostNet 使用体验

## 基本信息

- **KEP 编号**: 001
- **标题**: Express Mode - 简化 OutpostNet 使用体验
- **状态**: 提案 (Proposed)
- **提出者**: neov
- **创建日期**: 2025-01-16
- **目标版本**: v0.3.0

## 摘要

本 KEP 提出了 **Express Mode** 功能，作为 OutpostNet 高韧性分布式服务网格的**开发者友好入门工具**。Express Mode 的核心目标是降低新用户学习门槛，快速体验 OutpostNet 能力，而非替代 OutpostNet 的核心分布式服务网格功能。

Express Mode 提供：
1. 传统代理模式的兼容接口（类似 frp 体验）
2. 零配置快速启动能力
3. 平滑过渡到完整服务网格功能的升级路径
4. 保持与 OutpostNet 分布式架构的完全兼容性

**重要说明**：Express Mode 是学习和体验工具，生产环境推荐使用 OutpostNet 的完整分布式服务网格架构。

## 动机

### 架构背景

OutpostNet 是专为极端环境设计的高韧性分布式服务网格，采用去中心化、离线优先的架构理念。相比传统代理工具 frp，OutpostNet 提供了质的差异：

- **传统代理** (frp): 中心化架构，简单端口转发，适合开发测试
- **分布式服务网格** (OutpostNet): 去中心化架构，零信任安全，适合生产环境极端场景

### 学习曲线挑战

虽然 OutpostNet 的分布式架构提供了强大的能力，但也带来了学习成本：

#### 传统代理工具的简单性 (2 步)
```bash
# 1. 服务端
./frps -c frps.ini

# 2. 客户端  
./frpc -c frpc.ini
```

#### OutpostNet 完整架构的复杂性 (分布式服务网格)
```bash
# 1. 分布式服务注册
./OutpostNet-client register -config provider.yaml

# 2. 分布式服务发现与代理
./OutpostNet-client proxy -config consumer.yaml

# 3. PKI 证书管理
./OutpostNet-pki setup-ca && ./OutpostNet-pki issue-cert

# 4. Gossip 网络配置
./OutpostNet-node join-cluster -gossip-config cluster.yaml
```

### Express Mode 的定位

**核心理念**: Express Mode 是 OutpostNet 分布式服务网格的"训练轮"，帮助用户：

1. **快速体验**: 提供类似 frp 的简单接口，快速了解 OutpostNet 能力
2. **学习过渡**: 从传统代理概念逐步理解分布式服务网格
3. **功能探索**: 在简单场景下体验 OutpostNet 的高级特性
4. **平滑升级**: 提供升级路径到完整的分布式架构

**明确边界**: Express Mode 不是 OutpostNet 的核心功能，而是**开发者体验优化**工具。

## 设计方案

### 核心概念：Express Mode

Express Mode 是 OutpostNet 分布式服务网格的**简化入门接口**，提供类似传统代理工具的使用体验，同时保留向完整服务网格架构升级的能力：

1. **传统代理兼容**：提供类似 frp 的命令行接口和配置方式
2. **自动服务抽象**：在后台自动创建 Service/Endpoint 概念，用户无感知
3. **渐进式功能暴露**：支持逐步启用高级功能（负载均衡、健康检查等）
4. **升级路径明确**：提供清晰的迁移指南到完整分布式架构

**设计原则**：
- 降低门槛，不降低能力
- 兼容传统，不背离未来
- 简化配置，不简化架构
- 快速入门，不快速上限

### 使用示例

#### 服务端配置
```yaml
# server-express.yaml
server:
  listen_addr: "0.0.0.0:7000"
  auth_token: "demo-token"
  
express_mode:
  enabled: true
  auto_service_creation: true
  port_range: "8000-9000"
  cleanup_on_disconnect: true
```

#### 客户端使用
```bash
# 启动服务端
./OutpostNet-server -config server-express.yaml

# 启动客户端（Express Mode）
./OutpostNet-client --mode express \
                 --server 127.0.0.1:7000 \
                 --local-port 80 \
                 --remote-port 8080 \
                 --auth-token demo-token
```

## 实现计划

### Phase 1: 基础架构 (Week 1-2)
- [ ] 服务端 Express Mode 配置解析
- [ ] 自动服务创建逻辑
- [ ] 端口自动分配机制
- [ ] 基础的生命周期管理

### Phase 2: 客户端统一 (Week 3-4)
- [ ] 客户端 Express Mode 参数解析
- [ ] 统一的 register+proxy 逻辑
- [ ] 命令行接口优化
- [ ] 错误处理和重试机制

### Phase 3: 测试和文档 (Week 5-6)
- [ ] 单元测试覆盖
- [ ] 集成测试场景
- [ ] 用户文档和示例

## 总结

Express Mode 是 OutpostNet 高韧性分布式服务网格的**开发者友好入门工具**，核心价值在于：

### 战略定位
- **入门桥梁**：帮助传统代理工具用户理解分布式服务网格概念
- **学习工具**：提供渐进式学习路径，从简单到复杂
- **体验优化**：降低 OutpostNet 的首次使用门槛
- **生态完善**：与社区传统工具保持兼容性

### 架构价值
Express Mode 不改变 OutpostNet 的核心架构，而是在其之上提供简化的用户接口：
- 底层仍是分布式服务网格架构
- 自动处理 Service/Endpoint 抽象
- 保留向完整功能升级的能力
- 维持与 PKI/Gossip 等高级功能的兼容性

### 使用建议
- **学习阶段**：使用 Express Mode 快速上手和体验
- **开发测试**：适合简单的开发测试场景  
- **生产环境**：推荐升级到完整的分布式服务网格架构
- **极端环境**：必须使用完整架构以获得高韧性能力

Express Mode 是 OutpostNet 分布式服务网格生态的重要组成部分，但不是其核心价值所在。

---

**本 KEP 状态**: 提案 (Proposed)  
**下一步**: 等待社区反馈和批准，然后开始实现计划
