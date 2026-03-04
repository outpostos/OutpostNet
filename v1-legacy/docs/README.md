# OutpostNet 文档中心

## 📋 文档导航

### 核心设计文档 (KEP)

1. **[KEP-001: Express Mode Simplification](kep/KEP-001-express-mode-simplification.md)** ✅
   - 状态：已实现
   - 内容：Express Mode快速启动功能设计和实现

2. **[KEP-002: Site-Based Service Mesh](kep/KEP-002-site-based-service-mesh.md)** 🔄
   - 状态：开发中 (Phase 2)
   - 内容：站点服务网格架构，支持站点内多节点通信

3. **[KEP-003: Extreme Environment Distributed Service Mesh](kep/KEP-003-distributed-extreme-environment.md)** 🎯
   - 状态：未来愿景
   - 内容：极端环境下的完全去中心化服务网格（战场、救灾等场景）

### 架构和实现文档

- **[当前实现状态](current-implementation.md)** - Phase 1节点服务网格的详细说明
- **[Phase 2区域网格设计](phase2-cluster-design.md)** - Phase 2站点服务网格的技术设计
- **[架构演进总览](architecture.md)** - OutpostNet的长期架构愿景和演进路径

### 用户指南

- **[快速开始](quick-start.md)** - 5分钟上手OutpostNet
- **[Express Mode指南](express-mode-design.md)** - Express Mode的使用和配置

### 技术博客

- **[博客首页](blog/README.md)** - 技术博客索引
- **[边缘服务网格的演进](blog/edge-service-mesh-evolution.md)** - 深度技术解析

## 🎯 文档一致性保证

### Phase 演进定义 (统一标准)

- **Phase 1** (已完成): **节点服务网格** - 单节点内的完整服务网格能力
- **Phase 2** (开发中): **站点服务网格** - 站点内多节点的服务发现和通信
- **Phase 3** (待讨论): **跨站点服务网格** - 跨站点的分布式服务网格

### 关键概念定义

#### Site (站点)
- **定义**: 地理位置相关的边缘单元，如银行网点、收费站、零售门店
- **特征**: 站点内网络互通，站点间网络可能不稳定
- **Phase 2目标**: 实现站点内多节点的服务发现

#### Node (节点)
- **定义**: 单个OutpostNet-server实例
- **Phase 1**: 节点独立运行，提供节点内服务网格
- **Phase 2**: 节点加入站点，支持站点内服务共享

#### Service with Rich Metadata (丰富元数据的服务)
- **应用信息**: application, cluster标识
- **地理信息**: site, zone, region位置
- **业务信息**: namespace, labels, version等

### 职责分层 (OutpostNet vs ServiceKeel)

#### OutpostNet: 机制提供者
- 服务注册和发现
- 站点内服务同步
- 网络连接和代理
- 基础的负载均衡

#### ServiceKeel: 策略制定者  
- DNS智能路由
- 地理感知的服务选择
- 优雅降级策略
- 复杂的路由规则

## 📚 阅读建议

### 新用户入门路径
1. [快速开始](quick-start.md) - 了解基本使用
2. [当前实现状态](current-implementation.md) - 理解现有能力
3. [KEP-002](kep/KEP-002-site-based-service-mesh.md) - 了解发展方向

### 开发者路径
1. [架构演进总览](architecture.md) - 理解整体架构
2. [Phase 2设计](phase2-cluster-design.md) - 掌握技术细节
3. [技术博客](blog/edge-service-mesh-evolution.md) - 深度技术思考

### 架构师路径
1. [技术博客](blog/edge-service-mesh-evolution.md) - 架构理念和演进思考
2. [KEP-002](kep/KEP-002-site-based-service-mesh.md) - 当前技术规范
3. [KEP-003](kep/KEP-003-distributed-extreme-environment.md) - 未来技术愿景

## 🔄 文档维护

### 更新原则
- 所有文档必须与当前共识保持一致
- Phase定义在所有文档中统一
- 关键概念定义保持一致性
- 职责分层清晰明确

### 版本控制
- 每次重大架构变更时更新相关文档
- 保持KEP文档与实现的同步
- 定期review文档一致性

---

**说明**: 本文档中心确保OutpostNet项目的所有文档保持一致性和准确性，为用户、开发者和架构师提供清晰的技术指导。