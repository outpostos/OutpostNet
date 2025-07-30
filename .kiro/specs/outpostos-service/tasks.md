# OutpostNet Implementation Tasks

## Introduction

本任务列表基于 OutpostNet 的需求文档和设计文档，结合现有代码实现状况（85% 完成度），制定了具体的开发任务。任务按照优先级和依赖关系组织，专注于可执行的代码实现任务。

## Task Overview

基于当前实现状况，任务分为三个阶段：
- **Phase 1**: 完善单节点服务网格功能 (95% → 100%)
- **Phase 2**: 实现区域服务网格功能 (45% → 100%)  
- **Phase 3**: 实现跨区域联邦功能 (10% → 100%)

## Tasks

### Phase 1: 完善单节点服务网格功能

- [x] 1. 完善基础负载均衡算法
  - 优化 `pkg/loadbalancer/round_robin.go` 中的轮询算法实现
  - 完善 `pkg/loadbalancer/least_conn.go` 中的最少连接算法
  - 实现 `pkg/loadbalancer/weighted_round_robin.go` 加权轮询算法
  - 添加 `pkg/loadbalancer/consistent_hash.go` 一致性哈希算法
  - 编写所有负载均衡算法的单元测试
  - _Requirements: 2.6_

- [x] 1.1 实现加权轮询负载均衡器
  - 在 `pkg/loadbalancer/weighted_round_robin.go` 中实现加权轮询算法
  - 支持基于端点标签的权重配置
  - 实现动态权重调整机制
  - 添加权重配置验证和错误处理
  - _Requirements: 2.6_

- [x] 1.2 实现一致性哈希负载均衡器
  - 在 `pkg/loadbalancer/consistent_hash.go` 中实现一致性哈希算法
  - 支持虚拟节点以提高负载分布均匀性
  - 实现哈希环的动态重建机制
  - 添加哈希算法的单元测试
  - _Requirements: 2.6_

- [x] 2. 扩展服务约束条件支持
  - 扩展 `pkg/types/service.go` 中的 `ServiceConstraints` 结构
  - 实现基于标签的服务过滤逻辑（不涉及地理位置）
  - 在 `pkg/registry/memory_k8s.go` 中集成约束条件过滤
  - 添加约束条件验证和错误处理
  - _Requirements: 2.2_

- [x] 2.1 实现标签选择器过滤
  - 在 `pkg/registry/selector.go` 中实现标签选择器逻辑
  - 支持等值匹配、集合匹配等选择器操作
  - 实现选择器表达式解析和验证
  - 添加选择器过滤的单元测试
  - _Requirements: 2.2_

- [x] 2.2 实现服务健康状态过滤
  - 在服务查询时自动过滤不健康的端点
  - 实现基于心跳的健康状态判断逻辑
  - 添加健康检查超时和重试机制
  - 实现健康状态变化的事件通知
  - _Requirements: 1.8_

- [ ] 3. 完善元数据过期清理机制
  - 实现 `pkg/registry/cleanup.go` 中的自动清理逻辑
  - 实现基于 TTL 的智能过期策略
  - 添加手动清理 API 接口
  - 实现清理操作的监控指标
  - _Requirements: 6.4_

- [ ] 3.1 实现 TTL 过期策略
  - 为服务注册添加 TTL 配置支持
  - 实现基于最后心跳时间的过期判断
  - 添加过期服务的自动清理机制
  - 实现过期时间的动态配置
  - _Requirements: 6.4_

- [ ] 4. 去掉 Express Mode，实现智能默认配置
  - 移除 `cmd/outpostnet-server/main.go` 中的 Express Mode 相关代码
  - 移除 `cmd/outpostnet-client/main.go` 中的 Express Mode 相关代码
  - 将 Express Mode 的智能默认值合并到标准配置加载逻辑
  - 保留并完善 `-save-config` 配置导出功能
  - 更新命令行帮助信息和使用示例
  - _Requirements: 设计文档要求_

- [ ] 4.1 实现智能默认配置加载
  - 在 `pkg/config/defaults.go` 中定义所有组件的默认配置
  - 实现配置的优先级机制（命令行 > 配置文件 > 默认值）
  - 添加配置验证和错误提示机制
  - 实现配置的热重载功能
  - _Requirements: 设计文档要求_

- [ ] 4.2 完善配置导出功能
  - 实现 `-save-config` 命令行参数的配置导出
  - 支持导出当前运行时的完整配置
  - 添加配置文件格式验证和美化输出
  - 实现配置模板生成功能
  - _Requirements: 设计文档要求_

### Phase 2: 实现区域服务网格功能

- [ ] 5. 实现 Cluster 内服务协同
  - 实现 `pkg/mesh/cluster.go` 中的集群管理功能
  - 实现 Site 内多个 OutpostNet Server 的自动发现和组网
  - 实现集群内服务数据同步机制
  - 实现跨节点的服务请求转发
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 5.1 实现集群节点发现和管理
  - 在 `pkg/mesh/discovery.go` 中实现节点自动发现机制
  - 支持基于配置文件和环境变量的节点发现
  - 实现节点健康检查和状态同步
  - 添加节点加入和离开集群的处理逻辑
  - _Requirements: 3.1_

- [ ] 5.2 实现集群内服务数据同步
  - 在 `pkg/mesh/sync.go` 中实现 Gossip 协议的服务数据同步
  - 实现增量同步和全量同步机制
  - 添加数据一致性检查和冲突解决
  - 实现同步状态的监控和告警
  - _Requirements: 3.2_

- [ ] 5.3 实现跨节点服务请求转发
  - 在 `pkg/mesh/forwarding.go` 中实现请求转发逻辑
  - 根据端点归属判断是否需要转发到其他节点
  - 实现转发的负载均衡和故障转移
  - 添加转发性能监控和优化
  - _Requirements: 3.3, 3.4_

- [ ] 6. 实现地理位置感知负载均衡
  - 完善 `pkg/loadbalancer/geographic.go` 中的地理感知算法实现
  - 集成地理位置权重计算到现有负载均衡器工厂
  - 添加基于 Site 距离的端点优先级排序功能
  - 编写地理感知负载均衡的单元测试
  - _Requirements: 2.2, 2.3_

- [ ] 6.1 实现地理距离计算引擎
  - 在 `pkg/telemetry/distance.go` 中实现基于网络拓扑的距离计算
  - 支持 Site Hop Count 的逻辑距离计算
  - 集成到 `pkg/loadbalancer/geographic.go` 中
  - 添加距离缓存机制提升性能
  - _Requirements: 6.4_

- [ ] 6.2 实现基于地理位置的端点排序
  - 在地理感知负载均衡器中实现端点距离排序
  - 支持优先选择同 Site 内的端点
  - 实现跨 Site 的距离权重计算
  - 添加地理位置配置验证和错误处理
  - _Requirements: 2.2, 2.3_

### Phase 3: 实现跨区域联邦功能

- [ ] 7. 实现跨 Cluster 服务协同
  - 实现 `pkg/mesh/federation.go` 中的联邦管理功能
  - 实现跨 Cluster 的服务发现和请求转发
  - 实现联邦 Leader 选举和故障转移机制
  - 添加跨 Cluster 通信的安全认证
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7_

- [ ] 7.1 实现联邦管理器
  - 在 `pkg/mesh/federation.go` 中实现联邦管理逻辑
  - 支持从配置获取其他 Cluster 信息
  - 实现 Cluster 间的连接建立和维护
  - 添加联邦状态监控和健康检查
  - _Requirements: 4.1, 4.2_

- [ ] 7.2 实现联邦 Leader 选举
  - 在每个 Cluster 内实现 Leader 选举机制
  - 支持 Leader 故障时的自动切换
  - 实现 Leader 状态的集群内同步
  - 添加选举过程的监控和日志
  - _Requirements: 4.2_

- [ ] 7.3 实现跨 Cluster 服务发现
  - 实现跨 Cluster 的服务查询和响应机制
  - 支持服务信息的跨 Cluster 缓存
  - 实现跨 Cluster 服务状态同步
  - 添加跨 Cluster 服务发现的性能优化
  - _Requirements: 4.3, 4.4, 4.5_

- [ ] 7.4 实现跨 Cluster 请求转发
  - 实现本 Cluster 内无服务时的跨 Cluster 转发
  - 支持简单的请求转发（不涉及负载均衡）
  - 实现跨 Cluster 请求的路由和代理
  - 添加跨 Cluster 转发的监控和日志
  - _Requirements: 4.6, 4.7_

