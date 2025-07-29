# OutpostNet Requirements

## Introduction

OutpostNet 是一个面向边缘计算的开源网络服务层，专注于提供基础的服务发现、负载均衡和网络连接能力。作为独立的项目，OutpostNet 提供完整的 Workload 注册、元数据管理、遥测数据收集和订阅响应机制，为上层应用和路由系统提供标准化的 API 接口。

OutpostNet 的核心特性：
- **服务注册与发现**：支持 Workload 的动态注册和基于模式的服务发现
- **元数据管理**：维护 Workload 的地理位置、标签和网络拓扑信息
- **遥测数据收集**：实时收集网络和服务性能指标
- **订阅响应机制**：支持基于兴趣包的动态服务通知
- **负载均衡**：提供多种负载均衡算法和故障转移机制
- **高可用性**：支持集群部署和跨站点数据同步

## Concept Definitions

### Core System Components

**OutpostNet Server**: 运行在边缘节点上的服务端组件，负责接收客户端连接、管理服务注册表、执行负载均衡决策，并提供服务发现能力。Server 之间可以构建集群

**OutpostNet Client**: 运行在应用节点上的客户端组件，负责向 OutpostNet Server 注册本地服务实例，或作为代理转发应用请求到远程服务。



### Service Management Concepts

**Service Registration**: 指 OutpostNet Client 向 OutpostNet Server 声明本地服务实例可用性的过程，包括提交服务元数据和网络访问信息。

**Service HeartBeat**: 指 OutpostNet Client 向 OutpostNet Server 定期发送 HeartBeat 帮助 server 了解服务状态。

**Service Instance**: 一个具体的服务实现，运行在特定节点上的特定端口，具有唯一的实例标识符和网络地址。

**Service Name**: 服务的逻辑标识符，用于服务发现和路由，格式为 DNS 兼容的名称（如 "user-service", "payment-api"）。

**Namespace**: 服务的逻辑分组和隔离边界，类似 Kubernetes namespace 概念，用于多租户和环境隔离（如 "production", "staging", "team-a"），不提供时默认为 "default"。

**Labels**: 附加在服务实例上的键值对元数据，用于服务选择、路由策略和运维管理（如 "version=v1.2", "environment=prod", "team=backend"）。

**Endpoints**: 服务实例的网络访问点列表，包含 IP 地址、端口号和协议类型（如 "192.168.1.100:8080/tcp", "192.168.1.100:9090/http"）。这部分可以看下 K8s 中服务中的 Port 定义

**Protocols**: 服务实例支持的通信协议列表，用于协议适配和路由决策（如 ["HTTP/1.1", "HTTP/2", "gRPC", "WebSocket"]）。这部分不做强制要求，client 可以加上，也可以不加

### Geographic and Network Concepts

**Site**: 最小的地理部署单元，通常对应单个边缘节点或节点集群（如 "edge-node-001", "factory-floor-hub"）。是一个物理意义上的联系单元，Site 内网络是互通的

**Node ID**: 运行 OutpostNet 组件的物理或虚拟节点的唯一标识符（如 "node-uuid-12345", "edge-gateway-beijing-01"）。

**Site Hop Count**: 网络拓扑中两个节点间的逻辑跳数，用于计算Site 之间路由距离和优先级。

**Logical Distance**: 基于网络拓扑、地理位置和性能指标计算的综合距离度量，用于就近路由决策。

### Telemetry and Monitoring Concepts

**Network Latency**: 网络请求的往返时间，从发送请求到接收响应的时间间隔。

**Bandwidth**: 网络连接的数据传输能力，通常以 bps（bits per second）为单位。

**Packet Loss**: 网络传输中丢失数据包的比例，影响连接质量和可靠性。

**Jitter**: 网络延迟的变化程度，反映网络连接的稳定性。

**Response Time**: 服务处理单个请求的时间，从接收请求到返回响应的时间间隔。

**Load Level**: 服务实例的当前负载程度，通常以 CPU 使用率、内存使用率或请求队列长度表示。

**Health Score**: 服务实例的综合健康评分，基于响应时间、错误率、资源使用等指标计算。

### Interest-Response Mechanism Concepts

**Service Interest Packet**: OutpostNet Client 作为 Proxy 时发送的服务请求描述，包含服务名称模式、约束条件和源位置信息。

**Service Pattern**: 用于匹配服务的模式表达式，支持通配符和正则表达式（如 "user-*", "*.api.v1"）。

**Service Constraints**: OutpostNet Client 对服务的要求和限制条件，包括最大延迟、最小带宽、首选 Site 等。

**Service Subscription**: OutpostNet Server 基于 OutpostNet Client 的 Service Interest Packet 进行服务匹配。

### Load Balancing Concepts

**Service Candidate**: 满足服务请求条件的 Workload 实例，包含服务元数据和当前遥测数据。

**Load Balancing Algorithm**: OutpostNet Server 在多个 Workload 候选中分配请求的算法，如轮询、加权轮询、最少连接等。

**Failover**: 当主要 Workload 实例不可用时，OutpostNet Server 自动切换到备用实例的机制。

**Circuit Breaker**: OutpostNet Server 防止级联故障的保护机制，当 Workload 错误率过高时暂时停止请求转发。

## Requirements

### Requirement 1: 服务注册

**User Story:** 作为边缘计算节点运维人员，我需要 OutpostNet 提供完整的服务注册能力，支持 Workload 的动态注册、元数据管理和基于模式的服务查询。

#### Acceptance Criteria

1. WHEN OutpostNet Client 在节点上以 register 启动 THEN OutpostNet Client SHALL 启动，并从配置或环境变量中获取待注册的 Workload 服务信息以及 Workload 的地理位置元数据（site, nodeID）
2. WHEN OutpostNet Client 向 OutpostNet Server 注册 Workload THEN OutpostNet Server SHALL 记录 Workload 的基本信息（serviceName, namespace, labels, endpoints, protocols），并映射为一个 endpoint
3. WHEN OutpostNet Client 发送心跳 THEN OutpostNet Server SHALL 更新 Workload 的存活状态和最后心跳时间
8. WHEN 查询服务健康状态 THEN OutpostNet Server SHALL 提供基于心跳的健康检查结果
5. WHEN 外部系统查询 Workload 元数据 THEN OutpostNet Server SHALL 提供标准化的 REST API 接口，支持通过 Kubectl 查询
6. WHEN OutpostNet Client 停止 THEN OutpostNet Server SHALL 从服务注册表中移除该  OutpostNet Client 注册的相关记录



### Requirement 2: 服务订阅

**User Story:** 作为边缘计算节点运维人员，我需要 OutpostNet 提供完整的服务订阅能力，支持基于label 过滤的服务订阅。

#### Acceptance Criteria

1. WHEN OutpostNet Client 在节点上以 proxy 启动 THEN OutpostNet Client SHALL 启动，从配置或环境变量中获取的地理位置元数据（site, nodeID）以及需要代理的服务和对应的代理端口信息，包括代理的 本地IP/本地接口和端口
2. WHEN OutpostNet Client 收到需要代理的服务和对应的本地 IP/接口 和 端口信息后 THEN OutpostNet Client SHALL 向 Server 发送 Service Interest Packet（包含 Service Pattern、Service Constraints)
3. WHEN OutpostNet Server 收到 Client 的Service Interest Packet后 THEN OutpostNet Server SHALL 创建一个 Service Subscription 保存所有符合要求的 Workload endpoint，
5. WHEN Proxy 模式的 OutpostNet Client  在节点上的代理端口收到服务请求后 THEN Proxy 模式的 OutpostNet Client SHALL 需要将该服务请求转发到 OutpostNet Server 
6. WHEN OutpostNet Server 收到对应的服务请求 THEN OutpostNet Server SHALL 按照负载均衡算法从 Service Subscription 中选取合适的 Workload endpoint，并将服务请求发送到对应的 OutpostNet Client register
7. WHEN OutpostNet Client register 收到服务请求 THEN OutpostNet Client register SHALL 将与 Workload endpoint 对应的 Workload 服务端口建立连接并持续转发数据
6. WHEN 以 proxy 模式启动的 OutpostNet Client 在节点上注销或者中断与 OutpostNet Server 的信息 THEN OutpostNet Server SHALL 清理 Service Subscription 并且通知 OutpostNet Client register 清理连接
7. WHEN 以 register 模式启动的 OutpostNet Client 在节点上注销或者中断 THEN OutpostNet Server SHALL 移除相关的 workload endpoint 并且重新负载均衡 OutpostNet Client proxy 发送的服务请求


### Requirement 3: Cluster 内服务协同

**User Story:** 作为边缘计算节点运维人员，我需要 OutpostNet 可以提供跨节点的服务订阅能力。

#### Acceptance Criteria

1. WHEN OutpostNet Server 在节点上启动 THEN OutpostNet Server SHALL 从配置或环境变量中获取本 Site 的其他 OutpostNet Server 信息，并组成 Cluster
2. WHEN OutpostNet Server 加入 Cluster THEN OutpostNet Server SHALL 与集群其他节点同步 Workload endpoint 信息
3. WHEN OutpostNet Server 收到的服务请求目的地 Workload endpoint 归属本 OutpostNet Server THEN OutpostNet Server 直接将该服务转发到对应的 OutpostNet Client register
4. WHEN OutpostNet Server 收到的服务请求目的地 Workload endpoint 不归属本 OutpostNet Server THEN OutpostNet Server SHALL 将该服务请求转发到对应的 OutpostNet Server


### Requirement 4: 跨 Cluster 服务协同

**User Story:** 作为边缘计算节点运维人员，我需要 OutpostNet 可以提供跨 Cluster 的服务订阅能力。

#### Acceptance Criteria

1. WHEN OutpostNet Server 在节点上启动 THEN OutpostNet Server MAY 从配置或环境变量中获取其他 Cluster 的 OutpostNet Cluster 信息 
2. WHEN OutpostNet Server 启动后拿到其他 Cluster 的信息 THEN 本 Cluster 内选择1台 OutpostNet Server SHALL 连接其他 Cluster，当这台 OutpostNet Server 服务停止后，本 Cluster 内的其他OutpostNet Server可以接替
3. WHEN OutpostNet Server 收到对应的服务请求允许跨 Cluster 时 THEN OutpostNet Server SHALL 发送相关的服务请求到其他的 OutpostNet Cluster
4. WHEN OutpostNet Server 收到其他 Cluster 的 OutpostNet Server 发送的服务请求时 THEN OutpostNet Server SHALL 检查本 Cluster 内是否有符合要求的 Workload endpoint，如果存在则回应其他 Cluster 的 OutpostNet Server 
5. WHEN OutpostNet Server 收到其他 Cluster 的 OutpostNet Server 对于服务请求的回应时 THEN OutpostNet Server SHALL 在本 Cluster 内保存该信息
6. WHEN OutpostNet Server 收到对应的服务请求但是本 Cluster 内没有符合要求的响应 THEN OutpostNet Server SHALL 将此服务请求转发到其他 Cluster
7. WHEN OutpostNet Server 收到其他 Cluster 的服务请求 THEN OutpostNet Server SHALL 将此服务请求按照 Cluster 内的处理流程进行处理


### Requirement 5: 遥测数据收集

**User Story:** 作为性能监控人员，我需要 OutpostNet 能够实时收集和存储网络及 Workload 的遥测数据，并通过 API 提供给外部系统使用。

#### Acceptance Criteria

1. WHEN OutpostNet Client 与 OutpostNet Server 网络连接活跃 THEN OutpostNet Server SHALL 持续测量并记录网络延迟、带宽、丢包率和抖动
2. WHEN Workload 处理业务请求 THEN OutpostNet Client SHALL 收集 Workload 的响应时间、处理负载和健康评分并上报给 OutpostNet Server
3. WHEN OutpostNet Server 计算地理距离 THEN OutpostNet Server SHALL 基于网络拓扑计算逻辑跳数和距离权重
4. WHEN 遥测数据异常 THEN OutpostNet Server SHALL 触发告警并标记相关 Workload 或节点状态
5. WHEN OutpostNet Server 数据存储达到限制 THEN OutpostNet Server SHALL 自动清理历史数据并保留关键指标
6. WHEN 外部系统查询历史数据 THEN OutpostNet Server SHALL 通过 API 支持时间范围和指标类型的灵活查询
7. WHEN 外部系统需要数据聚合 THEN OutpostNet Server SHALL 提供多维度的数据聚合和统计功能
8. WHEN 外部系统建立性能基线 THEN OutpostNet Server SHALL 基于历史数据提供性能基线和异常检测数据

### Requirement 6: 元数据管理

**User Story:** 作为系统管理员，我需要 OutpostNet 提供完整的元数据管理能力，维护 Workload 的地理位置信息和网络拓扑关系。

#### Acceptance Criteria

1. WHEN OutpostNet Server 节点启动 THEN OutpostNet Server SHALL 从配置中获取并记录节点的地理位置信息（site, nodeID）
2. WHEN Workload 注册 THEN OutpostNet Server SHALL 继承节点的地理标签并支持 Workload 级别的标签覆盖
3. WHEN 查询 Workload 信息 THEN OutpostNet Server SHALL 提供基于标签选择器的 Workload 查询功能
4. WHEN Workload 元数据过期 THEN OutpostNet Server SHALL 自动清理过期的元数据记录


