# Story S30: 负载均衡接口与端点模型

**Epic:** Epic 6 — 负载均衡引擎
**Status:** Done

## 目标
定义负载均衡的核心接口和端点数据模型，为可插拔的负载均衡算法提供统一抽象。

## 内容
- `Endpoint` 结构体：包含 ID、Address、Site、Weight、Healthy、Labels 六个字段，描述一个可路由的服务端点
- `EffectiveWeight()` 方法：返回端点的有效权重，Weight <= 0 时视为 1，确保权重始终为正值
- `LoadBalancer` 接口：定义 `Select(endpoints []Endpoint, key string) (Endpoint, bool)` 和 `Name() string` 两个方法
- 最小化接口设计，key 参数为可选提示（如用于一致性哈希），Select 返回零值 Endpoint 和 false 表示无可用端点

## 验收标准
- `TestGeographicLB_ImplementsInterface`: 验证 GeographicLB 实现 LoadBalancer 接口
- `TestGeographicLB_EffectiveWeight`: 验证 Weight <= 0 时 EffectiveWeight 返回 1

## 依赖
无外部依赖，为纯数据模型和接口定义。

## 代码位置
- `pkg/loadbalancer/loadbalancer.go`
