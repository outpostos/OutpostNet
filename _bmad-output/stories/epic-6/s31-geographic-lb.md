# Story S31: 地理感知负载均衡

**Epic:** Epic 6 — 负载均衡引擎
**Status:** Done

## 目标
实现基于网络拓扑距离的地理感知负载均衡算法，优先选择距离近的端点，同时支持多种选择策略。

## 内容
- `GeographicConfig` 配置：LocalSite、Calculator、MaxDistance、SameSiteWeight(默认10)、NearSiteWeight(默认5)、FarSiteWeight(默认1)
- `GeographicLB` 结构体：使用 `atomic.Uint64` 实现线程安全的轮询索引，`sync.RWMutex` 保护配置更新
- `Select()` 算法：过滤健康端点 -> 计算到各端点站点的距离 -> 应用距离权重乘数 -> 按得分降序/距离升序排序 -> 在最高分组内加权轮询
- `SelectWithPreference()`: 返回所有健康端点，按距离偏好排序（得分降序 -> 距离升序 -> ID 字典序），适用于故障转移场景
- `SortByDistance()`: 感知健康状态的距离排序，不健康端点置于末尾
- `RandomWeightedSelect()`: 基于地理得分的加权随机选择，在保持距离偏好的同时提供更好的负载分布
- `SetLocalSite()`/`LocalSite()`: 线程安全的本地站点读写
- `scoreEndpoints()`: 内部评分逻辑，计算 EffectiveWeight * distanceMultiplier，支持 MaxDistance 截断
- `siteDistance()`: 站点距离计算，同站点=0，无站点信息=0，无 Calculator 时非本地站点=1，不可达=100
- `distanceMultiplier()`: 距离0=SameSiteWeight，距离1=NearSiteWeight，其他=FarSiteWeight

## 验收标准
- 24 个测试函数 + 1 个基准测试，覆盖以下场景：
- `TestGeographicLB_Name`: 算法名称为 "geographic_aware"
- `TestGeographicLB_DefaultWeights`: 默认权重 SameSite=10, NearSite=5, FarSite=1
- `TestGeographicLB_EmptyEndpoints`: 空端点列表返回 false
- `TestGeographicLB_AllUnhealthy`: 全部不健康时返回 false
- `TestGeographicLB_PrefersSameSite`: 优先选择同站点端点
- `TestGeographicLB_FallsBackToFartherSites`: 无同站点时回退到远端
- `TestGeographicLB_MaxDistance`: 超过最大距离的端点被排除
- `TestGeographicLB_MaxDistance_ExcludesAll`: MaxDistance 排除所有端点时返回 false
- `TestGeographicLB_WeightInfluence`: 端点权重影响选择结果
- `TestGeographicLB_NoCalculator`: 无 Calculator 时非本地站点距离为 1
- `TestGeographicLB_NoSiteInfo`: 无站点信息时距离为 0
- `TestGeographicLB_RoundRobinWithinTier`: 同分组内轮询分配
- `TestGeographicLB_SelectWithPreference`: 偏好排序正确性
- `TestGeographicLB_SortByDistance`: 距离排序含健康状态感知
- `TestGeographicLB_RandomWeightedSelect`: 加权随机选择
- `TestGeographicLB_SetLocalSite`: 线程安全站点更新
- `TestGeographicLB_DisconnectedSites`: 不可达站点处理（距离=100）
- `TestGeographicLB_CustomWeights`: 自定义权重乘数
- `TestGeographicLB_ConcurrentSelect`: 并发安全（20 goroutine x 100 次调用）
- `BenchmarkGeographicLB_Select`: 50 端点性能基准

## 依赖
- `pkg/telemetry.DistanceCalculator`: 网络拓扑距离计算
- `math/rand/v2`: 加权随机选择
- `sync/atomic`: 线程安全轮询计数器

## 代码位置
- `pkg/loadbalancer/geographic.go`
- `pkg/loadbalancer/geographic_test.go`
