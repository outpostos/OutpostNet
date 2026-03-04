package loadbalancer

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// GeographicAware 已在 loadbalancer.go 中定义

// geographicAwareLB 地理位置感知负载均衡器
type geographicAwareLB struct {
	// 地理位置权重配置 (简化版，只考虑Site)
	localSiteWeight   float64 // 本地站点权重
	remoteBaseWeight  float64 // 远程基础权重
	
	// 延迟权重配置
	latencyThreshold    time.Duration     // 延迟阈值
	latencyWeightFactor float64           // 延迟权重因子
	
	// 端点距离缓存
	distanceCache map[string]*endpointDistance
	cacheMutex    sync.RWMutex
	
	// 端点选择统计
	selectionStats map[string]*endpointStats
	statsMutex     sync.RWMutex
	
	// 客户端地理位置信息 (从配置或上下文获取)
	clientLocation *types.EndpointMetadata
}

// endpointDistance 端点距离信息
type endpointDistance struct {
	SiteDistance   int           // 站点跳数距离 (核心指标)
	NetworkLatency time.Duration // 网络延迟
	Weight         float64       // 综合权重
	UpdatedAt      time.Time     // 更新时间
}

// endpointStats 端点选择统计
type endpointStats struct {
	SelectCount    int64         // 选择次数
	SuccessCount   int64         // 成功次数
	FailureCount   int64         // 失败次数
	TotalLatency   time.Duration // 累计延迟
	AverageLatency time.Duration // 平均延迟
	LastSelected   time.Time     // 最后选择时间
	LastSuccess    time.Time     // 最后成功时间
}

// NewGeographicAwareLoadBalancer 创建地理位置感知负载均衡器
func NewGeographicAwareLoadBalancer(clientLocation *types.EndpointMetadata) *geographicAwareLB {
	return &geographicAwareLB{
		// 默认地理位置权重配置 (简化版)
		localSiteWeight:   1.0,   // 本地站点权重
		remoteBaseWeight:  0.3,   // 远程基础权重
		
		// 默认延迟权重配置
		latencyThreshold:    100 * time.Millisecond, // 100ms 延迟阈值
		latencyWeightFactor: 0.5,                    // 延迟权重因子
		
		// 初始化缓存和统计
		distanceCache:  make(map[string]*endpointDistance),
		selectionStats: make(map[string]*endpointStats),
		clientLocation: clientLocation,
	}
}

// Name 返回负载均衡器名称
func (lb *geographicAwareLB) Name() string {
	return string(GeographicAware)
}

// Select 实现 types.LoadBalancer 接口，选择最优端点
func (lb *geographicAwareLB) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}

	// 如果只有一个端点，直接返回
	if len(endpoints) == 1 {
		lb.updateSelectionStats(endpoints[0].ID)
		return endpoints[0], nil
	}

	// 计算每个端点的综合权重
	weightedEndpoints := lb.calculateEndpointWeights(endpoints)
	
	// 基于Site距离的多级排序，先按Site距离，再按综合权重
	lb.sortEndpointsBySiteDistance(weightedEndpoints)

	// 在前几个权重较高的端点中进行选择，增加负载分散性
	topEndpoints := lb.getTopEndpoints(weightedEndpoints, 3)
	selected := lb.selectFromTopEndpoints(topEndpoints, "")
	
	if selected != nil {
		lb.updateSelectionStats(selected.ID)
		return selected, nil
	}
	
	return endpoints[0], nil // 兜底返回第一个端点
}

// SelectEndpoint 选择最优端点，基于地理位置和网络质量（带key支持）
func (lb *geographicAwareLB) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	// 如果只有一个端点，直接返回
	if len(endpoints) == 1 {
		lb.updateSelectionStats(endpoints[0].ID)
		return endpoints[0]
	}

	// 计算每个端点的综合权重
	weightedEndpoints := lb.calculateEndpointWeights(endpoints)
	
	// 基于Site距离的多级排序，先按Site距离，再按综合权重
	lb.sortEndpointsBySiteDistance(weightedEndpoints)

	// 在前几个权重较高的端点中进行选择，增加负载分散性
	topEndpoints := lb.getTopEndpoints(weightedEndpoints, 3)
	selected := lb.selectFromTopEndpoints(topEndpoints, key)
	
	if selected != nil {
		lb.updateSelectionStats(selected.ID)
	}
	
	return selected
}

// calculateEndpointWeights 计算每个端点的综合权重
func (lb *geographicAwareLB) calculateEndpointWeights(endpoints []*types.Endpoint) []*weightedEndpoint {
	weightedEndpoints := make([]*weightedEndpoint, 0, len(endpoints))
	
	for _, ep := range endpoints {
		// 获取或计算端点距离信息
		distance := lb.getEndpointDistance(ep)
		
		// 计算综合权重
		weight := lb.calculateWeight(ep, distance)
		
		weightedEndpoints = append(weightedEndpoints, &weightedEndpoint{
			Endpoint: ep,
			Distance: distance,
			Weight:   weight,
		})
	}
	
	return weightedEndpoints
}

// weightedEndpoint 带权重的端点
type weightedEndpoint struct {
	Endpoint *types.Endpoint
	Distance *endpointDistance
	Weight   float64
}

// getEndpointDistance 获取端点距离信息 (带缓存)
func (lb *geographicAwareLB) getEndpointDistance(endpoint *types.Endpoint) *endpointDistance {
	lb.cacheMutex.RLock()
	cached, exists := lb.distanceCache[endpoint.ID]
	lb.cacheMutex.RUnlock()
	
	// 检查缓存是否有效 (5分钟内有效)
	if exists && time.Since(cached.UpdatedAt) < 5*time.Minute {
		return cached
	}
	
	// 计算新的距离信息
	distance := lb.calculateEndpointDistance(endpoint)
	
	// 更新缓存
	lb.cacheMutex.Lock()
	lb.distanceCache[endpoint.ID] = distance
	lb.cacheMutex.Unlock()
	
	return distance
}

// calculateEndpointDistance 计算端点距离信息 (简化版，只考虑Site距离)
func (lb *geographicAwareLB) calculateEndpointDistance(endpoint *types.Endpoint) *endpointDistance {
	distance := &endpointDistance{
		UpdatedAt: time.Now(),
	}
	
	// 如果没有客户端位置信息，使用默认权重
	if lb.clientLocation == nil {
		distance.Weight = lb.remoteBaseWeight
		distance.SiteDistance = 2 // 默认距离
		distance.NetworkLatency = 100 * time.Millisecond // 默认延迟
		return distance
	}
	
	epMeta := &endpoint.Metadata
	clientMeta := lb.clientLocation
	
	// 计算站点距离 (核心指标)
	distance.SiteDistance = lb.calculateSiteDistance(clientMeta.Site, epMeta.Site)
	
	// 网络延迟估算 (基于Site距离)
	distance.NetworkLatency = lb.estimateNetworkLatency(distance.SiteDistance, 0)
	
	return distance
}

// calculateSiteDistance 计算站点距离 (跳数)
func (lb *geographicAwareLB) calculateSiteDistance(clientSite, endpointSite string) int {
	if clientSite == "" || endpointSite == "" {
		return 2 // 未知站点默认为2跳
	}
	
	if clientSite == endpointSite {
		return 0 // 同站点
	}
	
	// TODO: 这里应该从拓扑信息中查询实际跳数
	// 现在使用简单的启发式规则
	return 1 // 不同站点默认为1跳
}

// 注意：Zone距离和Region距离计算已移除，仅保留Site距离计算

// estimateNetworkLatency 估算网络延迟 (简化版，仅基于Site距离)
func (lb *geographicAwareLB) estimateNetworkLatency(siteDistance, _ int) time.Duration {
	// 基于Site距离的延迟估算模型
	baseLatency := 5 * time.Millisecond  // 基础延迟
	siteLatency := time.Duration(siteDistance) * 10 * time.Millisecond // 每跳10ms
	
	return baseLatency + siteLatency
}

// calculateWeight 计算端点综合权重
func (lb *geographicAwareLB) calculateWeight(endpoint *types.Endpoint, distance *endpointDistance) float64 {
	// 基础权重 (从端点标签获取)
	baseWeight := lb.getEndpointBaseWeight(endpoint)
	
	// 地理位置权重
	geoWeight := lb.calculateGeoWeight(distance)
	
	// 延迟权重
	latencyWeight := lb.calculateLatencyWeight(distance.NetworkLatency)
	
	// 历史性能权重
	performanceWeight := lb.calculatePerformanceWeight(endpoint.ID)
	
	// 综合权重计算 (加权平均)
	totalWeight := baseWeight * 0.3 + geoWeight * 0.4 + latencyWeight * 0.2 + performanceWeight * 0.1
	
	return totalWeight
}

// getEndpointBaseWeight 获取端点基础权重
func (lb *geographicAwareLB) getEndpointBaseWeight(endpoint *types.Endpoint) float64 {
	// 从端点元数据获取权重
	if endpoint.Metadata.Weight > 0 {
		return float64(endpoint.Metadata.Weight) / 100.0 // 转换为0-1之间的值
	}
	
	// 从标签获取权重
	if weightLabel, exists := endpoint.Labels["weight"]; exists {
		switch weightLabel {
		case "high":
			return 1.0
		case "medium":
			return 0.7
		case "low":
			return 0.4
		}
	}
	
	return 0.6 // 默认权重
}

// calculateGeoWeight 计算地理位置权重 (简化版，只基于Site距离)
func (lb *geographicAwareLB) calculateGeoWeight(distance *endpointDistance) float64 {
	// Site权重：距离越近权重越高
	switch distance.SiteDistance {
	case 0:
		return lb.localSiteWeight // 同Site最高权重
	case 1:
		return lb.localSiteWeight * 0.7 // 相邻Site
	case 2:
		return lb.localSiteWeight * 0.4 // 2跳Site
	default:
		return lb.remoteBaseWeight // 远程Site使用基础权重
	}
}

// calculateLatencyWeight 计算延迟权重
func (lb *geographicAwareLB) calculateLatencyWeight(latency time.Duration) float64 {
	if latency <= lb.latencyThreshold {
		return 1.0 // 低延迟满分
	}
	
	// 延迟越高权重越低 (指数衰减)
	ratio := float64(latency) / float64(lb.latencyThreshold)
	return math.Max(0.1, math.Exp(-lb.latencyWeightFactor*ratio))
}

// calculatePerformanceWeight 基于历史性能计算权重
func (lb *geographicAwareLB) calculatePerformanceWeight(endpointID string) float64 {
	lb.statsMutex.RLock()
	stats, exists := lb.selectionStats[endpointID]
	lb.statsMutex.RUnlock()
	
	if !exists || stats.SelectCount == 0 {
		return 0.5 // 新端点默认权重
	}
	
	// 成功率权重
	successRate := float64(stats.SuccessCount) / float64(stats.SelectCount)
	
	// 延迟权重
	latencyWeight := lb.calculateLatencyWeight(stats.AverageLatency)
	
	// 综合性能权重
	return successRate*0.7 + latencyWeight*0.3
}

// sortEndpointsBySiteDistance 基于Site距离的排序算法 (简化版)
func (lb *geographicAwareLB) sortEndpointsBySiteDistance(weightedEndpoints []*weightedEndpoint) {
	sort.Slice(weightedEndpoints, func(i, j int) bool {
		epI := weightedEndpoints[i]
		epJ := weightedEndpoints[j]
		
		// 优先级1: Site距离 (距离越近越优先)
		if epI.Distance.SiteDistance != epJ.Distance.SiteDistance {
			return epI.Distance.SiteDistance < epJ.Distance.SiteDistance
		}
		
		// 优先级2: 网络延迟 (延迟越低越优先)
		if epI.Distance.NetworkLatency != epJ.Distance.NetworkLatency {
			return epI.Distance.NetworkLatency < epJ.Distance.NetworkLatency
		}
		
		// 优先级3: 综合权重 (权重越高越优先)
		return epI.Weight > epJ.Weight
	})
}

// getTopEndpoints 获取权重最高的几个端点
func (lb *geographicAwareLB) getTopEndpoints(weightedEndpoints []*weightedEndpoint, count int) []*weightedEndpoint {
	if len(weightedEndpoints) <= count {
		return weightedEndpoints
	}
	
	return weightedEndpoints[:count]
}

// getEndpointsBySiteDistance 按Site距离分组获取端点
func (lb *geographicAwareLB) getEndpointsBySiteDistance(weightedEndpoints []*weightedEndpoint) map[int][]*weightedEndpoint {
	siteGroups := make(map[int][]*weightedEndpoint)
	
	for _, ep := range weightedEndpoints {
		distance := ep.Distance.SiteDistance
		siteGroups[distance] = append(siteGroups[distance], ep)
	}
	
	// 对每个距离组内的端点按权重排序
	for distance := range siteGroups {
		group := siteGroups[distance]
		sort.Slice(group, func(i, j int) bool {
			return group[i].Weight > group[j].Weight
		})
		siteGroups[distance] = group
	}
	
	return siteGroups
}

// selectBestEndpointFromSites 从Site距离分组中选择最优端点
func (lb *geographicAwareLB) selectBestEndpointFromSites(siteGroups map[int][]*weightedEndpoint, key string) *types.Endpoint {
	// 优先选择距离最近的Site组
	minDistance := int(^uint(0) >> 1) // 最大整数
	for distance := range siteGroups {
		if distance < minDistance {
			minDistance = distance
		}
	}
	
	// 如果最近的Site组存在端点，从中选择
	if nearestGroup, exists := siteGroups[minDistance]; exists && len(nearestGroup) > 0 {
		if len(nearestGroup) == 1 {
			return nearestGroup[0].Endpoint
		}
		
		// 在最近的Site组中进行加权选择
		return lb.selectFromTopEndpoints(nearestGroup, key)
	}
	
	return nil
}

// selectFromTopEndpoints 从顶级端点中选择一个 (增加负载分散性)
func (lb *geographicAwareLB) selectFromTopEndpoints(topEndpoints []*weightedEndpoint, key string) *types.Endpoint {
	if len(topEndpoints) == 0 {
		return nil
	}
	
	if len(topEndpoints) == 1 {
		return topEndpoints[0].Endpoint
	}
	
	// 使用加权随机选择，但倾向于选择权重最高的
	totalWeight := 0.0
	for _, ep := range topEndpoints {
		totalWeight += ep.Weight
	}
	
	if totalWeight == 0 {
		return topEndpoints[0].Endpoint // 如果权重都为0，选择第一个
	}
	
	// 基于key生成一致性随机数
	hash := lb.hashKey(key)
	target := float64(hash%1000) / 1000.0 * totalWeight
	
	current := 0.0
	for _, ep := range topEndpoints {
		current += ep.Weight
		if current >= target {
			return ep.Endpoint
		}
	}
	
	return topEndpoints[0].Endpoint
}

// hashKey 对key进行哈希 (一致性选择)
func (lb *geographicAwareLB) hashKey(key string) uint32 {
	if key == "" {
		key = time.Now().String() // 如果没有key，使用时间戳
	}
	
	hash := uint32(0)
	for _, c := range key {
		hash = hash*31 + uint32(c)
	}
	return hash
}

// updateSelectionStats 更新端点选择统计
func (lb *geographicAwareLB) updateSelectionStats(endpointID string) {
	lb.statsMutex.Lock()
	defer lb.statsMutex.Unlock()
	
	if stats, exists := lb.selectionStats[endpointID]; exists {
		stats.SelectCount++
		stats.LastSelected = time.Now()
	} else {
		lb.selectionStats[endpointID] = &endpointStats{
			SelectCount:  1,
			LastSelected: time.Now(),
		}
	}
}

// UpdateStats 更新端点统计信息 (由外部调用)
func (lb *geographicAwareLB) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	lb.statsMutex.Lock()
	defer lb.statsMutex.Unlock()
	
	stats, exists := lb.selectionStats[endpoint.ID]
	if !exists {
		stats = &endpointStats{}
		lb.selectionStats[endpoint.ID] = stats
	}
	
	// 更新选择计数 (必须在成功/失败统计之前)
	stats.SelectCount++
	
	// 更新成功/失败统计
	if success {
		stats.SuccessCount++
		stats.LastSuccess = time.Now()
	} else {
		stats.FailureCount++
	}
	
	// 更新延迟统计
	stats.TotalLatency += latency
	if stats.SelectCount > 0 {
		stats.AverageLatency = stats.TotalLatency / time.Duration(stats.SelectCount)
	}
	
	// 清理过期缓存 (延迟影响权重计算)
	if time.Since(stats.LastSelected) > 10*time.Minute {
		lb.cacheMutex.Lock()
		delete(lb.distanceCache, endpoint.ID)
		lb.cacheMutex.Unlock()
	}
}

// SetClientLocation 设置客户端地理位置信息
func (lb *geographicAwareLB) SetClientLocation(location *types.EndpointMetadata) {
	lb.clientLocation = location
	
	// 清空距离缓存，重新计算
	lb.cacheMutex.Lock()
	lb.distanceCache = make(map[string]*endpointDistance)
	lb.cacheMutex.Unlock()
}

// SetLatencyThreshold 设置延迟阈值
func (lb *geographicAwareLB) SetLatencyThreshold(threshold time.Duration) {
	lb.latencyThreshold = threshold
}

// SetGeoWeights 设置地理位置权重配置 (简化版)
func (lb *geographicAwareLB) SetGeoWeights(localSite, remoteBase float64) {
	lb.localSiteWeight = localSite
	lb.remoteBaseWeight = remoteBase
}

// SelectWithSitePreference 基于Site距离优先级的高级选择算法
func (lb *geographicAwareLB) SelectWithSitePreference(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	if len(endpoints) == 1 {
		lb.updateSelectionStats(endpoints[0].ID)
		return endpoints[0]
	}

	// 计算每个端点的综合权重
	weightedEndpoints := lb.calculateEndpointWeights(endpoints)
	
	// 按Site距离分组
	siteGroups := lb.getEndpointsBySiteDistance(weightedEndpoints)
	
	// 从最近的Site组中选择最优端点
	selected := lb.selectBestEndpointFromSites(siteGroups, key)
	
	if selected != nil {
		lb.updateSelectionStats(selected.ID)
		return selected
	}
	
	// 兜底：如果没有找到合适的端点，返回第一个
	return endpoints[0]
}

// GetEndpointStats 获取端点统计信息 (用于监控和调试)
func (lb *geographicAwareLB) GetEndpointStats() map[string]*endpointStats {
	lb.statsMutex.RLock()
	defer lb.statsMutex.RUnlock()
	
	result := make(map[string]*endpointStats)
	for id, stats := range lb.selectionStats {
		// 复制统计信息
		result[id] = &endpointStats{
			SelectCount:    stats.SelectCount,
			SuccessCount:   stats.SuccessCount,
			FailureCount:   stats.FailureCount,
			TotalLatency:   stats.TotalLatency,
			AverageLatency: stats.AverageLatency,
			LastSelected:   stats.LastSelected,
			LastSuccess:    stats.LastSuccess,
		}
	}
	
	return result
}