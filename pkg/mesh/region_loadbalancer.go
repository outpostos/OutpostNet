package mesh

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// RegionLoadBalancer 区域负载均衡器 (Phase 2)
// 实现地理感知的负载均衡和智能路由
type RegionLoadBalancer struct {
	localNodeID    string
	localRegionID  string
	localSiteID    string
	localZoneID    string
	
	// 负载均衡策略
	strategy       LoadBalanceStrategy
	
	// 地理偏好配置
	geoPreference  *GeographicPreference
	
	// 健康检查器
	healthChecker  *RegionHealthChecker
	
	// 路由统计
	routingStats   *RoutingStats
	statsMu        sync.RWMutex
	
	// 会话保持 (for consistent hash)
	sessionStore   map[string]string // sessionID -> endpointID
	sessionMu      sync.RWMutex
}

// LoadBalanceStrategy 负载均衡策略
type LoadBalanceStrategy string

const (
	StrategyRoundRobin        LoadBalanceStrategy = "round_robin"
	StrategyWeighted          LoadBalanceStrategy = "weighted"
	StrategyLeastConnections  LoadBalanceStrategy = "least_connections"
	StrategyConsistentHash    LoadBalanceStrategy = "consistent_hash"
	StrategyGeographicAware   LoadBalanceStrategy = "geographic_aware"
	StrategyHealthAware       LoadBalanceStrategy = "health_aware"
	StrategyLatencyAware      LoadBalanceStrategy = "latency_aware"
)

// GeographicPreference 地理偏好配置
type GeographicPreference struct {
	LocalPreference    float64 // 本地偏好权重 (0-1)
	SitePreference     float64 // 站点偏好权重 (0-1)
	ZonePreference     float64 // 可用区偏好权重 (0-1)
	RegionPreference   float64 // 区域偏好权重 (0-1)
	
	// 距离惩罚因子
	DistancePenalty    float64 // 每增加一级距离的权重惩罚
	
	// 故障转移策略
	FailoverToRemote   bool    // 本地不可用时是否故障转移到远程
	MaxDistance        int     // 最大允许距离 (0=本地, 1=站点, 2=区域)
}

// RoutingStats 路由统计信息
type RoutingStats struct {
	TotalRequests      int64     `json:"total_requests"`
	LocalRequests      int64     `json:"local_requests"`
	SiteRequests       int64     `json:"site_requests"`
	ZoneRequests       int64     `json:"zone_requests"`
	RegionRequests     int64     `json:"region_requests"`
	RemoteRequests     int64     `json:"remote_requests"`
	FailedRequests     int64     `json:"failed_requests"`
	
	// 响应时间统计
	AverageLatency     float64   `json:"average_latency_ms"`
	P50Latency         float64   `json:"p50_latency_ms"`
	P95Latency         float64   `json:"p95_latency_ms"`
	P99Latency         float64   `json:"p99_latency_ms"`
	
	// 负载分布
	EndpointDistribution map[string]int64 `json:"endpoint_distribution"`
	
	LastUpdateTime     time.Time `json:"last_update_time"`
}

// EndpointScore 端点评分
type EndpointScore struct {
	Endpoint      *types.Endpoint
	Score         float64
	Distance      int    // 地理距离等级
	HealthScore   float64
	LatencyScore  float64
	LoadScore     float64
	Reason        string // 评分原因
}

// NewRegionLoadBalancer 创建区域负载均衡器
func NewRegionLoadBalancer(
	localNodeID, localRegionID, localSiteID, localZoneID string,
	strategy LoadBalanceStrategy,
	healthChecker *RegionHealthChecker,
) *RegionLoadBalancer {
	return &RegionLoadBalancer{
		localNodeID:   localNodeID,
		localRegionID: localRegionID,
		localSiteID:   localSiteID,
		localZoneID:   localZoneID,
		strategy:      strategy,
		geoPreference: DefaultGeographicPreference(),
		healthChecker: healthChecker,
		routingStats: &RoutingStats{
			EndpointDistribution: make(map[string]int64),
		},
		sessionStore: make(map[string]string),
	}
}

// DefaultGeographicPreference 默认地理偏好配置
func DefaultGeographicPreference() *GeographicPreference {
	return &GeographicPreference{
		LocalPreference:   1.0,   // 本地最高优先级
		SitePreference:    0.8,   // 站点内次优先
		ZonePreference:    0.6,   // 可用区内中等优先
		RegionPreference:  0.4,   // 区域内较低优先
		DistancePenalty:   0.2,   // 每增加一级距离扣除20%权重
		FailoverToRemote:  true,  // 允许故障转移到远程
		MaxDistance:       2,     // 最大允许跨区域
	}
}

// SelectEndpoint 选择端点
func (lb *RegionLoadBalancer) SelectEndpoint(
	endpoints []*types.Endpoint,
	sessionID string,
) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	// 记录请求统计
	lb.recordRequest()
	
	// 过滤健康的端点
	healthyEndpoints := lb.filterHealthyEndpoints(endpoints)
	if len(healthyEndpoints) == 0 {
		lb.recordFailedRequest()
		return nil, fmt.Errorf("no healthy endpoints available")
	}
	
	// 根据策略选择端点
	var selectedEndpoint *types.Endpoint
	
	switch lb.strategy {
	case StrategyGeographicAware:
		selectedEndpoint = lb.selectByGeography(healthyEndpoints)
	case StrategyHealthAware:
		selectedEndpoint = lb.selectByHealth(healthyEndpoints)
	case StrategyLatencyAware:
		selectedEndpoint = lb.selectByLatency(healthyEndpoints)
	case StrategyConsistentHash:
		selectedEndpoint = lb.selectByConsistentHash(healthyEndpoints, sessionID)
	case StrategyWeighted:
		selectedEndpoint = lb.selectByWeight(healthyEndpoints)
	case StrategyLeastConnections:
		selectedEndpoint = lb.selectByLeastConnections(healthyEndpoints)
	case StrategyRoundRobin:
		fallthrough
	default:
		selectedEndpoint = lb.selectByRoundRobin(healthyEndpoints)
	}
	
	if selectedEndpoint == nil {
		lb.recordFailedRequest()
		return nil, fmt.Errorf("failed to select endpoint")
	}
	
	// 记录路由统计
	lb.recordRouting(selectedEndpoint)
	
	return selectedEndpoint, nil
}

// SelectBestEndpoints 选择最佳端点列表 (用于批量操作)
func (lb *RegionLoadBalancer) SelectBestEndpoints(
	endpoints []*types.Endpoint,
	maxCount int,
) ([]*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	// 过滤健康的端点
	healthyEndpoints := lb.filterHealthyEndpoints(endpoints)
	if len(healthyEndpoints) == 0 {
		return nil, fmt.Errorf("no healthy endpoints available")
	}
	
	// 计算端点评分
	scores := lb.calculateEndpointScores(healthyEndpoints)
	
	// 按评分排序
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})
	
	// 选择最佳的N个端点
	resultCount := maxCount
	if resultCount > len(scores) {
		resultCount = len(scores)
	}
	
	result := make([]*types.Endpoint, resultCount)
	for i := 0; i < resultCount; i++ {
		result[i] = scores[i].Endpoint
	}
	
	return result, nil
}

// SetGeographicPreference 设置地理偏好
func (lb *RegionLoadBalancer) SetGeographicPreference(pref *GeographicPreference) {
	lb.geoPreference = pref
}

// GetRoutingStats 获取路由统计
func (lb *RegionLoadBalancer) GetRoutingStats() *RoutingStats {
	lb.statsMu.RLock()
	defer lb.statsMu.RUnlock()
	
	// 返回副本
	stats := *lb.routingStats
	stats.EndpointDistribution = make(map[string]int64)
	for k, v := range lb.routingStats.EndpointDistribution {
		stats.EndpointDistribution[k] = v
	}
	
	return &stats
}

// 端点选择策略实现

// selectByGeography 基于地理位置选择
func (lb *RegionLoadBalancer) selectByGeography(endpoints []*types.Endpoint) *types.Endpoint {
	scores := lb.calculateEndpointScores(endpoints)
	if len(scores) == 0 {
		return nil
	}
	
	// 按评分排序并选择最高分的端点
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})
	
	return scores[0].Endpoint
}

// selectByHealth 基于健康状态选择
func (lb *RegionLoadBalancer) selectByHealth(endpoints []*types.Endpoint) *types.Endpoint {
	bestEndpoint := endpoints[0]
	bestHealthScore := lb.getHealthScore(bestEndpoint)
	
	for _, endpoint := range endpoints[1:] {
		healthScore := lb.getHealthScore(endpoint)
		if healthScore > bestHealthScore {
			bestEndpoint = endpoint
			bestHealthScore = healthScore
		}
	}
	
	return bestEndpoint
}

// selectByLatency 基于延迟选择
func (lb *RegionLoadBalancer) selectByLatency(endpoints []*types.Endpoint) *types.Endpoint {
	bestEndpoint := endpoints[0]
	bestLatency := lb.getLatency(bestEndpoint)
	
	for _, endpoint := range endpoints[1:] {
		latency := lb.getLatency(endpoint)
		if latency < bestLatency {
			bestEndpoint = endpoint
			bestLatency = latency
		}
	}
	
	return bestEndpoint
}

// selectByConsistentHash 基于一致性哈希选择
func (lb *RegionLoadBalancer) selectByConsistentHash(endpoints []*types.Endpoint, sessionID string) *types.Endpoint {
	if sessionID == "" {
		// 没有session ID，使用轮询
		return lb.selectByRoundRobin(endpoints)
	}
	
	// 检查session是否已存在
	lb.sessionMu.RLock()
	if endpointID, exists := lb.sessionStore[sessionID]; exists {
		lb.sessionMu.RUnlock()
		
		// 查找对应的端点
		for _, endpoint := range endpoints {
			if endpoint.ID == endpointID {
				return endpoint
			}
		}
	} else {
		lb.sessionMu.RUnlock()
	}
	
	// 计算哈希值
	hash := lb.hashString(sessionID)
	
	// 创建一致性哈希环
	hashRing := make([]hashItem, 0, len(endpoints)*3) // 每个端点3个虚拟节点
	for _, endpoint := range endpoints {
		for i := 0; i < 3; i++ {
			virtualKey := fmt.Sprintf("%s-%d", endpoint.ID, i)
			hashRing = append(hashRing, hashItem{
				hash:     lb.hashString(virtualKey),
				endpoint: endpoint,
			})
		}
	}
	
	// 排序哈希环
	sort.Slice(hashRing, func(i, j int) bool {
		return hashRing[i].hash < hashRing[j].hash
	})
	
	// 查找最近的端点
	selectedEndpoint := hashRing[0].endpoint
	for _, item := range hashRing {
		if item.hash >= hash {
			selectedEndpoint = item.endpoint
			break
		}
	}
	
	// 保存session映射
	lb.sessionMu.Lock()
	lb.sessionStore[sessionID] = selectedEndpoint.ID
	lb.sessionMu.Unlock()
	
	return selectedEndpoint
}

type hashItem struct {
	hash     uint32
	endpoint *types.Endpoint
}

// selectByWeight 基于权重选择
func (lb *RegionLoadBalancer) selectByWeight(endpoints []*types.Endpoint) *types.Endpoint {
	// 计算总权重
	totalWeight := 0
	for _, endpoint := range endpoints {
		weight := endpoint.Metadata.Weight
		if weight <= 0 {
			weight = 1 // 默认权重
		}
		totalWeight += weight
	}
	
	if totalWeight == 0 {
		return lb.selectByRoundRobin(endpoints)
	}
	
	// 随机选择
	target := rand.Intn(totalWeight)
	current := 0
	
	for _, endpoint := range endpoints {
		weight := endpoint.Metadata.Weight
		if weight <= 0 {
			weight = 1
		}
		current += weight
		if current > target {
			return endpoint
		}
	}
	
	return endpoints[len(endpoints)-1]
}

// selectByLeastConnections 基于最少连接选择
func (lb *RegionLoadBalancer) selectByLeastConnections(endpoints []*types.Endpoint) *types.Endpoint {
	bestEndpoint := endpoints[0]
	minConnections := lb.getConnectionCount(bestEndpoint)
	
	for _, endpoint := range endpoints[1:] {
		connections := lb.getConnectionCount(endpoint)
		if connections < minConnections {
			bestEndpoint = endpoint
			minConnections = connections
		}
	}
	
	return bestEndpoint
}

// selectByRoundRobin 轮询选择
func (lb *RegionLoadBalancer) selectByRoundRobin(endpoints []*types.Endpoint) *types.Endpoint {
	// 简化实现：基于时间的伪轮询
	index := int(time.Now().UnixNano()) % len(endpoints)
	return endpoints[index]
}

// 评分计算

// calculateEndpointScores 计算端点评分
func (lb *RegionLoadBalancer) calculateEndpointScores(endpoints []*types.Endpoint) []EndpointScore {
	scores := make([]EndpointScore, len(endpoints))
	
	for i, endpoint := range endpoints {
		score := &scores[i]
		score.Endpoint = endpoint
		
		// 计算地理距离评分
		score.Distance = lb.calculateGeographicDistance(endpoint)
		geoScore := lb.calculateGeographicScore(score.Distance)
		
		// 计算健康评分
		score.HealthScore = lb.getHealthScore(endpoint)
		
		// 计算延迟评分
		score.LatencyScore = lb.calculateLatencyScore(endpoint)
		
		// 计算负载评分
		score.LoadScore = lb.calculateLoadScore(endpoint)
		
		// 综合评分 (可配置权重)
		score.Score = geoScore*0.4 + score.HealthScore*0.3 + score.LatencyScore*0.2 + score.LoadScore*0.1
		
		// 生成评分原因
		score.Reason = fmt.Sprintf("geo=%.1f, health=%.1f, latency=%.1f, load=%.1f",
			geoScore, score.HealthScore, score.LatencyScore, score.LoadScore)
	}
	
	return scores
}

// calculateGeographicDistance 计算地理距离
func (lb *RegionLoadBalancer) calculateGeographicDistance(endpoint *types.Endpoint) int {
	// 本地端点
	if endpoint.Metadata.NodeID == lb.localNodeID {
		return 0
	}
	
	// 同站点
	if endpoint.Metadata.Site == lb.localSiteID {
		return 1
	}
	
	// 同可用区
	if endpoint.Metadata.Zone == lb.localZoneID {
		return 2
	}
	
	// 同区域
	if endpoint.Metadata.Region == lb.localRegionID {
		return 3
	}
	
	// 跨区域
	return 4
}

// calculateGeographicScore 计算地理评分
func (lb *RegionLoadBalancer) calculateGeographicScore(distance int) float64 {
	switch distance {
	case 0: // 本地
		return lb.geoPreference.LocalPreference * 100
	case 1: // 同站点
		return lb.geoPreference.SitePreference * 100
	case 2: // 同可用区
		return lb.geoPreference.ZonePreference * 100
	case 3: // 同区域
		return lb.geoPreference.RegionPreference * 100
	default: // 跨区域
		score := lb.geoPreference.RegionPreference * 100
		// 应用距离惩罚
		penalty := lb.geoPreference.DistancePenalty * float64(distance-3) * 100
		score -= penalty
		if score < 0 {
			score = 0
		}
		return score
	}
}

// calculateLatencyScore 计算延迟评分
func (lb *RegionLoadBalancer) calculateLatencyScore(endpoint *types.Endpoint) float64 {
	latency := lb.getLatency(endpoint)
	
	// 延迟评分：延迟越低分数越高
	if latency <= 1*time.Millisecond {
		return 100
	} else if latency <= 10*time.Millisecond {
		return 90
	} else if latency <= 50*time.Millisecond {
		return 70
	} else if latency <= 100*time.Millisecond {
		return 50
	} else if latency <= 500*time.Millisecond {
		return 30
	} else {
		return 10
	}
}

// calculateLoadScore 计算负载评分
func (lb *RegionLoadBalancer) calculateLoadScore(endpoint *types.Endpoint) float64 {
	connections := lb.getConnectionCount(endpoint)
	maxConnections := endpoint.Metadata.MaxConn
	
	if maxConnections <= 0 {
		maxConnections = 100 // 默认最大连接数
	}
	
	loadRatio := float64(connections) / float64(maxConnections)
	
	// 负载评分：负载越低分数越高
	score := (1.0 - loadRatio) * 100
	if score < 0 {
		score = 0
	}
	
	return score
}

// 辅助方法

// filterHealthyEndpoints 过滤健康的端点
func (lb *RegionLoadBalancer) filterHealthyEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	healthy := make([]*types.Endpoint, 0, len(endpoints))
	
	for _, endpoint := range endpoints {
		if endpoint.Status.Ready && lb.getHealthScore(endpoint) > 0 {
			healthy = append(healthy, endpoint)
		}
	}
	
	return healthy
}

// getHealthScore 获取健康评分
func (lb *RegionLoadBalancer) getHealthScore(endpoint *types.Endpoint) float64 {
	if lb.healthChecker == nil {
		// 没有健康检查器，使用端点状态
		if endpoint.Status.Ready {
			return 100.0
		}
		return 0.0
	}
	
	// 如果是远程端点，检查源节点健康状态
	if endpoint.Metadata.IsRemote && endpoint.Metadata.SourceNode != "" {
		nodeHealth := lb.healthChecker.GetNodeHealth(endpoint.Metadata.SourceNode)
		if nodeHealth != nil {
			return nodeHealth.HealthScore
		}
	}
	
	// 本地端点或无法获取节点健康状态时，使用端点状态
	if endpoint.Status.Ready {
		return 100.0
	}
	return 0.0
}

// getLatency 获取延迟
func (lb *RegionLoadBalancer) getLatency(endpoint *types.Endpoint) time.Duration {
	if lb.healthChecker == nil {
		return 0
	}
	
	// 如果是远程端点，获取到源节点的RTT
	if endpoint.Metadata.IsRemote && endpoint.Metadata.SourceNode != "" {
		nodeHealth := lb.healthChecker.GetNodeHealth(endpoint.Metadata.SourceNode)
		if nodeHealth != nil {
			return nodeHealth.RTT
		}
	}
	
	// 本地端点延迟为0
	return 0
}

// getConnectionCount 获取连接数 (简化实现)
func (lb *RegionLoadBalancer) getConnectionCount(endpoint *types.Endpoint) int {
	// 这里应该从连接池或监控系统获取实际连接数
	// 简化实现：返回随机值用于演示
	return rand.Intn(50)
}

// hashString 计算字符串哈希值
func (lb *RegionLoadBalancer) hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// 统计记录方法

// recordRequest 记录请求
func (lb *RegionLoadBalancer) recordRequest() {
	lb.statsMu.Lock()
	defer lb.statsMu.Unlock()
	lb.routingStats.TotalRequests++
}

// recordFailedRequest 记录失败请求
func (lb *RegionLoadBalancer) recordFailedRequest() {
	lb.statsMu.Lock()
	defer lb.statsMu.Unlock()
	lb.routingStats.FailedRequests++
}

// recordRouting 记录路由选择
func (lb *RegionLoadBalancer) recordRouting(endpoint *types.Endpoint) {
	lb.statsMu.Lock()
	defer lb.statsMu.Unlock()
	
	// 更新端点分布统计
	lb.routingStats.EndpointDistribution[endpoint.ID]++
	
	// 更新地理分布统计
	distance := lb.calculateGeographicDistance(endpoint)
	switch distance {
	case 0:
		lb.routingStats.LocalRequests++
	case 1:
		lb.routingStats.SiteRequests++
	case 2:
		lb.routingStats.ZoneRequests++
	case 3:
		lb.routingStats.RegionRequests++
	default:
		lb.routingStats.RemoteRequests++
	}
	
	lb.routingStats.LastUpdateTime = time.Now()
}

// GetLoadBalanceStrategies 获取支持的负载均衡策略列表
func GetLoadBalanceStrategies() []LoadBalanceStrategy {
	return []LoadBalanceStrategy{
		StrategyRoundRobin,
		StrategyWeighted,
		StrategyLeastConnections,
		StrategyConsistentHash,
		StrategyGeographicAware,
		StrategyHealthAware,
		StrategyLatencyAware,
	}
}

// ValidateStrategy 验证负载均衡策略
func ValidateStrategy(strategy LoadBalanceStrategy) bool {
	strategies := GetLoadBalanceStrategies()
	for _, s := range strategies {
		if s == strategy {
			return true
		}
	}
	return false
}