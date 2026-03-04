package mesh

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ServiceRouter 智能服务路由器
type ServiceRouter struct {
	registry      *DistributedServiceRegistry
	meshCluster   ClusterInterface
	loadBalancers map[string]types.LoadBalancer
	
	// 路由策略
	routingPolicy *RoutingPolicy
	
	// 路由缓存
	routeCache    map[string]*RouteEntry
	cacheMu       sync.RWMutex
	cacheTimeout  time.Duration
	
	// 健康检查
	healthChecker *HealthChecker
	
	// 指标收集
	metrics *RouterMetrics
}

// RoutingPolicy 路由策略
type RoutingPolicy struct {
	// 路由优先级
	PreferLocal     bool              `yaml:"prefer_local" json:"prefer_local"`         // 优先本地服务
	PreferSameZone  bool              `yaml:"prefer_same_zone" json:"prefer_same_zone"` // 优先同区域服务
	PreferLowLatency bool             `yaml:"prefer_low_latency" json:"prefer_low_latency"` // 优先低延迟服务
	
	// 故障转移
	EnableFailover    bool            `yaml:"enable_failover" json:"enable_failover"`       // 启用故障转移
	FailoverTimeout   time.Duration   `yaml:"failover_timeout" json:"failover_timeout"`     // 故障转移超时
	MaxRetries        int             `yaml:"max_retries" json:"max_retries"`               // 最大重试次数
	
	// 负载均衡
	DefaultLBAlgorithm string         `yaml:"default_lb_algorithm" json:"default_lb_algorithm"` // 默认负载均衡算法
	StickySession     bool            `yaml:"sticky_session" json:"sticky_session"`           // 会话粘性
	
	// 服务发现
	DiscoveryTimeout  time.Duration   `yaml:"discovery_timeout" json:"discovery_timeout"`     // 服务发现超时
	RefreshInterval   time.Duration   `yaml:"refresh_interval" json:"refresh_interval"`       // 刷新间隔
	
	// 区域配置
	LocalZone         string          `yaml:"local_zone" json:"local_zone"`                   // 本地区域
	ZoneWeights       map[string]int  `yaml:"zone_weights" json:"zone_weights"`               // 区域权重
}

// RouteEntry 路由条目
type RouteEntry struct {
	ServiceName   string                `json:"service_name"`
	Namespace     string                `json:"namespace"`
	Endpoints     []*ScoredEndpoint     `json:"endpoints"`
	LastUpdated   time.Time             `json:"last_updated"`
	Version       int64                 `json:"version"`
	TTL           time.Duration         `json:"ttl"`
}

// ScoredEndpoint 评分端点
type ScoredEndpoint struct {
	*types.Endpoint
	Score      float64   `json:"score"`
	NodeID     string    `json:"node_id"`
	Zone       string    `json:"zone"`
	RTT        time.Duration `json:"rtt"`
	IsLocal    bool      `json:"is_local"`
	LastCheck  time.Time `json:"last_check"`
}

// RouterMetrics 路由器指标
type RouterMetrics struct {
	TotalRequests      int64            `json:"total_requests"`
	SuccessfulRoutes   int64            `json:"successful_routes"`
	FailedRoutes       int64            `json:"failed_routes"`
	LocalRoutes        int64            `json:"local_routes"`
	RemoteRoutes       int64            `json:"remote_routes"`
	FailoversTriggered int64            `json:"failovers_triggered"`
	AverageLatency     time.Duration    `json:"average_latency"`
	RoutesByAlgorithm  map[string]int64 `json:"routes_by_algorithm"`
}

// NewServiceRouter 创建服务路由器
func NewServiceRouter(registry *DistributedServiceRegistry, meshCluster ClusterInterface) *ServiceRouter {
	router := &ServiceRouter{
		registry:      registry,
		meshCluster:   meshCluster,
		loadBalancers: make(map[string]types.LoadBalancer),
		routingPolicy: DefaultRoutingPolicy(),
		routeCache:    make(map[string]*RouteEntry),
		cacheTimeout:  5 * time.Minute,
		metrics:       &RouterMetrics{
			RoutesByAlgorithm: make(map[string]int64),
		},
	}
	
	// 初始化负载均衡器
	router.initLoadBalancers()
	
	// 初始化健康检查器
	router.healthChecker = NewHealthChecker(router)
	
	return router
}

// initLoadBalancers 初始化负载均衡器
func (r *ServiceRouter) initLoadBalancers() {
	r.loadBalancers["round_robin"] = &RoundRobinLoadBalancer{}
	r.loadBalancers["weighted_round_robin"] = &WeightedRoundRobinLoadBalancer{}
	r.loadBalancers["least_connections"] = &LeastConnectionsLoadBalancer{}
	r.loadBalancers["random"] = &RandomLoadBalancer{}
	r.loadBalancers["consistent_hash"] = &ConsistentHashLoadBalancer{}
}

// RouteService 路由服务请求
func (r *ServiceRouter) RouteService(serviceName, namespace string, selector map[string]string) (*types.Endpoint, error) {
	r.metrics.TotalRequests++
	
	// 获取路由条目
	routeEntry, err := r.getRouteEntry(serviceName, namespace, selector)
	if err != nil {
		r.metrics.FailedRoutes++
		return nil, fmt.Errorf("failed to get route entry: %w", err)
	}
	
	// 如果没有可用端点，返回错误
	if len(routeEntry.Endpoints) == 0 {
		r.metrics.FailedRoutes++
		return nil, fmt.Errorf("no available endpoints for service %s.%s", serviceName, namespace)
	}
	
	// 应用路由策略
	selectedEndpoints := r.applyRoutingPolicy(routeEntry.Endpoints)
	
	// 选择最佳端点
	endpoint, err := r.selectBestEndpoint(selectedEndpoints, serviceName, namespace)
	if err != nil {
		r.metrics.FailedRoutes++
		return nil, fmt.Errorf("failed to select endpoint: %w", err)
	}
	
	// 更新指标
	r.updateMetrics(endpoint)
	
	return endpoint.Endpoint, nil
}

// getRouteEntry 获取路由条目
func (r *ServiceRouter) getRouteEntry(serviceName, namespace string, selector map[string]string) (*RouteEntry, error) {
	cacheKey := fmt.Sprintf("%s.%s", serviceName, namespace)
	
	// 检查缓存
	r.cacheMu.RLock()
	entry, exists := r.routeCache[cacheKey]
	r.cacheMu.RUnlock()
	
	if exists && !r.isCacheExpired(entry) {
		return entry, nil
	}
	
	// 从注册表获取端点
	endpoints, err := r.registry.GetEndpoints(serviceName, namespace, selector)
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoints: %w", err)
	}
	
	// 创建评分端点
	scoredEndpoints := make([]*ScoredEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		scoredEndpoint := &ScoredEndpoint{
			Endpoint:  endpoint,
			Score:     0.0,
			NodeID:    r.getEndpointNodeID(endpoint),
			Zone:      r.getEndpointZone(endpoint),
			RTT:       r.getEndpointRTT(endpoint),
			IsLocal:   r.isLocalEndpoint(endpoint),
			LastCheck: time.Now(),
		}
		
		// 计算评分
		scoredEndpoint.Score = r.calculateEndpointScore(scoredEndpoint)
		scoredEndpoints = append(scoredEndpoints, scoredEndpoint)
	}
	
	// 创建路由条目
	routeEntry := &RouteEntry{
		ServiceName: serviceName,
		Namespace:   namespace,
		Endpoints:   scoredEndpoints,
		LastUpdated: time.Now(),
		Version:     time.Now().Unix(),
		TTL:         r.cacheTimeout,
	}
	
	// 缓存路由条目
	r.cacheRouteEntry(cacheKey, routeEntry)
	
	return routeEntry, nil
}

// applyRoutingPolicy 应用路由策略
func (r *ServiceRouter) applyRoutingPolicy(endpoints []*ScoredEndpoint) []*ScoredEndpoint {
	var filteredEndpoints []*ScoredEndpoint
	
	// 优先本地服务
	if r.routingPolicy.PreferLocal {
		localEndpoints := r.filterLocalEndpoints(endpoints)
		if len(localEndpoints) > 0 {
			filteredEndpoints = localEndpoints
		}
	}
	
	// 优先同区域服务
	if r.routingPolicy.PreferSameZone && len(filteredEndpoints) == 0 {
		sameZoneEndpoints := r.filterSameZoneEndpoints(endpoints)
		if len(sameZoneEndpoints) > 0 {
			filteredEndpoints = sameZoneEndpoints
		}
	}
	
	// 优先低延迟服务
	if r.routingPolicy.PreferLowLatency && len(filteredEndpoints) == 0 {
		lowLatencyEndpoints := r.filterLowLatencyEndpoints(endpoints)
		if len(lowLatencyEndpoints) > 0 {
			filteredEndpoints = lowLatencyEndpoints
		}
	}
	
	// 如果没有匹配的端点，返回所有健康端点
	if len(filteredEndpoints) == 0 {
		filteredEndpoints = r.filterHealthyEndpoints(endpoints)
	}
	
	return filteredEndpoints
}

// selectBestEndpoint 选择最佳端点
func (r *ServiceRouter) selectBestEndpoint(endpoints []*ScoredEndpoint, serviceName, namespace string) (*ScoredEndpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	// 获取服务配置
	service, err := r.registry.GetService(serviceName, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get service: %w", err)
	}
	
	// 获取负载均衡算法
	lbAlgorithm := service.Spec.LoadBalancer
	if lbAlgorithm == "" {
		lbAlgorithm = r.routingPolicy.DefaultLBAlgorithm
	}
	
	// 转换为标准端点格式
	standardEndpoints := make([]*types.Endpoint, len(endpoints))
	for i, ep := range endpoints {
		standardEndpoints[i] = ep.Endpoint
	}
	
	// 获取负载均衡器
	loadBalancer, exists := r.loadBalancers[lbAlgorithm]
	if !exists {
		loadBalancer = r.loadBalancers["round_robin"]
	}
	
	// 选择端点
	selectedEndpoint, err := loadBalancer.Select(standardEndpoints)
	if err != nil {
		return nil, fmt.Errorf("load balancer selection failed: %w", err)
	}
	
	// 找到对应的评分端点
	for _, ep := range endpoints {
		if ep.Endpoint.ID == selectedEndpoint.ID {
			r.metrics.RoutesByAlgorithm[lbAlgorithm]++
			return ep, nil
		}
	}
	
	return nil, fmt.Errorf("selected endpoint not found in scored endpoints")
}

// calculateEndpointScore 计算端点评分
func (r *ServiceRouter) calculateEndpointScore(endpoint *ScoredEndpoint) float64 {
	score := 0.0
	
	// 基础健康评分
	if endpoint.Status.Ready {
		score += 50.0
	}
	
	// 本地优先
	if endpoint.IsLocal {
		score += 30.0
	}
	
	// 区域权重
	if weight, exists := r.routingPolicy.ZoneWeights[endpoint.Zone]; exists {
		score += float64(weight)
	}
	
	// 延迟评分（延迟越低分数越高）
	if endpoint.RTT > 0 {
		latencyScore := math.Max(0, 20.0-float64(endpoint.RTT.Milliseconds())/10.0)
		score += latencyScore
	}
	
	// 负载评分（简化处理）
	score += 10.0 // 基础负载分数
	
	return score
}

// filterLocalEndpoints 过滤本地端点
func (r *ServiceRouter) filterLocalEndpoints(endpoints []*ScoredEndpoint) []*ScoredEndpoint {
	var localEndpoints []*ScoredEndpoint
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			localEndpoints = append(localEndpoints, endpoint)
		}
	}
	return localEndpoints
}

// filterSameZoneEndpoints 过滤同区域端点
func (r *ServiceRouter) filterSameZoneEndpoints(endpoints []*ScoredEndpoint) []*ScoredEndpoint {
	var sameZoneEndpoints []*ScoredEndpoint
	localZone := r.routingPolicy.LocalZone
	
	for _, endpoint := range endpoints {
		if endpoint.Zone == localZone {
			sameZoneEndpoints = append(sameZoneEndpoints, endpoint)
		}
	}
	return sameZoneEndpoints
}

// filterLowLatencyEndpoints 过滤低延迟端点
func (r *ServiceRouter) filterLowLatencyEndpoints(endpoints []*ScoredEndpoint) []*ScoredEndpoint {
	// 排序端点（按RTT）
	sortedEndpoints := make([]*ScoredEndpoint, len(endpoints))
	copy(sortedEndpoints, endpoints)
	
	sort.Slice(sortedEndpoints, func(i, j int) bool {
		return sortedEndpoints[i].RTT < sortedEndpoints[j].RTT
	})
	
	// 返回延迟最低的50%端点
	count := len(sortedEndpoints) / 2
	if count == 0 {
		count = 1
	}
	
	return sortedEndpoints[:count]
}

// filterHealthyEndpoints 过滤健康端点
func (r *ServiceRouter) filterHealthyEndpoints(endpoints []*ScoredEndpoint) []*ScoredEndpoint {
	var healthyEndpoints []*ScoredEndpoint
	for _, endpoint := range endpoints {
		if endpoint.Status.Ready {
			healthyEndpoints = append(healthyEndpoints, endpoint)
		}
	}
	return healthyEndpoints
}

// getEndpointNodeID 获取端点所在节点ID
func (r *ServiceRouter) getEndpointNodeID(endpoint *types.Endpoint) string {
	if nodeID, exists := endpoint.Labels["node.servicekeel.io/id"]; exists {
		return nodeID
	}
	return "unknown"
}

// getEndpointZone 获取端点所在区域
func (r *ServiceRouter) getEndpointZone(endpoint *types.Endpoint) string {
	if zone, exists := endpoint.Labels["topology.kubernetes.io/zone"]; exists {
		return zone
	}
	return "unknown"
}

// getEndpointRTT 获取端点RTT
func (r *ServiceRouter) getEndpointRTT(endpoint *types.Endpoint) time.Duration {
	// 这里应该实现真实的RTT测量
	// 简化处理，返回模拟值
	return 10 * time.Millisecond
}

// isLocalEndpoint 判断是否为本地端点
func (r *ServiceRouter) isLocalEndpoint(endpoint *types.Endpoint) bool {
	nodeID := r.getEndpointNodeID(endpoint)
	return nodeID == r.meshCluster.GetLocalNode().ID
}

// isCacheExpired 检查缓存是否过期
func (r *ServiceRouter) isCacheExpired(entry *RouteEntry) bool {
	return time.Since(entry.LastUpdated) > entry.TTL
}

// cacheRouteEntry 缓存路由条目
func (r *ServiceRouter) cacheRouteEntry(key string, entry *RouteEntry) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	
	r.routeCache[key] = entry
}

// updateMetrics 更新指标
func (r *ServiceRouter) updateMetrics(endpoint *ScoredEndpoint) {
	r.metrics.SuccessfulRoutes++
	
	if endpoint.IsLocal {
		r.metrics.LocalRoutes++
	} else {
		r.metrics.RemoteRoutes++
	}
}

// GetMetrics 获取路由器指标
func (r *ServiceRouter) GetMetrics() *RouterMetrics {
	return r.metrics
}

// RefreshCache 刷新缓存
func (r *ServiceRouter) RefreshCache() {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	
	// 清空缓存
	r.routeCache = make(map[string]*RouteEntry)
}

// DefaultRoutingPolicy 默认路由策略
func DefaultRoutingPolicy() *RoutingPolicy {
	return &RoutingPolicy{
		PreferLocal:        true,
		PreferSameZone:     true,
		PreferLowLatency:   false,
		EnableFailover:     true,
		FailoverTimeout:    5 * time.Second,
		MaxRetries:         3,
		DefaultLBAlgorithm: "round_robin",
		StickySession:      false,
		DiscoveryTimeout:   10 * time.Second,
		RefreshInterval:    30 * time.Second,
		LocalZone:          "default",
		ZoneWeights:        make(map[string]int),
	}
}

// UpdateRoutingPolicy 更新路由策略
func (r *ServiceRouter) UpdateRoutingPolicy(policy *RoutingPolicy) {
	r.routingPolicy = policy
	r.RefreshCache()
}

// DiscoverServices 发现服务
func (r *ServiceRouter) DiscoverServices(namespace string) ([]*types.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.routingPolicy.DiscoveryTimeout)
	defer cancel()
	
	// 从注册表获取服务列表
	services, err := r.registry.ListServices(namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}
	
	// 过滤可用服务
	var availableServices []*types.Service
	for _, service := range services {
		select {
		case <-ctx.Done():
			return availableServices, ctx.Err()
		default:
			// 检查服务是否有可用端点
			endpoints, err := r.registry.GetEndpoints(service.Metadata.Name, service.Metadata.Namespace, nil)
			if err == nil && len(endpoints) > 0 {
				availableServices = append(availableServices, service)
			}
		}
	}
	
	return availableServices, nil
}

// RouteWithFailover 带故障转移的路由
func (r *ServiceRouter) RouteWithFailover(serviceName, namespace string, selector map[string]string) (*types.Endpoint, error) {
	var lastErr error
	
	for retry := 0; retry < r.routingPolicy.MaxRetries; retry++ {
		endpoint, err := r.RouteService(serviceName, namespace, selector)
		if err == nil {
			return endpoint, nil
		}
		
		lastErr = err
		
		// 触发故障转移
		if r.routingPolicy.EnableFailover {
			r.metrics.FailoversTriggered++
			time.Sleep(r.routingPolicy.FailoverTimeout)
			
			// 刷新缓存以获取最新端点信息
			r.RefreshCache()
		}
	}
	
	return nil, fmt.Errorf("all retries failed, last error: %w", lastErr)
}