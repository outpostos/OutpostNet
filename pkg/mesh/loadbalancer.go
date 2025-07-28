package mesh

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// RoundRobinLoadBalancer 轮询负载均衡器
type RoundRobinLoadBalancer struct {
	mu      sync.Mutex
	counter int
}

func (lb *RoundRobinLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	index := lb.counter % len(endpoints)
	lb.counter++
	
	return endpoints[index], nil
}

func (lb *RoundRobinLoadBalancer) Name() string {
	return "round_robin"
}

// WeightedRoundRobinLoadBalancer 加权轮询负载均衡器
type WeightedRoundRobinLoadBalancer struct {
	mu       sync.Mutex
	weights  map[string]int
	counters map[string]int
}

func (lb *WeightedRoundRobinLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	if lb.weights == nil {
		lb.weights = make(map[string]int)
		lb.counters = make(map[string]int)
	}
	
	// 初始化权重
	for _, endpoint := range endpoints {
		if _, exists := lb.weights[endpoint.ID]; !exists {
			lb.weights[endpoint.ID] = lb.getEndpointWeight(endpoint)
			lb.counters[endpoint.ID] = 0
		}
	}
	
	// 找到权重最高的端点
	var selectedEndpoint *types.Endpoint
	maxWeight := -1
	
	for _, endpoint := range endpoints {
		weight := lb.weights[endpoint.ID]
		counter := lb.counters[endpoint.ID]
		
		// 计算当前权重
		currentWeight := weight - counter
		
		if currentWeight > maxWeight {
			maxWeight = currentWeight
			selectedEndpoint = endpoint
		}
	}
	
	if selectedEndpoint != nil {
		lb.counters[selectedEndpoint.ID]++
		
		// 重置计数器
		totalWeight := 0
		for _, weight := range lb.weights {
			totalWeight += weight
		}
		
		for id, counter := range lb.counters {
			if counter >= totalWeight {
				lb.counters[id] = 0
			}
		}
	}
	
	return selectedEndpoint, nil
}

func (lb *WeightedRoundRobinLoadBalancer) getEndpointWeight(endpoint *types.Endpoint) int {
	// 从标签中获取权重
	if weightStr, exists := endpoint.Labels["servicekeel.io/weight"]; exists {
		// 这里应该解析权重值
		_ = weightStr
		return 100 // 简化处理
	}
	return 100 // 默认权重
}

func (lb *WeightedRoundRobinLoadBalancer) Name() string {
	return "weighted_round_robin"
}

// LeastConnectionsLoadBalancer 最少连接负载均衡器
type LeastConnectionsLoadBalancer struct {
	mu          sync.Mutex
	connections map[string]int
}

func (lb *LeastConnectionsLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	if lb.connections == nil {
		lb.connections = make(map[string]int)
	}
	
	// 初始化连接数
	for _, endpoint := range endpoints {
		if _, exists := lb.connections[endpoint.ID]; !exists {
			lb.connections[endpoint.ID] = 0
		}
	}
	
	// 找到连接数最少的端点
	var selectedEndpoint *types.Endpoint
	minConnections := int(^uint(0) >> 1) // 最大int值
	
	for _, endpoint := range endpoints {
		connections := lb.connections[endpoint.ID]
		if connections < minConnections {
			minConnections = connections
			selectedEndpoint = endpoint
		}
	}
	
	if selectedEndpoint != nil {
		lb.connections[selectedEndpoint.ID]++
	}
	
	return selectedEndpoint, nil
}

func (lb *LeastConnectionsLoadBalancer) Name() string {
	return "least_connections"
}

// ReleaseConnection 释放连接
func (lb *LeastConnectionsLoadBalancer) ReleaseConnection(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	if count, exists := lb.connections[endpointID]; exists && count > 0 {
		lb.connections[endpointID]--
	}
}

// RandomLoadBalancer 随机负载均衡器
type RandomLoadBalancer struct {
	random *rand.Rand
	mu     sync.Mutex
}

func (lb *RandomLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	if lb.random == nil {
		lb.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	
	index := lb.random.Intn(len(endpoints))
	return endpoints[index], nil
}

func (lb *RandomLoadBalancer) Name() string {
	return "random"
}

// ConsistentHashLoadBalancer 一致性哈希负载均衡器
type ConsistentHashLoadBalancer struct {
	mu       sync.Mutex
	hashRing map[uint32]*types.Endpoint
	replicas int
}

func (lb *ConsistentHashLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	if lb.hashRing == nil {
		lb.hashRing = make(map[uint32]*types.Endpoint)
		lb.replicas = 160
	}
	
	// 构建哈希环
	lb.buildHashRing(endpoints)
	
	// 使用时间戳作为哈希键（简化处理）
	key := fmt.Sprintf("%d", time.Now().UnixNano())
	hash := lb.hash(key)
	
	// 找到最近的端点
	var selectedEndpoint *types.Endpoint
	minDistance := uint32(^uint32(0))
	
	for hashValue, endpoint := range lb.hashRing {
		distance := hashValue - hash
		if distance < minDistance {
			minDistance = distance
			selectedEndpoint = endpoint
		}
	}
	
	if selectedEndpoint == nil && len(endpoints) > 0 {
		selectedEndpoint = endpoints[0]
	}
	
	return selectedEndpoint, nil
}

func (lb *ConsistentHashLoadBalancer) buildHashRing(endpoints []*types.Endpoint) {
	// 清空哈希环
	lb.hashRing = make(map[uint32]*types.Endpoint)
	
	// 为每个端点创建多个虚拟节点
	for _, endpoint := range endpoints {
		for i := 0; i < lb.replicas; i++ {
			key := fmt.Sprintf("%s:%d", endpoint.ID, i)
			hash := lb.hash(key)
			lb.hashRing[hash] = endpoint
		}
	}
}

func (lb *ConsistentHashLoadBalancer) hash(key string) uint32 {
	h := md5.New()
	h.Write([]byte(key))
	hashBytes := h.Sum(nil)
	hashStr := hex.EncodeToString(hashBytes)
	
	// 简化处理，取前8位作为uint32
	var hash uint32
	for i := 0; i < 8 && i < len(hashStr); i++ {
		hash = hash*16 + uint32(hashStr[i])
	}
	
	return hash
}

func (lb *ConsistentHashLoadBalancer) Name() string {
	return "consistent_hash"
}

// StickySessionLoadBalancer 会话粘性负载均衡器
type StickySessionLoadBalancer struct {
	mu             sync.Mutex
	sessions       map[string]*types.Endpoint
	baseBalancer   types.LoadBalancer
	sessionTimeout time.Duration
	lastAccess     map[string]time.Time
}

func NewStickySessionLoadBalancer(baseBalancer types.LoadBalancer) *StickySessionLoadBalancer {
	return &StickySessionLoadBalancer{
		sessions:       make(map[string]*types.Endpoint),
		baseBalancer:   baseBalancer,
		sessionTimeout: 30 * time.Minute,
		lastAccess:     make(map[string]time.Time),
	}
}

func (lb *StickySessionLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	return lb.SelectWithSessionID(endpoints, "")
}

func (lb *StickySessionLoadBalancer) SelectWithSessionID(endpoints []*types.Endpoint, sessionID string) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	// 清理过期会话
	lb.cleanupExpiredSessions()
	
	// 如果有会话ID，尝试获取粘性端点
	if sessionID != "" {
		if endpoint, exists := lb.sessions[sessionID]; exists {
			// 检查端点是否还在可用列表中
			for _, ep := range endpoints {
				if ep.ID == endpoint.ID {
					lb.lastAccess[sessionID] = time.Now()
					return endpoint, nil
				}
			}
			
			// 端点不可用，删除会话
			delete(lb.sessions, sessionID)
			delete(lb.lastAccess, sessionID)
		}
	}
	
	// 使用基础负载均衡器选择端点
	selectedEndpoint, err := lb.baseBalancer.Select(endpoints)
	if err != nil {
		return nil, err
	}
	
	// 如果有会话ID，建立粘性关系
	if sessionID != "" {
		lb.sessions[sessionID] = selectedEndpoint
		lb.lastAccess[sessionID] = time.Now()
	}
	
	return selectedEndpoint, nil
}

func (lb *StickySessionLoadBalancer) cleanupExpiredSessions() {
	now := time.Now()
	for sessionID, lastAccess := range lb.lastAccess {
		if now.Sub(lastAccess) > lb.sessionTimeout {
			delete(lb.sessions, sessionID)
			delete(lb.lastAccess, sessionID)
		}
	}
}

func (lb *StickySessionLoadBalancer) Name() string {
	return "sticky_session"
}

// HealthAwareLoadBalancer 健康感知负载均衡器
type HealthAwareLoadBalancer struct {
	mu           sync.Mutex
	baseBalancer types.LoadBalancer
	healthScores map[string]float64
}

func NewHealthAwareLoadBalancer(baseBalancer types.LoadBalancer) *HealthAwareLoadBalancer {
	return &HealthAwareLoadBalancer{
		baseBalancer: baseBalancer,
		healthScores: make(map[string]float64),
	}
}

func (lb *HealthAwareLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	// 过滤健康端点
	healthyEndpoints := lb.filterHealthyEndpoints(endpoints)
	
	if len(healthyEndpoints) == 0 {
		return nil, fmt.Errorf("no healthy endpoints available")
	}
	
	// 使用基础负载均衡器选择端点
	return lb.baseBalancer.Select(healthyEndpoints)
}

func (lb *HealthAwareLoadBalancer) filterHealthyEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	var healthyEndpoints []*types.Endpoint
	
	for _, endpoint := range endpoints {
		if endpoint.Status.Ready {
			healthyEndpoints = append(healthyEndpoints, endpoint)
		}
	}
	
	return healthyEndpoints
}

func (lb *HealthAwareLoadBalancer) UpdateHealthScore(endpointID string, score float64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	lb.healthScores[endpointID] = score
}

func (lb *HealthAwareLoadBalancer) Name() string {
	return "health_aware"
}

// GeolocationLoadBalancer 地理位置负载均衡器
type GeolocationLoadBalancer struct {
	mu           sync.Mutex
	baseBalancer types.LoadBalancer
	localZone    string
	zoneWeights  map[string]int
}

func NewGeolocationLoadBalancer(baseBalancer types.LoadBalancer, localZone string) *GeolocationLoadBalancer {
	return &GeolocationLoadBalancer{
		baseBalancer: baseBalancer,
		localZone:    localZone,
		zoneWeights:  make(map[string]int),
	}
}

func (lb *GeolocationLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	// 优先选择本地区域的端点
	localEndpoints := lb.filterLocalZoneEndpoints(endpoints)
	if len(localEndpoints) > 0 {
		return lb.baseBalancer.Select(localEndpoints)
	}
	
	// 按区域权重选择
	weightedEndpoints := lb.filterWeightedZoneEndpoints(endpoints)
	if len(weightedEndpoints) > 0 {
		return lb.baseBalancer.Select(weightedEndpoints)
	}
	
	// 使用所有端点
	return lb.baseBalancer.Select(endpoints)
}

func (lb *GeolocationLoadBalancer) filterLocalZoneEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	var localEndpoints []*types.Endpoint
	
	for _, endpoint := range endpoints {
		if zone, exists := endpoint.Labels["topology.kubernetes.io/zone"]; exists {
			if zone == lb.localZone {
				localEndpoints = append(localEndpoints, endpoint)
			}
		}
	}
	
	return localEndpoints
}

func (lb *GeolocationLoadBalancer) filterWeightedZoneEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	var weightedEndpoints []*types.Endpoint
	
	for _, endpoint := range endpoints {
		if zone, exists := endpoint.Labels["topology.kubernetes.io/zone"]; exists {
			if weight, exists := lb.zoneWeights[zone]; exists && weight > 0 {
				weightedEndpoints = append(weightedEndpoints, endpoint)
			}
		}
	}
	
	return weightedEndpoints
}

func (lb *GeolocationLoadBalancer) SetZoneWeight(zone string, weight int) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	lb.zoneWeights[zone] = weight
}

func (lb *GeolocationLoadBalancer) Name() string {
	return "geolocation"
}

// LoadBalancerFactory 负载均衡器工厂
type LoadBalancerFactory struct {
	balancers map[string]func() types.LoadBalancer
}

func NewLoadBalancerFactory() *LoadBalancerFactory {
	factory := &LoadBalancerFactory{
		balancers: make(map[string]func() types.LoadBalancer),
	}
	
	// 注册内置负载均衡器
	factory.Register("round_robin", func() types.LoadBalancer {
		return &RoundRobinLoadBalancer{}
	})
	
	factory.Register("weighted_round_robin", func() types.LoadBalancer {
		return &WeightedRoundRobinLoadBalancer{}
	})
	
	factory.Register("least_connections", func() types.LoadBalancer {
		return &LeastConnectionsLoadBalancer{}
	})
	
	factory.Register("random", func() types.LoadBalancer {
		return &RandomLoadBalancer{}
	})
	
	factory.Register("consistent_hash", func() types.LoadBalancer {
		return &ConsistentHashLoadBalancer{}
	})
	
	return factory
}

func (f *LoadBalancerFactory) Register(name string, creator func() types.LoadBalancer) {
	f.balancers[name] = creator
}

func (f *LoadBalancerFactory) Create(name string) (types.LoadBalancer, error) {
	creator, exists := f.balancers[name]
	if !exists {
		return nil, fmt.Errorf("unknown load balancer: %s", name)
	}
	
	return creator(), nil
}

func (f *LoadBalancerFactory) GetAvailableBalancers() []string {
	var names []string
	for name := range f.balancers {
		names = append(names, name)
	}
	return names
}