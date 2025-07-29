package loadbalancer

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// LoadBalancer 负载均衡器接口
type LoadBalancer interface {
	// SelectEndpoint 从可用端点中选择一个
	SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint
	// UpdateStats 更新端点统计信息
	UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool)
	// Name 返回负载均衡器名称
	Name() string
}

// LoadBalancerType 负载均衡类型
type LoadBalancerType string

const (
	RoundRobin     LoadBalancerType = "round_robin"
	WeightedRoundRobin LoadBalancerType = "weighted_round_robin"
	LeastConnections LoadBalancerType = "least_connections"
	Random         LoadBalancerType = "random"
	ConsistentHash LoadBalancerType = "consistent_hash"
	GeographicAware LoadBalancerType = "geographic_aware"
)

// NewLoadBalancerByType 根据类型创建负载均衡器
func NewLoadBalancerByType(lbType LoadBalancerType) LoadBalancer {
	switch lbType {
	case RoundRobin:
		return &roundRobinLB{}
	case WeightedRoundRobin:
		return &weightedRoundRobinLB{}
	case LeastConnections:
		return &leastConnectionsLB{}
	case Random:
		return &randomLB{}
	case ConsistentHash:
		return &consistentHashLB{}
	case GeographicAware:
		return NewGeographicAwareLoadBalancer(nil)
	default:
		return &roundRobinLB{} // 默认使用轮询
	}
}

// NewLoadBalancerByTypeWithContext 根据类型和客户端位置创建负载均衡器
func NewLoadBalancerByTypeWithContext(lbType LoadBalancerType, clientLocation *types.EndpointMetadata) LoadBalancer {
	switch lbType {
	case RoundRobin:
		return &roundRobinLB{}
	case WeightedRoundRobin:
		return &weightedRoundRobinLB{}
	case LeastConnections:
		return &leastConnectionsLB{}
	case Random:
		return &randomLB{}
	case ConsistentHash:
		return &consistentHashLB{}
	case GeographicAware:
		return NewGeographicAwareLoadBalancer(clientLocation)
	default:
		return &roundRobinLB{} // 默认使用轮询
	}
}

// 1. 轮询负载均衡器
type roundRobinLB struct {
	counter uint64
	mu      sync.Mutex
}

func (lb *roundRobinLB) Name() string {
	return string(RoundRobin)
}

func (lb *roundRobinLB) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	index := lb.counter % uint64(len(endpoints))
	lb.counter++
	lb.mu.Unlock()

	return endpoints[index]
}

func (lb *roundRobinLB) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 轮询负载均衡器不需要统计信息
}

// 2. 加权轮询负载均衡器
type weightedRoundRobinLB struct {
	weights map[string]int // endpoint ID -> weight
	current map[string]int // endpoint ID -> current weight
	mu      sync.Mutex
}

func (lb *weightedRoundRobinLB) Name() string {
	return string(WeightedRoundRobin)
}

func (lb *weightedRoundRobinLB) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.weights == nil {
		lb.weights = make(map[string]int)
		lb.current = make(map[string]int)
	}

	// 为新端点设置默认权重
	for _, ep := range endpoints {
		if _, exists := lb.weights[ep.ID]; !exists {
			lb.weights[ep.ID] = lb.getEndpointWeight(ep)
			lb.current[ep.ID] = 0
		}
	}

	// 加权轮询算法
	totalWeight := 0
	selected := endpoints[0]
	
	for _, ep := range endpoints {
		weight := lb.weights[ep.ID]
		lb.current[ep.ID] += weight
		totalWeight += weight
		
		if lb.current[ep.ID] > lb.current[selected.ID] {
			selected = ep
		}
	}

	lb.current[selected.ID] -= totalWeight
	return selected
}

func (lb *weightedRoundRobinLB) getEndpointWeight(endpoint *types.Endpoint) int {
	// 根据端点标签或其他属性计算权重
	if weightStr, exists := endpoint.Labels["weight"]; exists {
		if weight := parseWeight(weightStr); weight > 0 {
			return weight
		}
	}
	return 1 // 默认权重
}

func parseWeight(weightStr string) int {
	// 简单的权重解析
	switch weightStr {
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	default:
		return 1
	}
}

func (lb *weightedRoundRobinLB) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 可以根据性能动态调整权重
}

// 3. 最少连接负载均衡器
type leastConnectionsLB struct {
	connections map[string]int // endpoint ID -> active connections
	mu          sync.RWMutex
}

func (lb *leastConnectionsLB) Name() string {
	return string(LeastConnections)
}

func (lb *leastConnectionsLB) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.connections == nil {
		lb.connections = make(map[string]int)
	}

	// 查找连接数最少的端点
	var selected *types.Endpoint
	minConnections := int(^uint(0) >> 1) // 最大整数

	for _, ep := range endpoints {
		connections := lb.connections[ep.ID]
		if connections < minConnections {
			minConnections = connections
			selected = ep
		}
	}

	// 增加选中端点的连接数
	if selected != nil {
		lb.connections[selected.ID]++
	}

	return selected
}

func (lb *leastConnectionsLB) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 连接结束时减少连接数
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	if lb.connections != nil {
		if lb.connections[endpoint.ID] > 0 {
			lb.connections[endpoint.ID]--
		}
	}
}

// 4. 随机负载均衡器
type randomLB struct{}

func (lb *randomLB) Name() string {
	return string(Random)
}

func (lb *randomLB) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	index := rand.Intn(len(endpoints))
	return endpoints[index]
}

func (lb *randomLB) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 随机负载均衡器不需要统计信息
}

// 5. 一致性哈希负载均衡器
type consistentHashLB struct {
	hashRing map[uint32]string // hash -> endpoint ID
	sortedHashes []uint32
	mu       sync.RWMutex
}

func (lb *consistentHashLB) Name() string {
	return string(ConsistentHash)
}

func (lb *consistentHashLB) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 重建哈希环（如果端点发生变化）
	lb.rebuildHashRing(endpoints)

	// 计算键的哈希值
	keyHash := crc32.ChecksumIEEE([]byte(key))

	// 在哈希环中查找
	selectedID := lb.findInHashRing(keyHash)
	
	// 查找对应的端点
	for _, ep := range endpoints {
		if ep.ID == selectedID {
			return ep
		}
	}

	// 如果没找到，返回第一个
	return endpoints[0]
}

func (lb *consistentHashLB) rebuildHashRing(endpoints []*types.Endpoint) {
	if lb.hashRing == nil {
		lb.hashRing = make(map[uint32]string)
	}

	// 清空现有的哈希环
	lb.hashRing = make(map[uint32]string)
	lb.sortedHashes = lb.sortedHashes[:0]

	// 为每个端点添加多个虚拟节点
	virtualNodes := 150 // 每个端点的虚拟节点数
	for _, ep := range endpoints {
		for i := 0; i < virtualNodes; i++ {
			virtualKey := fmt.Sprintf("%s#%d", ep.ID, i)
			hash := crc32.ChecksumIEEE([]byte(virtualKey))
			lb.hashRing[hash] = ep.ID
			lb.sortedHashes = append(lb.sortedHashes, hash)
		}
	}

	// 排序哈希值
	sort.Slice(lb.sortedHashes, func(i, j int) bool {
		return lb.sortedHashes[i] < lb.sortedHashes[j]
	})
}

func (lb *consistentHashLB) findInHashRing(keyHash uint32) string {
	if len(lb.sortedHashes) == 0 {
		return ""
	}

	// 二分查找第一个大于等于 keyHash 的值
	idx := sort.Search(len(lb.sortedHashes), func(i int) bool {
		return lb.sortedHashes[i] >= keyHash
	})

	// 如果没找到，返回第一个（环形）
	if idx == len(lb.sortedHashes) {
		idx = 0
	}

	hash := lb.sortedHashes[idx]
	return lb.hashRing[hash]
}

func (lb *consistentHashLB) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 一致性哈希负载均衡器不需要统计信息
}

// LoadBalancerManager 负载均衡器管理器
type LoadBalancerManager struct {
	balancers map[string]LoadBalancer
	mu        sync.RWMutex
}

// NewLoadBalancerManager 创建负载均衡器管理器
func NewLoadBalancerManager() *LoadBalancerManager {
	return &LoadBalancerManager{
		balancers: make(map[string]LoadBalancer),
	}
}

// GetLoadBalancer 获取指定服务的负载均衡器
func (m *LoadBalancerManager) GetLoadBalancer(serviceName string, lbType LoadBalancerType) LoadBalancer {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", serviceName, lbType)
	if lb, exists := m.balancers[key]; exists {
		return lb
	}

	// 创建新的负载均衡器
	lb := NewLoadBalancerByType(lbType)
	m.balancers[key] = lb
	return lb
}

// GetLoadBalancerWithContext 获取指定服务的负载均衡器（带客户端位置信息）
func (m *LoadBalancerManager) GetLoadBalancerWithContext(serviceName string, lbType LoadBalancerType, clientLocation *types.EndpointMetadata) LoadBalancer {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 对于地理位置感知的负载均衡器，需要包含客户端位置信息在key中
	var key string
	if lbType == GeographicAware && clientLocation != nil {
		key = fmt.Sprintf("%s:%s:%s:%s:%s", serviceName, lbType, clientLocation.Site, clientLocation.Zone, clientLocation.Region)
	} else {
		key = fmt.Sprintf("%s:%s", serviceName, lbType)
	}

	if lb, exists := m.balancers[key]; exists {
		return lb
	}

	// 创建新的负载均衡器
	lb := NewLoadBalancerByTypeWithContext(lbType, clientLocation)
	m.balancers[key] = lb
	return lb
}

// RemoveLoadBalancer 移除负载均衡器
func (m *LoadBalancerManager) RemoveLoadBalancer(serviceName string, lbType LoadBalancerType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", serviceName, lbType)
	delete(m.balancers, key)
}