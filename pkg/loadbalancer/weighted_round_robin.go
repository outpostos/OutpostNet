package loadbalancer

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// WeightedRoundRobinLoadBalancer 加权轮询负载均衡器
type WeightedRoundRobinLoadBalancer struct {
	weights map[string]int // endpoint ID -> weight
	current map[string]int // endpoint ID -> current weight
	mu      sync.RWMutex
}

// NewWeightedRoundRobinLoadBalancer 创建加权轮询负载均衡器
func NewWeightedRoundRobinLoadBalancer() *WeightedRoundRobinLoadBalancer {
	return &WeightedRoundRobinLoadBalancer{
		weights: make(map[string]int),
		current: make(map[string]int),
	}
}

// Name 返回负载均衡器名称
func (lb *WeightedRoundRobinLoadBalancer) Name() string {
	return "weighted_round_robin"
}

// Select 实现 types.LoadBalancer 接口，从端点列表中选择一个端点
func (lb *WeightedRoundRobinLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	// 使用SelectEndpoint方法进行选择
	selected := lb.SelectEndpoint(endpoints, "")
	if selected == nil {
		return nil, fmt.Errorf("failed to select endpoint")
	}
	
	return selected, nil
}

// SelectEndpoint 使用加权轮询算法选择端点
func (lb *WeightedRoundRobinLoadBalancer) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 为新端点设置权重
	lb.initializeWeights(endpoints)

	// 加权轮询算法 (Weighted Round Robin)
	totalWeight := 0
	var selected *types.Endpoint

	for _, ep := range endpoints {
		weight := lb.weights[ep.ID]
		lb.current[ep.ID] += weight
		totalWeight += weight

		if selected == nil || lb.current[ep.ID] > lb.current[selected.ID] {
			selected = ep
		}
	}

	// 减去总权重
	if selected != nil {
		lb.current[selected.ID] -= totalWeight
	}

	return selected
}

// UpdateStats 更新端点统计信息，可用于动态调整权重
func (lb *WeightedRoundRobinLoadBalancer) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 可以根据性能动态调整权重
	// 这里可以实现基于延迟和成功率的权重调整逻辑
	if !success {
		lb.mu.Lock()
		defer lb.mu.Unlock()
		
		// 失败时降低权重
		if currentWeight, exists := lb.weights[endpoint.ID]; exists && currentWeight > 1 {
			lb.weights[endpoint.ID] = currentWeight - 1
		}
	}
}

// SetWeight 设置端点权重
func (lb *WeightedRoundRobinLoadBalancer) SetWeight(endpointID string, weight int) {
	if weight <= 0 {
		weight = 1
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	lb.weights[endpointID] = weight
	// 重置当前权重
	lb.current[endpointID] = 0
}

// GetWeight 获取端点权重
func (lb *WeightedRoundRobinLoadBalancer) GetWeight(endpointID string) int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	
	if weight, exists := lb.weights[endpointID]; exists {
		return weight
	}
	return 1 // 默认权重
}

// initializeWeights 初始化端点权重
func (lb *WeightedRoundRobinLoadBalancer) initializeWeights(endpoints []*types.Endpoint) {
	for _, ep := range endpoints {
		if _, exists := lb.weights[ep.ID]; !exists {
			lb.weights[ep.ID] = lb.extractEndpointWeight(ep)
			lb.current[ep.ID] = 0
		}
	}
}

// extractEndpointWeight 从端点标签中提取权重
func (lb *WeightedRoundRobinLoadBalancer) extractEndpointWeight(endpoint *types.Endpoint) int {
	// 1. 优先从 weight 标签获取数值权重
	if weightStr, exists := endpoint.Labels["weight"]; exists {
		if weight, err := strconv.Atoi(weightStr); err == nil && weight > 0 {
			return weight
		}
	}

	// 2. 从 priority 标签获取权重
	if priorityStr, exists := endpoint.Labels["priority"]; exists {
		return lb.parsePriorityWeight(priorityStr)
	}

	// 3. 从端点元数据获取权重
	if endpoint.Metadata.Weight > 0 {
		return endpoint.Metadata.Weight
	}

	// 4. 从端点元数据获取优先级
	if endpoint.Metadata.Priority > 0 {
		// 优先级越高，权重越大
		return endpoint.Metadata.Priority
	}

	// 5. 默认权重
	return 1
}

// parsePriorityWeight 解析优先级权重
func (lb *WeightedRoundRobinLoadBalancer) parsePriorityWeight(priorityStr string) int {
	switch priorityStr {
	case "high", "critical":
		return 5
	case "medium", "normal":
		return 3
	case "low":
		return 1
	default:
		// 尝试解析为数字
		if weight, err := strconv.Atoi(priorityStr); err == nil && weight > 0 {
			return weight
		}
		return 1
	}
}

// GetWeights 获取所有端点的权重信息（用于监控和调试）
func (lb *WeightedRoundRobinLoadBalancer) GetWeights() map[string]int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	
	weights := make(map[string]int)
	for id, weight := range lb.weights {
		weights[id] = weight
	}
	return weights
}

// ResetWeights 重置所有权重
func (lb *WeightedRoundRobinLoadBalancer) ResetWeights() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	for id := range lb.weights {
		lb.current[id] = 0
	}
}