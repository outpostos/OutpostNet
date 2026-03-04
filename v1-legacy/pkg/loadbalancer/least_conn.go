package loadbalancer

import (
	"fmt"
	"sync"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// LeastConnLoadBalancer 最少连接负载均衡器
type LeastConnLoadBalancer struct {
	mu          sync.RWMutex
	connections map[string]int // endpoint ID -> connection count
}

// NewLeastConnLoadBalancer 创建新的最少连接负载均衡器
func NewLeastConnLoadBalancer() *LeastConnLoadBalancer {
	return &LeastConnLoadBalancer{
		connections: make(map[string]int),
	}
}

// Select 从端点列表中选择连接数最少的端点
func (lb *LeastConnLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}

	// 过滤出就绪的端点
	readyEndpoints := make([]*types.Endpoint, 0)
	for _, ep := range endpoints {
		if ep.Status.Ready {
			readyEndpoints = append(readyEndpoints, ep)
		}
	}

	if len(readyEndpoints) == 0 {
		return nil, fmt.Errorf("no ready endpoints available")
	}

	lb.mu.RLock()
	defer lb.mu.RUnlock()

	// 查找连接数最少的端点
	var selected *types.Endpoint
	minConnections := -1

	for _, ep := range readyEndpoints {
		connections, exists := lb.connections[ep.ID]
		if !exists {
			connections = 0
		}

		if minConnections == -1 || connections < minConnections {
			minConnections = connections
			selected = ep
		}
	}

	return selected, nil
}

// Name 返回负载均衡算法名称
func (lb *LeastConnLoadBalancer) Name() string {
	return "least_conn"
}

// AddConnection 增加端点的连接数
func (lb *LeastConnLoadBalancer) AddConnection(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.connections[endpointID]++
}

// RemoveConnection 减少端点的连接数
func (lb *LeastConnLoadBalancer) RemoveConnection(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if count := lb.connections[endpointID]; count > 0 {
		lb.connections[endpointID] = count - 1
	}
}

// GetConnectionCount 获取端点的连接数
func (lb *LeastConnLoadBalancer) GetConnectionCount(endpointID string) int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.connections[endpointID]
}