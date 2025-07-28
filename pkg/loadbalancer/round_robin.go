package loadbalancer

import (
	"fmt"
	"sync"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// RoundRobinLoadBalancer 轮询负载均衡器
type RoundRobinLoadBalancer struct {
	mu    sync.Mutex
	index int
}

// NewRoundRobinLoadBalancer 创建新的轮询负载均衡器
func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	return &RoundRobinLoadBalancer{
		index: 0,
	}
}

// Select 从端点列表中选择一个端点
func (lb *RoundRobinLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
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

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 轮询选择
	selected := readyEndpoints[lb.index%len(readyEndpoints)]
	lb.index = (lb.index + 1) % len(readyEndpoints)

	return selected, nil
}

// Name 返回负载均衡算法名称
func (lb *RoundRobinLoadBalancer) Name() string {
	return "round_robin"
}