package loadbalancer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// RandomLoadBalancer 随机负载均衡器
type RandomLoadBalancer struct {
	rng *rand.Rand
}

// NewRandomLoadBalancer 创建新的随机负载均衡器
func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Select 从端点列表中随机选择一个端点
func (lb *RandomLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
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

	// 随机选择
	index := lb.rng.Intn(len(readyEndpoints))
	return readyEndpoints[index], nil
}

// Name 返回负载均衡算法名称
func (lb *RandomLoadBalancer) Name() string {
	return "random"
}