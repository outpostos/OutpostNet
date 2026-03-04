package loadbalancer

import (
	"fmt"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// NewLoadBalancer 根据算法名称创建负载均衡器
func NewLoadBalancer(algorithm string) (types.LoadBalancer, error) {
	switch algorithm {
	case "round_robin", "":
		return NewRoundRobinLoadBalancer(), nil
	case "weighted_round_robin":
		return NewWeightedRoundRobinLoadBalancer(), nil
	case "random":
		return NewRandomLoadBalancer(), nil
	case "least_conn":
		return NewLeastConnLoadBalancer(), nil
	case "consistent_hash":
		return NewConsistentHashLoadBalancer(), nil
	case "geographic_aware":
		return NewGeographicAwareLoadBalancer(nil), nil
	default:
		return nil, fmt.Errorf("unknown load balancer algorithm: %s", algorithm)
	}
}

// NewLoadBalancerWithContext 根据算法名称和客户端位置创建负载均衡器
func NewLoadBalancerWithContext(algorithm string, clientLocation *types.EndpointMetadata) (types.LoadBalancer, error) {
	switch algorithm {
	case "round_robin", "":
		return NewRoundRobinLoadBalancer(), nil
	case "weighted_round_robin":
		return NewWeightedRoundRobinLoadBalancer(), nil
	case "random":
		return NewRandomLoadBalancer(), nil
	case "least_conn":
		return NewLeastConnLoadBalancer(), nil
	case "consistent_hash":
		return NewConsistentHashLoadBalancer(), nil
	case "geographic_aware":
		return NewGeographicAwareLoadBalancer(clientLocation), nil
	default:
		return nil, fmt.Errorf("unknown load balancer algorithm: %s", algorithm)
	}
}

// GetSupportedAlgorithms 返回支持的负载均衡算法列表
func GetSupportedAlgorithms() []string {
	return []string{
		"round_robin",
		"weighted_round_robin",
		"random",
		"least_conn",
		"consistent_hash",
		"geographic_aware",
	}
}

// IsValidAlgorithm 检查算法名称是否有效
func IsValidAlgorithm(algorithm string) bool {
	for _, supported := range GetSupportedAlgorithms() {
		if algorithm == supported {
			return true
		}
	}
	return algorithm == "" // 空字符串默认为 round_robin
}