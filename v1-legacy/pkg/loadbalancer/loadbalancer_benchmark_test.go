package loadbalancer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// BenchmarkLoadBalancer_RoundRobin 测试轮询负载均衡性能
func BenchmarkLoadBalancer_RoundRobin(b *testing.B) {
	endpoints := createTestEndpoints(10)
	lb := NewLoadBalancerByType(RoundRobin)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			endpoint := lb.SelectEndpoint(endpoints, "test-key")
			_ = endpoint
		}
	})
}

// BenchmarkLoadBalancer_WeightedRoundRobin 测试加权轮询负载均衡性能
func BenchmarkLoadBalancer_WeightedRoundRobin(b *testing.B) {
	endpoints := createTestEndpointsWithWeights(10)
	lb := NewLoadBalancerByType(WeightedRoundRobin)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			endpoint := lb.SelectEndpoint(endpoints, "test-key")
			_ = endpoint
		}
	})
}

// BenchmarkLoadBalancer_LeastConnections 测试最少连接负载均衡性能
func BenchmarkLoadBalancer_LeastConnections(b *testing.B) {
	endpoints := createTestEndpoints(10)
	lb := NewLoadBalancerByType(LeastConnections)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			endpoint := lb.SelectEndpoint(endpoints, "test-key")
			if endpoint != nil {
				// 模拟连接结束
				lb.UpdateStats(endpoint, 100*time.Millisecond, true)
			}
		}
	})
}

// BenchmarkLoadBalancer_Random 测试随机负载均衡性能
func BenchmarkLoadBalancer_Random(b *testing.B) {
	endpoints := createTestEndpoints(10)
	lb := NewLoadBalancerByType(Random)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			endpoint := lb.SelectEndpoint(endpoints, "test-key")
			_ = endpoint
		}
	})
}

// BenchmarkLoadBalancer_ConsistentHash 测试一致性哈希负载均衡性能
func BenchmarkLoadBalancer_ConsistentHash(b *testing.B) {
	endpoints := createTestEndpoints(10)
	lb := NewLoadBalancerByType(ConsistentHash)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		key := fmt.Sprintf("client-%d", b.N)
		for pb.Next() {
			endpoint := lb.SelectEndpoint(endpoints, key)
			_ = endpoint
		}
	})
}

// BenchmarkLoadBalancer_DifferentEndpointCounts 测试不同端点数量的性能
func BenchmarkLoadBalancer_DifferentEndpointCounts(b *testing.B) {
	endpointCounts := []int{1, 5, 10, 50, 100, 500}
	lbTypes := []LoadBalancerType{RoundRobin, Random, ConsistentHash}

	for _, lbType := range lbTypes {
		for _, count := range endpointCounts {
			b.Run(fmt.Sprintf("%s_Endpoints_%d", lbType, count), func(b *testing.B) {
				endpoints := createTestEndpoints(count)
				lb := NewLoadBalancerByType(lbType)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					endpoint := lb.SelectEndpoint(endpoints, fmt.Sprintf("key-%d", i))
					_ = endpoint
				}
			})
		}
	}
}

// BenchmarkLoadBalancer_ConcurrentAccess 测试并发访问性能
func BenchmarkLoadBalancer_ConcurrentAccess(b *testing.B) {
	endpoints := createTestEndpoints(20)
	lbTypes := []LoadBalancerType{RoundRobin, WeightedRoundRobin, LeastConnections, Random, ConsistentHash}

	for _, lbType := range lbTypes {
		b.Run(string(lbType), func(b *testing.B) {
			lb := NewLoadBalancerByType(lbType)

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				clientID := 0
				for pb.Next() {
					key := fmt.Sprintf("client-%d", clientID)
					endpoint := lb.SelectEndpoint(endpoints, key)
					if endpoint != nil && lbType == LeastConnections {
						// 模拟连接结束
						lb.UpdateStats(endpoint, 50*time.Millisecond, true)
					}
					clientID++
				}
			})
		})
	}
}

// BenchmarkLoadBalancer_Manager 测试负载均衡器管理器性能
func BenchmarkLoadBalancer_Manager(b *testing.B) {
	manager := NewLoadBalancerManager()
	endpoints := createTestEndpoints(10)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		serviceID := 0
		for pb.Next() {
			serviceName := fmt.Sprintf("service-%d", serviceID%100)
			lb := manager.GetLoadBalancer(serviceName, RoundRobin)
			endpoint := lb.SelectEndpoint(endpoints, "test-key")
			_ = endpoint
			serviceID++
		}
	})
}

// BenchmarkLoadBalancer_ManagerMemoryUsage 测试负载均衡器管理器内存使用
func BenchmarkLoadBalancer_ManagerMemoryUsage(b *testing.B) {
	for _, numServices := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("Services_%d", numServices), func(b *testing.B) {
			manager := NewLoadBalancerManager()
			endpoints := createTestEndpoints(5)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 创建多个服务的负载均衡器
				for j := 0; j < numServices; j++ {
					serviceName := fmt.Sprintf("service-%d", j)
					lb := manager.GetLoadBalancer(serviceName, RoundRobin)
					endpoint := lb.SelectEndpoint(endpoints, "test-key")
					_ = endpoint
				}
			}
		})
	}
}

// BenchmarkLoadBalancer_ConsistentHashRebuild 测试一致性哈希环重建性能
func BenchmarkLoadBalancer_ConsistentHashRebuild(b *testing.B) {
	for _, count := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("Endpoints_%d", count), func(b *testing.B) {
			lb := &consistentHashLB{}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				endpoints := createTestEndpoints(count)
				lb.rebuildHashRing(endpoints)
			}
		})
	}
}

// BenchmarkLoadBalancer_WeightedRoundRobinWithStats 测试带统计信息的加权轮询
func BenchmarkLoadBalancer_WeightedRoundRobinWithStats(b *testing.B) {
	endpoints := createTestEndpointsWithWeights(20)
	lb := NewLoadBalancerByType(WeightedRoundRobin)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		endpoint := lb.SelectEndpoint(endpoints, "test-key")
		if endpoint != nil {
			// 模拟性能统计更新
			latency := time.Duration(50+i%100) * time.Millisecond
			success := i%10 != 0 // 90% 成功率
			lb.UpdateStats(endpoint, latency, success)
		}
	}
}

// BenchmarkLoadBalancer_SelectionConsistency 测试选择一致性（一致性哈希）
func BenchmarkLoadBalancer_SelectionConsistency(b *testing.B) {
	endpoints := createTestEndpoints(10)
	lb := NewLoadBalancerByType(ConsistentHash)

	// 预热哈希环
	lb.SelectEndpoint(endpoints, "warmup")

	testKeys := make([]string, 1000)
	for i := range testKeys {
		testKeys[i] = fmt.Sprintf("user-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := testKeys[i%len(testKeys)]
		endpoint := lb.SelectEndpoint(endpoints, key)
		_ = endpoint
	}
}

// BenchmarkLoadBalancer_FailoverScenario 测试故障转移场景
func BenchmarkLoadBalancer_FailoverScenario(b *testing.B) {
	allEndpoints := createTestEndpoints(10)
	lb := NewLoadBalancerByType(LeastConnections)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 模拟部分端点故障
		healthyCount := 5 + (i%6) // 5-10个健康端点
		healthyEndpoints := allEndpoints[:healthyCount]

		endpoint := lb.SelectEndpoint(healthyEndpoints, "test-key")
		if endpoint != nil {
			// 模拟连接结束
			lb.UpdateStats(endpoint, 100*time.Millisecond, true)
		}
	}
}

// BenchmarkLoadBalancer_HighConcurrency 测试高并发场景
func BenchmarkLoadBalancer_HighConcurrency(b *testing.B) {
	endpoints := createTestEndpoints(50)
	lb := NewLoadBalancerByType(RoundRobin)

	var wg sync.WaitGroup
	concurrency := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()
				key := fmt.Sprintf("client-%d", clientID)
				endpoint := lb.SelectEndpoint(endpoints, key)
				_ = endpoint
			}(j)
		}
		wg.Wait()
	}
}

// BenchmarkLoadBalancer_UpdateStatsPerformance 测试统计信息更新性能
func BenchmarkLoadBalancer_UpdateStatsPerformance(b *testing.B) {
	endpoints := createTestEndpoints(10)
	lbTypes := []LoadBalancerType{LeastConnections, WeightedRoundRobin}

	for _, lbType := range lbTypes {
		b.Run(string(lbType), func(b *testing.B) {
			lb := NewLoadBalancerByType(lbType)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				endpoint := endpoints[i%len(endpoints)]
				latency := time.Duration(50+i%100) * time.Millisecond
				success := i%10 != 0
				lb.UpdateStats(endpoint, latency, success)
			}
		})
	}
}

// 辅助函数：创建测试端点
func createTestEndpoints(count int) []*types.Endpoint {
	endpoints := make([]*types.Endpoint, count)
	for i := 0; i < count; i++ {
		endpoints[i] = &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
			Labels: map[string]string{
				"app":  "test",
				"zone": fmt.Sprintf("zone-%d", i%3),
			},
		}
	}
	return endpoints
}

// 辅助函数：创建带权重的测试端点
func createTestEndpointsWithWeights(count int) []*types.Endpoint {
	endpoints := make([]*types.Endpoint, count)
	weights := []string{"low", "medium", "high"}
	
	for i := 0; i < count; i++ {
		endpoints[i] = &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
			Labels: map[string]string{
				"app":    "test",
				"zone":   fmt.Sprintf("zone-%d", i%3),
				"weight": weights[i%len(weights)],
			},
		}
	}
	return endpoints
}