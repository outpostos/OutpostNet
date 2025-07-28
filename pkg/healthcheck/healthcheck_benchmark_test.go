package healthcheck

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// BenchmarkHealthCheckManager_NewManager 测试健康检查管理器创建性能
func BenchmarkHealthCheckManager_NewManager(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		manager := NewHealthCheckManager()
		_ = manager
	}
}

// BenchmarkHealthCheckManager_AddEndpoint 测试添加端点性能
func BenchmarkHealthCheckManager_AddEndpoint(b *testing.B) {
	manager := NewHealthCheckManager()
	
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
			Labels: map[string]string{
				"app": "test",
			},
		}
		manager.AddEndpoint(endpoint)
	}
}

// BenchmarkHealthCheckManager_RemoveEndpoint 测试移除端点性能
func BenchmarkHealthCheckManager_RemoveEndpoint(b *testing.B) {
	manager := NewHealthCheckManager()
	
	// 预先添加端点
	endpoints := make([]*types.Endpoint, b.N)
	for i := 0; i < b.N; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
		}
		endpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		manager.RemoveEndpoint(endpoints[i].ID)
	}
}

// BenchmarkHealthCheckManager_GetHealthyEndpoints 测试获取健康端点性能
func BenchmarkHealthCheckManager_GetHealthyEndpoints(b *testing.B) {
	manager := NewHealthCheckManager()
	
	// 添加测试端点
	endpoints := make([]*types.Endpoint, 100)
	for i := 0; i < 100; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
			Labels: map[string]string{
				"app": "test",
			},
		}
		endpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
	}

	// 模拟一些端点健康状态
	for i := 0; i < 50; i++ {
		// Simulate health status update by updating results directly
		manager.UpdateHealth(endpoints[i].ID, i%2 == 0) // 50% 健康
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			healthyEndpoints := manager.GetHealthyEndpoints(endpoints)
			_ = healthyEndpoints
		}
	})
}

// BenchmarkHealthCheckManager_UpdateEndpointHealth 测试更新端点健康状态性能
func BenchmarkHealthCheckManager_UpdateEndpointHealth(b *testing.B) {
	manager := NewHealthCheckManager()
	
	// 添加测试端点
	endpoints := make([]*types.Endpoint, 1000)
	for i := 0; i < 1000; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
		}
		endpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		endpointIndex := 0
		for pb.Next() {
			endpoint := endpoints[endpointIndex%len(endpoints)]
			isHealthy := endpointIndex%2 == 0
			// Simulate health status update
			manager.UpdateHealth(endpoint.ID, isHealthy)
			endpointIndex++
		}
	})
}

// BenchmarkHealthCheckManager_ConcurrentOperations 测试并发操作性能
func BenchmarkHealthCheckManager_ConcurrentOperations(b *testing.B) {
	manager := NewHealthCheckManager()
	
	// 预先添加一些端点
	initialEndpoints := 500
	endpoints := make([]*types.Endpoint, initialEndpoints)
	for i := 0; i < initialEndpoints; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("initial-endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
		}
		endpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		operationIndex := 0
		for pb.Next() {
			switch operationIndex % 4 {
			case 0:
				// 添加新端点
				newEndpoint := &types.Endpoint{
					ID:   fmt.Sprintf("new-endpoint-%d", operationIndex),
					IP:   fmt.Sprintf("192.168.1.%d", (operationIndex%254)+1),
					Port: 8080,
				}
				manager.AddEndpoint(newEndpoint)
				
			case 1:
				// 更新健康状态
				if len(endpoints) > 0 {
					endpoint := endpoints[operationIndex%len(endpoints)]
					// Simulate health status update
					manager.UpdateHealth(endpoint.ID, operationIndex%3 != 0)
				}
				
			case 2:
				// 获取健康端点
				healthyEndpoints := manager.GetHealthyEndpoints(endpoints[:10])
				_ = healthyEndpoints
				
			case 3:
				// 移除端点（偶尔）
				if operationIndex%20 == 0 && len(endpoints) > 100 {
					endpoint := endpoints[operationIndex%len(endpoints)]
					manager.RemoveEndpoint(endpoint.ID)
				}
			}
			operationIndex++
		}
	})
}

// BenchmarkHealthCheckManager_LargeScale 测试大规模端点性能
func BenchmarkHealthCheckManager_LargeScale(b *testing.B) {
	endpointCounts := []int{100, 1000, 5000, 10000}

	for _, count := range endpointCounts {
		b.Run(fmt.Sprintf("Endpoints_%d", count), func(b *testing.B) {
			manager := NewHealthCheckManager()
			
			// 添加大量端点
			endpoints := make([]*types.Endpoint, count)
			for i := 0; i < count; i++ {
				endpoint := &types.Endpoint{
					ID:   fmt.Sprintf("endpoint-%d", i),
					IP:   fmt.Sprintf("10.%d.%d.%d", i/65536, (i/256)%256, i%256),
					Port: 8080,
					Labels: map[string]string{
						"app":  fmt.Sprintf("app-%d", i%10),
						"zone": fmt.Sprintf("zone-%d", i%5),
					},
				}
				endpoints[i] = endpoint
				manager.AddEndpoint(endpoint)
			}

			// 设置随机健康状态
			for i := 0; i < count; i++ {
				// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoints[i].ID, i%3 != 0) // 67% 健康
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 测试获取健康端点的性能
				healthyEndpoints := manager.GetHealthyEndpoints(endpoints)
				_ = healthyEndpoints
			}
		})
	}
}

// BenchmarkHealthCheckManager_MemoryUsage 测试内存使用情况
func BenchmarkHealthCheckManager_MemoryUsage(b *testing.B) {
	for _, count := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("MemoryUsage_%dEndpoints", count), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				manager := NewHealthCheckManager()
				
				// 添加大量端点
				for j := 0; j < count; j++ {
					endpoint := &types.Endpoint{
						ID:   fmt.Sprintf("endpoint-%d-%d", i, j),
						IP:   fmt.Sprintf("10.%d.%d.%d", j/65536, (j/256)%256, j%256),
						Port: 8080,
						Labels: map[string]string{
							"app":       fmt.Sprintf("app-%d", j%100),
							"instance":  fmt.Sprintf("instance-%d", j),
							"version":   "v1.0",
							"component": "backend",
						},
					}
					manager.AddEndpoint(endpoint)
					
					// 设置健康状态
					// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, j%4 != 0) // 75% 健康
				}

				// 执行一些操作来测试内存使用
				endpoints := make([]*types.Endpoint, 100)
				for k := 0; k < 100; k++ {
					endpoints[k] = &types.Endpoint{
						ID: fmt.Sprintf("endpoint-%d-%d", i, k),
					}
				}
				healthyEndpoints := manager.GetHealthyEndpoints(endpoints)
				_ = healthyEndpoints
			}
		})
	}
}

// BenchmarkHealthCheckManager_HealthStatusUpdate 测试健康状态更新模式
func BenchmarkHealthCheckManager_HealthStatusUpdate(b *testing.B) {
	manager := NewHealthCheckManager()
	
	// 添加端点
	numEndpoints := 1000
	endpoints := make([]*types.Endpoint, numEndpoints)
	for i := 0; i < numEndpoints; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.%d.%d", i/256, i%256),
			Port: 8080,
		}
		endpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
	}

	// 测试不同的更新模式
	b.Run("AllHealthy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			endpoint := endpoints[i%numEndpoints]
			// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, true)
		}
	})

	b.Run("AllUnhealthy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			endpoint := endpoints[i%numEndpoints]
			// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, false)
		}
	})

	b.Run("Mixed", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			endpoint := endpoints[i%numEndpoints]
			isHealthy := i%3 != 0 // 67% 健康
			// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, isHealthy)
		}
	})

	b.Run("Flapping", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			endpoint := endpoints[i%numEndpoints]
			isHealthy := i%2 == 0 // 50% 健康，频繁变化
			// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, isHealthy)
		}
	})
}

// BenchmarkHealthCheckManager_FilterPerformance 测试过滤性能
func BenchmarkHealthCheckManager_FilterPerformance(b *testing.B) {
	manager := NewHealthCheckManager()
	
	// 添加不同健康状态的端点
	allEndpoints := make([]*types.Endpoint, 1000)
	for i := 0; i < 1000; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.%d.%d", i/256, i%256),
			Port: 8080,
			Labels: map[string]string{
				"app": fmt.Sprintf("app-%d", i%10),
			},
		}
		allEndpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
		
		// 设置不同的健康状态分布
		// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, i%5 != 0) // 80% 健康
	}

	// 测试不同大小的端点列表过滤性能
	testSizes := []int{10, 50, 100, 500, 1000}
	
	for _, size := range testSizes {
		b.Run(fmt.Sprintf("FilterSize_%d", size), func(b *testing.B) {
			testEndpoints := allEndpoints[:size]
			
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				healthyEndpoints := manager.GetHealthyEndpoints(testEndpoints)
				_ = healthyEndpoints
			}
		})
	}
}

// BenchmarkHealthCheckManager_Start 测试启动健康检查管理器性能
func BenchmarkHealthCheckManager_Start(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		manager := NewHealthCheckManager()
		ctx, cancel := context.WithCancel(context.Background())
		
		// 启动管理器
		manager.Start(ctx)
		
		// 添加一些端点
		for j := 0; j < 10; j++ {
			endpoint := &types.Endpoint{
				ID:   fmt.Sprintf("endpoint-%d-%d", i, j),
				IP:   fmt.Sprintf("10.0.0.%d", j+1),
				Port: 8080,
			}
			manager.AddEndpoint(endpoint)
		}
		
		// 短暂等待
		time.Sleep(time.Millisecond)
		
		// 停止管理器
		cancel()
	}
}

// BenchmarkHealthCheckManager_RealTimeUpdates 测试实时更新性能
func BenchmarkHealthCheckManager_RealTimeUpdates(b *testing.B) {
	manager := NewHealthCheckManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	manager.Start(ctx)
	
	// 添加端点
	numEndpoints := 100
	endpoints := make([]*types.Endpoint, numEndpoints)
	for i := 0; i < numEndpoints; i++ {
		endpoint := &types.Endpoint{
			ID:   fmt.Sprintf("endpoint-%d", i),
			IP:   fmt.Sprintf("10.0.0.%d", (i%254)+1),
			Port: 8080,
		}
		endpoints[i] = endpoint
		manager.AddEndpoint(endpoint)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// 模拟高频率的健康状态更新
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()
			
			endpoint := endpoints[iteration%numEndpoints]
			isHealthy := iteration%3 != 0
			// Simulate health status update by updating results directly
		hm.mu.Lock()
		if result, exists := hm.results[endpoint.ID]; exists {
			result.Status = HealthCheckStatus("healthy")
		}
		hm.mu.Unlock()(endpoint.ID, isHealthy)
			
			// 偶尔查询健康端点
			if iteration%10 == 0 {
				healthyEndpoints := manager.GetHealthyEndpoints(endpoints[:10])
				_ = healthyEndpoints
			}
		}(i)
	}
	
	wg.Wait()
}