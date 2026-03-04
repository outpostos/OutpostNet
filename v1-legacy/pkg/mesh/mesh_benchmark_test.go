package mesh

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// BenchmarkSuite 性能基准测试套件
type BenchmarkSuite struct {
	registry        *MockServiceRegistry
	localNode       *LocalNode
	cluster         *MockMeshCluster
	distributedReg  *DistributedServiceRegistry
	router          *ServiceRouter
	gossipManager   *GossipManager
	healthChecker   *HealthChecker
}

func NewBenchmarkSuite() (*BenchmarkSuite, error) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	config.NodeID = "benchmark-node"
	
	localNode, err := NewLocalNode(config, registry)
	if err != nil {
		return nil, err
	}
	
	cluster := NewMockMeshCluster(localNode)
	distributedReg := NewDistributedServiceRegistry(registry, cluster)
	router := NewServiceRouter(distributedReg, cluster)
	
	gossipManager, err := NewGossipManager(cluster)
	if err != nil {
		return nil, err
	}
	
	healthChecker := NewHealthChecker(router)
	
	return &BenchmarkSuite{
		registry:       registry,
		localNode:      localNode,
		cluster:        cluster,
		distributedReg: distributedReg,
		router:         router,
		gossipManager:  gossipManager,
		healthChecker:  healthChecker,
	}, nil
}

// 基准测试：节点创建性能
func BenchmarkNodeCreationPerformance(b *testing.B) {
	registry := NewMockServiceRegistry()
	config := DefaultNodeConfig()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			config.NodeID = generateNodeID()
			localNode, err := NewLocalNode(config, registry)
			if err != nil {
				b.Fatal(err)
			}
			_ = localNode
		}
	})
}

// 基准测试：服务注册性能
func BenchmarkServiceRegistration(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		service := &types.Service{
			Metadata: types.ServiceMetadata{
				Name:      fmt.Sprintf("bench-service-%d", i),
				Namespace: "default",
				Labels:    make(map[string]string),
			},
			Spec: types.ServiceSpec{
				Selector:     make(map[string]string),
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
				Ports: []types.ServicePort{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
				},
			},
		}
		
		err := suite.distributedReg.RegisterService(service)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// 基准测试：并发服务注册
func BenchmarkConcurrentServiceRegistration(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			service := &types.Service{
				Metadata: types.ServiceMetadata{
					Name:      fmt.Sprintf("concurrent-service-%d-%d", b.N, i),
					Namespace: "default",
					Labels:    make(map[string]string),
				},
				Spec: types.ServiceSpec{
					Selector:     make(map[string]string),
					ProxyMode:    "tcp",
					LoadBalancer: "round_robin",
					Ports: []types.ServicePort{
						{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
					},
				},
			}
			
			err := suite.distributedReg.RegisterService(service)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// 基准测试：工作负载注册性能
func BenchmarkWorkloadRegistration(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		workload := &types.Workload{
			Metadata: types.WorkloadMetadata{
				ID:        fmt.Sprintf("bench-workload-%d", i),
				Labels:    map[string]string{"app": "bench"},
				CreatedAt: time.Now(),
			},
			Spec: types.WorkloadSpec{
				LocalAddr: "127.0.0.1",
				LocalPort: 8080 + i%1000,
				Protocol:  "tcp",
			},
			Status: types.WorkloadStatus{
				Ready:      true,
				LastSeen:   time.Now(),
				RegisterIP: "127.0.0.1",
			},
		}
		
		err := suite.distributedReg.RegisterWorkload(workload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// 基准测试：服务路由性能
func BenchmarkServiceRouting(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	// 准备测试数据
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "routing-service",
			Namespace: "default",
			Labels:    make(map[string]string),
		},
		Spec: types.ServiceSpec{
			Selector:     make(map[string]string),
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
			Ports: []types.ServicePort{
				{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
			},
		},
	}
	
	suite.registry.RegisterService(service)
	
	// 创建端点
	endpoints := make([]*types.Endpoint, 10)
	for i := 0; i < 10; i++ {
		endpoints[i] = &types.Endpoint{
			ID:       fmt.Sprintf("endpoint-%d", i),
			IP:       "127.0.0.1",
			Port:     8080 + i,
			Protocol: "tcp",
			Labels:   make(map[string]string),
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		}
	}
	suite.registry.endpoints["routing-service.default"] = endpoints
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.router.RouteService("routing-service", "default", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// 基准测试：并发服务路由
func BenchmarkConcurrentServiceRouting(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	// 准备测试数据
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "concurrent-routing-service",
			Namespace: "default",
			Labels:    make(map[string]string),
		},
		Spec: types.ServiceSpec{
			Selector:     make(map[string]string),
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
			Ports: []types.ServicePort{
				{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
			},
		},
	}
	
	suite.registry.RegisterService(service)
	
	// 创建端点
	endpoints := make([]*types.Endpoint, 20)
	for i := 0; i < 20; i++ {
		endpoints[i] = &types.Endpoint{
			ID:       fmt.Sprintf("concurrent-endpoint-%d", i),
			IP:       "127.0.0.1",
			Port:     8080 + i,
			Protocol: "tcp",
			Labels:   make(map[string]string),
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		}
	}
	suite.registry.endpoints["concurrent-routing-service.default"] = endpoints
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := suite.router.RouteService("concurrent-routing-service", "default", nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// 基准测试：负载均衡算法性能比较
func BenchmarkLoadBalancerComparison(b *testing.B) {
	endpoints := createTestEndpoints(50)
	
	loadBalancers := map[string]types.LoadBalancer{
		"round_robin":           &RoundRobinLoadBalancer{},
		"weighted_round_robin":  &WeightedRoundRobinLoadBalancer{},
		"least_connections":     &LeastConnectionsLoadBalancer{},
		"random":                &RandomLoadBalancer{},
		"consistent_hash":       &ConsistentHashLoadBalancer{},
	}
	
	for name, lb := range loadBalancers {
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := lb.Select(endpoints)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// 基准测试：Gossip消息传播性能
func BenchmarkGossipBroadcastPerformance(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	heartbeat := &protocol.HeartbeatPayload{
		Status:            "active",
		ActiveConnections: 10,
		CpuUsage:          50,
		MemoryUsage:       60,
		Metrics:           make(map[string]string),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		suite.gossipManager.BroadcastHeartbeat(heartbeat)
	}
}

// 基准测试：大规模Gossip消息处理
func BenchmarkLargeScaleGossipProcessing(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	// 创建大量消息
	messages := make([]*GossipMessage, 1000)
	for i := 0; i < 1000; i++ {
		messages[i] = &GossipMessage{
			ID:        fmt.Sprintf("msg-%d", i),
			Type:      GossipMessageHeartbeat,
			Payload:   []byte("test payload"),
			Timestamp: time.Now(),
			Source:    "test-node",
			TTL:       5,
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, msg := range messages {
			suite.gossipManager.addToCache(msg)
		}
	}
}

// 基准测试：健康检查性能
func BenchmarkHealthCheckPerformance(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	endpoints := make([]*types.Endpoint, 100)
	for i := 0; i < 100; i++ {
		endpoints[i] = &types.Endpoint{
			ID:       fmt.Sprintf("health-endpoint-%d", i),
			IP:       "127.0.0.1",
			Port:     8080 + i,
			Protocol: "tcp",
			Labels:   make(map[string]string),
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		endpoint := endpoints[i%len(endpoints)]
		suite.healthChecker.performHealthCheck(endpoint)
	}
}

// 基准测试：大规模服务发现
func BenchmarkLargeScaleServiceDiscovery(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	// 注册大量服务
	for i := 0; i < 1000; i++ {
		service := &types.Service{
			Metadata: types.ServiceMetadata{
				Name:      fmt.Sprintf("discovery-service-%d", i),
				Namespace: "default",
				Labels:    make(map[string]string),
			},
			Spec: types.ServiceSpec{
				Selector:     make(map[string]string),
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
				Ports: []types.ServicePort{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
				},
			},
		}
		suite.registry.RegisterService(service)
		
		// 为每个服务创建端点
		serviceKey := fmt.Sprintf("discovery-service-%d.default", i)
		suite.registry.endpoints[serviceKey] = []*types.Endpoint{
			{
				ID:       fmt.Sprintf("endpoint-%d", i),
				IP:       "127.0.0.1",
				Port:     8080 + i%1000,
				Protocol: "tcp",
				Labels:   make(map[string]string),
				Status:   types.EndpointStatus{Ready: true},
				LastSeen: time.Now(),
			},
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.router.DiscoverServices("default")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// 基准测试：内存使用效率
func BenchmarkMemoryEfficiency(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 创建节点
		config := DefaultNodeConfig()
		config.NodeID = fmt.Sprintf("memory-node-%d", i)
		localNode, _ := NewLocalNode(config, NewMockServiceRegistry())
		
		// 创建服务
		service := &types.Service{
			Metadata: types.ServiceMetadata{
				Name:      fmt.Sprintf("memory-service-%d", i),
				Namespace: "default",
				Labels:    make(map[string]string),
			},
			Spec: types.ServiceSpec{
				Selector:     make(map[string]string),
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
				Ports: []types.ServicePort{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
				},
			},
		}
		suite.distributedReg.RegisterService(service)
		
		// 创建健康状态
		suite.healthChecker.getOrCreateHealthStatus(fmt.Sprintf("memory-endpoint-%d", i))
		
		// 清理
		_ = localNode
	}
}

// 基准测试：端到端性能
func BenchmarkEndToEndPerformance(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serviceName := fmt.Sprintf("e2e-service-%d", i)
		
		// 1. 注册服务
		service := &types.Service{
			Metadata: types.ServiceMetadata{
				Name:      serviceName,
				Namespace: "default",
				Labels:    make(map[string]string),
			},
			Spec: types.ServiceSpec{
				Selector:     make(map[string]string),
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
				Ports: []types.ServicePort{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
				},
			},
		}
		suite.distributedReg.RegisterService(service)
		
		// 2. 注册端点
		serviceKey := serviceName + ".default"
		suite.registry.endpoints[serviceKey] = []*types.Endpoint{
			{
				ID:       fmt.Sprintf("e2e-endpoint-%d", i),
				IP:       "127.0.0.1",
				Port:     8080,
				Protocol: "tcp",
				Labels:   make(map[string]string),
				Status:   types.EndpointStatus{Ready: true},
				LastSeen: time.Now(),
			},
		}
		
		// 3. 执行健康检查
		endpoint := suite.registry.endpoints[serviceKey][0]
		suite.healthChecker.performHealthCheck(endpoint)
		
		// 4. 路由服务
		_, err := suite.router.RouteService(serviceName, "default", nil)
		if err != nil {
			b.Fatal(err)
		}
		
		// 5. 广播消息
		heartbeat := &protocol.HeartbeatPayload{
			Status: "active",
			Metrics: map[string]string{
				"service": serviceName,
			},
		}
		suite.gossipManager.BroadcastHeartbeat(heartbeat)
	}
}

// 基准测试：并发系统压力测试
func BenchmarkConcurrentSystemStress(b *testing.B) {
	suite, err := NewBenchmarkSuite()
	if err != nil {
		b.Fatal(err)
	}
	
	// 预创建一些服务和端点
	for i := 0; i < 100; i++ {
		serviceName := fmt.Sprintf("stress-service-%d", i)
		service := &types.Service{
			Metadata: types.ServiceMetadata{
				Name:      serviceName,
				Namespace: "default",
				Labels:    make(map[string]string),
			},
			Spec: types.ServiceSpec{
				Selector:     make(map[string]string),
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
				Ports: []types.ServicePort{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
				},
			},
		}
		suite.registry.RegisterService(service)
		
		serviceKey := serviceName + ".default"
		suite.registry.endpoints[serviceKey] = []*types.Endpoint{
			{
				ID:       fmt.Sprintf("stress-endpoint-%d", i),
				IP:       "127.0.0.1",
				Port:     8080 + i,
				Protocol: "tcp",
				Labels:   make(map[string]string),
				Status:   types.EndpointStatus{Ready: true},
				LastSeen: time.Now(),
			},
		}
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			serviceIndex := i % 100
			serviceName := fmt.Sprintf("stress-service-%d", serviceIndex)
			
			// 并发执行多种操作
			var wg sync.WaitGroup
			
			// 路由操作
			wg.Add(1)
			go func() {
				defer wg.Done()
				suite.router.RouteService(serviceName, "default", nil)
			}()
			
			// 健康检查操作
			wg.Add(1)
			go func() {
				defer wg.Done()
				endpointID := fmt.Sprintf("stress-endpoint-%d", serviceIndex)
				suite.healthChecker.IsHealthy(endpointID)
			}()
			
			// Gossip操作
			wg.Add(1)
			go func() {
				defer wg.Done()
				heartbeat := &protocol.HeartbeatPayload{
					Status: "active",
					Metrics: map[string]string{
						"service": serviceName,
					},
				}
				suite.gossipManager.BroadcastHeartbeat(heartbeat)
			}()
			
			wg.Wait()
			i++
		}
	})
}

// 性能结果报告
type PerformanceReport struct {
	TestName           string
	OperationsPerSec   float64
	AverageLatency     time.Duration
	MemoryUsage        int64
	ConcurrencyLevel   int
	ErrorRate          float64
}

// 生成性能报告
func GeneratePerformanceReport(b *testing.B, testName string) *PerformanceReport {
	return &PerformanceReport{
		TestName:         testName,
		OperationsPerSec: float64(b.N) / b.Elapsed().Seconds(),
		AverageLatency:   b.Elapsed() / time.Duration(b.N),
		// 这里可以添加更多的性能指标收集
	}
}

// 压力测试：资源限制测试
func BenchmarkResourceLimits(b *testing.B) {
	testCases := []struct {
		name      string
		services  int
		endpoints int
	}{
		{"Small", 100, 500},
		{"Medium", 500, 2500},
		{"Large", 1000, 5000},
		{"ExtraLarge", 2000, 10000},
	}
	
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			suite, err := NewBenchmarkSuite()
			if err != nil {
				b.Fatal(err)
			}
			
			// 创建测试数据
			for i := 0; i < tc.services; i++ {
				serviceName := fmt.Sprintf("resource-service-%d", i)
				service := &types.Service{
					Metadata: types.ServiceMetadata{
						Name:      serviceName,
						Namespace: "default",
						Labels:    make(map[string]string),
					},
					Spec: types.ServiceSpec{
						Selector:     make(map[string]string),
						ProxyMode:    "tcp",
						LoadBalancer: "round_robin",
						Ports: []types.ServicePort{
							{Name: "http", Port: 80, TargetPort: 8080, Protocol: "tcp"},
						},
					},
				}
				suite.registry.RegisterService(service)
				
				// 为每个服务创建多个端点
				endpointsPerService := tc.endpoints / tc.services
				endpoints := make([]*types.Endpoint, endpointsPerService)
				for j := 0; j < endpointsPerService; j++ {
					endpoints[j] = &types.Endpoint{
						ID:       fmt.Sprintf("resource-endpoint-%d-%d", i, j),
						IP:       "127.0.0.1",
						Port:     8080 + j,
						Protocol: "tcp",
						Labels:   make(map[string]string),
						Status:   types.EndpointStatus{Ready: true},
						LastSeen: time.Now(),
					}
				}
				serviceKey := serviceName + ".default"
				suite.registry.endpoints[serviceKey] = endpoints
			}
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				serviceIndex := i % tc.services
				serviceName := fmt.Sprintf("resource-service-%d", serviceIndex)
				
				_, err := suite.router.RouteService(serviceName, "default", nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}