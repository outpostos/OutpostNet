package loadbalancer

import (
	"fmt"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// 创建测试端点的辅助函数 (简化版，只使用Site)
func createTestEndpoint(id, site string, weight int) *types.Endpoint {
	return &types.Endpoint{
		ID:       id,
		IP:       "10.0.0." + id[len(id)-1:], // 简单的IP生成
		Port:     8080,
		Protocol: "tcp",
		Labels:   map[string]string{"weight": "medium"},
		Status:   types.EndpointStatus{Ready: true},
		LastSeen: time.Now(),
		Metadata: types.EndpointMetadata{
			Site:   site,
			Weight: weight,
		},
	}
}

func TestNewGeographicAwareLoadBalancer(t *testing.T) {
	clientLocation := &types.EndpointMetadata{
		Site: "site-1",
	}

	lb := NewGeographicAwareLoadBalancer(clientLocation)

	if lb == nil {
		t.Fatal("期望创建负载均衡器，但得到nil")
	}

	if lb.Name() != string(GeographicAware) {
		t.Errorf("期望负载均衡器名称为 %s，实际得到 %s", GeographicAware, lb.Name())
	}

	if lb.clientLocation != clientLocation {
		t.Error("客户端位置信息未正确设置")
	}
}

func TestGeographicAwareLoadBalancer_Select_EmptyEndpoints(t *testing.T) {
	lb := NewGeographicAwareLoadBalancer(nil)
	
	selected, err := lb.Select([]*types.Endpoint{})
	
	if err == nil {
		t.Error("期望返回错误，但得到nil")
	}
	
	if selected != nil {
		t.Error("期望返回nil端点，但得到非nil值")
	}
}

func TestGeographicAwareLoadBalancer_Select_SingleEndpoint(t *testing.T) {
	lb := NewGeographicAwareLoadBalancer(nil)
	
	endpoint := createTestEndpoint("ep1", "factory-site-1", 100)
	endpoints := []*types.Endpoint{endpoint}
	
	selected, err := lb.Select(endpoints)
	
	if err != nil {
		t.Fatalf("期望无错误，但得到: %v", err)
	}
	
	if selected != endpoint {
		t.Error("期望返回唯一的端点")
	}
}

func TestGeographicAwareLoadBalancer_IntraClusterSelection(t *testing.T) {
	// 设置客户端位置 (同一个Site内的某个节点)
	clientLocation := &types.EndpointMetadata{
		Site:   "factory-site-1",
		NodeID: "server-node-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 模拟 Requirement 3: Cluster内服务协同的场景
	// 同一个Site内有多个OutpostNet Server，每个Server有不同的endpoints
	endpoints := []*types.Endpoint{
		createTestEndpoint("ep1", "factory-site-1", 100), // 同一Site内的endpoint-1 
		createTestEndpoint("ep2", "factory-site-1", 80),  // 同一Site内的endpoint-2 (权重较低)
		createTestEndpoint("ep3", "factory-site-1", 120), // 同一Site内的endpoint-3 (权重较高)
	}

	// 多次选择，验证负载均衡在同一Cluster内的表现
	endpointSelections := make(map[string]int)
	for i := 0; i < 300; i++ {
		selected, err := lb.Select(endpoints)
		if err != nil {
			t.Fatalf("选择端点失败: %v", err)
		}
		endpointSelections[selected.ID]++
	}

	// 验证在同一Site内，负载均衡算法考虑权重和其他因素
	// ep3权重最高(120)，应该被选择更多次
	if endpointSelections["ep3"] < endpointSelections["ep2"] {
		t.Errorf("期望高权重端点被选择更多次: ep1=%d, ep2=%d, ep3=%d",
			endpointSelections["ep1"], endpointSelections["ep2"], endpointSelections["ep3"])
	}
}

func TestGeographicAwareLoadBalancer_NetworkTopologyAware(t *testing.T) {
	clientLocation := &types.EndpointMetadata{
		Site:   "factory-site-1",
		NodeID: "client-node-5",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 模拟同一Site内不同网络位置的endpoints (基于Site Hop Count概念)
	// 在真实场景中，即使在同一Site内，不同节点间也可能有网络距离差异
	endpoints := []*types.Endpoint{
		createTestEndpoint("ep1", "factory-site-1", 100), // 网络上较近的endpoint
		createTestEndpoint("ep2", "factory-site-1", 100), // 网络上较远的endpoint  
		createTestEndpoint("ep3", "factory-site-1", 100), // 网络上中等距离的endpoint
	}

	// 验证选择算法的基本功能 (所有endpoints都在同一Site)
	selected, err := lb.Select(endpoints)
	if err != nil {
		t.Fatalf("选择端点失败: %v", err)
	}
	
	if selected.Metadata.Site != "factory-site-1" {
		t.Errorf("期望选择同Site的端点，实际选择了: %s", selected.Metadata.Site)
	}
}

func TestGeographicAwareLoadBalancer_WeightCalculation(t *testing.T) {
	clientLocation := &types.EndpointMetadata{
		Site: "factory-site-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 创建权重不同的端点
	endpoints := []*types.Endpoint{
		createTestEndpoint("ep1", "factory-site-1", 100), // 高权重
		createTestEndpoint("ep2", "factory-site-1", 50),  // 低权重
	}

	// 计算权重
	weightedEndpoints := lb.calculateEndpointWeights(endpoints)
	
	if len(weightedEndpoints) != 2 {
		t.Fatalf("期望2个加权端点，实际得到%d个", len(weightedEndpoints))
	}

	// 验证高权重端点的权重确实更高
	ep1Weight := weightedEndpoints[0].Weight
	ep2Weight := weightedEndpoints[1].Weight
	
	if weightedEndpoints[0].Endpoint.Metadata.Weight == 100 {
		if ep1Weight <= ep2Weight {
			t.Error("期望高权重端点的综合权重更高")
		}
	} else {
		if ep2Weight <= ep1Weight {
			t.Error("期望高权重端点的综合权重更高")
		}
	}
}

func TestGeographicAwareLoadBalancer_SiteDistanceSorting(t *testing.T) {
	clientLocation := &types.EndpointMetadata{
		Site: "factory-site-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 创建不同距离的端点
	endpoints := []*types.Endpoint{
		createTestEndpoint("ep1", "remote-site-3", 100), // 远程
		createTestEndpoint("ep2", "factory-site-1", 100), // 本地
		createTestEndpoint("ep3", "nearby-site-2", 100), // 中等距离
	}

	// 计算权重并排序
	weightedEndpoints := lb.calculateEndpointWeights(endpoints)
	lb.sortEndpointsBySiteDistance(weightedEndpoints)

	// 验证排序结果：本地端点应该排在第一位
	if weightedEndpoints[0].Endpoint.Metadata.Site != "factory-site-1" {
		t.Errorf("期望本地端点排在第一位，实际第一位是: %s", 
			weightedEndpoints[0].Endpoint.Metadata.Site)
	}
}

func TestGeographicAwareLoadBalancer_UpdateStats(t *testing.T) {
	lb := NewGeographicAwareLoadBalancer(nil)
	
	endpoint := createTestEndpoint("ep1", "factory-site-1", 100)
	
	// 更新统计信息
	lb.UpdateStats(endpoint, 50*time.Millisecond, true)
	
	// 获取统计信息
	stats := lb.GetEndpointStats()
	
	if len(stats) != 1 {
		t.Fatalf("期望1个端点统计，实际得到%d个", len(stats))
	}
	
	epStats, exists := stats[endpoint.ID]
	if !exists {
		t.Fatal("未找到端点统计信息")
	}
	
	if epStats.SuccessCount != 1 {
		t.Errorf("期望成功次数为1，实际为%d", epStats.SuccessCount)
	}
	
	if epStats.FailureCount != 0 {
		t.Errorf("期望失败次数为0，实际为%d", epStats.FailureCount)
	}
}

func TestGeographicAwareLoadBalancer_SetClientLocation(t *testing.T) {
	lb := NewGeographicAwareLoadBalancer(nil)
	
	newLocation := &types.EndpointMetadata{
		Site: "new-factory-site",
	}
	
	lb.SetClientLocation(newLocation)
	
	if lb.clientLocation != newLocation {
		t.Error("客户端位置未正确更新")
	}
}

func TestGeographicAwareLoadBalancer_SelectWithSitePreference(t *testing.T) {
	clientLocation := &types.EndpointMetadata{
		Site: "factory-site-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 模拟现实场景：不同Site的端点，这时SelectWithSitePreference应该强制优先本地
	endpoints := []*types.Endpoint{
		createTestEndpoint("ep1", "factory-site-1", 100), // 同Site，权重较低
		createTestEndpoint("ep2", "remote-site-2", 200), // 不同Site，更高权重
		createTestEndpoint("ep3", "distant-site-3", 300), // 远程Site，最高权重
	}

	// 使用Site优先选择算法
	selected := lb.SelectWithSitePreference(endpoints, "test-key")
	
	// 应该优先选择同Site的端点，即使权重较低
	if selected.Metadata.Site != "factory-site-1" {
		t.Errorf("期望选择同Site端点 factory-site-1，实际选择了 %s", selected.Metadata.Site)
	}
}

func TestGeographicAwareLoadBalancer_DistanceCalculation(t *testing.T) {
	clientLocation := &types.EndpointMetadata{
		Site: "factory-site-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 测试同Site端点
	sameSiteEndpoint := createTestEndpoint("ep1", "factory-site-1", 100)
	distance1 := lb.calculateEndpointDistance(sameSiteEndpoint)
	
	// 测试不同Site端点
	diffSiteEndpoint := createTestEndpoint("ep2", "remote-site-2", 100)
	distance2 := lb.calculateEndpointDistance(diffSiteEndpoint)

	// 验证同Site距离为0
	if distance1.SiteDistance != 0 {
		t.Errorf("期望同Site距离为0，实际为%d", distance1.SiteDistance)
	}

	// 验证不同Site距离大于0
	if distance2.SiteDistance <= 0 {
		t.Errorf("期望不同Site距离大于0，实际为%d", distance2.SiteDistance)
	}

	// 验证延迟估算
	if distance1.NetworkLatency >= distance2.NetworkLatency {
		t.Error("期望同Site延迟小于跨Site延迟")
	}
}

func TestGeographicAwareLoadBalancer_PerformanceWeighting(t *testing.T) {
	lb := NewGeographicAwareLoadBalancer(nil)
	
	endpoint := createTestEndpoint("ep1", "factory-site-1", 100)
	
	// 模拟多次成功和失败
	lb.UpdateStats(endpoint, 20*time.Millisecond, true)  // 成功
	lb.UpdateStats(endpoint, 30*time.Millisecond, true)  // 成功
	lb.UpdateStats(endpoint, 100*time.Millisecond, false) // 失败

	// 计算性能权重
	performanceWeight := lb.calculatePerformanceWeight(endpoint.ID)
	
	// 验证性能权重在合理范围内
	if performanceWeight < 0 || performanceWeight > 1 {
		t.Errorf("性能权重应该在0-1之间，实际为%f", performanceWeight)
	}

	// 成功率应该是2/3
	expectedSuccessRate := 2.0 / 3.0
	stats := lb.GetEndpointStats()[endpoint.ID]
	actualSuccessRate := float64(stats.SuccessCount) / float64(stats.SelectCount)
	
	if actualSuccessRate != expectedSuccessRate {
		t.Errorf("期望成功率为%f，实际为%f", expectedSuccessRate, actualSuccessRate)
	}
}

// 基准测试
func BenchmarkGeographicAwareLoadBalancer_Select(b *testing.B) {
	clientLocation := &types.EndpointMetadata{
		Site: "factory-site-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	// 创建大量端点进行基准测试
	endpoints := make([]*types.Endpoint, 100)
	for i := 0; i < 100; i++ {
		endpoints[i] = createTestEndpoint(
			fmt.Sprintf("ep%d", i),
			fmt.Sprintf("site-%d", i%10),
			100+i%50,
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := lb.Select(endpoints)
		if err != nil {
			b.Fatalf("选择端点失败: %v", err)
		}
	}
}

func BenchmarkGeographicAwareLoadBalancer_SelectWithSitePreference(b *testing.B) {
	clientLocation := &types.EndpointMetadata{
		Site: "factory-site-1",
	}
	
	lb := NewGeographicAwareLoadBalancer(clientLocation)

	endpoints := make([]*types.Endpoint, 100)
	for i := 0; i < 100; i++ {
		endpoints[i] = createTestEndpoint(
			fmt.Sprintf("ep%d", i),
			fmt.Sprintf("site-%d", i%10),
			100+i%50,
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected := lb.SelectWithSitePreference(endpoints, "test-key")
		if selected == nil {
			b.Fatal("选择端点失败")
		}
	}
}