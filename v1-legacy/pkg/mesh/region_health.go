package mesh

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegionHealthChecker 区域健康检查器 (Phase 2)
// 负责监控区域内节点和服务的健康状态
type RegionHealthChecker struct {
	networkManager *RegionNetworkManager
	registry       *RegionServiceRegistry
	
	// 健康检查配置
	checkInterval    time.Duration
	timeoutDuration  time.Duration
	maxFailures      int
	recoveryThreshold int
	
	// 节点健康状态
	nodeHealth       map[string]*NodeHealthInfo
	nodeHealthMu     sync.RWMutex
	
	// 服务健康状态
	serviceHealth    map[string]*ServiceHealthInfo
	serviceHealthMu  sync.RWMutex
	
	// 故障检测
	failureDetector  *FailureDetector
	
	// 控制
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	
	// 事件通知
	eventHandlers    []HealthEventHandler
	eventHandlersMu  sync.RWMutex
}

// NodeHealthInfo 节点健康信息
type NodeHealthInfo struct {
	NodeID           string
	Status           string // "healthy", "degraded", "unhealthy", "unknown"
	LastCheckTime    time.Time
	LastResponseTime time.Time
	RTT              time.Duration
	FailureCount     int
	ConsecutiveFailures int
	RecoveryCount    int
	HealthScore      float64 // 0-100
	Metrics          map[string]string
	LastError        string
}

// ServiceHealthInfo 服务健康信息
type ServiceHealthInfo struct {
	ServiceName      string
	ServiceNamespace string
	Status           string // "healthy", "degraded", "unhealthy", "unknown"
	HealthyEndpoints int
	TotalEndpoints   int
	FailedEndpoints  map[string]*EndpointFailureInfo
	LastCheckTime    time.Time
	HealthScore      float64 // 0-100
	RecoveryActions  []string
}

// EndpointFailureInfo 端点故障信息
type EndpointFailureInfo struct {
	EndpointID      string
	FailureTime     time.Time
	FailureReason   string
	FailureCount    int
	LastRecoveryAttempt time.Time
	RecoveryAttempts    int
	Severity        string // "low", "medium", "high", "critical"
}

// HealthEventHandler 健康事件处理器
type HealthEventHandler interface {
	OnNodeHealthChanged(nodeID string, oldStatus, newStatus string, healthInfo *NodeHealthInfo)
	OnServiceHealthChanged(serviceName, namespace string, oldStatus, newStatus string, healthInfo *ServiceHealthInfo)
	OnEndpointFailure(endpointID string, failureInfo *EndpointFailureInfo)
	OnEndpointRecovery(endpointID string, recoveryTime time.Time)
}

// FailureDetector φ (phi) 故障检测器
type FailureDetector struct {
	// φ 故障检测器参数
	phiThreshold     float64
	windowSize       int
	minSamples       int
	
	// 节点采样数据
	samples          map[string]*HeartbeatSamples
	samplesMu        sync.RWMutex
}

// HeartbeatSamples 心跳采样数据
type HeartbeatSamples struct {
	intervals        []time.Duration
	lastHeartbeat    time.Time
	mean             float64
	variance         float64
	sampleCount      int
}

// NewRegionHealthChecker 创建区域健康检查器
func NewRegionHealthChecker(networkManager *RegionNetworkManager) *RegionHealthChecker {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RegionHealthChecker{
		networkManager:    networkManager,
		checkInterval:     30 * time.Second,
		timeoutDuration:   10 * time.Second,
		maxFailures:       3,
		recoveryThreshold: 2,
		nodeHealth:        make(map[string]*NodeHealthInfo),
		serviceHealth:     make(map[string]*ServiceHealthInfo),
		failureDetector:   NewFailureDetector(),
		ctx:               ctx,
		cancel:            cancel,
		eventHandlers:     make([]HealthEventHandler, 0),
	}
}

// Start 启动健康检查器
func (h *RegionHealthChecker) Start() {
	h.wg.Add(1)
	go h.runHealthChecks()
	
	h.wg.Add(1)
	go h.runServiceHealthMonitoring()
	
	h.wg.Add(1)
	go h.runFailureDetection()
}

// Stop 停止健康检查器
func (h *RegionHealthChecker) Stop() {
	h.cancel()
	h.wg.Wait()
}

// AddEventHandler 添加事件处理器
func (h *RegionHealthChecker) AddEventHandler(handler HealthEventHandler) {
	h.eventHandlersMu.Lock()
	defer h.eventHandlersMu.Unlock()
	h.eventHandlers = append(h.eventHandlers, handler)
}

// GetNodeHealth 获取节点健康信息
func (h *RegionHealthChecker) GetNodeHealth(nodeID string) *NodeHealthInfo {
	h.nodeHealthMu.RLock()
	defer h.nodeHealthMu.RUnlock()
	
	if health, exists := h.nodeHealth[nodeID]; exists {
		// 返回副本
		healthCopy := *health
		return &healthCopy
	}
	
	return nil
}

// GetServiceHealth 获取服务健康信息
func (h *RegionHealthChecker) GetServiceHealth(serviceName, namespace string) *ServiceHealthInfo {
	h.serviceHealthMu.RLock()
	defer h.serviceHealthMu.RUnlock()
	
	key := fmt.Sprintf("%s.%s", serviceName, namespace)
	if health, exists := h.serviceHealth[key]; exists {
		// 返回副本
		healthCopy := *health
		return &healthCopy
	}
	
	return nil
}

// GetHealthyNodes 获取健康的节点列表
func (h *RegionHealthChecker) GetHealthyNodes() []string {
	h.nodeHealthMu.RLock()
	defer h.nodeHealthMu.RUnlock()
	
	var healthyNodes []string
	for nodeID, health := range h.nodeHealth {
		if health.Status == "healthy" {
			healthyNodes = append(healthyNodes, nodeID)
		}
	}
	
	return healthyNodes
}

// MarkEndpointUnhealthy 标记端点为不健康
func (h *RegionHealthChecker) MarkEndpointUnhealthy(endpointID, reason, severity string) {
	h.serviceHealthMu.Lock()
	defer h.serviceHealthMu.Unlock()
	
	// 创建故障信息
	failureInfo := &EndpointFailureInfo{
		EndpointID:    endpointID,
		FailureTime:   time.Now(),
		FailureReason: reason,
		FailureCount:  1,
		Severity:      severity,
	}
	
	// 查找对应的服务
	for serviceKey, serviceHealth := range h.serviceHealth {
		if serviceHealth.FailedEndpoints == nil {
			serviceHealth.FailedEndpoints = make(map[string]*EndpointFailureInfo)
		}
		
		// 如果已存在，更新故障计数
		if existing, exists := serviceHealth.FailedEndpoints[endpointID]; exists {
			existing.FailureCount++
			existing.FailureTime = time.Now()
			existing.FailureReason = reason
		} else {
			serviceHealth.FailedEndpoints[endpointID] = failureInfo
		}
		
		// 重新计算服务健康状态
		h.recalculateServiceHealth(serviceKey, serviceHealth)
		
		// 通知事件处理器
		h.notifyEndpointFailure(endpointID, failureInfo)
		break
	}
}

// RecoverEndpoint 恢复端点
func (h *RegionHealthChecker) RecoverEndpoint(endpointID string) {
	h.serviceHealthMu.Lock()
	defer h.serviceHealthMu.Unlock()
	
	recoveryTime := time.Now()
	
	// 从所有服务的故障端点中移除
	for serviceKey, serviceHealth := range h.serviceHealth {
		if serviceHealth.FailedEndpoints != nil {
			if _, exists := serviceHealth.FailedEndpoints[endpointID]; exists {
				delete(serviceHealth.FailedEndpoints, endpointID)
				
				// 重新计算服务健康状态
				h.recalculateServiceHealth(serviceKey, serviceHealth)
				
				// 通知事件处理器
				h.notifyEndpointRecovery(endpointID, recoveryTime)
				break
			}
		}
	}
}

// runHealthChecks 运行节点健康检查
func (h *RegionHealthChecker) runHealthChecks() {
	defer h.wg.Done()
	
	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.performNodeHealthChecks()
		}
	}
}

// performNodeHealthChecks 执行节点健康检查
func (h *RegionHealthChecker) performNodeHealthChecks() {
	connectedNodes := h.networkManager.GetConnectedNodes()
	
	for _, nodeID := range connectedNodes {
		h.wg.Add(1)
		go h.checkNodeHealth(nodeID)
	}
}

// checkNodeHealth 检查单个节点健康状态
func (h *RegionHealthChecker) checkNodeHealth(nodeID string) {
	defer h.wg.Done()
	
	// 获取或创建健康信息
	h.nodeHealthMu.Lock()
	healthInfo, exists := h.nodeHealth[nodeID]
	if !exists {
		healthInfo = &NodeHealthInfo{
			NodeID:  nodeID,
			Status:  "unknown",
			Metrics: make(map[string]string),
		}
		h.nodeHealth[nodeID] = healthInfo
	}
	h.nodeHealthMu.Unlock()
	
	// 执行健康检查
	checkStart := time.Now()
	healthInfo.LastCheckTime = checkStart
	
	// 创建健康检查请求
	check := &protocol.NodeHealthCheck{
		NodeId:       nodeID,
		CheckTime:    timestamppb.New(checkStart),
		HealthMetrics: map[string]string{
			"checker": h.networkManager.localNodeID,
		},
	}
	
	// 发送健康检查
	response, err := h.networkManager.SendHealthCheck(nodeID, check)
	
	oldStatus := healthInfo.Status
	
	if err != nil {
		// 健康检查失败
		h.handleHealthCheckFailure(healthInfo, err.Error())
	} else {
		// 健康检查成功
		h.handleHealthCheckSuccess(healthInfo, response, time.Since(checkStart))
	}
	
	// 如果状态发生变化，通知事件处理器
	if oldStatus != healthInfo.Status {
		h.notifyNodeHealthChanged(nodeID, oldStatus, healthInfo.Status, healthInfo)
	}
	
	// 更新φ故障检测器
	h.failureDetector.RecordHeartbeat(nodeID, time.Now())
}

// handleHealthCheckFailure 处理健康检查失败
func (h *RegionHealthChecker) handleHealthCheckFailure(healthInfo *NodeHealthInfo, errorMsg string) {
	healthInfo.FailureCount++
	healthInfo.ConsecutiveFailures++
	healthInfo.LastError = errorMsg
	
	// 根据连续失败次数确定状态
	if healthInfo.ConsecutiveFailures >= h.maxFailures {
		healthInfo.Status = "unhealthy"
		healthInfo.HealthScore = 0
	} else if healthInfo.ConsecutiveFailures >= h.maxFailures/2 {
		healthInfo.Status = "degraded"
		healthInfo.HealthScore = 50 - float64(healthInfo.ConsecutiveFailures)*10
	}
	
	if healthInfo.HealthScore < 0 {
		healthInfo.HealthScore = 0
	}
}

// handleHealthCheckSuccess 处理健康检查成功
func (h *RegionHealthChecker) handleHealthCheckSuccess(
	healthInfo *NodeHealthInfo,
	response *protocol.NodeHealthResponse,
	rtt time.Duration,
) {
	healthInfo.LastResponseTime = time.Now()
	healthInfo.RTT = rtt
	healthInfo.ConsecutiveFailures = 0
	healthInfo.RecoveryCount++
	healthInfo.LastError = ""
	
	// 更新指标
	for key, value := range response.Metrics {
		healthInfo.Metrics[key] = value
	}
	
	// 计算健康分数
	baseScore := 100.0
	if rtt > 1*time.Second {
		baseScore -= 20 // RTT过高扣分
	}
	if healthInfo.FailureCount > 0 {
		baseScore -= float64(healthInfo.FailureCount) * 5 // 历史失败扣分
	}
	
	healthInfo.HealthScore = baseScore
	if healthInfo.HealthScore > 100 {
		healthInfo.HealthScore = 100
	}
	
	// 确定状态
	if healthInfo.HealthScore >= 80 {
		healthInfo.Status = "healthy"
	} else if healthInfo.HealthScore >= 50 {
		healthInfo.Status = "degraded"
	} else {
		healthInfo.Status = "unhealthy"
	}
}

// runServiceHealthMonitoring 运行服务健康监控
func (h *RegionHealthChecker) runServiceHealthMonitoring() {
	defer h.wg.Done()
	
	ticker := time.NewTicker(60 * time.Second) // 服务健康检查间隔较长
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.monitorServiceHealth()
		}
	}
}

// monitorServiceHealth 监控服务健康状态
func (h *RegionHealthChecker) monitorServiceHealth() {
	if h.registry == nil {
		return
	}
	
	// 获取所有服务
	services, err := h.registry.ListServices("")
	if err != nil {
		return
	}
	
	for _, service := range services {
		h.checkServiceHealth(service)
	}
}

// checkServiceHealth 检查服务健康状态
func (h *RegionHealthChecker) checkServiceHealth(service *types.Service) {
	serviceKey := fmt.Sprintf("%s.%s", service.Metadata.Name, service.Metadata.Namespace)
	
	// 获取服务端点
	endpoints, err := h.registry.GetEndpoints(
		service.Metadata.Name,
		service.Metadata.Namespace,
		nil, // 获取所有端点
	)
	if err != nil {
		return
	}
	
	h.serviceHealthMu.Lock()
	defer h.serviceHealthMu.Unlock()
	
	// 获取或创建服务健康信息
	healthInfo, exists := h.serviceHealth[serviceKey]
	if !exists {
		healthInfo = &ServiceHealthInfo{
			ServiceName:      service.Metadata.Name,
			ServiceNamespace: service.Metadata.Namespace,
			Status:           "unknown",
			FailedEndpoints:  make(map[string]*EndpointFailureInfo),
		}
		h.serviceHealth[serviceKey] = healthInfo
	}
	
	oldStatus := healthInfo.Status
	
	// 更新端点统计
	healthInfo.TotalEndpoints = len(endpoints)
	healthInfo.LastCheckTime = time.Now()
	
	// 计算健康端点数量
	healthyCount := 0
	for _, endpoint := range endpoints {
		if endpoint.Status.Ready {
			healthyCount++
		}
	}
	healthInfo.HealthyEndpoints = healthyCount
	
	// 重新计算健康状态
	h.recalculateServiceHealth(serviceKey, healthInfo)
	
	// 如果状态发生变化，通知事件处理器
	if oldStatus != healthInfo.Status {
		h.notifyServiceHealthChanged(
			service.Metadata.Name,
			service.Metadata.Namespace,
			oldStatus,
			healthInfo.Status,
			healthInfo,
		)
	}
}

// recalculateServiceHealth 重新计算服务健康状态
func (h *RegionHealthChecker) recalculateServiceHealth(serviceKey string, healthInfo *ServiceHealthInfo) {
	if healthInfo.TotalEndpoints == 0 {
		healthInfo.Status = "unknown"
		healthInfo.HealthScore = 0
		return
	}
	
	// 计算健康比例
	healthRatio := float64(healthInfo.HealthyEndpoints) / float64(healthInfo.TotalEndpoints)
	healthInfo.HealthScore = healthRatio * 100
	
	// 确定状态
	if healthRatio >= 0.8 {
		healthInfo.Status = "healthy"
	} else if healthRatio >= 0.5 {
		healthInfo.Status = "degraded"
	} else if healthRatio > 0 {
		healthInfo.Status = "unhealthy"
	} else {
		healthInfo.Status = "critical"
	}
	
	// 检查是否有关键端点故障
	criticalFailures := 0
	for _, failure := range healthInfo.FailedEndpoints {
		if failure.Severity == "critical" {
			criticalFailures++
		}
	}
	
	if criticalFailures > 0 && healthInfo.Status != "critical" {
		healthInfo.Status = "degraded" // 有关键故障时至少为降级状态
	}
}

// runFailureDetection 运行φ故障检测
func (h *RegionHealthChecker) runFailureDetection() {
	defer h.wg.Done()
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.detectFailures()
		}
	}
}

// detectFailures 检测故障
func (h *RegionHealthChecker) detectFailures() {
	connectedNodes := h.networkManager.GetConnectedNodes()
	
	for _, nodeID := range connectedNodes {
		phi := h.failureDetector.CalculatePhi(nodeID)
		if phi > h.failureDetector.phiThreshold {
			// 节点可能出现故障
			h.handleSuspectedFailure(nodeID, phi)
		}
	}
}

// handleSuspectedFailure 处理疑似故障
func (h *RegionHealthChecker) handleSuspectedFailure(nodeID string, phi float64) {
	h.nodeHealthMu.Lock()
	defer h.nodeHealthMu.Unlock()
	
	if healthInfo, exists := h.nodeHealth[nodeID]; exists {
		oldStatus := healthInfo.Status
		
		// 根据φ值确定状态
		if phi > 8.0 {
			healthInfo.Status = "unhealthy"
		} else if phi > 5.0 {
			healthInfo.Status = "degraded"
		}
		
		// 记录φ值
		healthInfo.Metrics["phi"] = fmt.Sprintf("%.2f", phi)
		
		// 如果状态发生变化，通知事件处理器
		if oldStatus != healthInfo.Status {
			h.notifyNodeHealthChanged(nodeID, oldStatus, healthInfo.Status, healthInfo)
		}
	}
}

// 事件通知方法

// notifyNodeHealthChanged 通知节点健康状态变化
func (h *RegionHealthChecker) notifyNodeHealthChanged(nodeID, oldStatus, newStatus string, healthInfo *NodeHealthInfo) {
	h.eventHandlersMu.RLock()
	defer h.eventHandlersMu.RUnlock()
	
	for _, handler := range h.eventHandlers {
		go handler.OnNodeHealthChanged(nodeID, oldStatus, newStatus, healthInfo)
	}
}

// notifyServiceHealthChanged 通知服务健康状态变化
func (h *RegionHealthChecker) notifyServiceHealthChanged(serviceName, namespace, oldStatus, newStatus string, healthInfo *ServiceHealthInfo) {
	h.eventHandlersMu.RLock()
	defer h.eventHandlersMu.RUnlock()
	
	for _, handler := range h.eventHandlers {
		go handler.OnServiceHealthChanged(serviceName, namespace, oldStatus, newStatus, healthInfo)
	}
}

// notifyEndpointFailure 通知端点故障
func (h *RegionHealthChecker) notifyEndpointFailure(endpointID string, failureInfo *EndpointFailureInfo) {
	h.eventHandlersMu.RLock()
	defer h.eventHandlersMu.RUnlock()
	
	for _, handler := range h.eventHandlers {
		go handler.OnEndpointFailure(endpointID, failureInfo)
	}
}

// notifyEndpointRecovery 通知端点恢复
func (h *RegionHealthChecker) notifyEndpointRecovery(endpointID string, recoveryTime time.Time) {
	h.eventHandlersMu.RLock()
	defer h.eventHandlersMu.RUnlock()
	
	for _, handler := range h.eventHandlers {
		go handler.OnEndpointRecovery(endpointID, recoveryTime)
	}
}

// FailureDetector 实现

// NewFailureDetector 创建故障检测器
func NewFailureDetector() *FailureDetector {
	return &FailureDetector{
		phiThreshold: 8.0,  // φ阈值
		windowSize:   100,  // 采样窗口大小
		minSamples:   10,   // 最小采样数量
		samples:      make(map[string]*HeartbeatSamples),
	}
}

// RecordHeartbeat 记录心跳
func (fd *FailureDetector) RecordHeartbeat(nodeID string, timestamp time.Time) {
	fd.samplesMu.Lock()
	defer fd.samplesMu.Unlock()
	
	samples, exists := fd.samples[nodeID]
	if !exists {
		samples = &HeartbeatSamples{
			intervals: make([]time.Duration, 0, fd.windowSize),
		}
		fd.samples[nodeID] = samples
	}
	
	// 计算间隔
	if !samples.lastHeartbeat.IsZero() {
		interval := timestamp.Sub(samples.lastHeartbeat)
		samples.intervals = append(samples.intervals, interval)
		
		// 保持窗口大小
		if len(samples.intervals) > fd.windowSize {
			samples.intervals = samples.intervals[1:]
		}
		
		// 更新统计信息
		samples.updateStats()
	}
	
	samples.lastHeartbeat = timestamp
	samples.sampleCount++
}

// CalculatePhi 计算φ值
func (fd *FailureDetector) CalculatePhi(nodeID string) float64 {
	fd.samplesMu.RLock()
	defer fd.samplesMu.RUnlock()
	
	samples, exists := fd.samples[nodeID]
	if !exists || len(samples.intervals) < fd.minSamples {
		return 0.0
	}
	
	// 计算当前时间距离上次心跳的时间
	timeSinceLastHeartbeat := time.Since(samples.lastHeartbeat)
	
	// 计算φ值
	if samples.variance <= 0 {
		return 0.0
	}
	
	// φ = -log10(P(t > timeSinceLastHeartbeat))
	// 使用正态分布近似
	std := samples.variance
	z := (float64(timeSinceLastHeartbeat) - samples.mean) / std
	
	// 简化的φ计算
	phi := z * z / 2.0
	if phi < 0 {
		phi = 0
	}
	
	return phi
}

// updateStats 更新统计信息
func (s *HeartbeatSamples) updateStats() {
	if len(s.intervals) == 0 {
		return
	}
	
	// 计算平均值
	sum := float64(0)
	for _, interval := range s.intervals {
		sum += float64(interval)
	}
	s.mean = sum / float64(len(s.intervals))
	
	// 计算方差
	varianceSum := float64(0)
	for _, interval := range s.intervals {
		diff := float64(interval) - s.mean
		varianceSum += diff * diff
	}
	s.variance = varianceSum / float64(len(s.intervals))
}