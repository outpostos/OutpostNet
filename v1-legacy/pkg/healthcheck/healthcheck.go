package healthcheck

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// HealthCheckStatus 健康检查状态
type HealthCheckStatus string

const (
	StatusHealthy    HealthCheckStatus = "healthy"
	StatusUnhealthy  HealthCheckStatus = "unhealthy"
	StatusUnknown    HealthCheckStatus = "unknown"
)

// HealthCheckResult 健康检查结果
type HealthCheckResult struct {
	Endpoint    *types.Endpoint
	Status      HealthCheckStatus
	Latency     time.Duration
	Error       error
	CheckedAt   time.Time
	ConsecutiveFails int
}

// HealthChecker 健康检查器接口
type HealthChecker interface {
	// CheckHealth 检查单个端点的健康状态
	CheckHealth(ctx context.Context, endpoint *types.Endpoint) *HealthCheckResult
	// Name 返回检查器名称
	Name() string
}

// TCPHealthChecker TCP 健康检查器
type TCPHealthChecker struct {
	timeout time.Duration
}

// NewTCPHealthChecker 创建 TCP 健康检查器
func NewTCPHealthChecker(timeout time.Duration) *TCPHealthChecker {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &TCPHealthChecker{timeout: timeout}
}

func (hc *TCPHealthChecker) Name() string {
	return "tcp"
}

func (hc *TCPHealthChecker) CheckHealth(ctx context.Context, endpoint *types.Endpoint) *HealthCheckResult {
	start := time.Now()
	result := &HealthCheckResult{
		Endpoint:  endpoint,
		Status:    StatusUnknown,
		CheckedAt: start,
	}

	// 建立 TCP 连接测试
	addr := net.JoinHostPort(endpoint.IP, fmt.Sprintf("%d", endpoint.Port))
	dialer := &net.Dialer{Timeout: hc.timeout}
	
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		result.Status = StatusUnhealthy
		result.Error = err
		result.Latency = time.Since(start)
		return result
	}
	
	conn.Close()
	result.Status = StatusHealthy
	result.Latency = time.Since(start)
	return result
}

// UDPHealthChecker UDP 健康检查器
type UDPHealthChecker struct {
	timeout time.Duration
}

// NewUDPHealthChecker 创建 UDP 健康检查器
func NewUDPHealthChecker(timeout time.Duration) *UDPHealthChecker {
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	return &UDPHealthChecker{timeout: timeout}
}

func (hc *UDPHealthChecker) Name() string {
	return "udp"
}

func (hc *UDPHealthChecker) CheckHealth(ctx context.Context, endpoint *types.Endpoint) *HealthCheckResult {
	start := time.Now()
	result := &HealthCheckResult{
		Endpoint:  endpoint,
		Status:    StatusUnknown,
		CheckedAt: start,
	}

	// UDP 连接测试（发送一个空包）
	addr := net.JoinHostPort(endpoint.IP, fmt.Sprintf("%d", endpoint.Port))
	conn, err := net.DialTimeout("udp", addr, hc.timeout)
	if err != nil {
		result.Status = StatusUnhealthy
		result.Error = err
		result.Latency = time.Since(start)
		return result
	}
	defer conn.Close()

	// 发送一个空的 UDP 包
	_, err = conn.Write([]byte{})
	if err != nil {
		result.Status = StatusUnhealthy
		result.Error = err
		result.Latency = time.Since(start)
		return result
	}

	result.Status = StatusHealthy
	result.Latency = time.Since(start)
	return result
}

// HealthCheckManager 健康检查管理器
type HealthCheckManager struct {
	checkers map[string]HealthChecker
	results  map[string]*HealthCheckResult // endpoint ID -> result
	mu       sync.RWMutex

	// 配置
	checkInterval    time.Duration
	unhealthyThreshold int // 连续失败多少次标记为不健康
	healthyThreshold   int // 连续成功多少次标记为健康

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewHealthCheckManager 创建健康检查管理器
func NewHealthCheckManager() *HealthCheckManager {
	return &HealthCheckManager{
		checkers:           make(map[string]HealthChecker),
		results:            make(map[string]*HealthCheckResult),
		checkInterval:      30 * time.Second,
		unhealthyThreshold: 3,
		healthyThreshold:   2,
	}
}

// SetCheckInterval 设置检查间隔
func (hm *HealthCheckManager) SetCheckInterval(interval time.Duration) {
	hm.checkInterval = interval
}

// SetThresholds 设置健康检查阈值
func (hm *HealthCheckManager) SetThresholds(unhealthy, healthy int) {
	hm.unhealthyThreshold = unhealthy
	hm.healthyThreshold = healthy
}

// RegisterChecker 注册健康检查器
func (hm *HealthCheckManager) RegisterChecker(protocol string, checker HealthChecker) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.checkers[protocol] = checker
}

// Start 启动健康检查
func (hm *HealthCheckManager) Start(ctx context.Context) {
	hm.ctx, hm.cancel = context.WithCancel(ctx)
	
	// 默认注册检查器
	if len(hm.checkers) == 0 {
		hm.checkers["tcp"] = NewTCPHealthChecker(5 * time.Second)
		hm.checkers["udp"] = NewUDPHealthChecker(3 * time.Second)
	}

	hm.wg.Add(1)
	go hm.healthCheckLoop()
}

// Stop 停止健康检查
func (hm *HealthCheckManager) Stop() {
	if hm.cancel != nil {
		hm.cancel()
	}
	hm.wg.Wait()
}

// AddEndpoint 添加端点进行健康检查
func (hm *HealthCheckManager) AddEndpoint(endpoint *types.Endpoint) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	
	if _, exists := hm.results[endpoint.ID]; !exists {
		hm.results[endpoint.ID] = &HealthCheckResult{
			Endpoint:  endpoint,
			Status:    StatusUnknown,
			CheckedAt: time.Now(),
		}
	}
}

// RemoveEndpoint 移除端点
func (hm *HealthCheckManager) RemoveEndpoint(endpointID string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.results, endpointID)
}

// UpdateHealth 手动更新端点健康状态 (用于测试)
func (hm *HealthCheckManager) UpdateHealth(endpointID string, healthy bool) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	
	if result, exists := hm.results[endpointID]; exists {
		if healthy {
			result.Status = StatusHealthy
		} else {
			result.Status = StatusUnhealthy
		}
		result.CheckedAt = time.Now()
	}
}

// GetHealth 获取端点健康状态
func (hm *HealthCheckManager) GetHealth(endpointID string) *HealthCheckResult {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	
	if result, exists := hm.results[endpointID]; exists {
		// 返回结果的副本，避免并发问题
		resultCopy := *result
		return &resultCopy
	}
	return nil
}

// GetHealthyEndpoints 获取健康的端点
func (hm *HealthCheckManager) GetHealthyEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var healthy []*types.Endpoint
	for _, endpoint := range endpoints {
		if result, exists := hm.results[endpoint.ID]; exists {
			if result.Status == StatusHealthy {
				healthy = append(healthy, endpoint)
			}
		} else {
			// 如果没有健康检查结果，默认认为是健康的
			healthy = append(healthy, endpoint)
		}
	}
	return healthy
}

// IsHealthy 检查端点是否健康
func (hm *HealthCheckManager) IsHealthy(endpointID string) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	
	if result, exists := hm.results[endpointID]; exists {
		return result.Status == StatusHealthy
	}
	return true // 默认健康
}

// GetStats 获取健康检查统计信息
func (hm *HealthCheckManager) GetStats() map[string]interface{} {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	stats := make(map[string]interface{})
	var healthy, unhealthy, unknown int

	for _, result := range hm.results {
		switch result.Status {
		case StatusHealthy:
			healthy++
		case StatusUnhealthy:
			unhealthy++
		case StatusUnknown:
			unknown++
		}
	}

	stats["total_endpoints"] = len(hm.results)
	stats["healthy"] = healthy
	stats["unhealthy"] = unhealthy
	stats["unknown"] = unknown
	stats["check_interval"] = hm.checkInterval.String()

	return stats
}

// healthCheckLoop 健康检查循环
func (hm *HealthCheckManager) healthCheckLoop() {
	defer hm.wg.Done()
	
	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-ticker.C:
			hm.performHealthChecks()
		}
	}
}

// performHealthChecks 执行健康检查
func (hm *HealthCheckManager) performHealthChecks() {
	hm.mu.RLock()
	endpointsToCheck := make([]*types.Endpoint, 0, len(hm.results))
	for _, result := range hm.results {
		endpointsToCheck = append(endpointsToCheck, result.Endpoint)
	}
	checkers := make(map[string]HealthChecker)
	for k, v := range hm.checkers {
		checkers[k] = v
	}
	hm.mu.RUnlock()

	// 并发执行健康检查
	var wg sync.WaitGroup
	for _, endpoint := range endpointsToCheck {
		wg.Add(1)
		go func(ep *types.Endpoint) {
			defer wg.Done()
			hm.checkSingleEndpoint(ep, checkers)
		}(endpoint)
	}
	wg.Wait()
}

// checkSingleEndpoint 检查单个端点
func (hm *HealthCheckManager) checkSingleEndpoint(endpoint *types.Endpoint, checkers map[string]HealthChecker) {
	protocol := endpoint.Protocol
	if protocol == "" {
		protocol = "tcp" // 默认 TCP
	}

	checker, exists := checkers[protocol]
	if !exists {
		log.Printf("no health checker found for protocol %s, endpoint %s", protocol, endpoint.ID)
		return
	}

	// 执行健康检查
	ctx, cancel := context.WithTimeout(hm.ctx, 10*time.Second)
	defer cancel()

	result := checker.CheckHealth(ctx, endpoint)

	// 更新结果
	hm.mu.Lock()
	defer hm.mu.Unlock()

	oldResult, exists := hm.results[endpoint.ID]
	if exists {
		// 计算连续失败/成功次数
		if result.Status == StatusUnhealthy {
			if oldResult.Status == StatusUnhealthy {
				result.ConsecutiveFails = oldResult.ConsecutiveFails + 1
			} else {
				result.ConsecutiveFails = 1
			}
		} else {
			result.ConsecutiveFails = 0
		}

		// 应用阈值逻辑
		if result.Status == StatusUnhealthy && result.ConsecutiveFails >= hm.unhealthyThreshold {
			result.Status = StatusUnhealthy
		} else if result.Status == StatusHealthy && oldResult.Status == StatusUnhealthy {
			// 从不健康状态恢复需要连续成功
			if oldResult.ConsecutiveFails == 0 { // 第一次成功
				result.Status = StatusHealthy
			}
		}
	}

	hm.results[endpoint.ID] = result

	// 记录状态变化
	if exists && oldResult.Status != result.Status {
		log.Printf("endpoint %s (%s:%d) health status changed: %s -> %s (latency: %v)", 
			endpoint.ID, endpoint.IP, endpoint.Port, oldResult.Status, result.Status, result.Latency)
	}
}

// UpdateEndpointStatus 更新端点状态到注册表
func (hm *HealthCheckManager) UpdateEndpointStatus(registry types.ServiceRegistry) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	// 这里可以扩展为更新服务注册表中的端点状态
	// 目前只记录日志
	for _, result := range hm.results {
		if result.Status == StatusUnhealthy {
			log.Printf("endpoint %s is unhealthy, should be removed from load balancing", result.Endpoint.ID)
		}
	}
}