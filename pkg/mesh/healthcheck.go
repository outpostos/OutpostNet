package mesh

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// HealthChecker 健康检查器
type HealthChecker struct {
	router        *ServiceRouter
	config        *HealthCheckConfig
	
	// 健康状态缓存
	healthStatus  map[string]*EndpointHealth
	healthMu      sync.RWMutex
	
	// 检查器状态
	isRunning     bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	
	// 检查队列
	checkQueue    chan *types.Endpoint
	
	// 统计信息
	metrics       *HealthCheckMetrics
}

// HealthCheckConfig 健康检查配置
type HealthCheckConfig struct {
	// 检查间隔
	CheckInterval     time.Duration `yaml:"check_interval" json:"check_interval"`
	UnhealthyThreshold int          `yaml:"unhealthy_threshold" json:"unhealthy_threshold"`
	HealthyThreshold   int          `yaml:"healthy_threshold" json:"healthy_threshold"`
	
	// 超时配置
	ConnectTimeout    time.Duration `yaml:"connect_timeout" json:"connect_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout" json:"write_timeout"`
	
	// 检查方式
	CheckType         string        `yaml:"check_type" json:"check_type"` // tcp, http, grpc
	CheckPath         string        `yaml:"check_path" json:"check_path"` // HTTP路径
	CheckMethod       string        `yaml:"check_method" json:"check_method"` // HTTP方法
	
	// 并发控制
	MaxConcurrentChecks int         `yaml:"max_concurrent_checks" json:"max_concurrent_checks"`
	QueueSize          int          `yaml:"queue_size" json:"queue_size"`
	
	// 故障恢复
	RecoveryCheckInterval time.Duration `yaml:"recovery_check_interval" json:"recovery_check_interval"`
	FastRecoveryEnabled   bool         `yaml:"fast_recovery_enabled" json:"fast_recovery_enabled"`
}

// EndpointHealth 端点健康状态
type EndpointHealth struct {
	EndpointID        string        `json:"endpoint_id"`
	IsHealthy         bool          `json:"is_healthy"`
	LastCheck         time.Time     `json:"last_check"`
	LastHealthyCheck  time.Time     `json:"last_healthy_check"`
	LastUnhealthyCheck time.Time    `json:"last_unhealthy_check"`
	ConsecutiveFailures int         `json:"consecutive_failures"`
	ConsecutiveSuccesses int        `json:"consecutive_successes"`
	ResponseTime      time.Duration `json:"response_time"`
	ErrorMessage      string        `json:"error_message"`
	HealthScore       float64       `json:"health_score"`
}

// HealthCheckMetrics 健康检查指标
type HealthCheckMetrics struct {
	TotalChecks       int64         `json:"total_checks"`
	SuccessfulChecks  int64         `json:"successful_checks"`
	FailedChecks      int64         `json:"failed_checks"`
	HealthyEndpoints  int           `json:"healthy_endpoints"`
	UnhealthyEndpoints int          `json:"unhealthy_endpoints"`
	AverageResponseTime time.Duration `json:"average_response_time"`
	LastCheckTime     time.Time     `json:"last_check_time"`
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(router *ServiceRouter) *HealthChecker {
	config := DefaultHealthCheckConfig()
	ctx, cancel := context.WithCancel(context.Background())
	
	return &HealthChecker{
		router:       router,
		config:       config,
		healthStatus: make(map[string]*EndpointHealth),
		ctx:          ctx,
		cancel:       cancel,
		checkQueue:   make(chan *types.Endpoint, config.QueueSize),
		metrics:      &HealthCheckMetrics{},
	}
}

// Start 启动健康检查器
func (hc *HealthChecker) Start() error {
	hc.healthMu.Lock()
	defer hc.healthMu.Unlock()
	
	if hc.isRunning {
		return nil
	}
	
	hc.isRunning = true
	
	// 启动检查工作器
	for i := 0; i < hc.config.MaxConcurrentChecks; i++ {
		hc.wg.Add(1)
		go hc.checkWorker()
	}
	
	// 启动调度器
	hc.wg.Add(1)
	go hc.scheduler()
	
	// 启动恢复检查器
	if hc.config.FastRecoveryEnabled {
		hc.wg.Add(1)
		go hc.recoveryChecker()
	}
	
	return nil
}

// Stop 停止健康检查器
func (hc *HealthChecker) Stop() {
	hc.healthMu.Lock()
	defer hc.healthMu.Unlock()
	
	if !hc.isRunning {
		return
	}
	
	hc.isRunning = false
	hc.cancel()
	close(hc.checkQueue)
	hc.wg.Wait()
}

// scheduler 调度器
func (hc *HealthChecker) scheduler() {
	defer hc.wg.Done()
	
	ticker := time.NewTicker(hc.config.CheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.scheduleHealthChecks()
		}
	}
}

// scheduleHealthChecks 调度健康检查
func (hc *HealthChecker) scheduleHealthChecks() {
	// 获取所有服务的端点
	services, err := hc.router.DiscoverServices("")
	if err != nil {
		return
	}
	
	for _, service := range services {
		endpoints, err := hc.router.registry.GetEndpoints(
			service.Metadata.Name,
			service.Metadata.Namespace,
			nil,
		)
		if err != nil {
			continue
		}
		
		for _, endpoint := range endpoints {
			select {
			case hc.checkQueue <- endpoint:
			case <-hc.ctx.Done():
				return
			default:
				// 队列满了，跳过这个端点
			}
		}
	}
}

// checkWorker 检查工作器
func (hc *HealthChecker) checkWorker() {
	defer hc.wg.Done()
	
	for {
		select {
		case <-hc.ctx.Done():
			return
		case endpoint := <-hc.checkQueue:
			if endpoint != nil {
				hc.performHealthCheck(endpoint)
			}
		}
	}
}

// performHealthCheck 执行健康检查
func (hc *HealthChecker) performHealthCheck(endpoint *types.Endpoint) {
	startTime := time.Now()
	
	// 获取或创建健康状态
	_ = hc.getOrCreateHealthStatus(endpoint.ID)
	
	// 执行检查
	var isHealthy bool
	var errorMessage string
	
	switch hc.config.CheckType {
	case "tcp":
		isHealthy, errorMessage = hc.performTCPCheck(endpoint)
	case "http":
		isHealthy, errorMessage = hc.performHTTPCheck(endpoint)
	case "grpc":
		isHealthy, errorMessage = hc.performGRPCCheck(endpoint)
	default:
		isHealthy, errorMessage = hc.performTCPCheck(endpoint)
	}
	
	responseTime := time.Since(startTime)
	
	// 更新健康状态
	hc.updateHealthStatus(endpoint.ID, isHealthy, responseTime, errorMessage)
	
	// 更新指标
	hc.updateMetrics(isHealthy, responseTime)
}

// performTCPCheck 执行TCP检查
func (hc *HealthChecker) performTCPCheck(endpoint *types.Endpoint) (bool, string) {
	address := net.JoinHostPort(endpoint.IP, fmt.Sprintf("%d", endpoint.Port))
	
	conn, err := net.DialTimeout("tcp", address, hc.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Sprintf("TCP connection failed: %v", err)
	}
	defer conn.Close()
	
	return true, ""
}

// performHTTPCheck 执行HTTP检查
func (hc *HealthChecker) performHTTPCheck(endpoint *types.Endpoint) (bool, string) {
	// 这里应该实现HTTP健康检查
	// 简化处理，先执行TCP检查
	return hc.performTCPCheck(endpoint)
}

// performGRPCCheck 执行gRPC检查
func (hc *HealthChecker) performGRPCCheck(endpoint *types.Endpoint) (bool, string) {
	// 这里应该实现gRPC健康检查
	// 简化处理，先执行TCP检查
	return hc.performTCPCheck(endpoint)
}

// getOrCreateHealthStatus 获取或创建健康状态
func (hc *HealthChecker) getOrCreateHealthStatus(endpointID string) *EndpointHealth {
	hc.healthMu.Lock()
	defer hc.healthMu.Unlock()
	
	health, exists := hc.healthStatus[endpointID]
	if !exists {
		health = &EndpointHealth{
			EndpointID:           endpointID,
			IsHealthy:            true,
			LastCheck:            time.Now(),
			ConsecutiveFailures:  0,
			ConsecutiveSuccesses: 0,
			HealthScore:          100.0,
		}
		hc.healthStatus[endpointID] = health
	}
	
	return health
}

// updateHealthStatus 更新健康状态
func (hc *HealthChecker) updateHealthStatus(endpointID string, isHealthy bool, responseTime time.Duration, errorMessage string) {
	hc.healthMu.Lock()
	defer hc.healthMu.Unlock()
	
	health, exists := hc.healthStatus[endpointID]
	if !exists {
		return
	}
	
	health.LastCheck = time.Now()
	health.ResponseTime = responseTime
	health.ErrorMessage = errorMessage
	
	if isHealthy {
		health.LastHealthyCheck = time.Now()
		health.ConsecutiveSuccesses++
		health.ConsecutiveFailures = 0
		
		// 如果连续成功次数达到阈值，标记为健康
		if health.ConsecutiveSuccesses >= hc.config.HealthyThreshold {
			health.IsHealthy = true
		}
	} else {
		health.LastUnhealthyCheck = time.Now()
		health.ConsecutiveFailures++
		health.ConsecutiveSuccesses = 0
		
		// 如果连续失败次数达到阈值，标记为不健康
		if health.ConsecutiveFailures >= hc.config.UnhealthyThreshold {
			health.IsHealthy = false
		}
	}
	
	// 计算健康评分
	health.HealthScore = hc.calculateHealthScore(health)
}

// calculateHealthScore 计算健康评分
func (hc *HealthChecker) calculateHealthScore(health *EndpointHealth) float64 {
	score := 100.0
	
	// 根据连续失败次数降低分数
	if health.ConsecutiveFailures > 0 {
		score -= float64(health.ConsecutiveFailures) * 20.0
	}
	
	// 根据响应时间调整分数
	if health.ResponseTime > 0 {
		if health.ResponseTime > 1*time.Second {
			score -= 30.0
		} else if health.ResponseTime > 500*time.Millisecond {
			score -= 15.0
		} else if health.ResponseTime > 100*time.Millisecond {
			score -= 5.0
		}
	}
	
	// 确保分数在0-100之间
	if score < 0 {
		score = 0
	}
	
	return score
}

// updateMetrics 更新指标
func (hc *HealthChecker) updateMetrics(isHealthy bool, responseTime time.Duration) {
	hc.metrics.TotalChecks++
	hc.metrics.LastCheckTime = time.Now()
	
	if isHealthy {
		hc.metrics.SuccessfulChecks++
	} else {
		hc.metrics.FailedChecks++
	}
	
	// 更新平均响应时间
	if hc.metrics.TotalChecks > 0 {
		totalTime := hc.metrics.AverageResponseTime * time.Duration(hc.metrics.TotalChecks-1)
		hc.metrics.AverageResponseTime = (totalTime + responseTime) / time.Duration(hc.metrics.TotalChecks)
	} else {
		hc.metrics.AverageResponseTime = responseTime
	}
}

// recoveryChecker 恢复检查器
func (hc *HealthChecker) recoveryChecker() {
	defer hc.wg.Done()
	
	ticker := time.NewTicker(hc.config.RecoveryCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.performRecoveryChecks()
		}
	}
}

// performRecoveryChecks 执行恢复检查
func (hc *HealthChecker) performRecoveryChecks() {
	hc.healthMu.RLock()
	var unhealthyEndpoints []string
	for endpointID, health := range hc.healthStatus {
		if !health.IsHealthy {
			unhealthyEndpoints = append(unhealthyEndpoints, endpointID)
		}
	}
	hc.healthMu.RUnlock()
	
	// 对不健康的端点进行快速恢复检查
	for range unhealthyEndpoints {
		// 这里需要根据端点ID获取端点信息
		// 简化处理，跳过具体实现
	}
}

// IsHealthy 检查端点是否健康
func (hc *HealthChecker) IsHealthy(endpointID string) bool {
	hc.healthMu.RLock()
	defer hc.healthMu.RUnlock()
	
	health, exists := hc.healthStatus[endpointID]
	if !exists {
		return true // 默认健康
	}
	
	return health.IsHealthy
}

// GetHealthStatus 获取端点健康状态
func (hc *HealthChecker) GetHealthStatus(endpointID string) *EndpointHealth {
	hc.healthMu.RLock()
	defer hc.healthMu.RUnlock()
	
	health, exists := hc.healthStatus[endpointID]
	if !exists {
		return nil
	}
	
	// 返回副本
	healthCopy := *health
	return &healthCopy
}

// GetAllHealthStatus 获取所有端点健康状态
func (hc *HealthChecker) GetAllHealthStatus() map[string]*EndpointHealth {
	hc.healthMu.RLock()
	defer hc.healthMu.RUnlock()
	
	result := make(map[string]*EndpointHealth)
	for endpointID, health := range hc.healthStatus {
		healthCopy := *health
		result[endpointID] = &healthCopy
	}
	
	return result
}

// GetMetrics 获取健康检查指标
func (hc *HealthChecker) GetMetrics() *HealthCheckMetrics {
	hc.healthMu.RLock()
	defer hc.healthMu.RUnlock()
	
	// 统计健康和不健康的端点数量
	healthyCount := 0
	unhealthyCount := 0
	
	for _, health := range hc.healthStatus {
		if health.IsHealthy {
			healthyCount++
		} else {
			unhealthyCount++
		}
	}
	
	metrics := *hc.metrics
	metrics.HealthyEndpoints = healthyCount
	metrics.UnhealthyEndpoints = unhealthyCount
	
	return &metrics
}

// ForceCheck 强制检查端点
func (hc *HealthChecker) ForceCheck(endpoint *types.Endpoint) {
	select {
	case hc.checkQueue <- endpoint:
	case <-hc.ctx.Done():
	default:
		// 队列满了，异步执行
		go hc.performHealthCheck(endpoint)
	}
}

// RemoveEndpoint 移除端点
func (hc *HealthChecker) RemoveEndpoint(endpointID string) {
	hc.healthMu.Lock()
	defer hc.healthMu.Unlock()
	
	delete(hc.healthStatus, endpointID)
}

// UpdateConfig 更新配置
func (hc *HealthChecker) UpdateConfig(config *HealthCheckConfig) {
	hc.config = config
}

// DefaultHealthCheckConfig 默认健康检查配置
func DefaultHealthCheckConfig() *HealthCheckConfig {
	return &HealthCheckConfig{
		CheckInterval:          30 * time.Second,
		UnhealthyThreshold:     3,
		HealthyThreshold:       2,
		ConnectTimeout:         5 * time.Second,
		ReadTimeout:            5 * time.Second,
		WriteTimeout:           5 * time.Second,
		CheckType:              "tcp",
		CheckPath:              "/health",
		CheckMethod:            "GET",
		MaxConcurrentChecks:    10,
		QueueSize:              1000,
		RecoveryCheckInterval:  10 * time.Second,
		FastRecoveryEnabled:    true,
	}
}