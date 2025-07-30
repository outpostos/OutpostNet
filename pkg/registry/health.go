package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// HealthChecker 健康检查器
type HealthChecker struct {
	mu               sync.RWMutex
	healthStatus     map[string]*HealthStatus    // endpointID -> HealthStatus
	checkInterval    time.Duration               // 健康检查间隔
	timeout          time.Duration               // 健康检查超时
	retryCount       int                        // 重试次数
	heartbeatTimeout time.Duration               // 心跳超时时间
	eventListeners   []HealthEventListener      // 健康状态变化监听器
	stopChannel      chan bool                  // 停止信号
	running          bool                       // 是否正在运行
}

// HealthStatus 健康状态
type HealthStatus struct {
	EndpointID    string              `json:"endpoint_id"`
	Ready         bool                `json:"ready"`
	LastCheck     time.Time           `json:"last_check"`
	LastSeen      time.Time           `json:"last_seen"`
	CheckCount    int                 `json:"check_count"`
	FailureCount  int                 `json:"failure_count"`
	Reason        string              `json:"reason"`
	Message       string              `json:"message"`
	CheckType     HealthCheckType     `json:"check_type"`
}

// HealthCheckType 健康检查类型
type HealthCheckType string

const (
	HealthCheckHeartbeat HealthCheckType = "heartbeat"  // 基于心跳的健康检查
	HealthCheckTimeout   HealthCheckType = "timeout"    // 基于超时的健康检查
	HealthCheckManual    HealthCheckType = "manual"     // 手动设置的健康状态
)

// HealthEvent 健康状态变化事件
type HealthEvent struct {
	Type       HealthEventType `json:"type"`
	EndpointID string          `json:"endpoint_id"`
	OldStatus  bool            `json:"old_status"`
	NewStatus  bool            `json:"new_status"`
	Reason     string          `json:"reason"`
	Timestamp  time.Time       `json:"timestamp"`
}

// HealthEventType 健康事件类型
type HealthEventType string

const (
	HealthEventBecameHealthy   HealthEventType = "became_healthy"
	HealthEventBecameUnhealthy HealthEventType = "became_unhealthy"
	HealthEventCheckFailed     HealthEventType = "check_failed"
)

// HealthEventListener 健康事件监听器
type HealthEventListener interface {
	OnHealthEvent(event *HealthEvent)
}

// HealthCheckConfig 健康检查配置
type HealthCheckConfig struct {
	CheckInterval    time.Duration `yaml:"check_interval" json:"check_interval"`       // 检查间隔，默认30秒
	Timeout          time.Duration `yaml:"timeout" json:"timeout"`                     // 超时时间，默认5秒
	RetryCount       int           `yaml:"retry_count" json:"retry_count"`             // 重试次数，默认3次
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout" json:"heartbeat_timeout"` // 心跳超时，默认60秒
}

// DefaultHealthCheckConfig 默认健康检查配置
func DefaultHealthCheckConfig() *HealthCheckConfig {
	return &HealthCheckConfig{
		CheckInterval:    30 * time.Second,
		Timeout:          5 * time.Second,
		RetryCount:       3,
		HeartbeatTimeout: 60 * time.Second,
	}
}

// NewHealthChecker 创建新的健康检查器
func NewHealthChecker(config *HealthCheckConfig) *HealthChecker {
	if config == nil {
		config = DefaultHealthCheckConfig()
	}

	return &HealthChecker{
		healthStatus:     make(map[string]*HealthStatus),
		checkInterval:    config.CheckInterval,
		timeout:          config.Timeout,
		retryCount:       config.RetryCount,
		heartbeatTimeout: config.HeartbeatTimeout,
		eventListeners:   make([]HealthEventListener, 0),
		stopChannel:      make(chan bool),
		running:          false,
	}
}

// Start 启动健康检查器
func (hc *HealthChecker) Start(ctx context.Context) error {
	hc.mu.Lock()
	if hc.running {
		hc.mu.Unlock()
		return fmt.Errorf("health checker is already running")
	}
	hc.running = true
	hc.mu.Unlock()

	go hc.healthCheckLoop(ctx)
	return nil
}

// Stop 停止健康检查器
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hc.running {
		hc.running = false
		close(hc.stopChannel)
	}
}

// RegisterEndpoint 注册端点进行健康检查
func (hc *HealthChecker) RegisterEndpoint(endpointID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if _, exists := hc.healthStatus[endpointID]; !exists {
		hc.healthStatus[endpointID] = &HealthStatus{
			EndpointID:    endpointID,
			Ready:         true, // 默认为健康状态
			LastCheck:     time.Now(),
			LastSeen:      time.Now(),
			CheckCount:    0,
			FailureCount:  0,
			Reason:        "registered",
			Message:       "endpoint registered for health checking",
			CheckType:     HealthCheckHeartbeat,
		}
	}
}

// UnregisterEndpoint 取消注册端点
func (hc *HealthChecker) UnregisterEndpoint(endpointID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	delete(hc.healthStatus, endpointID)
}

// UpdateEndpointLastSeen 更新端点最后可见时间（心跳机制）
func (hc *HealthChecker) UpdateEndpointLastSeen(endpointID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if status, exists := hc.healthStatus[endpointID]; exists {
		status.LastSeen = time.Now()

		// 如果端点之前不健康，现在有心跳了，标记为健康
		if !status.Ready {
			hc.markEndpointHealthy(endpointID, "heartbeat_received", "endpoint heartbeat received")
		}
	}
}

// IsEndpointHealthy 检查端点是否健康
func (hc *HealthChecker) IsEndpointHealthy(endpointID string) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	if status, exists := hc.healthStatus[endpointID]; exists {
		return status.Ready
	}
	
	// 如果端点没有注册到健康检查器，默认认为是健康的
	return true
}

// GetEndpointHealthStatus 获取端点健康状态
func (hc *HealthChecker) GetEndpointHealthStatus(endpointID string) (*HealthStatus, bool) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	status, exists := hc.healthStatus[endpointID]
	if exists {
		statusCopy := *status
		return &statusCopy, true
	}
	return nil, false
}

// GetAllHealthStatus 获取所有端点健康状态
func (hc *HealthChecker) GetAllHealthStatus() map[string]*HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	result := make(map[string]*HealthStatus)
	for id, status := range hc.healthStatus {
		statusCopy := *status
		result[id] = &statusCopy
	}
	return result
}

// MarkEndpointUnhealthy 手动标记端点为不健康
func (hc *HealthChecker) MarkEndpointUnhealthy(endpointID, reason, message string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.markEndpointUnhealthy(endpointID, reason, message)
}

// MarkEndpointHealthy 手动标记端点为健康
func (hc *HealthChecker) MarkEndpointHealthy(endpointID, reason, message string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.markEndpointHealthy(endpointID, reason, message)
}

// AddEventListener 添加健康事件监听器
func (hc *HealthChecker) AddEventListener(listener HealthEventListener) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.eventListeners = append(hc.eventListeners, listener)
}

// FilterHealthyEndpoints 过滤健康的端点
func (hc *HealthChecker) FilterHealthyEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	var healthyEndpoints []*types.Endpoint

	for _, ep := range endpoints {
		if hc.IsEndpointHealthy(ep.ID) {
			healthyEndpoints = append(healthyEndpoints, ep)
		}
	}

	return healthyEndpoints
}

// 内部方法：健康检查循环
func (hc *HealthChecker) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hc.stopChannel:
			return
		case <-ticker.C:
			hc.performHealthCheck()
		}
	}
}

// 内部方法：执行健康检查
func (hc *HealthChecker) performHealthCheck() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	now := time.Now()
	heartbeatTimeout := hc.heartbeatTimeout // 使用配置的超时时间

	for endpointID, status := range hc.healthStatus {
		status.CheckCount++
		status.LastCheck = now

		// 基于心跳超时的健康检查
		if status.CheckType == HealthCheckHeartbeat {
			timeSinceLastSeen := now.Sub(status.LastSeen)
			
			if timeSinceLastSeen > heartbeatTimeout {
				if status.Ready {
					// 端点从健康变为不健康
					hc.markEndpointUnhealthy(endpointID, "heartbeat_timeout", 
						fmt.Sprintf("no heartbeat for %v", timeSinceLastSeen))
				}
				status.FailureCount++
			} else {
				if !status.Ready {
					// 端点从不健康变为健康
					hc.markEndpointHealthy(endpointID, "heartbeat_recovered", "heartbeat recovered")
				}
				status.FailureCount = 0
			}
		}
	}
}

// 内部方法：标记端点为不健康 (需要持有锁)
func (hc *HealthChecker) markEndpointUnhealthy(endpointID, reason, message string) {
	status, exists := hc.healthStatus[endpointID]
	if !exists {
		return
	}

	oldStatus := status.Ready
	status.Ready = false
	status.Reason = reason
	status.Message = message

	// 如果状态发生变化，发送事件
	if oldStatus != status.Ready {
		event := &HealthEvent{
			Type:       HealthEventBecameUnhealthy,
			EndpointID: endpointID,
			OldStatus:  oldStatus,
			NewStatus:  status.Ready,
			Reason:     reason,
			Timestamp:  time.Now(),
		}
		hc.notifyEventListeners(event)
	}
}

// 内部方法：标记端点为健康 (需要持有锁)
func (hc *HealthChecker) markEndpointHealthy(endpointID, reason, message string) {
	status, exists := hc.healthStatus[endpointID]
	if !exists {
		return
	}

	oldStatus := status.Ready
	status.Ready = true
	status.Reason = reason
	status.Message = message
	status.FailureCount = 0

	// 如果状态发生变化，发送事件
	if oldStatus != status.Ready {
		event := &HealthEvent{
			Type:       HealthEventBecameHealthy,
			EndpointID: endpointID,
			OldStatus:  oldStatus,
			NewStatus:  status.Ready,
			Reason:     reason,
			Timestamp:  time.Now(),
		}
		hc.notifyEventListeners(event)
	}
}

// 内部方法：通知事件监听器
func (hc *HealthChecker) notifyEventListeners(event *HealthEvent) {
	// 同步调用监听器，确保测试中能立即收到事件
	for _, listener := range hc.eventListeners {
		listener.OnHealthEvent(event)
	}
}

// GetHealthStats 获取健康检查统计信息
func (hc *HealthChecker) GetHealthStats() map[string]interface{} {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	totalEndpoints := len(hc.healthStatus)
	healthyEndpoints := 0
	unhealthyEndpoints := 0

	for _, status := range hc.healthStatus {
		if status.Ready {
			healthyEndpoints++
		} else {
			unhealthyEndpoints++
		}
	}

	return map[string]interface{}{
		"total_endpoints":     totalEndpoints,
		"healthy_endpoints":   healthyEndpoints,
		"unhealthy_endpoints": unhealthyEndpoints,
		"check_interval":      hc.checkInterval.String(),
		"timeout":             hc.timeout.String(),
		"retry_count":         hc.retryCount,
		"running":             hc.running,
	}
}