package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// CleanupManager 元数据清理管理器
type CleanupManager struct {
	mu                  sync.RWMutex
	registry            *K8sMemoryRegistry
	config              *CleanupConfig
	stopChannel         chan bool
	running             bool
	
	// 清理统计信息
	stats               *CleanupStats
	
	// 清理规则
	cleanupRules        map[string]*CleanupRule  // resource_type -> cleanup_rule
	
	// TTL 管理器
	ttlManager          *TTLManager
	
	// 事件监听器
	eventListeners      []CleanupEventListener
}

// CleanupConfig 清理配置
type CleanupConfig struct {
	// 清理间隔
	CleanupInterval     time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
	
	// TTL 配置
	ServiceTTL          time.Duration `yaml:"service_ttl" json:"service_ttl"`          // 服务 TTL
	WorkloadTTL         time.Duration `yaml:"workload_ttl" json:"workload_ttl"`        // 工作负载 TTL
	EndpointTTL         time.Duration `yaml:"endpoint_ttl" json:"endpoint_ttl"`        // 端点 TTL
	
	// 高级配置
	MaxCleanupBatch     int           `yaml:"max_cleanup_batch" json:"max_cleanup_batch"`     // 单次清理最大数量
	GracePeriod         time.Duration `yaml:"grace_period" json:"grace_period"`               // 宽限期
	EnableAutoCleanup   bool          `yaml:"enable_auto_cleanup" json:"enable_auto_cleanup"` // 是否启用自动清理
	
	// 保护配置
	ProtectedNamespaces []string      `yaml:"protected_namespaces" json:"protected_namespaces"` // 受保护的命名空间
	MinRetentionTime    time.Duration `yaml:"min_retention_time" json:"min_retention_time"`     // 最小保留时间
}

// CleanupRule 清理规则
type CleanupRule struct {
	ResourceType    string        `yaml:"resource_type" json:"resource_type"`       // 资源类型: service, workload, endpoint
	TTL             time.Duration `yaml:"ttl" json:"ttl"`                           // 生存时间
	Enabled         bool          `yaml:"enabled" json:"enabled"`                   // 是否启用
	GracePeriod     time.Duration `yaml:"grace_period" json:"grace_period"`         // 宽限期
	MaxBatchSize    int           `yaml:"max_batch_size" json:"max_batch_size"`     // 单次清理批次大小
	
	// 条件配置
	Conditions      []CleanupCondition `yaml:"conditions" json:"conditions"`         // 清理条件
}

// CleanupCondition 清理条件
type CleanupCondition struct {
	Type     CleanupConditionType `yaml:"type" json:"type"`         // 条件类型
	Field    string               `yaml:"field" json:"field"`       // 字段名
	Operator string               `yaml:"operator" json:"operator"` // 操作符: gt, lt, eq, contains
	Value    interface{}          `yaml:"value" json:"value"`       // 期望值
}

// CleanupConditionType 清理条件类型
type CleanupConditionType string

const (
	ConditionLastSeen     CleanupConditionType = "last_seen"      // 基于最后可见时间
	ConditionCreatedAt    CleanupConditionType = "created_at"     // 基于创建时间
	ConditionHealthStatus CleanupConditionType = "health_status"  // 基于健康状态
	ConditionLabel        CleanupConditionType = "label"          // 基于标签
	ConditionStatus       CleanupConditionType = "status"         // 基于状态
)

// CleanupStats 清理统计信息
type CleanupStats struct {
	mu                     sync.RWMutex
	LastCleanupTime        time.Time         `json:"last_cleanup_time"`
	TotalCleanupRuns       int64             `json:"total_cleanup_runs"`
	TotalCleaned           int64             `json:"total_cleaned"`
	
	// 按资源类型分类的统计
	ServicesCleaned        int64             `json:"services_cleaned"`
	WorkloadsCleaned       int64             `json:"workloads_cleaned"`
	EndpointsCleaned       int64             `json:"endpoints_cleaned"`
	
	// 清理耗时统计
	AverageCleanupDuration time.Duration     `json:"average_cleanup_duration"`
	LastCleanupDuration    time.Duration     `json:"last_cleanup_duration"`
	
	// 错误统计
	CleanupErrors          int64             `json:"cleanup_errors"`
	LastError              string            `json:"last_error,omitempty"`
	LastErrorTime          time.Time         `json:"last_error_time,omitempty"`
}

// CleanupEvent 清理事件
type CleanupEvent struct {
	Type         CleanupEventType `json:"type"`
	ResourceType string           `json:"resource_type"`  // service, workload, endpoint
	ResourceID   string           `json:"resource_id"`
	Reason       string           `json:"reason"`
	Timestamp    time.Time        `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// CleanupEventType 清理事件类型
type CleanupEventType string

const (
	CleanupEventStarted    CleanupEventType = "cleanup_started"
	CleanupEventCompleted  CleanupEventType = "cleanup_completed"
	CleanupEventCleaned    CleanupEventType = "resource_cleaned"
	CleanupEventSkipped    CleanupEventType = "resource_skipped"
	CleanupEventError      CleanupEventType = "cleanup_error"
)

// CleanupEventListener 清理事件监听器
type CleanupEventListener interface {
	OnCleanupEvent(event *CleanupEvent)
}

// DefaultCleanupConfig 默认清理配置
func DefaultCleanupConfig() *CleanupConfig {
	return &CleanupConfig{
		CleanupInterval:     5 * time.Minute,
		ServiceTTL:          24 * time.Hour,     // 服务保留24小时
		WorkloadTTL:         2 * time.Hour,      // 工作负载保留2小时
		EndpointTTL:         30 * time.Minute,   // 端点保留30分钟
		MaxCleanupBatch:     100,
		GracePeriod:         5 * time.Minute,
		EnableAutoCleanup:   true,
		ProtectedNamespaces: []string{"default", "kube-system"},
		MinRetentionTime:    5 * time.Minute,    // 最小保留5分钟
	}
}

// NewCleanupManager 创建清理管理器
func NewCleanupManager(registry *K8sMemoryRegistry, config *CleanupConfig) *CleanupManager {
	if config == nil {
		config = DefaultCleanupConfig()
	}

	manager := &CleanupManager{
		registry:       registry,
		config:         config,
		stopChannel:    make(chan bool),
		running:        false,
		stats:          &CleanupStats{},
		cleanupRules:   make(map[string]*CleanupRule),
		eventListeners: make([]CleanupEventListener, 0),
	}

	// 初始化 TTL 管理器
	ttlConfig := &TTLConfig{
		ServiceTTL:           config.ServiceTTL,
		WorkloadTTL:          config.WorkloadTTL,
		EndpointTTL:          config.EndpointTTL,
		ServiceHeartbeatTTL:  config.ServiceTTL / 2,     // 心跳 TTL 设为 TTL 的一半
		WorkloadHeartbeatTTL: config.WorkloadTTL / 2,
		EndpointHeartbeatTTL: config.EndpointTTL / 2,
	}
	manager.ttlManager = NewTTLManager(registry, ttlConfig)

	// 初始化默认清理规则
	manager.initializeDefaultRules()

	return manager
}

// initializeDefaultRules 初始化默认清理规则
func (cm *CleanupManager) initializeDefaultRules() {
	// 服务清理规则
	cm.cleanupRules["service"] = &CleanupRule{
		ResourceType: "service",
		TTL:          cm.config.ServiceTTL,
		Enabled:      true,
		GracePeriod:  cm.config.GracePeriod,
		MaxBatchSize: cm.config.MaxCleanupBatch,
		Conditions: []CleanupCondition{
			{
				Type:     ConditionLastSeen,
				Operator: "gt",
				Value:    cm.config.ServiceTTL,
			},
		},
	}

	// 工作负载清理规则
	cm.cleanupRules["workload"] = &CleanupRule{
		ResourceType: "workload",
		TTL:          cm.config.WorkloadTTL,
		Enabled:      true,
		GracePeriod:  cm.config.GracePeriod,
		MaxBatchSize: cm.config.MaxCleanupBatch,
		Conditions: []CleanupCondition{
			{
				Type:     ConditionLastSeen,
				Operator: "gt",
				Value:    cm.config.WorkloadTTL,
			},
			{
				Type:     ConditionHealthStatus,
				Operator: "eq",
				Value:    false,
			},
		},
	}

	// 端点清理规则
	cm.cleanupRules["endpoint"] = &CleanupRule{
		ResourceType: "endpoint",
		TTL:          cm.config.EndpointTTL,
		Enabled:      true,
		GracePeriod:  cm.config.GracePeriod,
		MaxBatchSize: cm.config.MaxCleanupBatch,
		Conditions: []CleanupCondition{
			{
				Type:     ConditionLastSeen,
				Operator: "gt",
				Value:    cm.config.EndpointTTL,
			},
		},
	}
}

// Start 启动清理管理器
func (cm *CleanupManager) Start(ctx context.Context) error {
	cm.mu.Lock()
	if cm.running {
		cm.mu.Unlock()
		return fmt.Errorf("cleanup manager is already running")
	}
	cm.running = true
	cm.mu.Unlock()

	if !cm.config.EnableAutoCleanup {
		fmt.Println("Auto cleanup is disabled")
		return nil
	}

	go cm.cleanupLoop(ctx)
	
	cm.notifyEventListeners(&CleanupEvent{
		Type:      CleanupEventStarted,
		Reason:    "cleanup_manager_started",
		Timestamp: time.Now(),
	})

	return nil
}

// Stop 停止清理管理器
func (cm *CleanupManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.running {
		cm.running = false
		close(cm.stopChannel)
	}
}

// ManualCleanup 手动触发清理
func (cm *CleanupManager) ManualCleanup() (*CleanupResult, error) {
	return cm.performCleanup()
}

// CleanupResult 清理结果
type CleanupResult struct {
	StartTime        time.Time         `json:"start_time"`
	EndTime          time.Time         `json:"end_time"`
	Duration         time.Duration     `json:"duration"`
	TotalCleaned     int               `json:"total_cleaned"`
	ServicesCleaned  int               `json:"services_cleaned"`
	WorkloadsCleaned int               `json:"workloads_cleaned"`
	EndpointsCleaned int               `json:"endpoints_cleaned"`
	Errors           []string          `json:"errors,omitempty"`
}

// GetStats 获取清理统计信息
func (cm *CleanupManager) GetStats() *CleanupStats {
	cm.stats.mu.RLock()
	defer cm.stats.mu.RUnlock()
	
	// 返回副本
	statsCopy := *cm.stats
	return &statsCopy
}

// GetCleanupRules 获取清理规则
func (cm *CleanupManager) GetCleanupRules() map[string]*CleanupRule {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	rules := make(map[string]*CleanupRule)
	for k, v := range cm.cleanupRules {
		ruleCopy := *v
		rules[k] = &ruleCopy
	}
	return rules
}

// UpdateCleanupRule 更新清理规则
func (cm *CleanupManager) UpdateCleanupRule(resourceType string, rule *CleanupRule) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	if rule == nil {
		return fmt.Errorf("cleanup rule cannot be nil")
	}
	
	if rule.TTL < cm.config.MinRetentionTime {
		return fmt.Errorf("TTL cannot be less than minimum retention time (%v)", cm.config.MinRetentionTime)
	}
	
	ruleCopy := *rule
	cm.cleanupRules[resourceType] = &ruleCopy
	
	return nil
}

// AddEventListener 添加事件监听器
func (cm *CleanupManager) AddEventListener(listener CleanupEventListener) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	cm.eventListeners = append(cm.eventListeners, listener)
}

// GetTTLManager 获取 TTL 管理器
func (cm *CleanupManager) GetTTLManager() *TTLManager {
	return cm.ttlManager
}

// 内部方法：清理循环
func (cm *CleanupManager) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(cm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cm.stopChannel:
			return
		case <-ticker.C:
			if _, err := cm.performCleanup(); err != nil {
				cm.updateErrorStats(err)
			}
		}
	}
}

// 内部方法：执行清理
func (cm *CleanupManager) performCleanup() (*CleanupResult, error) {
	startTime := time.Now()
	result := &CleanupResult{
		StartTime: startTime,
		Errors:    make([]string, 0),
	}

	cm.notifyEventListeners(&CleanupEvent{
		Type:      CleanupEventStarted,
		Reason:    "scheduled_cleanup",
		Timestamp: startTime,
	})

	// 清理工作负载（先清理依赖项）
	if workloadsCleaned, err := cm.cleanupWorkloads(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("workload cleanup error: %v", err))
	} else {
		result.WorkloadsCleaned = workloadsCleaned
	}

	// 清理端点
	if endpointsCleaned, err := cm.cleanupEndpoints(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("endpoint cleanup error: %v", err))
	} else {
		result.EndpointsCleaned = endpointsCleaned
	}

	// 清理服务（最后清理）
	if servicesCleaned, err := cm.cleanupServices(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("service cleanup error: %v", err))
	} else {
		result.ServicesCleaned = servicesCleaned
	}

	// 完成统计
	endTime := time.Now()
	result.EndTime = endTime
	result.Duration = endTime.Sub(startTime)
	result.TotalCleaned = result.ServicesCleaned + result.WorkloadsCleaned + result.EndpointsCleaned

	// 更新统计信息
	cm.updateCleanupStats(result)

	cm.notifyEventListeners(&CleanupEvent{
		Type:      CleanupEventCompleted,
		Reason:    "cleanup_completed",
		Timestamp: endTime,
		Metadata: map[string]interface{}{
			"duration":          result.Duration.String(),
			"total_cleaned":     result.TotalCleaned,
			"services_cleaned":  result.ServicesCleaned,
			"workloads_cleaned": result.WorkloadsCleaned,
			"endpoints_cleaned": result.EndpointsCleaned,
		},
	})

	if len(result.Errors) > 0 {
		return result, fmt.Errorf("cleanup completed with %d errors", len(result.Errors))
	}

	return result, nil
}

// 内部方法：清理服务
func (cm *CleanupManager) cleanupServices() (int, error) {
	rule, exists := cm.cleanupRules["service"]
	if !exists || !rule.Enabled {
		return 0, nil
	}

	services, err := cm.registry.ListServices("")
	if err != nil {
		return 0, fmt.Errorf("failed to list services: %w", err)
	}

	cleaned := 0
	now := time.Now()

	for _, service := range services {
		// 检查是否为受保护命名空间
		if cm.isProtectedNamespace(service.Metadata.Namespace) {
			continue
		}

		// 检查清理条件
		if cm.shouldCleanupService(service, rule, now) {
			// 执行清理
			if err := cm.cleanupService(service); err != nil {
				cm.notifyEventListeners(&CleanupEvent{
					Type:         CleanupEventError,
					ResourceType: "service",
					ResourceID:   fmt.Sprintf("%s/%s", service.Metadata.Namespace, service.Metadata.Name),
					Reason:       fmt.Sprintf("cleanup_error: %v", err),
					Timestamp:    now,
				})
				continue
			}

			cleaned++
			cm.notifyEventListeners(&CleanupEvent{
				Type:         CleanupEventCleaned,
				ResourceType: "service",
				ResourceID:   fmt.Sprintf("%s/%s", service.Metadata.Namespace, service.Metadata.Name),
				Reason:       "ttl_expired",
				Timestamp:    now,
			})

			// 批次限制
			if cleaned >= rule.MaxBatchSize {
				break
			}
		}
	}

	return cleaned, nil
}

// 内部方法：清理工作负载
func (cm *CleanupManager) cleanupWorkloads() (int, error) {
	rule, exists := cm.cleanupRules["workload"]
	if !exists || !rule.Enabled {
		return 0, nil
	}

	workloads, err := cm.registry.ListWorkloads(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to list workloads: %w", err)
	}

	cleaned := 0
	now := time.Now()

	for _, workload := range workloads {
		// 检查清理条件
		if cm.shouldCleanupWorkload(workload, rule, now) {
			// 执行清理
			if err := cm.registry.UnregisterWorkload(workload.Metadata.ID); err != nil {
				cm.notifyEventListeners(&CleanupEvent{
					Type:         CleanupEventError,
					ResourceType: "workload",
					ResourceID:   workload.Metadata.ID,
					Reason:       fmt.Sprintf("cleanup_error: %v", err),
					Timestamp:    now,
				})
				continue
			}

			cleaned++
			cm.notifyEventListeners(&CleanupEvent{
				Type:         CleanupEventCleaned,
				ResourceType: "workload",
				ResourceID:   workload.Metadata.ID,
				Reason:       "ttl_expired_or_unhealthy",
				Timestamp:    now,
			})

			// 批次限制
			if cleaned >= rule.MaxBatchSize {
				break
			}
		}
	}

	return cleaned, nil
}

// 内部方法：清理端点
func (cm *CleanupManager) cleanupEndpoints() (int, error) {
	rule, exists := cm.cleanupRules["endpoint"]
	if !exists || !rule.Enabled {
		return 0, nil
	}

	// 这里需要实现端点的清理逻辑
	// 由于端点是自动生成的，主要通过清理工作负载来间接清理端点
	// 这里可以添加额外的端点清理逻辑，比如清理孤儿端点

	return 0, nil
}

// 内部方法：清理单个服务
func (cm *CleanupManager) cleanupService(service *types.Service) error {
	// 这里可以添加服务清理前的预处理逻辑
	// 比如检查是否有依赖的端点等
	
	// 目前服务是配置性的，不自动清理
	// 但可以在这里记录日志或发送通知
	return nil
}

// 内部方法：检查是否应该清理服务
func (cm *CleanupManager) shouldCleanupService(service *types.Service, rule *CleanupRule, now time.Time) bool {
	// 基于 TTL 的服务过期检查
	if cm.ttlManager.IsServiceExpired(service, now) {
		return true
	}
	
	// 服务通常是配置性的，但可以基于其他条件清理
	// 这里可以实现基于其他条件的清理逻辑
	return false
}

// 内部方法：检查是否应该清理工作负载
func (cm *CleanupManager) shouldCleanupWorkload(workload *types.Workload, rule *CleanupRule, now time.Time) bool {
	// 检查是否应该清理工作负载
	// 宽限期逻辑：只要任意一个TTL还在宽限期内，就不清理
	if rule.GracePeriod > 0 {
		// 检查绝对TTL的宽限期
		if !workload.Metadata.CreatedAt.IsZero() && workload.Metadata.TTL > 0 {
			timeSinceCreated := now.Sub(workload.Metadata.CreatedAt)
			if timeSinceCreated < workload.Metadata.TTL+rule.GracePeriod {
				return false // 绝对TTL还在宽限期内
			}
		}
		
		// 检查心跳TTL的宽限期
		if !workload.Status.LastSeen.IsZero() {
			heartbeatTTL := cm.ttlManager.GetHeartbeatTTL("workload")
			timeSinceLastSeen := now.Sub(workload.Status.LastSeen)
			if timeSinceLastSeen < heartbeatTTL+rule.GracePeriod {
				return false // 心跳TTL还在宽限期内
			}
		}
		
		// 检查元数据心跳TTL的宽限期
		if !workload.Metadata.LastSeen.IsZero() {
			heartbeatTTL := cm.ttlManager.GetHeartbeatTTL("workload")
			timeSinceLastSeen := now.Sub(workload.Metadata.LastSeen)
			if timeSinceLastSeen < heartbeatTTL+rule.GracePeriod {
				return false // 元数据心跳TTL还在宽限期内
			}
		}
	}
	
	// 所有TTL都超过宽限期了，检查是否真的过期
	if cm.ttlManager.IsWorkloadExpired(workload, now) {
		return true
	}

	// 检查健康状态（如果配置了健康状态条件）
	for _, condition := range rule.Conditions {
		if condition.Type == ConditionHealthStatus {
			expectedHealthy, ok := condition.Value.(bool)
			if ok && workload.Status.Ready == expectedHealthy {
				// 如果工作负载状态匹配条件（比如不健康），并且过期，则可以清理
				if cm.ttlManager.IsWorkloadExpired(workload, now) {
					return true
				}
			}
		}
	}

	return false
}

// 内部方法：检查是否为受保护命名空间
func (cm *CleanupManager) isProtectedNamespace(namespace string) bool {
	for _, protected := range cm.config.ProtectedNamespaces {
		if namespace == protected {
			return true
		}
	}
	return false
}

// 内部方法：更新清理统计信息
func (cm *CleanupManager) updateCleanupStats(result *CleanupResult) {
	cm.stats.mu.Lock()
	defer cm.stats.mu.Unlock()

	cm.stats.LastCleanupTime = result.EndTime
	cm.stats.TotalCleanupRuns++
	cm.stats.TotalCleaned += int64(result.TotalCleaned)
	cm.stats.ServicesCleaned += int64(result.ServicesCleaned)
	cm.stats.WorkloadsCleaned += int64(result.WorkloadsCleaned)
	cm.stats.EndpointsCleaned += int64(result.EndpointsCleaned)
	cm.stats.LastCleanupDuration = result.Duration

	// 计算平均清理时间
	if cm.stats.TotalCleanupRuns > 0 {
		totalDuration := time.Duration(cm.stats.TotalCleanupRuns) * cm.stats.AverageCleanupDuration + result.Duration
		cm.stats.AverageCleanupDuration = totalDuration / time.Duration(cm.stats.TotalCleanupRuns)
	} else {
		cm.stats.AverageCleanupDuration = result.Duration
	}
}

// 内部方法：更新错误统计信息
func (cm *CleanupManager) updateErrorStats(err error) {
	cm.stats.mu.Lock()
	defer cm.stats.mu.Unlock()

	cm.stats.CleanupErrors++
	cm.stats.LastError = err.Error()
	cm.stats.LastErrorTime = time.Now()
}

// 内部方法：通知事件监听器
func (cm *CleanupManager) notifyEventListeners(event *CleanupEvent) {
	// 同步调用监听器，确保测试中能立即收到事件
	for _, listener := range cm.eventListeners {
		listener.OnCleanupEvent(event)
	}
}