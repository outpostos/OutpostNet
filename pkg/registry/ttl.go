package registry

import (
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// TTLManager TTL 管理器
type TTLManager struct {
	mu             sync.RWMutex
	registry       *K8sMemoryRegistry
	defaultTTLs    map[string]time.Duration  // resource_type -> default_ttl
	heartbeatTTLs  map[string]time.Duration  // resource_type -> heartbeat_ttl
}

// TTLConfig TTL 配置
type TTLConfig struct {
	ServiceTTL           time.Duration `yaml:"service_ttl" json:"service_ttl"`
	WorkloadTTL          time.Duration `yaml:"workload_ttl" json:"workload_ttl"`
	EndpointTTL          time.Duration `yaml:"endpoint_ttl" json:"endpoint_ttl"`
	ServiceHeartbeatTTL  time.Duration `yaml:"service_heartbeat_ttl" json:"service_heartbeat_ttl"`
	WorkloadHeartbeatTTL time.Duration `yaml:"workload_heartbeat_ttl" json:"workload_heartbeat_ttl"`
	EndpointHeartbeatTTL time.Duration `yaml:"endpoint_heartbeat_ttl" json:"endpoint_heartbeat_ttl"`
}

// DefaultTTLConfig 默认 TTL 配置
func DefaultTTLConfig() *TTLConfig {
	return &TTLConfig{
		ServiceTTL:           24 * time.Hour,    // 服务配置保留 24 小时
		WorkloadTTL:          2 * time.Hour,     // 工作负载保留 2 小时
		EndpointTTL:          30 * time.Minute,  // 端点保留 30 分钟
		ServiceHeartbeatTTL:  1 * time.Hour,     // 服务心跳 TTL 1 小时
		WorkloadHeartbeatTTL: 30 * time.Minute,  // 工作负载心跳 TTL 30 分钟
		EndpointHeartbeatTTL: 5 * time.Minute,   // 端点心跳 TTL 5 分钟
	}
}

// NewTTLManager 创建 TTL 管理器
func NewTTLManager(registry *K8sMemoryRegistry, config *TTLConfig) *TTLManager {
	if config == nil {
		config = DefaultTTLConfig()
	}

	return &TTLManager{
		registry: registry,
		defaultTTLs: map[string]time.Duration{
			"service":  config.ServiceTTL,
			"workload": config.WorkloadTTL,
			"endpoint": config.EndpointTTL,
		},
		heartbeatTTLs: map[string]time.Duration{
			"service":  config.ServiceHeartbeatTTL,
			"workload": config.WorkloadHeartbeatTTL,
			"endpoint": config.EndpointHeartbeatTTL,
		},
	}
}

// GetDefaultTTL 获取资源类型的默认 TTL
func (tm *TTLManager) GetDefaultTTL(resourceType string) time.Duration {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	if ttl, exists := tm.defaultTTLs[resourceType]; exists {
		return ttl
	}
	return 1 * time.Hour // 默认 1 小时
}

// GetHeartbeatTTL 获取资源类型的心跳 TTL
func (tm *TTLManager) GetHeartbeatTTL(resourceType string) time.Duration {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	if ttl, exists := tm.heartbeatTTLs[resourceType]; exists {
		return ttl
	}
	return 30 * time.Minute // 默认 30 分钟
}

// SetDefaultTTL 设置资源类型的默认 TTL
func (tm *TTLManager) SetDefaultTTL(resourceType string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("TTL must be positive")
	}
	
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	tm.defaultTTLs[resourceType] = ttl
	return nil
}

// SetHeartbeatTTL 设置资源类型的心跳 TTL
func (tm *TTLManager) SetHeartbeatTTL(resourceType string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("heartbeat TTL must be positive")
	}
	
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	tm.heartbeatTTLs[resourceType] = ttl
	return nil
}

// IsServiceExpired 检查服务是否过期
func (tm *TTLManager) IsServiceExpired(service *types.Service, now time.Time) bool {
	// 基于创建时间 + TTL 的过期检查
	if !service.Metadata.CreatedAt.IsZero() && service.Metadata.TTL > 0 {
		expiryTime := service.Metadata.CreatedAt.Add(service.Metadata.TTL)
		if now.After(expiryTime) {
			return true
		}
	}
	
	// 基于最后心跳时间的过期检查
	if !service.Metadata.LastSeen.IsZero() {
		heartbeatTTL := tm.GetHeartbeatTTL("service")
		heartbeatExpiryTime := service.Metadata.LastSeen.Add(heartbeatTTL)
		if now.After(heartbeatExpiryTime) {
			return true
		}
	}
	
	return false
}

// IsWorkloadExpired 检查工作负载是否过期
func (tm *TTLManager) IsWorkloadExpired(workload *types.Workload, now time.Time) bool {
	// 基于创建时间 + TTL 的过期检查
	if !workload.Metadata.CreatedAt.IsZero() && workload.Metadata.TTL > 0 {
		expiryTime := workload.Metadata.CreatedAt.Add(workload.Metadata.TTL)
		if now.After(expiryTime) {
			return true
		}
	}
	
	// 基于最后心跳时间的过期检查 (使用 Status.LastSeen)
	if !workload.Status.LastSeen.IsZero() {
		heartbeatTTL := tm.GetHeartbeatTTL("workload")
		heartbeatExpiryTime := workload.Status.LastSeen.Add(heartbeatTTL)
		if now.After(heartbeatExpiryTime) {
			return true
		}
	}
	
	// 基于元数据的最后心跳时间检查
	if !workload.Metadata.LastSeen.IsZero() {
		heartbeatTTL := tm.GetHeartbeatTTL("workload")
		heartbeatExpiryTime := workload.Metadata.LastSeen.Add(heartbeatTTL)
		if now.After(heartbeatExpiryTime) {
			return true
		}
	}
	
	return false
}

// IsEndpointExpired 检查端点是否过期
func (tm *TTLManager) IsEndpointExpired(endpoint *types.Endpoint, now time.Time) bool {
	// 基于最后可见时间的过期检查
	if !endpoint.LastSeen.IsZero() {
		heartbeatTTL := tm.GetHeartbeatTTL("endpoint")
		heartbeatExpiryTime := endpoint.LastSeen.Add(heartbeatTTL)
		if now.After(heartbeatExpiryTime) {
			return true
		}
	}
	
	return false
}

// UpdateServiceLastSeen 更新服务最后可见时间
func (tm *TTLManager) UpdateServiceLastSeen(serviceName, namespace string) error {
	key := fmt.Sprintf("%s/%s", namespace, serviceName)
	
	tm.registry.mu.Lock()
	defer tm.registry.mu.Unlock()
	
	if service, exists := tm.registry.services[key]; exists {
		service.Metadata.LastSeen = time.Now()
		return nil
	}
	
	return fmt.Errorf("service %s not found", key)
}

// UpdateWorkloadLastSeen 更新工作负载最后可见时间
func (tm *TTLManager) UpdateWorkloadLastSeen(workloadID string) error {
	tm.registry.mu.Lock()
	defer tm.registry.mu.Unlock()
	
	if workload, exists := tm.registry.workloads[workloadID]; exists {
		now := time.Now()
		workload.Status.LastSeen = now
		workload.Metadata.LastSeen = now
		return nil
	}
	
	return fmt.Errorf("workload %s not found", workloadID)
}

// GetExpiredServices 获取过期的服务列表
func (tm *TTLManager) GetExpiredServices() ([]*types.Service, error) {
	tm.registry.mu.RLock()
	defer tm.registry.mu.RUnlock()
	
	var expired []*types.Service
	now := time.Now()
	
	for _, service := range tm.registry.services {
		if tm.IsServiceExpired(service, now) {
			serviceCopy := *service
			expired = append(expired, &serviceCopy)
		}
	}
	
	return expired, nil
}

// GetExpiredWorkloads 获取过期的工作负载列表
func (tm *TTLManager) GetExpiredWorkloads() ([]*types.Workload, error) {
	tm.registry.mu.RLock()
	defer tm.registry.mu.RUnlock()
	
	var expired []*types.Workload
	now := time.Now()
	
	for _, workload := range tm.registry.workloads {
		if tm.IsWorkloadExpired(workload, now) {
			workloadCopy := *workload
			expired = append(expired, &workloadCopy)
		}
	}
	
	return expired, nil
}

// GetExpiredEndpoints 获取过期的端点列表
func (tm *TTLManager) GetExpiredEndpoints() ([]*types.Endpoint, error) {
	tm.registry.mu.RLock()
	defer tm.registry.mu.RUnlock()
	
	var expired []*types.Endpoint
	now := time.Now()
	
	for _, endpoints := range tm.registry.endpoints {
		for _, endpoint := range endpoints {
			if tm.IsEndpointExpired(endpoint, now) {
				endpointCopy := *endpoint
				expired = append(expired, &endpointCopy)
			}
		}
	}
	
	return expired, nil
}

// GetTTLStats 获取 TTL 统计信息
func (tm *TTLManager) GetTTLStats() map[string]interface{} {
	tm.registry.mu.RLock()
	defer tm.registry.mu.RUnlock()
	
	now := time.Now()
	
	totalServices := len(tm.registry.services)
	expiredServices := 0
	totalWorkloads := len(tm.registry.workloads)
	expiredWorkloads := 0
	totalEndpoints := 0
	expiredEndpoints := 0
	
	// 统计过期的服务
	for _, service := range tm.registry.services {
		if tm.IsServiceExpired(service, now) {
			expiredServices++
		}
	}
	
	// 统计过期的工作负载
	for _, workload := range tm.registry.workloads {
		if tm.IsWorkloadExpired(workload, now) {
			expiredWorkloads++
		}
	}
	
	// 统计过期的端点
	for _, endpoints := range tm.registry.endpoints {
		totalEndpoints += len(endpoints)
		for _, endpoint := range endpoints {
			if tm.IsEndpointExpired(endpoint, now) {
				expiredEndpoints++
			}
		}
	}
	
	return map[string]interface{}{
		"total_services":     totalServices,
		"expired_services":   expiredServices,
		"total_workloads":    totalWorkloads,
		"expired_workloads":  expiredWorkloads,
		"total_endpoints":    totalEndpoints,
		"expired_endpoints":  expiredEndpoints,
		"default_ttls":       tm.defaultTTLs,
		"heartbeat_ttls":     tm.heartbeatTTLs,
	}
}