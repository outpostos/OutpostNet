package registry

import (
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// MemoryRegistry 基于内存的服务注册表实现
type MemoryRegistry struct {
	services  map[string]*types.Service   // key: namespace/name
	workloads map[string]*types.Workload  // key: workload_id
	endpoints map[string][]*types.Endpoint // key: namespace/name (自动生成)
	mu        sync.RWMutex
}

// NewMemoryRegistry 创建新的内存注册表
func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		services:  make(map[string]*types.Service),
		workloads: make(map[string]*types.Workload),
		endpoints: make(map[string][]*types.Endpoint),
	}
}

// RegisterService 注册服务
func (r *MemoryRegistry) RegisterService(service *types.Service) error {
	if service == nil {
		return fmt.Errorf("service is nil")
	}

	if service.Metadata.Name == "" {
		return fmt.Errorf("service name is required")
	}

	if service.Metadata.Namespace == "" {
		return fmt.Errorf("service namespace is required")
	}

	key := r.serviceKey(service.Metadata.Namespace, service.Metadata.Name)

	r.mu.Lock()
	defer r.mu.Unlock()

	// 复制服务对象
	serviceCopy := *service
	r.services[key] = &serviceCopy

	// 如果该服务的端点列表不存在，初始化为空切片
	if _, exists := r.endpoints[key]; !exists {
		r.endpoints[key] = make([]*types.Endpoint, 0)
	}

	return nil
}

// RegisterEndpoint 注册端点
func (r *MemoryRegistry) RegisterEndpoint(serviceName, namespace string, endpoint *types.Endpoint) error {
	if endpoint == nil {
		return fmt.Errorf("endpoint is nil")
	}

	if endpoint.ID == "" {
		return fmt.Errorf("endpoint ID is required")
	}

	if endpoint.IP == "" {
		return fmt.Errorf("endpoint IP is required")
	}

	if endpoint.Port <= 0 {
		return fmt.Errorf("endpoint port must be positive")
	}

	key := r.serviceKey(namespace, serviceName)

	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查服务是否存在
	if _, exists := r.services[key]; !exists {
		return fmt.Errorf("service %s/%s not found", namespace, serviceName)
	}

	// 初始化端点列表
	if r.endpoints[key] == nil {
		r.endpoints[key] = make([]*types.Endpoint, 0)
	}

	// 检查端点是否已存在
	for i, ep := range r.endpoints[key] {
		if ep.ID == endpoint.ID {
			// 更新现有端点
			endpointCopy := *endpoint
			endpointCopy.LastSeen = time.Now()
			r.endpoints[key][i] = &endpointCopy
			return nil
		}
	}

	// 添加新端点
	endpointCopy := *endpoint
	endpointCopy.LastSeen = time.Now()
	if endpointCopy.Status.Ready == false && endpointCopy.Status.Reason == "" {
		endpointCopy.Status.Ready = true
		endpointCopy.Status.Reason = "registered"
	}

	r.endpoints[key] = append(r.endpoints[key], &endpointCopy)

	return nil
}

// GetService 获取服务
func (r *MemoryRegistry) GetService(name, namespace string) (*types.Service, error) {
	key := r.serviceKey(namespace, name)

	r.mu.RLock()
	defer r.mu.RUnlock()

	service, exists := r.services[key]
	if !exists {
		return nil, fmt.Errorf("service %s/%s not found", namespace, name)
	}

	// 返回服务的副本
	serviceCopy := *service
	return &serviceCopy, nil
}

// GetEndpoints 获取服务的端点列表
func (r *MemoryRegistry) GetEndpoints(serviceName, namespace string, selector map[string]string) ([]*types.Endpoint, error) {
	key := r.serviceKey(namespace, serviceName)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// 检查服务是否存在
	if _, exists := r.services[key]; !exists {
		return nil, fmt.Errorf("service %s/%s not found", namespace, serviceName)
	}

	allEndpoints := r.endpoints[key]
	if allEndpoints == nil {
		return []*types.Endpoint{}, nil
	}

	// 如果没有选择器，返回所有就绪的端点
	if len(selector) == 0 {
		var readyEndpoints []*types.Endpoint
		for _, ep := range allEndpoints {
			if ep.Status.Ready {
				epCopy := *ep
				readyEndpoints = append(readyEndpoints, &epCopy)
			}
		}
		return readyEndpoints, nil
	}

	// 根据选择器过滤端点
	var matchedEndpoints []*types.Endpoint
	for _, ep := range allEndpoints {
		if !ep.Status.Ready {
			continue
		}

		// 检查端点标签是否匹配选择器
		if r.matchLabels(ep.Labels, selector) {
			epCopy := *ep
			matchedEndpoints = append(matchedEndpoints, &epCopy)
		}
	}

	return matchedEndpoints, nil
}

// UnregisterEndpoint 删除端点
func (r *MemoryRegistry) UnregisterEndpoint(serviceName, namespace, endpointID string) error {
	key := r.serviceKey(namespace, serviceName)

	r.mu.Lock()
	defer r.mu.Unlock()

	endpoints := r.endpoints[key]
	if endpoints == nil {
		return fmt.Errorf("service %s/%s not found", namespace, serviceName)
	}

	// 查找并删除端点
	for i, ep := range endpoints {
		if ep.ID == endpointID {
			// 从切片中删除元素
			r.endpoints[key] = append(endpoints[:i], endpoints[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("endpoint %s not found in service %s/%s", endpointID, namespace, serviceName)
}

// ListServices 列出所有服务
func (r *MemoryRegistry) ListServices(namespace string) ([]*types.Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var services []*types.Service
	for _, service := range r.services {
		if namespace == "" || service.Metadata.Namespace == namespace {
			serviceCopy := *service
			services = append(services, &serviceCopy)
		}
	}

	return services, nil
}

// serviceKey 生成服务的唯一键
func (r *MemoryRegistry) serviceKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// matchLabels 检查标签是否匹配选择器
func (r *MemoryRegistry) matchLabels(labels, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}

	for key, value := range selector {
		labelValue, exists := labels[key]
		if !exists || labelValue != value {
			return false
		}
	}

	return true
}

// GetStats 获取注册表统计信息
func (r *MemoryRegistry) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	totalEndpoints := 0
	readyEndpoints := 0

	for _, endpoints := range r.endpoints {
		totalEndpoints += len(endpoints)
		for _, ep := range endpoints {
			if ep.Status.Ready {
				readyEndpoints++
			}
		}
	}

	return map[string]interface{}{
		"total_services":   len(r.services),
		"total_endpoints":  totalEndpoints,
		"ready_endpoints":  readyEndpoints,
		"registry_type":    "memory",
	}
}