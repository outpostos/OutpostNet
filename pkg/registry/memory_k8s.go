package registry

import (
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// K8sMemoryRegistry 基于内存的 K8s 风格服务注册表实现
type K8sMemoryRegistry struct {
	services   map[string]*types.Service     // key: namespace/name
	workloads  map[string]*types.Workload    // key: workload_id  
	endpoints  map[string][]*types.Endpoint  // key: namespace/name (自动生成)
	namespaces map[string]*K8sNamespace      // key: namespace_name
	mu         sync.RWMutex
	
	// K8s 兼容性增强
	labelIndexes    map[string]map[string][]string   // label_key -> label_value -> [workload_ids]
	serviceIndexes  map[string][]string               // namespace -> [service_names]
	watchChannels   map[string]chan *K8sEvent         // watch_id -> event_channel
	nextWatchID     int64
	
	// 约束条件过滤器和健康检查器
	constraintFilter *ConstraintFilter
	healthChecker    *HealthChecker
}

// K8sNamespace K8s命名空间定义
type K8sNamespace struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Status      string            `json:"status"` // Active, Terminating
	CreatedAt   time.Time         `json:"created_at"`
}

// K8sEvent K8s风格的事件
type K8sEvent struct {
	Type   string      `json:"type"`   // ADDED, MODIFIED, DELETED
	Object interface{} `json:"object"` // Service, Workload, Endpoint
}

// NewK8sMemoryRegistry 创建新的 K8s 风格内存注册表
func NewK8sMemoryRegistry() *K8sMemoryRegistry {
	registry := &K8sMemoryRegistry{
		services:         make(map[string]*types.Service),
		workloads:        make(map[string]*types.Workload),
		endpoints:        make(map[string][]*types.Endpoint),
		namespaces:       make(map[string]*K8sNamespace),
		labelIndexes:     make(map[string]map[string][]string),
		serviceIndexes:   make(map[string][]string),
		watchChannels:    make(map[string]chan *K8sEvent),
		nextWatchID:      1,
		constraintFilter: NewConstraintFilter(),
		healthChecker:    NewHealthChecker(DefaultHealthCheckConfig()),
	}
	
	// 创建默认命名空间
	registry.ensureNamespace("default")
	registry.ensureNamespace("kube-system")
	
	return registry
}

// GetHealthChecker 获取健康检查器 (用于集成到服务器中)
func (r *K8sMemoryRegistry) GetHealthChecker() *HealthChecker {
	return r.healthChecker
}

// RegisterService 注册服务定义
func (r *K8sMemoryRegistry) RegisterService(service *types.Service) error {
	if service == nil {
		return fmt.Errorf("service is nil")
	}

	if service.Metadata.Name == "" {
		return fmt.Errorf("service name is required")
	}

	if service.Metadata.Namespace == "" {
		return fmt.Errorf("service namespace is required")
	}

	// K8s标准验证
	if err := r.validateK8sServiceName(service.Metadata.Name); err != nil {
		return fmt.Errorf("invalid service name: %v", err)
	}

	if err := r.validateK8sNamespaceName(service.Metadata.Namespace); err != nil {
		return fmt.Errorf("invalid namespace name: %v", err)
	}

	// 验证约束条件
	if service.Spec.Constraints != nil {
		if err := r.constraintFilter.ValidateConstraints(service.Spec.Constraints); err != nil {
			return fmt.Errorf("invalid service constraints: %w", err)
		}
	}

	key := r.serviceKey(service.Metadata.Namespace, service.Metadata.Name)

	r.mu.Lock()
	defer r.mu.Unlock()

	// 确保命名空间存在
	r.ensureNamespace(service.Metadata.Namespace)

	// 检查是否为更新操作
	isUpdate := false
	if _, exists := r.services[key]; exists {
		isUpdate = true
	}

	// 复制服务对象
	serviceCopy := *service
	r.services[key] = &serviceCopy

	// 更新服务索引
	r.updateServiceIndex(service.Metadata.Namespace, service.Metadata.Name)

	// 初始化端点列表
	if _, exists := r.endpoints[key]; !exists {
		r.endpoints[key] = make([]*types.Endpoint, 0)
	}

	// 注册服务后立即同步端点
	r.syncEndpointsForService(&serviceCopy)

	// 发送K8s事件
	eventType := "ADDED"
	if isUpdate {
		eventType = "MODIFIED"
	}
	r.broadcastEvent(&K8sEvent{
		Type:   eventType,
		Object: &serviceCopy,
	})

	return nil
}

// GetService 获取服务定义
func (r *K8sMemoryRegistry) GetService(name, namespace string) (*types.Service, error) {
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

// ListServices 列出所有服务
func (r *K8sMemoryRegistry) ListServices(namespace string) ([]*types.Service, error) {
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

// RegisterWorkload 注册工作负载实例 (K8s Pod 风格)
func (r *K8sMemoryRegistry) RegisterWorkload(workload *types.Workload) error {
	if workload == nil {
		return fmt.Errorf("workload is nil")
	}

	// 生成工作负载 ID (如果未设置)
	if workload.Metadata.ID == "" {
		workload.Metadata.ID = r.generateWorkloadID()
	}

	// 设置创建时间和状态
	workload.Metadata.CreatedAt = time.Now()
	workload.Status.Ready = true
	workload.Status.LastSeen = time.Now()
	workload.Status.Reason = "registered"

	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查是否为更新操作
	isUpdate := false
	if _, exists := r.workloads[workload.Metadata.ID]; exists {
		isUpdate = true
	}

	// 复制工作负载对象
	workloadCopy := *workload
	r.workloads[workload.Metadata.ID] = &workloadCopy

	// 更新标签索引
	r.updateLabelIndex(workload.Metadata.ID, workload.Metadata.Labels)

	// 注册工作负载后立即同步所有服务的端点
	r.syncAllEndpoints()

	// 发送K8s事件
	eventType := "ADDED"
	if isUpdate {
		eventType = "MODIFIED"
	}
	r.broadcastEvent(&K8sEvent{
		Type:   eventType,
		Object: &workloadCopy,
	})

	return nil
}

// UnregisterWorkload 注销工作负载实例
func (r *K8sMemoryRegistry) UnregisterWorkload(workloadID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	workload, exists := r.workloads[workloadID]
	if !exists {
		return fmt.Errorf("workload %s not found", workloadID)
	}

	// 从标签索引中移除
	r.removeFromLabelIndex(workloadID, workload.Metadata.Labels)

	// 从健康检查器中移除相关端点
	// 注意：这里需要找到与该工作负载相关的所有端点ID
	for _, endpoints := range r.endpoints {
		for _, ep := range endpoints {
			if ep.ID != "" && ep.ID[:len(workloadID)] == workloadID {
				r.healthChecker.UnregisterEndpoint(ep.ID)
			}
		}
	}

	// 删除工作负载
	delete(r.workloads, workloadID)

	// 注销工作负载后立即同步所有服务的端点
	r.syncAllEndpoints()

	// 发送K8s事件
	r.broadcastEvent(&K8sEvent{
		Type:   "DELETED",
		Object: workload,
	})

	return nil
}

// GetWorkload 获取工作负载实例
func (r *K8sMemoryRegistry) GetWorkload(workloadID string) (*types.Workload, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workload, exists := r.workloads[workloadID]
	if !exists {
		return nil, fmt.Errorf("workload %s not found", workloadID)
	}

	// 返回工作负载的副本
	workloadCopy := *workload
	return &workloadCopy, nil
}

// ListWorkloads 列出工作负载实例 (支持标签选择器)
func (r *K8sMemoryRegistry) ListWorkloads(selector map[string]string) ([]*types.Workload, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var workloads []*types.Workload
	for _, workload := range r.workloads {
		if r.matchLabels(workload.Metadata.Labels, selector) {
			workloadCopy := *workload
			workloads = append(workloads, &workloadCopy)
		}
	}

	return workloads, nil
}

// GetEndpoints 获取服务的端点列表 (由工作负载自动生成)
func (r *K8sMemoryRegistry) GetEndpoints(serviceName, namespace string, selector map[string]string) ([]*types.Endpoint, error) {
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

	// 如果没有额外的选择器，返回所有就绪的端点
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

// GetEndpointsWithConstraints 根据服务约束条件获取端点列表
func (r *K8sMemoryRegistry) GetEndpointsWithConstraints(serviceName, namespace string) ([]*types.Endpoint, error) {
	key := r.serviceKey(namespace, serviceName)

	r.mu.RLock()
	service, exists := r.services[key]
	if !exists {
		r.mu.RUnlock()
		return nil, fmt.Errorf("service %s/%s not found", namespace, serviceName)
	}

	allEndpoints := r.endpoints[key]
	r.mu.RUnlock()

	if allEndpoints == nil {
		return []*types.Endpoint{}, nil
	}

	// 首先应用基本的选择器过滤（如果有）
	var filteredEndpoints []*types.Endpoint
	for _, ep := range allEndpoints {
		if r.matchLabels(ep.Labels, service.Spec.Selector) {
			epCopy := *ep
			filteredEndpoints = append(filteredEndpoints, &epCopy)
		}
	}

	// 然后应用约束条件过滤
	if service.Spec.Constraints != nil {
		var err error
		filteredEndpoints, err = r.constraintFilter.FilterEndpoints(filteredEndpoints, service.Spec.Constraints)
		if err != nil {
			return nil, fmt.Errorf("constraint filtering failed: %w", err)
		}
	} else {
		// 如果没有约束条件，默认只返回健康的端点
		var healthyEndpoints []*types.Endpoint
		for _, ep := range filteredEndpoints {
			if ep.Status.Ready {
				healthyEndpoints = append(healthyEndpoints, ep)
			}
		}
		filteredEndpoints = healthyEndpoints
	}
	
	// 最后应用健康检查过滤 (所有端点都要经过健康检查)
	filteredEndpoints = r.healthChecker.FilterHealthyEndpoints(filteredEndpoints)

	return filteredEndpoints, nil
}

// RegisterEndpoint 注册端点到指定服务
func (r *K8sMemoryRegistry) RegisterEndpoint(serviceName, namespace string, endpoint *types.Endpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	key := r.serviceKey(namespace, serviceName)
	if _, exists := r.services[key]; !exists {
		return fmt.Errorf("service %s/%s not found", namespace, serviceName)
	}
	
	// 添加端点到服务的端点列表
	r.endpoints[key] = append(r.endpoints[key], endpoint)
	
	return nil
}

// UnregisterEndpoint 从指定服务移除端点
func (r *K8sMemoryRegistry) UnregisterEndpoint(serviceName, namespace, endpointID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	key := r.serviceKey(namespace, serviceName)
	endpoints := r.endpoints[key]
	
	// 查找并移除端点
	for i, ep := range endpoints {
		if ep.ID == endpointID {
			// 移除端点
			r.endpoints[key] = append(endpoints[:i], endpoints[i+1:]...)
			return nil
		}
	}
	
	return fmt.Errorf("endpoint %s not found in service %s/%s", endpointID, namespace, serviceName)
}

// SyncEndpoints 同步所有服务的端点 (Controller 调用)
func (r *K8sMemoryRegistry) SyncEndpoints() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.syncAllEndpoints()
	return nil
}

// 内部方法：同步所有服务的端点
func (r *K8sMemoryRegistry) syncAllEndpoints() {
	for _, service := range r.services {
		r.syncEndpointsForService(service)
	}
}

// 内部方法：为特定服务同步端点
func (r *K8sMemoryRegistry) syncEndpointsForService(service *types.Service) {
	key := r.serviceKey(service.Metadata.Namespace, service.Metadata.Name)
	
	// 清空现有端点
	r.endpoints[key] = make([]*types.Endpoint, 0)

	// 根据服务的 selector 查找匹配的工作负载
	for _, workload := range r.workloads {
		if !workload.Status.Ready {
			continue
		}

		// 检查工作负载标签是否匹配服务选择器
		if r.matchLabels(workload.Metadata.Labels, service.Spec.Selector) {
			// 为每个服务端口创建端点
			for _, port := range service.Spec.Ports {
				endpoint := &types.Endpoint{
					ID:       fmt.Sprintf("%s-%s", workload.Metadata.ID, port.Name),
					IP:       workload.Status.RegisterIP,
					Port:     workload.Spec.LocalPort,
					Protocol: port.Protocol,
					Labels:   make(map[string]string),
					Status: types.EndpointStatus{
						Ready:   true,
						Reason:  "auto-discovered",
						Message: fmt.Sprintf("auto-generated from workload %s", workload.Metadata.ID),
					},
					LastSeen: workload.Status.LastSeen,
				}

				// 复制工作负载的标签
				for k, v := range workload.Metadata.Labels {
					endpoint.Labels[k] = v
				}

				r.endpoints[key] = append(r.endpoints[key], endpoint)
				
				// 注册端点到健康检查器
				r.healthChecker.RegisterEndpoint(endpoint.ID)
				r.healthChecker.UpdateEndpointLastSeen(endpoint.ID)
			}
		}
	}
}

// 内部方法：生成工作负载 ID
func (r *K8sMemoryRegistry) generateWorkloadID() string {
	return fmt.Sprintf("workload-%d", time.Now().UnixNano())
}

// 内部方法：生成服务键
func (r *K8sMemoryRegistry) serviceKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// 内部方法：检查标签是否匹配选择器
func (r *K8sMemoryRegistry) matchLabels(labels, selector map[string]string) bool {
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
func (r *K8sMemoryRegistry) GetStats() map[string]interface{} {
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
		"total_workloads":  len(r.workloads),
		"total_endpoints":  totalEndpoints,
		"ready_endpoints":  readyEndpoints,
		"total_namespaces": len(r.namespaces),
		"active_watchers":  len(r.watchChannels),
		"registry_type":    "k8s-memory",
	}
}

// === K8s 兼容性增强方法 ===

// CreateNamespace 创建命名空间
func (r *K8sMemoryRegistry) CreateNamespace(name string, labels, annotations map[string]string) error {
	if err := r.validateK8sNamespaceName(name); err != nil {
		return fmt.Errorf("invalid namespace name: %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.namespaces[name]; exists {
		return fmt.Errorf("namespace %s already exists", name)
	}

	namespace := &K8sNamespace{
		Name:        name,
		Labels:      labels,
		Annotations: annotations,
		Status:      "Active",
		CreatedAt:   time.Now(),
	}

	r.namespaces[name] = namespace
	r.serviceIndexes[name] = make([]string, 0)

	// 发送事件
	r.broadcastEvent(&K8sEvent{
		Type:   "ADDED",
		Object: namespace,
	})

	return nil
}

// DeleteNamespace 删除命名空间
func (r *K8sMemoryRegistry) DeleteNamespace(name string) error {
	if name == "default" || name == "kube-system" {
		return fmt.Errorf("cannot delete system namespace: %s", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	namespace, exists := r.namespaces[name]
	if !exists {
		return fmt.Errorf("namespace %s not found", name)
	}

	// 检查命名空间是否为空
	services := r.serviceIndexes[name]
	if len(services) > 0 {
		return fmt.Errorf("namespace %s is not empty, contains %d services", name, len(services))
	}

	// 删除命名空间
	delete(r.namespaces, name)
	delete(r.serviceIndexes, name)

	// 发送事件
	r.broadcastEvent(&K8sEvent{
		Type:   "DELETED",
		Object: namespace,
	})

	return nil
}

// ListNamespaces 列出所有命名空间
func (r *K8sMemoryRegistry) ListNamespaces() ([]*K8sNamespace, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	namespaces := make([]*K8sNamespace, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		nsCopy := *ns
		namespaces = append(namespaces, &nsCopy)
	}

	return namespaces, nil
}

// GetNamespace 获取命名空间
func (r *K8sMemoryRegistry) GetNamespace(name string) (*K8sNamespace, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	namespace, exists := r.namespaces[name]
	if !exists {
		return nil, fmt.Errorf("namespace %s not found", name)
	}

	nsCopy := *namespace
	return &nsCopy, nil
}

// ListServicesByLabels 根据标签选择器列出服务（K8s标准）
func (r *K8sMemoryRegistry) ListServicesByLabels(namespace string, labelSelector map[string]string) ([]*types.Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var services []*types.Service
	for _, service := range r.services {
		// 命名空间过滤
		if namespace != "" && service.Metadata.Namespace != namespace {
			continue
		}

		// 标签选择器过滤
		if r.matchLabels(service.Metadata.Labels, labelSelector) {
			serviceCopy := *service
			services = append(services, &serviceCopy)
		}
	}

	return services, nil
}

// WatchServices 监控服务变化（K8s Watch API风格）
func (r *K8sMemoryRegistry) WatchServices(namespace string, labelSelector map[string]string) (string, <-chan *K8sEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	watchID := fmt.Sprintf("watch-%d", r.nextWatchID)
	r.nextWatchID++

	eventChan := make(chan *K8sEvent, 100)
	r.watchChannels[watchID] = eventChan

	// 发送现有服务的ADDED事件
	go func() {
		r.mu.RLock()
		defer r.mu.RUnlock()

		for _, service := range r.services {
			if namespace != "" && service.Metadata.Namespace != namespace {
				continue
			}

			if r.matchLabels(service.Metadata.Labels, labelSelector) {
				select {
				case eventChan <- &K8sEvent{
					Type:   "ADDED",
					Object: service,
				}:
				default:
					// 缓冲区满，跳过
				}
			}
		}
	}()

	return watchID, eventChan, nil
}

// StopWatch 停止监控
func (r *K8sMemoryRegistry) StopWatch(watchID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if eventChan, exists := r.watchChannels[watchID]; exists {
		close(eventChan)
		delete(r.watchChannels, watchID)
		return nil
	}

	return fmt.Errorf("watch %s not found", watchID)
}

// GetServiceEndpoints 获取服务的端点（K8s Endpoints API风格）
func (r *K8sMemoryRegistry) GetServiceEndpoints(serviceName, namespace string) ([]*types.Endpoint, error) {
	return r.GetEndpoints(serviceName, namespace, nil)
}

// === 内部辅助方法 ===

// ensureNamespace 确保命名空间存在
func (r *K8sMemoryRegistry) ensureNamespace(namespace string) {
	if _, exists := r.namespaces[namespace]; !exists {
		r.namespaces[namespace] = &K8sNamespace{
			Name:      namespace,
			Labels:    make(map[string]string),
			Status:    "Active",
			CreatedAt: time.Now(),
		}
		r.serviceIndexes[namespace] = make([]string, 0)
	}
}

// updateServiceIndex 更新服务索引
func (r *K8sMemoryRegistry) updateServiceIndex(namespace, serviceName string) {
	services := r.serviceIndexes[namespace]
	
	// 检查是否已存在
	for _, name := range services {
		if name == serviceName {
			return
		}
	}
	
	// 添加到索引
	r.serviceIndexes[namespace] = append(services, serviceName)
}

// removeFromServiceIndex 从服务索引中移除
func (r *K8sMemoryRegistry) removeFromServiceIndex(namespace, serviceName string) {
	services := r.serviceIndexes[namespace]
	for i, name := range services {
		if name == serviceName {
			r.serviceIndexes[namespace] = append(services[:i], services[i+1:]...)
			break
		}
	}
}

// updateLabelIndex 更新标签索引
func (r *K8sMemoryRegistry) updateLabelIndex(workloadID string, labels map[string]string) {
	for key, value := range labels {
		if r.labelIndexes[key] == nil {
			r.labelIndexes[key] = make(map[string][]string)
		}
		
		if r.labelIndexes[key][value] == nil {
			r.labelIndexes[key][value] = make([]string, 0)
		}
		
		// 检查是否已存在
		workloadIDs := r.labelIndexes[key][value]
		for _, id := range workloadIDs {
			if id == workloadID {
				return
			}
		}
		
		r.labelIndexes[key][value] = append(workloadIDs, workloadID)
	}
}

// removeFromLabelIndex 从标签索引中移除
func (r *K8sMemoryRegistry) removeFromLabelIndex(workloadID string, labels map[string]string) {
	for key, value := range labels {
		if r.labelIndexes[key] != nil && r.labelIndexes[key][value] != nil {
			workloadIDs := r.labelIndexes[key][value]
			for i, id := range workloadIDs {
				if id == workloadID {
					r.labelIndexes[key][value] = append(workloadIDs[:i], workloadIDs[i+1:]...)
					break
				}
			}
		}
	}
}

// broadcastEvent 广播事件到所有监控者
func (r *K8sMemoryRegistry) broadcastEvent(event *K8sEvent) {
	for _, eventChan := range r.watchChannels {
		select {
		case eventChan <- event:
		default:
			// 缓冲区满，跳过
		}
	}
}

// validateK8sServiceName 验证K8s服务名称
func (r *K8sMemoryRegistry) validateK8sServiceName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("service name cannot be empty")
	}
	
	if len(name) > 63 {
		return fmt.Errorf("service name cannot exceed 63 characters")
	}
	
	// K8s DNS标准：只能包含字母、数字和连字符，且不能以连字符开始或结束
	for i, r := range name {
		if i == 0 || i == len(name)-1 {
			if r == '-' {
				return fmt.Errorf("service name cannot start or end with hyphen")
			}
		}
		
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			return fmt.Errorf("service name can only contain lowercase letters, numbers, and hyphens")
		}
	}
	
	return nil
}

// validateK8sNamespaceName 验证K8s命名空间名称
func (r *K8sMemoryRegistry) validateK8sNamespaceName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("namespace name cannot be empty")
	}
	
	if len(name) > 63 {
		return fmt.Errorf("namespace name cannot exceed 63 characters")
	}
	
	// K8s DNS标准
	for i, r := range name {
		if i == 0 || i == len(name)-1 {
			if r == '-' {
				return fmt.Errorf("namespace name cannot start or end with hyphen")
			}
		}
		
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			return fmt.Errorf("namespace name can only contain lowercase letters, numbers, and hyphens")
		}
	}
	
	return nil
}