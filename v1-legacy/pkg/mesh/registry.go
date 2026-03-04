package mesh

import (
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DistributedServiceRegistry 分布式服务注册表
type DistributedServiceRegistry struct {
	localRegistry  types.ServiceRegistry
	meshCluster    ClusterInterface
	
	// 远程服务缓存
	remoteServices map[string]*RemoteService
	remoteEndpoints map[string][]*RemoteEndpoint
	remoteMu       sync.RWMutex
	
	// 同步配置
	syncInterval   time.Duration
	cacheTimeout   time.Duration
	
	// 事件通知
	eventHandlers  []ServiceEventHandler
	eventMu        sync.RWMutex
}

// RemoteService 远程服务信息
type RemoteService struct {
	*types.Service
	SourceNode    string    `json:"source_node"`
	LastSync      time.Time `json:"last_sync"`
	Version       int64     `json:"version"`
	IsLocal       bool      `json:"is_local"`
}

// RemoteEndpoint 远程端点信息
type RemoteEndpoint struct {
	*types.Endpoint
	SourceNode    string    `json:"source_node"`
	LastSync      time.Time `json:"last_sync"`
	Version       int64     `json:"version"`
	IsLocal       bool      `json:"is_local"`
}

// ServiceEventHandler 服务事件处理器
type ServiceEventHandler interface {
	OnServiceRegistered(service *types.Service)
	OnServiceUnregistered(serviceName, namespace string)
	OnServiceUpdated(service *types.Service)
	OnEndpointRegistered(endpoint *types.Endpoint)
	OnEndpointUnregistered(endpointID string)
	OnEndpointUpdated(endpoint *types.Endpoint)
}

// NewDistributedServiceRegistry 创建分布式服务注册表
func NewDistributedServiceRegistry(localRegistry types.ServiceRegistry, meshCluster ClusterInterface) *DistributedServiceRegistry {
	return &DistributedServiceRegistry{
		localRegistry:   localRegistry,
		meshCluster:     meshCluster,
		remoteServices:  make(map[string]*RemoteService),
		remoteEndpoints: make(map[string][]*RemoteEndpoint),
		syncInterval:    30 * time.Second,
		cacheTimeout:    5 * time.Minute,
		eventHandlers:   make([]ServiceEventHandler, 0),
	}
}

// RegisterService 注册服务
func (r *DistributedServiceRegistry) RegisterService(service *types.Service) error {
	// 本地注册
	if err := r.localRegistry.RegisterService(service); err != nil {
		return fmt.Errorf("failed to register service locally: %w", err)
	}
	
	// 通知事件处理器
	r.notifyServiceRegistered(service)
	
	// 同步到其他节点
	go r.syncServiceToCluster(service)
	
	return nil
}

// GetService 获取服务
func (r *DistributedServiceRegistry) GetService(name, namespace string) (*types.Service, error) {
	serviceKey := fmt.Sprintf("%s.%s", name, namespace)
	
	// 首先尝试本地注册表
	if service, err := r.localRegistry.GetService(name, namespace); err == nil {
		return service, nil
	}
	
	// 检查远程服务缓存
	r.remoteMu.RLock()
	remoteService, exists := r.remoteServices[serviceKey]
	r.remoteMu.RUnlock()
	
	if exists && !r.isCacheExpired(remoteService.LastSync) {
		return remoteService.Service, nil
	}
	
	// 从集群查询服务
	if service, err := r.queryServiceFromCluster(name, namespace); err == nil {
		return service, nil
	}
	
	return nil, fmt.Errorf("service %s.%s not found", name, namespace)
}

// ListServices 列出服务
func (r *DistributedServiceRegistry) ListServices(namespace string) ([]*types.Service, error) {
	// 获取本地服务
	localServices, err := r.localRegistry.ListServices(namespace)
	if err != nil {
		return nil, err
	}
	
	// 获取远程服务
	remoteServices := r.getRemoteServices(namespace)
	
	// 合并结果
	serviceMap := make(map[string]*types.Service)
	
	// 添加本地服务
	for _, service := range localServices {
		key := fmt.Sprintf("%s.%s", service.Metadata.Name, service.Metadata.Namespace)
		serviceMap[key] = service
	}
	
	// 添加远程服务（本地服务优先）
	for _, remoteService := range remoteServices {
		key := fmt.Sprintf("%s.%s", remoteService.Service.Metadata.Name, remoteService.Service.Metadata.Namespace)
		if _, exists := serviceMap[key]; !exists {
			serviceMap[key] = remoteService.Service
		}
	}
	
	// 转换为切片
	result := make([]*types.Service, 0, len(serviceMap))
	for _, service := range serviceMap {
		result = append(result, service)
	}
	
	return result, nil
}

// RegisterWorkload 注册工作负载
func (r *DistributedServiceRegistry) RegisterWorkload(workload *types.Workload) error {
	// 本地注册
	if err := r.localRegistry.RegisterWorkload(workload); err != nil {
		return fmt.Errorf("failed to register workload locally: %w", err)
	}
	
	// 同步端点到集群
	go r.syncWorkloadToCluster(workload)
	
	return nil
}

// UnregisterWorkload 注销工作负载
func (r *DistributedServiceRegistry) UnregisterWorkload(workloadID string) error {
	// 本地注销
	if err := r.localRegistry.UnregisterWorkload(workloadID); err != nil {
		return fmt.Errorf("failed to unregister workload locally: %w", err)
	}
	
	// 同步到集群
	go r.syncWorkloadUnregistrationToCluster(workloadID)
	
	return nil
}

// GetWorkload 获取工作负载
func (r *DistributedServiceRegistry) GetWorkload(workloadID string) (*types.Workload, error) {
	return r.localRegistry.GetWorkload(workloadID)
}

// ListWorkloads 列出工作负载
func (r *DistributedServiceRegistry) ListWorkloads(selector map[string]string) ([]*types.Workload, error) {
	return r.localRegistry.ListWorkloads(selector)
}

// GetEndpoints 获取端点
func (r *DistributedServiceRegistry) GetEndpoints(serviceName, namespace string, selector map[string]string) ([]*types.Endpoint, error) {
	// 获取本地端点
	localEndpoints, err := r.localRegistry.GetEndpoints(serviceName, namespace, selector)
	if err != nil {
		localEndpoints = []*types.Endpoint{}
	}
	
	// 获取远程端点
	remoteEndpoints := r.getRemoteEndpoints(serviceName, namespace, selector)
	
	// 合并结果
	allEndpoints := make([]*types.Endpoint, 0, len(localEndpoints)+len(remoteEndpoints))
	allEndpoints = append(allEndpoints, localEndpoints...)
	
	for _, remoteEndpoint := range remoteEndpoints {
		allEndpoints = append(allEndpoints, remoteEndpoint.Endpoint)
	}
	
	return allEndpoints, nil
}

// SyncEndpoints 同步端点
func (r *DistributedServiceRegistry) SyncEndpoints() error {
	return r.localRegistry.SyncEndpoints()
}

// syncServiceToCluster 同步服务到集群
func (r *DistributedServiceRegistry) syncServiceToCluster(service *types.Service) {
	// 创建服务同步消息
	protoService := r.convertToProtoService(service)
	
	serviceSync := &protocol.ServiceSync{
		Services:  []*protocol.Service{protoService},
		Endpoints: []*protocol.Endpoint{},
		Version:   int32(time.Now().Unix()),
		SyncTime:  timestamppb.Now(),
	}
	
	// 通过gossip协议广播
	if gossipManager := r.meshCluster.GetGossipManager(); gossipManager != nil {
		gossipManager.BroadcastServiceSync(serviceSync)
	}
}

// syncWorkloadToCluster 同步工作负载到集群
func (r *DistributedServiceRegistry) syncWorkloadToCluster(workload *types.Workload) {
	// 获取工作负载对应的端点
	endpoints, err := r.localRegistry.GetEndpoints("", "", workload.Metadata.Labels)
	if err != nil {
		return
	}
	
	// 转换为协议格式
	protoEndpoints := make([]*protocol.Endpoint, len(endpoints))
	for i, endpoint := range endpoints {
		protoEndpoints[i] = r.convertToProtoEndpoint(endpoint)
	}
	
	// 创建服务同步消息
	serviceSync := &protocol.ServiceSync{
		Services:  []*protocol.Service{},
		Endpoints: protoEndpoints,
		Version:   int32(time.Now().Unix()),
		SyncTime:  timestamppb.Now(),
	}
	
	// 广播
	if gossipManager := r.meshCluster.GetGossipManager(); gossipManager != nil {
		gossipManager.BroadcastServiceSync(serviceSync)
	}
}

// syncWorkloadUnregistrationToCluster 同步工作负载注销到集群
func (r *DistributedServiceRegistry) syncWorkloadUnregistrationToCluster(workloadID string) {
	// 创建空的同步消息（表示删除）
	serviceSync := &protocol.ServiceSync{
		Services:  []*protocol.Service{},
		Endpoints: []*protocol.Endpoint{},
		Version:   int32(time.Now().Unix()),
		SyncTime:  timestamppb.Now(),
	}
	
	// 广播
	if gossipManager := r.meshCluster.GetGossipManager(); gossipManager != nil {
		gossipManager.BroadcastServiceSync(serviceSync)
	}
}

// queryServiceFromCluster 从集群查询服务
func (r *DistributedServiceRegistry) queryServiceFromCluster(serviceName, namespace string) (*types.Service, error) {
	// 获取健康节点
	healthyNodes := r.meshCluster.GetHealthyNodes()
	
	for _, node := range healthyNodes {
		if service, err := r.queryServiceFromNode(node.ID, serviceName, namespace); err == nil {
			return service, nil
		}
	}
	
	return nil, fmt.Errorf("service not found in cluster")
}

// queryServiceFromNode 从指定节点查询服务
func (r *DistributedServiceRegistry) queryServiceFromNode(nodeID, serviceName, namespace string) (*types.Service, error) {
	// 连接到节点
	conn, err := r.meshCluster.ConnectToNode(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s: %w", nodeID, err)
	}
	defer conn.Close()
	
	// 创建编解码器
	codec := protocol.NewCodec(conn)
	defer codec.Close()
	
	// 创建查询请求
	query := &protocol.QueryServicePayload{
		ServiceName:      serviceName,
		ServiceNamespace: namespace,
		Selector:         make(map[string]string),
		MaxEndpoints:     100,
		IncludeUnhealthy: false,
	}
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeQueryService).
		WithPayload(query).
		Build()
	
	// 发送请求
	if err := codec.WriteMessage(msg); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}
	
	// 接收响应
	response, err := codec.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	if response.Type == protocol.MsgTypeError {
		return nil, fmt.Errorf("query failed")
	}
	
	// 解析响应
	serviceInfo := &protocol.ServiceInfoPayload{}
	if err := proto.Unmarshal(response.Payload, serviceInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	
	// 转换为本地格式
	service := r.convertFromProtoService(serviceInfo.Service)
	
	// 缓存服务
	r.cacheRemoteService(service, nodeID)
	
	return service, nil
}

// getRemoteServices 获取远程服务
func (r *DistributedServiceRegistry) getRemoteServices(namespace string) []*RemoteService {
	r.remoteMu.RLock()
	defer r.remoteMu.RUnlock()
	
	var services []*RemoteService
	for _, service := range r.remoteServices {
		if namespace == "" || service.Service.Metadata.Namespace == namespace {
			if !r.isCacheExpired(service.LastSync) {
				services = append(services, service)
			}
		}
	}
	
	return services
}

// getRemoteEndpoints 获取远程端点
func (r *DistributedServiceRegistry) getRemoteEndpoints(serviceName, namespace string, selector map[string]string) []*RemoteEndpoint {
	r.remoteMu.RLock()
	defer r.remoteMu.RUnlock()
	
	serviceKey := fmt.Sprintf("%s.%s", serviceName, namespace)
	endpoints, exists := r.remoteEndpoints[serviceKey]
	if !exists {
		return nil
	}
	
	var validEndpoints []*RemoteEndpoint
	for _, endpoint := range endpoints {
		if !r.isCacheExpired(endpoint.LastSync) {
			if r.matchesSelector(endpoint.Endpoint.Labels, selector) {
				validEndpoints = append(validEndpoints, endpoint)
			}
		}
	}
	
	return validEndpoints
}

// cacheRemoteService 缓存远程服务
func (r *DistributedServiceRegistry) cacheRemoteService(service *types.Service, sourceNode string) {
	r.remoteMu.Lock()
	defer r.remoteMu.Unlock()
	
	serviceKey := fmt.Sprintf("%s.%s", service.Metadata.Name, service.Metadata.Namespace)
	
	r.remoteServices[serviceKey] = &RemoteService{
		Service:    service,
		SourceNode: sourceNode,
		LastSync:   time.Now(),
		Version:    time.Now().Unix(),
		IsLocal:    false,
	}
}

// cacheRemoteEndpoint 缓存远程端点
func (r *DistributedServiceRegistry) cacheRemoteEndpoint(endpoint *types.Endpoint, serviceName, namespace, sourceNode string) {
	r.remoteMu.Lock()
	defer r.remoteMu.Unlock()
	
	serviceKey := fmt.Sprintf("%s.%s", serviceName, namespace)
	
	remoteEndpoint := &RemoteEndpoint{
		Endpoint:   endpoint,
		SourceNode: sourceNode,
		LastSync:   time.Now(),
		Version:    time.Now().Unix(),
		IsLocal:    false,
	}
	
	r.remoteEndpoints[serviceKey] = append(r.remoteEndpoints[serviceKey], remoteEndpoint)
}

// isCacheExpired 检查缓存是否过期
func (r *DistributedServiceRegistry) isCacheExpired(lastSync time.Time) bool {
	return time.Since(lastSync) > r.cacheTimeout
}

// matchesSelector 检查标签是否匹配选择器
func (r *DistributedServiceRegistry) matchesSelector(labels map[string]string, selector map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// convertToProtoService 转换为协议服务格式
func (r *DistributedServiceRegistry) convertToProtoService(service *types.Service) *protocol.Service {
	protoService := &protocol.Service{
		Metadata: &protocol.ServiceMetadata{
			Name:      service.Metadata.Name,
			Namespace: service.Metadata.Namespace,
			Labels:    service.Metadata.Labels,
		},
		Spec: &protocol.ServiceSpec{
			Selector:     service.Spec.Selector,
			ProxyMode:    service.Spec.ProxyMode,
			LoadBalancer: service.Spec.LoadBalancer,
			Ports:        make([]*protocol.ServicePort, len(service.Spec.Ports)),
		},
	}
	
	for i, port := range service.Spec.Ports {
		protoService.Spec.Ports[i] = &protocol.ServicePort{
			Name:       port.Name,
			Port:       int32(port.Port),
			TargetPort: int32(port.TargetPort),
			Protocol:   port.Protocol,
		}
	}
	
	return protoService
}

// convertFromProtoService 从协议服务格式转换
func (r *DistributedServiceRegistry) convertFromProtoService(protoService *protocol.Service) *types.Service {
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      protoService.Metadata.Name,
			Namespace: protoService.Metadata.Namespace,
			Labels:    protoService.Metadata.Labels,
		},
		Spec: types.ServiceSpec{
			Selector:     protoService.Spec.Selector,
			ProxyMode:    protoService.Spec.ProxyMode,
			LoadBalancer: protoService.Spec.LoadBalancer,
			Ports:        make([]types.ServicePort, len(protoService.Spec.Ports)),
		},
	}
	
	for i, port := range protoService.Spec.Ports {
		service.Spec.Ports[i] = types.ServicePort{
			Name:       port.Name,
			Port:       int(port.Port),
			TargetPort: int(port.TargetPort),
			Protocol:   port.Protocol,
		}
	}
	
	return service
}

// convertToProtoEndpoint 转换为协议端点格式
func (r *DistributedServiceRegistry) convertToProtoEndpoint(endpoint *types.Endpoint) *protocol.Endpoint {
	return &protocol.Endpoint{
		Id:       endpoint.ID,
		Ip:       endpoint.IP,
		Port:     int32(endpoint.Port),
		Protocol: endpoint.Protocol,
		Labels:   endpoint.Labels,
		Status: &protocol.EndpointStatus{
			Ready:   endpoint.Status.Ready,
			Reason:  endpoint.Status.Reason,
			Message: endpoint.Status.Message,
		},
		LastSeen: timestamppb.New(endpoint.LastSeen),
	}
}

// convertFromProtoEndpoint 从协议端点格式转换
func (r *DistributedServiceRegistry) convertFromProtoEndpoint(protoEndpoint *protocol.Endpoint) *types.Endpoint {
	return &types.Endpoint{
		ID:       protoEndpoint.Id,
		IP:       protoEndpoint.Ip,
		Port:     int(protoEndpoint.Port),
		Protocol: protoEndpoint.Protocol,
		Labels:   protoEndpoint.Labels,
		Status: types.EndpointStatus{
			Ready:   protoEndpoint.Status.Ready,
			Reason:  protoEndpoint.Status.Reason,
			Message: protoEndpoint.Status.Message,
		},
		LastSeen: protoEndpoint.LastSeen.AsTime(),
	}
}

// AddEventHandler 添加事件处理器
func (r *DistributedServiceRegistry) AddEventHandler(handler ServiceEventHandler) {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	
	r.eventHandlers = append(r.eventHandlers, handler)
}

// notifyServiceRegistered 通知服务注册
func (r *DistributedServiceRegistry) notifyServiceRegistered(service *types.Service) {
	r.eventMu.RLock()
	defer r.eventMu.RUnlock()
	
	for _, handler := range r.eventHandlers {
		go handler.OnServiceRegistered(service)
	}
}

// ProcessServiceSync 处理服务同步消息
func (r *DistributedServiceRegistry) ProcessServiceSync(sync *protocol.ServiceSync, sourceNode string) {
	// 更新远程服务
	for _, protoService := range sync.Services {
		service := r.convertFromProtoService(protoService)
		r.cacheRemoteService(service, sourceNode)
	}
	
	// 更新远程端点
	for _, protoEndpoint := range sync.Endpoints {
		endpoint := r.convertFromProtoEndpoint(protoEndpoint)
		// 这里需要根据端点的标签找到对应的服务
		// 简化处理，假设端点有服务信息
		r.cacheRemoteEndpoint(endpoint, "unknown", "default", sourceNode)
	}
}

// GetStats 获取注册表统计信息
func (r *DistributedServiceRegistry) GetStats() *RegistryStats {
	r.remoteMu.RLock()
	defer r.remoteMu.RUnlock()
	
	stats := &RegistryStats{
		LocalServices:    0,
		RemoteServices:   len(r.remoteServices),
		LocalEndpoints:   0,
		RemoteEndpoints:  0,
		UpdateTime:       time.Now(),
	}
	
	// 计算本地服务数量
	if services, err := r.localRegistry.ListServices(""); err == nil {
		stats.LocalServices = len(services)
	}
	
	// 计算远程端点数量
	for _, endpoints := range r.remoteEndpoints {
		stats.RemoteEndpoints += len(endpoints)
	}
	
	return stats
}

// RegistryStats 注册表统计信息
type RegistryStats struct {
	LocalServices   int       `json:"local_services"`
	RemoteServices  int       `json:"remote_services"`
	LocalEndpoints  int       `json:"local_endpoints"`
	RemoteEndpoints int       `json:"remote_endpoints"`
	UpdateTime      time.Time `json:"update_time"`
}