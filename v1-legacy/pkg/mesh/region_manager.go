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

// RegionMeshManager 区域服务网格管理器 (Phase 2)
// 统一管理区域内的服务发现、负载均衡、健康检查和故障处理
type RegionMeshManager struct {
	// 配置信息
	config         *types.RegionConfig
	localRegistry  types.ServiceRegistry
	
	// 核心组件
	regionRegistry *RegionServiceRegistry
	networkManager *RegionNetworkManager
	healthChecker  *RegionHealthChecker
	loadBalancer   *RegionLoadBalancer
	
	// 状态管理
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	
	// 事件处理
	eventHandlers  []RegionMeshEventHandler
	eventMu        sync.RWMutex
	
	// 同步状态
	lastSyncTime   time.Time
	syncErrors     int
	
	// 统计信息
	stats          *RegionMeshStats
	statsMu        sync.RWMutex
}

// RegionMeshEventHandler 区域网格事件处理器
type RegionMeshEventHandler interface {
	// 节点事件
	OnNodeJoined(nodeID string)
	OnNodeLeft(nodeID string)
	OnNodeHealthChanged(nodeID string, oldStatus, newStatus string)
	
	// 服务事件
	OnServiceRegistered(service *types.Service, sourceNode string)
	OnServiceUnregistered(serviceName, namespace, sourceNode string)
	OnServiceUpdated(service *types.Service, sourceNode string)
	
	// 端点事件
	OnEndpointAdded(endpoint *types.Endpoint, serviceName, namespace string)
	OnEndpointRemoved(endpointID string, serviceName, namespace string)
	OnEndpointHealthChanged(endpointID string, healthy bool)
	
	// 网格事件
	OnRegionPartitioned(partition []string) // 网络分区
	OnRegionHealed(healedNodes []string)     // 分区恢复
}

// RegionMeshStats 区域网格统计信息
type RegionMeshStats struct {
	// 节点统计
	TotalNodes       int       `json:"total_nodes"`
	HealthyNodes     int       `json:"healthy_nodes"`
	UnhealthyNodes   int       `json:"unhealthy_nodes"`
	
	// 服务统计
	LocalServices    int       `json:"local_services"`
	RemoteServices   int       `json:"remote_services"`
	TotalServices    int       `json:"total_services"`
	
	// 端点统计
	LocalEndpoints   int       `json:"local_endpoints"`
	RemoteEndpoints  int       `json:"remote_endpoints"`
	TotalEndpoints   int       `json:"total_endpoints"`
	HealthyEndpoints int       `json:"healthy_endpoints"`
	
	// 网络统计
	ActiveConnections int      `json:"active_connections"`
	TotalMessages     int64    `json:"total_messages"`
	MessagesSent      int64    `json:"messages_sent"`
	MessagesReceived  int64    `json:"messages_received"`
	AverageRTT        float64  `json:"average_rtt_ms"`
	
	// 同步统计
	LastSyncTime      time.Time `json:"last_sync_time"`
	SyncErrors        int       `json:"sync_errors"`
	SyncSuccess       bool      `json:"sync_success"`
	
	// 负载均衡统计
	TotalRequests     int64     `json:"total_requests"`
	LocalRequests     int64     `json:"local_requests"`
	RemoteRequests    int64     `json:"remote_requests"`
	FailedRequests    int64     `json:"failed_requests"`
	
	UpdateTime        time.Time `json:"update_time"`
}

// NewRegionMeshManager 创建区域服务网格管理器
func NewRegionMeshManager(
	config *types.RegionConfig,
	localRegistry types.ServiceRegistry,
	listenAddr string,
) (*RegionMeshManager, error) {
	if config == nil || !config.Enabled {
		return nil, fmt.Errorf("region config is required and must be enabled")
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	manager := &RegionMeshManager{
		config:        config,
		localRegistry: localRegistry,
		ctx:           ctx,
		cancel:        cancel,
		eventHandlers: make([]RegionMeshEventHandler, 0),
		stats:         &RegionMeshStats{},
	}
	
	// 创建分布式注册表
	distributedRegistry := NewDistributedServiceRegistry(localRegistry, nil) // TODO: 需要实现ClusterInterface
	
	// 创建区域注册表
	manager.regionRegistry = NewRegionServiceRegistry(distributedRegistry, config)
	
	// 创建网络管理器
	manager.networkManager = NewRegionNetworkManager(
		config.NodeID,
		config.RegionID,
		listenAddr,
		manager, // 实现RegionMessageHandler接口
	)
	
	// 创建健康检查器
	manager.healthChecker = NewRegionHealthChecker(manager.networkManager)
	manager.healthChecker.AddEventHandler(manager) // 实现HealthEventHandler接口
	
	// 设置注册表的健康检查器引用
	manager.regionRegistry.SetHealthChecker(manager.healthChecker)
	
	// 创建负载均衡器
	manager.loadBalancer = NewRegionLoadBalancer(
		config.NodeID,
		config.RegionID,
		config.NodeID, // 简化：使用nodeID作为siteID
		"",            // TODO: 添加zone配置
		StrategyGeographicAware,
		manager.healthChecker,
	)
	
	return manager, nil
}

// Start 启动区域服务网格管理器
func (m *RegionMeshManager) Start() error {
	// 启动网络管理器
	if err := m.networkManager.Start(); err != nil {
		return fmt.Errorf("failed to start network manager: %w", err)
	}
	
	// 启动健康检查器
	m.healthChecker.Start()
	
	// 连接到邻居节点
	for _, neighbor := range m.config.Neighbors {
		go func(neighbor types.NeighborNode) {
			if err := m.networkManager.ConnectToNode(neighbor.NodeID, neighbor.Address); err != nil {
				// 记录错误但不阻止启动
				m.incrementSyncErrors()
			}
		}(neighbor)
	}
	
	// 启动同步任务
	m.wg.Add(1)
	go m.runPeriodicSync()
	
	// 启动统计更新任务
	m.wg.Add(1)
	go m.runStatsUpdate()
	
	return nil
}

// Stop 停止区域服务网格管理器
func (m *RegionMeshManager) Stop() error {
	m.cancel()
	
	// 停止健康检查器
	m.healthChecker.Stop()
	
	// 停止网络管理器
	if err := m.networkManager.Stop(); err != nil {
		// 记录错误但继续停止流程
	}
	
	// 等待所有goroutine退出
	m.wg.Wait()
	
	return nil
}

// AddEventHandler 添加事件处理器
func (m *RegionMeshManager) AddEventHandler(handler RegionMeshEventHandler) {
	m.eventMu.Lock()
	defer m.eventMu.Unlock()
	m.eventHandlers = append(m.eventHandlers, handler)
}

// GetStats 获取统计信息
func (m *RegionMeshManager) GetStats() *RegionMeshStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	
	// 返回统计信息副本
	stats := *m.stats
	return &stats
}

// GetService 获取服务 (优先本地，然后区域)
func (m *RegionMeshManager) GetService(name, namespace string) (*types.Service, error) {
	return m.regionRegistry.GetService(name, namespace)
}

// GetEndpoints 获取端点 (智能选择)
func (m *RegionMeshManager) GetEndpoints(
	serviceName, namespace string,
	selector map[string]string,
) ([]*types.Endpoint, error) {
	// 使用区域注册表获取所有可用端点
	endpoints, err := m.regionRegistry.GetEndpoints(serviceName, namespace, selector)
	if err != nil {
		return nil, err
	}
	
	// 使用负载均衡器筛选和排序最佳端点
	bestEndpoints, err := m.loadBalancer.SelectBestEndpoints(endpoints, 10) // 最多返回10个端点
	if err != nil {
		return endpoints, nil // 如果负载均衡器失败，返回原始列表
	}
	
	return bestEndpoints, nil
}

// SelectEndpoint 选择最佳端点
func (m *RegionMeshManager) SelectEndpoint(
	serviceName, namespace string,
	selector map[string]string,
	sessionID string,
) (*types.Endpoint, error) {
	// 获取所有可用端点
	endpoints, err := m.GetEndpoints(serviceName, namespace, selector)
	if err != nil {
		return nil, err
	}
	
	// 使用负载均衡器选择最佳端点
	return m.loadBalancer.SelectEndpoint(endpoints, sessionID)
}

// RegisterService 注册服务
func (m *RegionMeshManager) RegisterService(service *types.Service) error {
	// 在本地注册表中注册
	if err := m.regionRegistry.RegisterService(service); err != nil {
		return err
	}
	
	// 广播到其他节点
	sync := &protocol.ServiceSync{
		Services: []*protocol.Service{
			m.convertServiceToProto(service),
		},
		Version:      int32(time.Now().Unix()),
		SyncTime:     timestamppb.Now(),
		SourceNodeId: m.config.NodeID,
		RegionId:     m.config.RegionID,
		SyncScope:    "region",
	}
	
	if err := m.networkManager.BroadcastServiceSync(sync); err != nil {
		// 记录错误但不失败本地注册
		m.incrementSyncErrors()
	}
	
	// 通知事件处理器
	m.notifyServiceRegistered(service, m.config.NodeID)
	
	return nil
}

// UnregisterService 注销服务
func (m *RegionMeshManager) UnregisterService(serviceName, namespace string) error {
	// TODO: 实现服务注销逻辑
	return nil
}

// 实现RegionMessageHandler接口

// HandleServiceSync 处理服务同步消息
func (m *RegionMeshManager) HandleServiceSync(nodeID string, sync *protocol.ServiceSync) error {
	for _, protoService := range sync.Services {
		service := m.convertServiceFromProto(protoService)
		
		// 缓存远程服务
		endpoints := make([]*types.Endpoint, len(sync.Endpoints))
		for i, protoEndpoint := range sync.Endpoints {
			endpoints[i] = m.convertEndpointFromProto(protoEndpoint)
		}
		
		if err := m.regionRegistry.CacheRemoteService(nodeID, service, endpoints); err != nil {
			return fmt.Errorf("failed to cache remote service: %w", err)
		}
		
		// 通知事件处理器
		m.notifyServiceRegistered(service, nodeID)
	}
	
	m.lastSyncTime = time.Now()
	return nil
}

// HandleRegionServiceQuery 处理区域服务查询
func (m *RegionMeshManager) HandleRegionServiceQuery(
	nodeID string,
	query *protocol.RegionServiceQuery,
) (*protocol.RegionServiceResponse, error) {
	// 查询本地服务
	endpoints, err := m.regionRegistry.GetEndpoints(
		query.ServiceName,
		query.ServiceNamespace,
		query.Selector,
	)
	if err != nil {
		return nil, err
	}
	
	// 转换为协议格式
	protoEndpoints := make([]*protocol.Endpoint, len(endpoints))
	for i, endpoint := range endpoints {
		protoEndpoints[i] = m.convertEndpointToProto(endpoint)
	}
	
	response := &protocol.RegionServiceResponse{
		Endpoints:    protoEndpoints,
		SourceRegion: m.config.RegionID,
		SourceNode:   m.config.NodeID,
		CacheExpiry:  timestamppb.New(time.Now().Add(m.config.ServiceSync.CacheTimeout)),
		TotalEndpoints: int32(len(endpoints)),
		HasMore:      false,
	}
	
	return response, nil
}

// HandleNodeHealthCheck 处理节点健康检查
func (m *RegionMeshManager) HandleNodeHealthCheck(
	nodeID string,
	check *protocol.NodeHealthCheck,
) (*protocol.NodeHealthResponse, error) {
	// 创建健康响应
	response := &protocol.NodeHealthResponse{
		NodeId:       m.config.NodeID,
		Healthy:      true,
		Status:       "healthy",
		ResponseTime: timestamppb.Now(),
		Metrics: map[string]string{
			"services": fmt.Sprintf("%d", len(m.getLocalServices())),
			"uptime":   fmt.Sprintf("%d", int(time.Since(time.Now()).Seconds())), // TODO: 实际运行时间
		},
	}
	
	return response, nil
}

// HandleServiceFailureNotification 处理服务故障通知
func (m *RegionMeshManager) HandleServiceFailureNotification(
	notification *protocol.ServiceFailureNotification,
) error {
	// 标记端点为不健康
	m.healthChecker.MarkEndpointUnhealthy(
		notification.EndpointId,
		notification.FailureReason,
		notification.Severity,
	)
	
	return nil
}

// HandleServiceRecoveryNotification 处理服务恢复通知
func (m *RegionMeshManager) HandleServiceRecoveryNotification(
	notification *protocol.ServiceRecoveryNotification,
) error {
	// 恢复端点
	m.healthChecker.RecoverEndpoint(notification.EndpointId)
	
	return nil
}

// HandleRegionTopologyQuery 处理区域拓扑查询
func (m *RegionMeshManager) HandleRegionTopologyQuery(
	query *protocol.RegionTopologyQuery,
) (*protocol.RegionTopologyResponse, error) {
	response := &protocol.RegionTopologyResponse{
		RegionId:     m.config.RegionID,
		TopologyTime: timestamppb.Now(),
	}
	
	if query.IncludeServices {
		services := m.getLocalServices()
		response.Services = make([]*protocol.Service, len(services))
		for i, service := range services {
			response.Services[i] = m.convertServiceToProto(service)
		}
	}
	
	return response, nil
}

// 实现HealthEventHandler接口

// OnNodeHealthChanged 节点健康状态变化
func (m *RegionMeshManager) OnNodeHealthChanged(nodeID string, oldStatus, newStatus string, healthInfo *NodeHealthInfo) {
	// 通知事件处理器
	m.notifyNodeHealthChanged(nodeID, oldStatus, newStatus)
}

// OnServiceHealthChanged 服务健康状态变化
func (m *RegionMeshManager) OnServiceHealthChanged(serviceName, namespace string, oldStatus, newStatus string, healthInfo *ServiceHealthInfo) {
	// 处理服务健康状态变化
}

// OnEndpointFailure 端点故障
func (m *RegionMeshManager) OnEndpointFailure(endpointID string, failureInfo *EndpointFailureInfo) {
	// 广播故障通知
	notification := &protocol.ServiceFailureNotification{
		EndpointId:    endpointID,
		FailureReason: failureInfo.FailureReason,
		ReporterNode:  m.config.NodeID,
		FailureTime:   timestamppb.New(failureInfo.FailureTime),
		Severity:      failureInfo.Severity,
	}
	
	m.networkManager.NotifyServiceFailure(notification)
}

// OnEndpointRecovery 端点恢复
func (m *RegionMeshManager) OnEndpointRecovery(endpointID string, recoveryTime time.Time) {
	// 广播恢复通知
	notification := &protocol.ServiceRecoveryNotification{
		EndpointId:    endpointID,
		ReporterNode:  m.config.NodeID,
		RecoveryTime:  timestamppb.New(recoveryTime),
	}
	
	m.networkManager.NotifyServiceRecovery(notification)
}

// 内部方法

// runPeriodicSync 运行周期性同步
func (m *RegionMeshManager) runPeriodicSync() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.config.ServiceSync.SyncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performSync()
		}
	}
}

// performSync 执行同步
func (m *RegionMeshManager) performSync() {
	// 创建同步消息
	services := m.getLocalServices()
	protoServices := make([]*protocol.Service, len(services))
	for i, service := range services {
		protoServices[i] = m.convertServiceToProto(service)
	}
	
	sync := &protocol.ServiceSync{
		Services:     protoServices,
		Version:      int32(time.Now().Unix()),
		SyncTime:     timestamppb.Now(),
		SourceNodeId: m.config.NodeID,
		RegionId:     m.config.RegionID,
		SyncScope:    "region",
	}
	
	// 广播同步消息
	if err := m.networkManager.BroadcastServiceSync(sync); err != nil {
		m.incrementSyncErrors()
	} else {
		m.lastSyncTime = time.Now()
	}
}

// runStatsUpdate 运行统计更新
func (m *RegionMeshManager) runStatsUpdate() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.updateStats()
		}
	}
}

// updateStats 更新统计信息
func (m *RegionMeshManager) updateStats() {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	
	// 获取网络统计
	networkStats := m.networkManager.GetNetworkStats()
	m.stats.ActiveConnections = networkStats.ActiveConnections
	m.stats.TotalMessages = networkStats.MessagesSent + networkStats.MessagesReceived
	m.stats.MessagesSent = networkStats.MessagesSent
	m.stats.MessagesReceived = networkStats.MessagesReceived
	m.stats.AverageRTT = networkStats.AverageRTT
	
	// 获取路由统计
	routingStats := m.loadBalancer.GetRoutingStats()
	m.stats.TotalRequests = routingStats.TotalRequests
	m.stats.LocalRequests = routingStats.LocalRequests
	m.stats.RemoteRequests = routingStats.RemoteRequests
	m.stats.FailedRequests = routingStats.FailedRequests
	
	// 获取注册表统计
	m.stats.LocalServices = len(m.getLocalServices())
	
	// 更新同步状态
	m.stats.LastSyncTime = m.lastSyncTime
	m.stats.SyncErrors = m.syncErrors
	m.stats.SyncSuccess = time.Since(m.lastSyncTime) < 2*m.config.ServiceSync.SyncInterval
	
	m.stats.UpdateTime = time.Now()
}

// getLocalServices 获取本地服务
func (m *RegionMeshManager) getLocalServices() []*types.Service {
	services, err := m.localRegistry.ListServices("")
	if err != nil {
		return nil
	}
	return services
}

// incrementSyncErrors 增加同步错误计数
func (m *RegionMeshManager) incrementSyncErrors() {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	m.syncErrors++
}

// 类型转换方法

// convertServiceToProto 转换服务到协议格式
func (m *RegionMeshManager) convertServiceToProto(service *types.Service) *protocol.Service {
	// TODO: 实现完整的转换逻辑
	return &protocol.Service{
		Metadata: &protocol.ServiceMetadata{
			Name:        service.Metadata.Name,
			Namespace:   service.Metadata.Namespace,
			Labels:      service.Metadata.Labels,
			Annotations: service.Metadata.Annotations,
			Application: service.Metadata.Application,
			Cluster:     service.Metadata.Cluster,
			Version:     service.Metadata.Version,
			Site:        service.Metadata.Site,
			Zone:        service.Metadata.Zone,
			Region:      service.Metadata.Region,
		},
		Spec: &protocol.ServiceSpec{
			Selector:     service.Spec.Selector,
			ProxyMode:    service.Spec.ProxyMode,
			LoadBalancer: service.Spec.LoadBalancer,
		},
	}
}

// convertServiceFromProto 从协议格式转换服务
func (m *RegionMeshManager) convertServiceFromProto(protoService *protocol.Service) *types.Service {
	// TODO: 实现完整的转换逻辑
	return &types.Service{
		Metadata: types.ServiceMetadata{
			Name:        protoService.Metadata.Name,
			Namespace:   protoService.Metadata.Namespace,
			Labels:      protoService.Metadata.Labels,
			Annotations: protoService.Metadata.Annotations,
			Application: protoService.Metadata.Application,
			Cluster:     protoService.Metadata.Cluster,
			Version:     protoService.Metadata.Version,
			Site:        protoService.Metadata.Site,
			Zone:        protoService.Metadata.Zone,
			Region:      protoService.Metadata.Region,
		},
		Spec: types.ServiceSpec{
			Selector:     protoService.Spec.Selector,
			ProxyMode:    protoService.Spec.ProxyMode,
			LoadBalancer: protoService.Spec.LoadBalancer,
		},
	}
}

// convertEndpointToProto 转换端点到协议格式
func (m *RegionMeshManager) convertEndpointToProto(endpoint *types.Endpoint) *protocol.Endpoint {
	// TODO: 实现完整的转换逻辑
	return &protocol.Endpoint{
		Id:       endpoint.ID,
		Ip:       endpoint.IP,
		Port:     int32(endpoint.Port),
		Protocol: endpoint.Protocol,
		Labels:   endpoint.Labels,
	}
}

// convertEndpointFromProto 从协议格式转换端点
func (m *RegionMeshManager) convertEndpointFromProto(protoEndpoint *protocol.Endpoint) *types.Endpoint {
	// TODO: 实现完整的转换逻辑
	return &types.Endpoint{
		ID:       protoEndpoint.Id,
		IP:       protoEndpoint.Ip,
		Port:     int(protoEndpoint.Port),
		Protocol: protoEndpoint.Protocol,
		Labels:   protoEndpoint.Labels,
	}
}

// 事件通知方法

// notifyServiceRegistered 通知服务注册
func (m *RegionMeshManager) notifyServiceRegistered(service *types.Service, sourceNode string) {
	m.eventMu.RLock()
	defer m.eventMu.RUnlock()
	
	for _, handler := range m.eventHandlers {
		go handler.OnServiceRegistered(service, sourceNode)
	}
}

// notifyNodeHealthChanged 通知节点健康状态变化
func (m *RegionMeshManager) notifyNodeHealthChanged(nodeID, oldStatus, newStatus string) {
	m.eventMu.RLock()
	defer m.eventMu.RUnlock()
	
	for _, handler := range m.eventHandlers {
		go handler.OnNodeHealthChanged(nodeID, oldStatus, newStatus)
	}
}