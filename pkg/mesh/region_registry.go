package mesh

import (
	"fmt"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// RegionServiceRegistry 区域服务注册表 (Phase 2 新增)
// 扩展 DistributedServiceRegistry 以支持区域感知的服务发现
type RegionServiceRegistry struct {
	*DistributedServiceRegistry // 嵌入分布式注册表
	
	// 区域配置
	regionConfig *types.RegionConfig
	
	// 邻居节点管理
	neighborNodes map[string]*types.NeighborNode
	neighborMu    sync.RWMutex
	
	// 区域服务缓存 (按地理位置组织)
	regionServiceCache  map[string][]*types.RemoteServiceInfo // regionID -> services
	siteServiceCache    map[string][]*types.RemoteServiceInfo // siteID -> services  
	zoneServiceCache    map[string][]*types.RemoteServiceInfo // zoneID -> services
	regionCacheMu       sync.RWMutex
	
	// 健康检查器
	healthChecker *RegionHealthChecker
	unhealthyEndpoints map[string]*types.Endpoint
	healthMu           sync.RWMutex
	
	// 统计信息
	stats *RegionRegistryStats
	statsMu sync.RWMutex
}

// RegionRegistryStats 区域注册表统计信息
type RegionRegistryStats struct {
	LocalServices     int       `json:"local_services"`
	RegionServices    int       `json:"region_services"`
	SiteServices      int       `json:"site_services"`
	ZoneServices      int       `json:"zone_services"`
	HealthyNodes      int       `json:"healthy_nodes"`
	UnhealthyNodes    int       `json:"unhealthy_nodes"`
	CacheHitRate      float64   `json:"cache_hit_rate"`
	LastSyncTime      time.Time `json:"last_sync_time"`
	UpdateTime        time.Time `json:"update_time"`
}

// NewRegionServiceRegistry 创建区域服务注册表
func NewRegionServiceRegistry(
	distributedRegistry *DistributedServiceRegistry,
	regionConfig *types.RegionConfig,
) *RegionServiceRegistry {
	
	r := &RegionServiceRegistry{
		DistributedServiceRegistry: distributedRegistry,
		regionConfig:               regionConfig,
		neighborNodes:              make(map[string]*types.NeighborNode),
		regionServiceCache:         make(map[string][]*types.RemoteServiceInfo),
		siteServiceCache:           make(map[string][]*types.RemoteServiceInfo),
		zoneServiceCache:           make(map[string][]*types.RemoteServiceInfo),
		unhealthyEndpoints:         make(map[string]*types.Endpoint),
		stats:                      &RegionRegistryStats{},
	}
	
	// 初始化邻居节点
	for _, neighbor := range regionConfig.Neighbors {
		neighborCopy := neighbor
		r.neighborNodes[neighbor.NodeID] = &neighborCopy
	}
	
	return r
}

// SetHealthChecker 设置健康检查器
func (r *RegionServiceRegistry) SetHealthChecker(hc *RegionHealthChecker) {
	r.healthChecker = hc
}

// Phase 2: 区域服务发现实现

// GetEndpointsInRegion 获取区域内所有端点
func (r *RegionServiceRegistry) GetEndpointsInRegion(
	serviceName, namespace, regionID string, 
	selector map[string]string,
) ([]*types.Endpoint, error) {
	// 1. 获取本地端点
	localEndpoints, _ := r.GetEndpoints(serviceName, namespace, selector)
	
	// 2. 获取区域缓存中的端点
	r.regionCacheMu.RLock()
	cachedServices := r.regionServiceCache[regionID]
	r.regionCacheMu.RUnlock()
	
	var allEndpoints []*types.Endpoint
	allEndpoints = append(allEndpoints, localEndpoints...)
	
	// 3. 从缓存中提取匹配的端点
	for _, serviceInfo := range cachedServices {
		if r.matchesService(serviceInfo.Service, serviceName, namespace) {
			for _, endpoint := range serviceInfo.Endpoints {
				if r.matchesSelector(endpoint.Labels, selector) && 
				   endpoint.Metadata.Region == regionID {
					allEndpoints = append(allEndpoints, endpoint)
				}
			}
		}
	}
	
	// 4. 如果缓存不足，主动查询邻居节点
	if len(allEndpoints) == 0 {
		remoteEndpoints := r.queryEndpointsFromNeighbors(serviceName, namespace, selector, regionID)
		allEndpoints = append(allEndpoints, remoteEndpoints...)
	}
	
	return r.filterHealthyEndpoints(allEndpoints), nil
}

// GetEndpointsInSite 获取站点内所有端点
func (r *RegionServiceRegistry) GetEndpointsInSite(
	serviceName, namespace, siteID string,
	selector map[string]string,
) ([]*types.Endpoint, error) {
	// 1. 获取本地端点
	localEndpoints, _ := r.GetEndpoints(serviceName, namespace, selector)
	
	// 2. 获取站点缓存中的端点
	r.regionCacheMu.RLock()
	cachedServices := r.siteServiceCache[siteID]
	r.regionCacheMu.RUnlock()
	
	var allEndpoints []*types.Endpoint
	allEndpoints = append(allEndpoints, localEndpoints...)
	
	// 3. 从缓存中提取匹配的端点
	for _, serviceInfo := range cachedServices {
		if r.matchesService(serviceInfo.Service, serviceName, namespace) {
			for _, endpoint := range serviceInfo.Endpoints {
				if r.matchesSelector(endpoint.Labels, selector) && 
				   endpoint.Metadata.Site == siteID {
					allEndpoints = append(allEndpoints, endpoint)
				}
			}
		}
	}
	
	return r.filterHealthyEndpoints(allEndpoints), nil
}

// GetEndpointsInZone 获取可用区内所有端点
func (r *RegionServiceRegistry) GetEndpointsInZone(
	serviceName, namespace, zone string,
	selector map[string]string,
) ([]*types.Endpoint, error) {
	// 1. 获取本地端点
	localEndpoints, _ := r.GetEndpoints(serviceName, namespace, selector)
	
	// 2. 获取可用区缓存中的端点
	r.regionCacheMu.RLock()
	cachedServices := r.zoneServiceCache[zone]
	r.regionCacheMu.RUnlock()
	
	var allEndpoints []*types.Endpoint
	allEndpoints = append(allEndpoints, localEndpoints...)
	
	// 3. 从缓存中提取匹配的端点
	for _, serviceInfo := range cachedServices {
		if r.matchesService(serviceInfo.Service, serviceName, namespace) {
			for _, endpoint := range serviceInfo.Endpoints {
				if r.matchesSelector(endpoint.Labels, selector) && 
				   endpoint.Metadata.Zone == zone {
					allEndpoints = append(allEndpoints, endpoint)
				}
			}
		}
	}
	
	return r.filterHealthyEndpoints(allEndpoints), nil
}

// Phase 2: 跨节点服务同步实现

// SyncServiceFromNode 从指定节点同步服务
func (r *RegionServiceRegistry) SyncServiceFromNode(nodeID string, service *types.Service) error {
	// 更新邻居节点状态
	r.updateNeighborLastSeen(nodeID)
	
	// 创建远程服务信息
	remoteInfo := &types.RemoteServiceInfo{
		Service:    service,
		Endpoints:  []*types.Endpoint{}, // 端点单独同步
		SourceNode: nodeID,
		CachedAt:   time.Now(),
		TTL:        r.regionConfig.ServiceSync.CacheTimeout,
	}
	
	// 按地理位置缓存
	r.cacheServiceByLocation(remoteInfo)
	
	// 更新统计信息
	r.updateStats()
	
	return nil
}

// SyncEndpointsFromNode 从指定节点同步端点
func (r *RegionServiceRegistry) SyncEndpointsFromNode(
	nodeID string, 
	serviceName, namespace string,
	endpoints []*types.Endpoint,
) error {
	// 更新邻居节点状态
	r.updateNeighborLastSeen(nodeID)
	
	// 为每个端点添加源节点信息
	for _, endpoint := range endpoints {
		endpoint.Metadata.SourceNode = nodeID
		endpoint.Metadata.IsRemote = true
	}
	
	// 创建或更新远程服务信息
	r.regionCacheMu.Lock()
	
	// 查找现有的服务信息
	var foundService *types.RemoteServiceInfo
	for _, serviceInfos := range r.regionServiceCache {
		for _, serviceInfo := range serviceInfos {
			if r.matchesService(serviceInfo.Service, serviceName, namespace) &&
			   serviceInfo.SourceNode == nodeID {
				foundService = serviceInfo
				break
			}
		}
		if foundService != nil {
			break
		}
	}
	
	// 如果找到了，更新端点；否则创建新的服务信息
	if foundService != nil {
		foundService.Endpoints = endpoints
		foundService.CachedAt = time.Now()
	} else {
		// 创建新的服务信息（服务定义为空，只包含端点）
		remoteInfo := &types.RemoteServiceInfo{
			Service:    nil, // 服务定义由另外的同步提供
			Endpoints:  endpoints,
			SourceNode: nodeID,
			CachedAt:   time.Now(),
			TTL:        r.regionConfig.ServiceSync.CacheTimeout,
		}
		
		// 按地理位置缓存（基于端点的地理信息）
		r.cacheEndpointsByLocation(remoteInfo)
	}
	
	r.regionCacheMu.Unlock()
	
	// 更新统计信息
	r.updateStats()
	
	return nil
}

// BroadcastServiceUpdate 广播服务更新
func (r *RegionServiceRegistry) BroadcastServiceUpdate(service *types.Service) error {
	// 首先调用基础的广播功能
	if err := r.DistributedServiceRegistry.RegisterService(service); err != nil {
		return fmt.Errorf("failed to register service in distributed registry: %w", err)
	}
	
	// 向所有活跃的邻居节点发送服务更新
	activeNeighbors, err := r.GetActiveNeighbors()
	if err != nil {
		return fmt.Errorf("failed to get active neighbors: %w", err)
	}
	for _, neighbor := range activeNeighbors {
		go r.sendServiceUpdateToNeighbor(neighbor.NodeID, service)
	}
	
	return nil
}

// BroadcastEndpointUpdate 广播端点更新
func (r *RegionServiceRegistry) BroadcastEndpointUpdate(
	serviceName, namespace string, 
	endpoints []*types.Endpoint,
) error {
	// 向所有活跃的邻居节点发送端点更新
	activeNeighbors, err := r.GetActiveNeighbors()
	if err != nil {
		return fmt.Errorf("failed to get active neighbors: %w", err)
	}
	for _, neighbor := range activeNeighbors {
		go r.sendEndpointUpdateToNeighbor(neighbor.NodeID, serviceName, namespace, endpoints)
	}
	
	return nil
}

// Phase 2: 远程服务缓存管理实现

// CacheRemoteService 缓存远程服务
func (r *RegionServiceRegistry) CacheRemoteService(
	nodeID string, 
	service *types.Service, 
	endpoints []*types.Endpoint,
) error {
	remoteInfo := &types.RemoteServiceInfo{
		Service:    service,
		Endpoints:  endpoints,
		SourceNode: nodeID,
		CachedAt:   time.Now(),
		TTL:        r.regionConfig.ServiceSync.CacheTimeout,
	}
	
	// 按地理位置缓存
	r.cacheServiceByLocation(remoteInfo)
	
	return nil
}

// GetCachedRemoteServices 获取指定节点的缓存服务
func (r *RegionServiceRegistry) GetCachedRemoteServices(nodeID string) ([]*types.RemoteServiceInfo, error) {
	r.regionCacheMu.RLock()
	defer r.regionCacheMu.RUnlock()
	
	var services []*types.RemoteServiceInfo
	
	// 从所有缓存中查找指定节点的服务
	for _, serviceInfos := range r.regionServiceCache {
		for _, serviceInfo := range serviceInfos {
			if serviceInfo.SourceNode == nodeID && !r.isCacheExpired(serviceInfo) {
				services = append(services, serviceInfo)
			}
		}
	}
	
	return services, nil
}

// InvalidateRemoteServiceCache 使远程服务缓存失效
func (r *RegionServiceRegistry) InvalidateRemoteServiceCache(
	nodeID string, 
	serviceName, namespace string,
) error {
	r.regionCacheMu.Lock()
	defer r.regionCacheMu.Unlock()
	
	// 从所有缓存中移除指定的服务
	for cacheKey, serviceInfos := range r.regionServiceCache {
		for i := len(serviceInfos) - 1; i >= 0; i-- {
			serviceInfo := serviceInfos[i]
			if serviceInfo.SourceNode == nodeID &&
			   r.matchesService(serviceInfo.Service, serviceName, namespace) {
				// 从切片中移除
				serviceInfos = append(serviceInfos[:i], serviceInfos[i+1:]...)
			}
		}
		r.regionServiceCache[cacheKey] = serviceInfos
	}
	
	// 同样处理站点和区域缓存
	for cacheKey, serviceInfos := range r.siteServiceCache {
		for i := len(serviceInfos) - 1; i >= 0; i-- {
			serviceInfo := serviceInfos[i]
			if serviceInfo.SourceNode == nodeID &&
			   r.matchesService(serviceInfo.Service, serviceName, namespace) {
				serviceInfos = append(serviceInfos[:i], serviceInfos[i+1:]...)
			}
		}
		r.siteServiceCache[cacheKey] = serviceInfos
	}
	
	for cacheKey, serviceInfos := range r.zoneServiceCache {
		for i := len(serviceInfos) - 1; i >= 0; i-- {
			serviceInfo := serviceInfos[i]
			if serviceInfo.SourceNode == nodeID &&
			   r.matchesService(serviceInfo.Service, serviceName, namespace) {
				serviceInfos = append(serviceInfos[:i], serviceInfos[i+1:]...)
			}
		}
		r.zoneServiceCache[cacheKey] = serviceInfos
	}
	
	return nil
}

// CleanupExpiredCache 清理过期缓存
func (r *RegionServiceRegistry) CleanupExpiredCache() error {
	r.regionCacheMu.Lock()
	defer r.regionCacheMu.Unlock()
	
	// 清理区域缓存
	for cacheKey, serviceInfos := range r.regionServiceCache {
		var validServices []*types.RemoteServiceInfo
		for _, serviceInfo := range serviceInfos {
			if !r.isCacheExpired(serviceInfo) {
				validServices = append(validServices, serviceInfo)
			}
		}
		r.regionServiceCache[cacheKey] = validServices
	}
	
	// 清理站点缓存
	for cacheKey, serviceInfos := range r.siteServiceCache {
		var validServices []*types.RemoteServiceInfo
		for _, serviceInfo := range serviceInfos {
			if !r.isCacheExpired(serviceInfo) {
				validServices = append(validServices, serviceInfo)
			}
		}
		r.siteServiceCache[cacheKey] = validServices
	}
	
	// 清理可用区缓存
	for cacheKey, serviceInfos := range r.zoneServiceCache {
		var validServices []*types.RemoteServiceInfo
		for _, serviceInfo := range serviceInfos {
			if !r.isCacheExpired(serviceInfo) {
				validServices = append(validServices, serviceInfo)
			}
		}
		r.zoneServiceCache[cacheKey] = validServices
	}
	
	// 更新统计信息
	r.updateStats()
	
	return nil
}

// Phase 2: 节点管理实现

// RegisterNeighborNode 注册邻居节点
func (r *RegionServiceRegistry) RegisterNeighborNode(node *types.NeighborNode) error {
	r.neighborMu.Lock()
	defer r.neighborMu.Unlock()
	
	node.LastSeen = time.Now()
	node.Status = "online"
	
	r.neighborNodes[node.NodeID] = node
	
	// 启动健康检查
	if r.regionConfig.ServiceSync.HealthCheck {
		go r.startNeighborHealthCheck(node.NodeID)
	}
	
	return nil
}

// UpdateNeighborStatus 更新邻居节点状态
func (r *RegionServiceRegistry) UpdateNeighborStatus(nodeID, status string) error {
	r.neighborMu.Lock()
	defer r.neighborMu.Unlock()
	
	if node, exists := r.neighborNodes[nodeID]; exists {
		node.Status = status
		node.LastSeen = time.Now()
		
		// 如果节点下线，清理其缓存
		if status == "offline" {
			go r.cleanupNodeCache(nodeID)
		}
	}
	
	return nil
}

// GetNeighborNodes 获取所有邻居节点
func (r *RegionServiceRegistry) GetNeighborNodes() ([]*types.NeighborNode, error) {
	r.neighborMu.RLock()
	defer r.neighborMu.RUnlock()
	
	var nodes []*types.NeighborNode
	for _, node := range r.neighborNodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	
	return nodes, nil
}

// GetActiveNeighbors 获取活跃的邻居节点
func (r *RegionServiceRegistry) GetActiveNeighbors() ([]*types.NeighborNode, error) {
	r.neighborMu.RLock()
	defer r.neighborMu.RUnlock()
	
	var activeNodes []*types.NeighborNode
	for _, node := range r.neighborNodes {
		if node.Status == "online" && 
		   time.Since(node.LastSeen) < 2*r.regionConfig.ServiceSync.SyncInterval {
			nodeCopy := *node
			activeNodes = append(activeNodes, &nodeCopy)
		}
	}
	
	return activeNodes, nil
}

// Phase 2: 服务查询增强实现

// QueryServiceMultiLevel 多层次服务查询
func (r *RegionServiceRegistry) QueryServiceMultiLevel(
	serviceName, namespace string,
	selector map[string]string,
	scope types.ServiceScope,
) ([]*types.Endpoint, error) {
	
	switch scope {
	case types.ScopeLocal:
		return r.GetEndpoints(serviceName, namespace, selector)
		
	case types.ScopeRegion:
		return r.GetEndpointsInRegion(serviceName, namespace, r.regionConfig.RegionID, selector)
		
	case types.ScopeSite:
		// 使用当前节点ID作为站点ID（简化设计）
		return r.GetEndpointsInSite(serviceName, namespace, r.regionConfig.NodeID, selector)
		
	case types.ScopeZone:
		// 从邻居节点中获取同可用区的服务
		var allEndpoints []*types.Endpoint
		
		activeNeighbors, err := r.GetActiveNeighbors()
		if err != nil {
			return nil, fmt.Errorf("failed to get active neighbors: %w", err)
		}
		for _, neighbor := range activeNeighbors {
			if neighbor.Zone != "" {
				endpoints, _ := r.GetEndpointsInZone(serviceName, namespace, neighbor.Zone, selector)
				allEndpoints = append(allEndpoints, endpoints...)
			}
		}
		
		return allEndpoints, nil
		
	case types.ScopeGlobal:
		// Phase 3 功能，暂时返回区域范围
		return r.GetEndpointsInRegion(serviceName, namespace, r.regionConfig.RegionID, selector)
		
	default:
		return r.GetEndpoints(serviceName, namespace, selector)
	}
}

// Phase 2: 健康检查和故障处理实现

// MarkEndpointUnhealthy 标记端点为不健康
func (r *RegionServiceRegistry) MarkEndpointUnhealthy(endpointID string, reason string) error {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	
	// 从本地注册表获取端点
	if endpoint := r.findEndpointByID(endpointID); endpoint != nil {
		endpoint.Status.Ready = false
		endpoint.Status.Reason = reason
		endpoint.Status.Message = fmt.Sprintf("Marked unhealthy at %s", time.Now().Format(time.RFC3339))
		
		r.unhealthyEndpoints[endpointID] = endpoint
	}
	
	return nil
}

// RecoverEndpoint 恢复端点
func (r *RegionServiceRegistry) RecoverEndpoint(endpointID string) error {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	
	// 从不健康列表中移除
	delete(r.unhealthyEndpoints, endpointID)
	
	// 更新端点状态
	if endpoint := r.findEndpointByID(endpointID); endpoint != nil {
		endpoint.Status.Ready = true
		endpoint.Status.Reason = ""
		endpoint.Status.Message = fmt.Sprintf("Recovered at %s", time.Now().Format(time.RFC3339))
	}
	
	return nil
}

// GetUnhealthyEndpoints 获取不健康的端点
func (r *RegionServiceRegistry) GetUnhealthyEndpoints() ([]*types.Endpoint, error) {
	r.healthMu.RLock()
	defer r.healthMu.RUnlock()
	
	var endpoints []*types.Endpoint
	for _, endpoint := range r.unhealthyEndpoints {
		endpointCopy := *endpoint
		endpoints = append(endpoints, &endpointCopy)
	}
	
	return endpoints, nil
}

// 辅助方法

// cacheServiceByLocation 按地理位置缓存服务
func (r *RegionServiceRegistry) cacheServiceByLocation(serviceInfo *types.RemoteServiceInfo) {
	r.regionCacheMu.Lock()
	defer r.regionCacheMu.Unlock()
	
	if serviceInfo.Service != nil {
		// 按区域缓存
		if region := serviceInfo.Service.Metadata.Region; region != "" {
			r.regionServiceCache[region] = append(r.regionServiceCache[region], serviceInfo)
		}
		
		// 按站点缓存
		if site := serviceInfo.Service.Metadata.Site; site != "" {
			r.siteServiceCache[site] = append(r.siteServiceCache[site], serviceInfo)
		}
		
		// 按可用区缓存
		if zone := serviceInfo.Service.Metadata.Zone; zone != "" {
			r.zoneServiceCache[zone] = append(r.zoneServiceCache[zone], serviceInfo)
		}
	}
}

// cacheEndpointsByLocation 按端点地理位置缓存
func (r *RegionServiceRegistry) cacheEndpointsByLocation(serviceInfo *types.RemoteServiceInfo) {
	// 基于端点的地理信息进行缓存
	for _, endpoint := range serviceInfo.Endpoints {
		if region := endpoint.Metadata.Region; region != "" {
			r.regionServiceCache[region] = append(r.regionServiceCache[region], serviceInfo)
		}
		if site := endpoint.Metadata.Site; site != "" {
			r.siteServiceCache[site] = append(r.siteServiceCache[site], serviceInfo)
		}
		if zone := endpoint.Metadata.Zone; zone != "" {
			r.zoneServiceCache[zone] = append(r.zoneServiceCache[zone], serviceInfo)
		}
	}
}

// matchesService 检查服务是否匹配
func (r *RegionServiceRegistry) matchesService(service *types.Service, name, namespace string) bool {
	return service != nil && service.Metadata.Name == name && service.Metadata.Namespace == namespace
}

// matchesSelector 检查标签选择器匹配
func (r *RegionServiceRegistry) matchesSelector(labels, selector map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// isCacheExpired 检查缓存是否过期
func (r *RegionServiceRegistry) isCacheExpired(serviceInfo *types.RemoteServiceInfo) bool {
	return time.Since(serviceInfo.CachedAt) > serviceInfo.TTL
}

// filterHealthyEndpoints 过滤健康的端点
func (r *RegionServiceRegistry) filterHealthyEndpoints(endpoints []*types.Endpoint) []*types.Endpoint {
	r.healthMu.RLock()
	defer r.healthMu.RUnlock()
	
	var healthy []*types.Endpoint
	for _, endpoint := range endpoints {
		if _, isUnhealthy := r.unhealthyEndpoints[endpoint.ID]; !isUnhealthy && endpoint.Status.Ready {
			healthy = append(healthy, endpoint)
		}
	}
	
	return healthy
}

// findEndpointByID 根据ID查找端点
func (r *RegionServiceRegistry) findEndpointByID(endpointID string) *types.Endpoint {
	// 这里需要遍历所有服务的端点来查找
	// 简化实现，实际应该建立ID到端点的索引
	if services, err := r.ListServices(""); err == nil {
		for _, service := range services {
			if endpoints, err := r.GetEndpoints(service.Metadata.Name, service.Metadata.Namespace, nil); err == nil {
				for _, endpoint := range endpoints {
					if endpoint.ID == endpointID {
						return endpoint
					}
				}
			}
		}
	}
	return nil
}

// updateNeighborLastSeen 更新邻居节点最后可见时间
func (r *RegionServiceRegistry) updateNeighborLastSeen(nodeID string) {
	r.neighborMu.Lock()
	defer r.neighborMu.Unlock()
	
	if node, exists := r.neighborNodes[nodeID]; exists {
		node.LastSeen = time.Now()
		if node.Status == "offline" {
			node.Status = "online"
		}
	}
}

// queryEndpointsFromNeighbors 从邻居节点查询端点
func (r *RegionServiceRegistry) queryEndpointsFromNeighbors(
	serviceName, namespace string,
	selector map[string]string,
	regionID string,
) []*types.Endpoint {
	// 这里应该实现从邻居节点的主动查询
	// 简化实现，返回空切片
	return []*types.Endpoint{}
}

// sendServiceUpdateToNeighbor 向邻居节点发送服务更新
func (r *RegionServiceRegistry) sendServiceUpdateToNeighbor(nodeID string, service *types.Service) {
	// 实现向邻居节点发送服务更新的逻辑
	// 这里应该通过网络协议发送更新消息
}

// sendEndpointUpdateToNeighbor 向邻居节点发送端点更新
func (r *RegionServiceRegistry) sendEndpointUpdateToNeighbor(
	nodeID string,
	serviceName, namespace string,
	endpoints []*types.Endpoint,
) {
	// 实现向邻居节点发送端点更新的逻辑
}

// startNeighborHealthCheck 启动邻居节点健康检查
func (r *RegionServiceRegistry) startNeighborHealthCheck(nodeID string) {
	// 实现邻居节点的健康检查逻辑
	ticker := time.NewTicker(r.regionConfig.ServiceSync.SyncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// 执行健康检查
			if err := r.checkNeighborHealth(nodeID); err != nil {
				r.UpdateNeighborStatus(nodeID, "offline")
			}
		}
	}
}

// checkNeighborHealth 检查邻居节点健康状态
func (r *RegionServiceRegistry) checkNeighborHealth(nodeID string) error {
	// 实现健康检查逻辑
	// 简化实现，总是返回健康
	return nil
}

// cleanupNodeCache 清理节点缓存
func (r *RegionServiceRegistry) cleanupNodeCache(nodeID string) {
	// 移除指定节点的所有缓存服务
	r.regionCacheMu.Lock()
	defer r.regionCacheMu.Unlock()
	
	for cacheKey, serviceInfos := range r.regionServiceCache {
		var validServices []*types.RemoteServiceInfo
		for _, serviceInfo := range serviceInfos {
			if serviceInfo.SourceNode != nodeID {
				validServices = append(validServices, serviceInfo)
			}
		}
		r.regionServiceCache[cacheKey] = validServices
	}
	
	// 同样清理站点和可用区缓存
	for cacheKey, serviceInfos := range r.siteServiceCache {
		var validServices []*types.RemoteServiceInfo
		for _, serviceInfo := range serviceInfos {
			if serviceInfo.SourceNode != nodeID {
				validServices = append(validServices, serviceInfo)
			}
		}
		r.siteServiceCache[cacheKey] = validServices
	}
	
	for cacheKey, serviceInfos := range r.zoneServiceCache {
		var validServices []*types.RemoteServiceInfo
		for _, serviceInfo := range serviceInfos {
			if serviceInfo.SourceNode != nodeID {
				validServices = append(validServices, serviceInfo)
			}
		}
		r.zoneServiceCache[cacheKey] = validServices
	}
}

// updateStats 更新统计信息
func (r *RegionServiceRegistry) updateStats() {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	
	r.stats.UpdateTime = time.Now()
	
	// 计算本地服务数量
	if services, err := r.ListServices(""); err == nil {
		r.stats.LocalServices = len(services)
	}
	
	// 计算区域、站点、可用区服务数量
	r.regionCacheMu.RLock()
	r.stats.RegionServices = len(r.regionServiceCache)
	r.stats.SiteServices = len(r.siteServiceCache)
	r.stats.ZoneServices = len(r.zoneServiceCache)
	r.regionCacheMu.RUnlock()
	
	// 计算健康和不健康节点数量
	activeNeighbors, _ := r.GetActiveNeighbors()
	allNeighbors, _ := r.GetNeighborNodes()
	r.stats.HealthyNodes = len(activeNeighbors)
	r.stats.UnhealthyNodes = len(allNeighbors) - len(activeNeighbors)
}

// GetRegionStats 获取区域注册表统计信息
func (r *RegionServiceRegistry) GetRegionStats() *RegionRegistryStats {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()
	
	// 返回统计信息副本
	statsCopy := *r.stats
	return &statsCopy
}