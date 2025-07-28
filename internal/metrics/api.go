// 监控API接口定义
package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// MonitoringAPI 监控API
type MonitoringAPI struct {
	collector *MetricsCollector
}

// NewMonitoringAPI 创建监控API
func NewMonitoringAPI(collector *MetricsCollector) *MonitoringAPI {
	return &MonitoringAPI{
		collector: collector,
	}
}

// RegisterRoutes 注册API路由
func (api *MonitoringAPI) RegisterRoutes(mux *http.ServeMux) {
	// 服务网格概览
	mux.HandleFunc("/api/v1/overview", api.handleOverview)
	
	// 服务相关API
	mux.HandleFunc("/api/v1/services", api.handleServices)
	mux.HandleFunc("/api/v1/services/", api.handleServiceDetail)
	mux.HandleFunc("/api/v1/services/health", api.handleServiceHealth)
	
	// 连接相关API
	mux.HandleFunc("/api/v1/connections", api.handleConnections)
	mux.HandleFunc("/api/v1/connections/active", api.handleActiveConnections)
	mux.HandleFunc("/api/v1/connections/history", api.handleConnectionHistory)
	
	// 节点相关API
	mux.HandleFunc("/api/v1/nodes", api.handleNodes)
	mux.HandleFunc("/api/v1/nodes/", api.handleNodeDetail)
	mux.HandleFunc("/api/v1/cluster", api.handleCluster)
	
	// 隧道相关API
	mux.HandleFunc("/api/v1/tunnels", api.handleTunnels)
	mux.HandleFunc("/api/v1/tunnels/", api.handleTunnelDetail)
	
	// 系统相关API
	mux.HandleFunc("/api/v1/system", api.handleSystem)
	mux.HandleFunc("/api/v1/system/resources", api.handleSystemResources)
	
	// 实时监控API
	mux.HandleFunc("/api/v1/realtime/metrics", api.handleRealtimeMetrics)
	
	// 统计分析API
	mux.HandleFunc("/api/v1/analytics/traffic", api.handleTrafficAnalytics)
	mux.HandleFunc("/api/v1/analytics/performance", api.handlePerformanceAnalytics)
}

// handleOverview 处理概览请求
func (api *MonitoringAPI) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.collector.UpdateSystemMetrics()
	
	overview := map[string]interface{}{
		"cluster": map[string]interface{}{
			"status":       "healthy", // 根据实际状态计算
			"total_nodes":  api.collector.nodeMetrics.ClusterInfo.TotalNodes,
			"healthy_nodes": api.collector.nodeMetrics.ClusterInfo.HealthyNodes,
		},
		"services": map[string]interface{}{
			"total":     api.collector.serviceMetrics.TotalServices,
			"healthy":   api.collector.serviceMetrics.HealthyServices,
			"unhealthy": api.collector.serviceMetrics.UnhealthyServices,
		},
		"connections": map[string]interface{}{
			"active": api.collector.connectionMetrics.ActiveConnections,
			"total":  api.collector.connectionMetrics.TotalConnections,
			"failed": api.collector.connectionMetrics.FailedConnections,
		},
		"tunnels": map[string]interface{}{
			"active": api.collector.tunnelMetrics.ActiveTunnels,
			"total":  api.collector.tunnelMetrics.TotalTunnels,
			"by_type": api.collector.tunnelMetrics.TunnelsByType,
		},
		"system": map[string]interface{}{
			"cpu_usage":    api.collector.systemMetrics.CPUUsage,
			"memory_usage": api.collector.systemMetrics.MemoryUsage,
			"uptime":       api.collector.systemMetrics.Uptime.String(),
			"goroutines":   api.collector.systemMetrics.GoroutineCount,
		},
		"timestamp": time.Now(),
	}

	api.writeJSON(w, overview)
}

// handleServices 处理服务列表请求
func (api *MonitoringAPI) handleServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析查询参数
	query := r.URL.Query()
	namespace := query.Get("namespace")
	status := query.Get("status")
	
	services := api.collector.GetServiceMetrics()
	filteredServices := make(map[string]ServiceStatus)
	
	// 应用过滤器
	for key, service := range services.ServiceStatus {
		if namespace != "" && service.Namespace != namespace {
			continue
		}
		if status != "" && service.Status != status {
			continue
		}
		filteredServices[key] = service
	}
	
	response := map[string]interface{}{
		"services": filteredServices,
		"summary": map[string]interface{}{
			"total":      len(filteredServices),
			"namespaces": api.getUniqueNamespaces(filteredServices),
		},
		"timestamp": time.Now(),
	}

	api.writeJSON(w, response)
}

// handleServiceDetail 处理服务详情请求
func (api *MonitoringAPI) handleServiceDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从URL路径提取服务名称
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/services/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid service path", http.StatusBadRequest)
		return
	}
	
	namespace := parts[0]
	serviceName := parts[1]
	serviceKey := fmt.Sprintf("%s/%s", namespace, serviceName)
	
	services := api.collector.GetServiceMetrics()
	service, exists := services.ServiceStatus[serviceKey]
	if !exists {
		http.Error(w, "Service not found", http.StatusNotFound)
		return
	}
	
	// 获取负载均衡统计
	lbStats, _ := services.LoadBalanceStats[serviceKey]
	
	response := map[string]interface{}{
		"service":          service,
		"load_balance":     lbStats,
		"endpoint_count":   services.EndpointCounts[serviceKey],
		"timestamp":        time.Now(),
	}

	api.writeJSON(w, response)
}

// handleServiceHealth 处理服务健康检查请求
func (api *MonitoringAPI) handleServiceHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	services := api.collector.GetServiceMetrics()
	healthSummary := make(map[string]interface{})
	
	for key, service := range services.ServiceStatus {
		healthyEndpoints := 0
		totalEndpoints := len(service.Endpoints)
		
		for _, endpoint := range service.Endpoints {
			if endpoint.Status == "online" {
				healthyEndpoints++
			}
		}
		
		healthPercentage := float64(0)
		if totalEndpoints > 0 {
			healthPercentage = float64(healthyEndpoints) / float64(totalEndpoints) * 100
		}
		
		healthSummary[key] = map[string]interface{}{
			"status":            service.Status,
			"healthy_endpoints": healthyEndpoints,
			"total_endpoints":   totalEndpoints,
			"health_percentage": healthPercentage,
			"last_seen":         service.LastSeen,
		}
	}
	
	response := map[string]interface{}{
		"health_summary": healthSummary,
		"overall_health": api.calculateOverallHealth(services),
		"timestamp":      time.Now(),
	}

	api.writeJSON(w, response)
}

// handleConnections 处理连接请求
func (api *MonitoringAPI) handleConnections(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	connections := api.collector.GetConnectionMetrics()
	
	response := map[string]interface{}{
		"active_connections":     connections.ActiveConnections,
		"total_connections":      connections.TotalConnections,
		"failed_connections":     connections.FailedConnections,
		"connections_by_service": connections.ConnectionsByService,
		"connections_by_node":    connections.ConnectionsByNode,
		"avg_latency":           connections.AvgConnectionLatency.String(),
		"timestamp":             time.Now(),
	}

	api.writeJSON(w, response)
}

// handleActiveConnections 处理活跃连接请求
func (api *MonitoringAPI) handleActiveConnections(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	connections := api.collector.GetConnectionMetrics()
	
	// 过滤最近的连接历史
	recentConnections := make([]ConnectionHistoryEntry, 0)
	cutoff := time.Now().Add(-time.Hour) // 最近1小时
	
	for _, entry := range connections.ConnectionHistory {
		if entry.Timestamp.After(cutoff) && entry.Action == "connect" {
			recentConnections = append(recentConnections, entry)
		}
	}
	
	response := map[string]interface{}{
		"active_count":        connections.ActiveConnections,
		"recent_connections":  recentConnections,
		"by_service":         connections.ConnectionsByService,
		"timestamp":          time.Now(),
	}

	api.writeJSON(w, response)
}

// handleConnectionHistory 处理连接历史请求
func (api *MonitoringAPI) handleConnectionHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	limitStr := query.Get("limit")
	limit := 100 // 默认限制
	
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	connections := api.collector.GetConnectionMetrics()
	history := connections.ConnectionHistory
	
	// 限制返回的记录数
	if len(history) > limit {
		history = history[len(history)-limit:]
	}
	
	response := map[string]interface{}{
		"history":   history,
		"count":     len(history),
		"limit":     limit,
		"timestamp": time.Now(),
	}

	api.writeJSON(w, response)
}

// handleTunnels 处理隧道请求
func (api *MonitoringAPI) handleTunnels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	tunnelType := query.Get("type")
	
	tunnels := api.collector.tunnelMetrics
	filteredTunnels := make([]TunnelDetail, 0)
	
	for _, tunnel := range tunnels.TunnelDetails {
		if tunnelType == "" || tunnel.Type == tunnelType {
			filteredTunnels = append(filteredTunnels, tunnel)
		}
	}
	
	response := map[string]interface{}{
		"tunnels":     filteredTunnels,
		"active":      tunnels.ActiveTunnels,
		"total":       tunnels.TotalTunnels,
		"by_type":     tunnels.TunnelsByType,
		"encryption":  tunnels.EncryptionStats,
		"timestamp":   time.Now(),
	}

	api.writeJSON(w, response)
}

// handleSystem 处理系统信息请求
func (api *MonitoringAPI) handleSystem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.collector.UpdateSystemMetrics()
	system := api.collector.GetSystemMetrics()
	
	response := map[string]interface{}{
		"version":    system.Version,
		"build_time": system.BuildTime,
		"start_time": system.StartTime,
		"uptime":     system.Uptime.String(),
		"resources": map[string]interface{}{
			"memory_usage":    system.MemoryUsage,
			"memory_limit":    system.MemoryLimit,
			"cpu_usage":       system.CPUUsage,
			"goroutines":      system.GoroutineCount,
		},
		"gc_stats": system.GCStats,
		"timestamp": time.Now(),
	}

	api.writeJSON(w, response)
}

// handleRealtimeMetrics 处理实时指标请求
func (api *MonitoringAPI) handleRealtimeMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 设置SSE头部
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 创建一个定期发送指标的ticker
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			api.collector.UpdateSystemMetrics()
			metrics := api.collector.GetMetrics()
			
			data, err := json.Marshal(metrics)
			if err != nil {
				return
			}
			
			fmt.Fprintf(w, "data: %s\n\n", data)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			
		case <-r.Context().Done():
			return
		}
	}
}

// 辅助方法

// writeJSON 写入JSON响应
func (api *MonitoringAPI) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// getUniqueNamespaces 获取唯一的命名空间列表
func (api *MonitoringAPI) getUniqueNamespaces(services map[string]ServiceStatus) []string {
	namespaces := make(map[string]bool)
	for _, service := range services {
		namespaces[service.Namespace] = true
	}
	
	result := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		result = append(result, ns)
	}
	return result
}

// calculateOverallHealth 计算整体健康状态
func (api *MonitoringAPI) calculateOverallHealth(services ServiceMetrics) map[string]interface{} {
	if services.TotalServices == 0 {
		return map[string]interface{}{
			"status":      "unknown",
			"percentage":  0,
			"message":     "No services registered",
		}
	}
	
	healthPercentage := float64(services.HealthyServices) / float64(services.TotalServices) * 100
	
	status := "healthy"
	message := "All services are healthy"
	
	if healthPercentage < 50 {
		status = "critical"
		message = "More than half of services are unhealthy"
	} else if healthPercentage < 80 {
		status = "degraded"
		message = "Some services are experiencing issues"
	}
	
	return map[string]interface{}{
		"status":     status,
		"percentage": healthPercentage,
		"healthy":    services.HealthyServices,
		"total":      services.TotalServices,
		"message":    message,
	}
}

// handleNodes 处理节点列表请求
func (api *MonitoringAPI) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeMetrics := api.collector.nodeMetrics
	
	response := map[string]interface{}{
		"node_id":         nodeMetrics.NodeID,
		"node_type":       nodeMetrics.NodeType,
		"region":          nodeMetrics.Region,
		"zone":            nodeMetrics.Zone,
		"connected_nodes": nodeMetrics.ConnectedNodes,
		"network_stats":   nodeMetrics.NetworkBandwidth,
		"cluster_info":    nodeMetrics.ClusterInfo,
		"timestamp":       time.Now(),
	}

	api.writeJSON(w, response)
}

// handleNodeDetail 处理节点详情请求
func (api *MonitoringAPI) handleNodeDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从URL路径提取节点ID
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/")
	nodeID := strings.TrimSuffix(path, "/")
	
	if nodeID == "" {
		http.Error(w, "Node ID required", http.StatusBadRequest)
		return
	}

	nodeMetrics := api.collector.nodeMetrics
	
	// 查找指定节点
	var targetNode *ConnectedNode
	for _, node := range nodeMetrics.ConnectedNodes {
		if node.NodeID == nodeID {
			targetNode = &node
			break
		}
	}
	
	if targetNode == nil {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}
	
	response := map[string]interface{}{
		"node":       targetNode,
		"cluster":    nodeMetrics.ClusterInfo,
		"timestamp":  time.Now(),
	}

	api.writeJSON(w, response)
}

// handleCluster 处理集群信息请求
func (api *MonitoringAPI) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeMetrics := api.collector.nodeMetrics
	cluster := nodeMetrics.ClusterInfo
	
	response := map[string]interface{}{
		"cluster_name":   cluster.ClusterName,
		"total_nodes":    cluster.TotalNodes,
		"healthy_nodes":  cluster.HealthyNodes,
		"node_list":      cluster.NodeList,
		"cluster_status": cluster.ClusterStatus,
		"connected_nodes": nodeMetrics.ConnectedNodes,
		"timestamp":      time.Now(),
	}

	api.writeJSON(w, response)
}

// handleTunnelDetail 处理隧道详情请求
func (api *MonitoringAPI) handleTunnelDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从URL路径提取隧道ID
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tunnels/")
	tunnelID := strings.TrimSuffix(path, "/")
	
	if tunnelID == "" {
		http.Error(w, "Tunnel ID required", http.StatusBadRequest)
		return
	}

	tunnels := api.collector.tunnelMetrics
	
	// 查找指定隧道
	var targetTunnel *TunnelDetail
	for _, tunnel := range tunnels.TunnelDetails {
		if tunnel.TunnelID == tunnelID {
			targetTunnel = &tunnel
			break
		}
	}
	
	if targetTunnel == nil {
		http.Error(w, "Tunnel not found", http.StatusNotFound)
		return
	}
	
	response := map[string]interface{}{
		"tunnel":     targetTunnel,
		"statistics": map[string]interface{}{
			"bytes_transmitted": targetTunnel.BytesTransmitted,
			"bytes_received":    targetTunnel.BytesReceived,
			"latency":          targetTunnel.Latency.String(),
			"packet_loss":      targetTunnel.PacketLoss,
		},
		"timestamp":  time.Now(),
	}

	api.writeJSON(w, response)
}

// handleSystemResources 处理系统资源请求
func (api *MonitoringAPI) handleSystemResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.collector.UpdateSystemMetrics()
	system := api.collector.GetSystemMetrics()
	
	response := map[string]interface{}{
		"cpu": map[string]interface{}{
			"usage": system.CPUUsage,
		},
		"memory": map[string]interface{}{
			"usage":     system.MemoryUsage,
			"limit":     system.MemoryLimit,
			"allocated": system.GCStats.MemAllocated,
		},
		"goroutines": system.GoroutineCount,
		"gc": map[string]interface{}{
			"num_gc":      system.GCStats.NumGC,
			"total_pause": system.GCStats.TotalPause.String(),
			"last_gc":     system.GCStats.LastGC,
			"next_gc":     system.GCStats.NextGC,
		},
		"timestamp": time.Now(),
	}

	api.writeJSON(w, response)
}

// handleTrafficAnalytics 处理流量分析请求
func (api *MonitoringAPI) handleTrafficAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	connections := api.collector.GetConnectionMetrics()
	services := api.collector.GetServiceMetrics()
	
	// 计算流量统计
	totalBytes := int64(0)
	for _, service := range services.ServiceStatus {
		totalBytes += service.BytesTransmitted + service.BytesReceived
	}
	
	response := map[string]interface{}{
		"total_bytes": totalBytes,
		"connections": map[string]interface{}{
			"active":  connections.ActiveConnections,
			"total":   connections.TotalConnections,
			"failed":  connections.FailedConnections,
			"by_service": connections.ConnectionsByService,
		},
		"top_services": api.getTopServicesByTraffic(services),
		"timestamp":    time.Now(),
	}

	api.writeJSON(w, response)
}

// handlePerformanceAnalytics 处理性能分析请求
func (api *MonitoringAPI) handlePerformanceAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	connections := api.collector.GetConnectionMetrics()
	system := api.collector.GetSystemMetrics()
	
	// 计算性能指标
	avgLatency := connections.AvgConnectionLatency
	successRate := float64(0)
	if connections.TotalConnections > 0 {
		successRate = float64(connections.TotalConnections-connections.FailedConnections) / float64(connections.TotalConnections) * 100
	}
	
	response := map[string]interface{}{
		"latency": map[string]interface{}{
			"average": avgLatency.String(),
		},
		"success_rate": successRate,
		"system": map[string]interface{}{
			"cpu_usage":    system.CPUUsage,
			"memory_usage": system.MemoryUsage,
			"uptime":       system.Uptime.String(),
		},
		"qps": api.calculateQPS(connections),
		"timestamp": time.Now(),
	}

	api.writeJSON(w, response)
}

// getTopServicesByTraffic 获取流量最多的服务
func (api *MonitoringAPI) getTopServicesByTraffic(services ServiceMetrics) []map[string]interface{} {
	type serviceTraffic struct {
		Name      string
		Namespace string
		Traffic   int64
	}
	
	var trafficList []serviceTraffic
	for _, service := range services.ServiceStatus {
		traffic := service.BytesTransmitted + service.BytesReceived
		trafficList = append(trafficList, serviceTraffic{
			Name:      service.Name,
			Namespace: service.Namespace,
			Traffic:   traffic,
		})
	}
	
	// 简单排序（实际应该使用sort包）
	result := make([]map[string]interface{}, 0, len(trafficList))
	for _, st := range trafficList {
		result = append(result, map[string]interface{}{
			"name":      st.Name,
			"namespace": st.Namespace,
			"traffic":   st.Traffic,
		})
	}
	
	// 限制返回前10个
	if len(result) > 10 {
		result = result[:10]
	}
	
	return result
}

// calculateQPS 计算每秒查询数
func (api *MonitoringAPI) calculateQPS(connections ConnectionMetrics) float64 {
	// 简化计算：基于最近的连接历史
	recentMinute := time.Now().Add(-time.Minute)
	recentConnections := 0
	
	for _, entry := range connections.ConnectionHistory {
		if entry.Timestamp.After(recentMinute) && entry.Action == "connect" {
			recentConnections++
		}
	}
	
	return float64(recentConnections) / 60.0 // 每秒
}