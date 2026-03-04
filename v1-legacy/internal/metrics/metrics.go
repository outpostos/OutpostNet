// 服务网格指标收集和监控
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// MetricsCollector 指标收集器
type MetricsCollector struct {
	mu               sync.RWMutex
	connectionMetrics ConnectionMetrics
	serviceMetrics   ServiceMetrics
	systemMetrics    SystemMetrics
	nodeMetrics      NodeMetrics
	tunnelMetrics    TunnelMetrics
}

// ConnectionMetrics 连接指标
type ConnectionMetrics struct {
	ActiveConnections    int64                     `json:"active_connections"`
	TotalConnections     int64                     `json:"total_connections"`
	FailedConnections    int64                     `json:"failed_connections"`
	ConnectionsByService map[string]int64          `json:"connections_by_service"`
	ConnectionsByNode    map[string]int64          `json:"connections_by_node"`
	AvgConnectionLatency time.Duration             `json:"avg_connection_latency"`
	ConnectionHistory    []ConnectionHistoryEntry `json:"connection_history"`
}

// ConnectionHistoryEntry 连接历史条目
type ConnectionHistoryEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	Action      string    `json:"action"` // connect, disconnect, failed
	ServiceName string    `json:"service_name"`
	ClientID    string    `json:"client_id"`
	Latency     int64     `json:"latency_ms"`
}

// ServiceMetrics 服务指标
type ServiceMetrics struct {
	TotalServices     int64                       `json:"total_services"`
	HealthyServices   int64                       `json:"healthy_services"`
	UnhealthyServices int64                       `json:"unhealthy_services"`
	ServiceStatus     map[string]ServiceStatus    `json:"service_status"`
	EndpointCounts    map[string]int              `json:"endpoint_counts"`
	LoadBalanceStats  map[string]LoadBalanceStats `json:"load_balance_stats"`
}

// ServiceStatus 服务状态
type ServiceStatus struct {
	Name            string               `json:"name"`
	Namespace       string               `json:"namespace"`
	Status          string               `json:"status"` // healthy, unhealthy, unknown
	LastSeen        time.Time            `json:"last_seen"`
	Endpoints       []EndpointStatus     `json:"endpoints"`
	ProxyMode       string               `json:"proxy_mode"`
	ConnectionCount int64                `json:"connection_count"`
	BytesTransmitted int64               `json:"bytes_transmitted"`
	BytesReceived   int64                `json:"bytes_received"`
	Labels          map[string]string    `json:"labels"`
}

// EndpointStatus 端点状态
type EndpointStatus struct {
	ID               string            `json:"id"`
	Address          string            `json:"address"`
	Port             int               `json:"port"`
	Status           string            `json:"status"` // online, offline, degraded
	LastHealthCheck  time.Time         `json:"last_health_check"`
	ResponseTime     time.Duration     `json:"response_time"`
	FailureCount     int               `json:"failure_count"`
	Labels           map[string]string `json:"labels"`
	ConnectionCount  int64             `json:"connection_count"`
	BytesTransmitted int64             `json:"bytes_transmitted"`
	BytesReceived    int64             `json:"bytes_received"`
}

// LoadBalanceStats 负载均衡统计
type LoadBalanceStats struct {
	Algorithm         string                 `json:"algorithm"`
	RequestCount      int64                  `json:"request_count"`
	EndpointStats     map[string]int64       `json:"endpoint_stats"`
	FailoverCount     int64                  `json:"failover_count"`
	AvgResponseTime   time.Duration          `json:"avg_response_time"`
	LastFailover      time.Time              `json:"last_failover"`
}

// SystemMetrics 系统指标
type SystemMetrics struct {
	CPUUsage       float64   `json:"cpu_usage"`
	MemoryUsage    int64     `json:"memory_usage"`
	MemoryLimit    int64     `json:"memory_limit"`
	GoroutineCount int       `json:"goroutine_count"`
	GCStats        GCStats   `json:"gc_stats"`
	Uptime         time.Duration `json:"uptime"`
	Version        string    `json:"version"`
	BuildTime      string    `json:"build_time"`
	StartTime      time.Time `json:"start_time"`
}

// GCStats 垃圾回收统计
type GCStats struct {
	NumGC        uint32        `json:"num_gc"`
	TotalPause   time.Duration `json:"total_pause"`
	LastGC       time.Time     `json:"last_gc"`
	NextGC       uint64        `json:"next_gc"`
	MemAllocated uint64        `json:"mem_allocated"`
}

// NodeMetrics 节点指标
type NodeMetrics struct {
	NodeID           string                 `json:"node_id"`
	NodeType         string                 `json:"node_type"` // server, client
	Region           string                 `json:"region"`
	Zone             string                 `json:"zone"`
	ConnectedNodes   []ConnectedNode        `json:"connected_nodes"`
	NetworkBandwidth NetworkBandwidthStats  `json:"network_bandwidth"`
	ClusterInfo      ClusterInfo            `json:"cluster_info"`
}

// ConnectedNode 已连接节点信息
type ConnectedNode struct {
	NodeID       string        `json:"node_id"`
	Address      string        `json:"address"`
	NodeType     string        `json:"node_type"`
	ConnectedAt  time.Time     `json:"connected_at"`
	LastSeen     time.Time     `json:"last_seen"`
	Latency      time.Duration `json:"latency"`
	Status       string        `json:"status"` // connected, disconnecting, failed
}

// NetworkBandwidthStats 网络带宽统计
type NetworkBandwidthStats struct {
	InboundBps  int64 `json:"inbound_bps"`
	OutboundBps int64 `json:"outbound_bps"`
	InboundTotal  int64 `json:"inbound_total"`
	OutboundTotal int64 `json:"outbound_total"`
}

// ClusterInfo 集群信息
type ClusterInfo struct {
	ClusterName   string   `json:"cluster_name"`
	TotalNodes    int      `json:"total_nodes"`
	HealthyNodes  int      `json:"healthy_nodes"`
	NodeList      []string `json:"node_list"`
	ClusterStatus string   `json:"cluster_status"` // healthy, degraded, critical
}

// TunnelMetrics 隧道指标
type TunnelMetrics struct {
	ActiveTunnels   int64                      `json:"active_tunnels"`
	TotalTunnels    int64                      `json:"total_tunnels"`
	TunnelsByType   map[string]int64           `json:"tunnels_by_type"` // tcp, udp, stcp, sudp
	TunnelDetails   []TunnelDetail             `json:"tunnel_details"`
	EncryptionStats EncryptionStats            `json:"encryption_stats"`
}

// TunnelDetail 隧道详情
type TunnelDetail struct {
	TunnelID        string        `json:"tunnel_id"`
	Type            string        `json:"type"` // tcp, udp, stcp, sudp
	LocalEndpoint   string        `json:"local_endpoint"`
	RemoteEndpoint  string        `json:"remote_endpoint"`
	ServiceName     string        `json:"service_name"`
	Status          string        `json:"status"` // active, idle, failed
	CreatedAt       time.Time     `json:"created_at"`
	LastActivity    time.Time     `json:"last_activity"`
	BytesTransmitted int64        `json:"bytes_transmitted"`
	BytesReceived   int64         `json:"bytes_received"`
	Latency         time.Duration `json:"latency"`
	PacketLoss      float64       `json:"packet_loss"`
}

// EncryptionStats 加密统计
type EncryptionStats struct {
	EncryptedTunnels   int64 `json:"encrypted_tunnels"`
	UnencryptedTunnels int64 `json:"unencrypted_tunnels"`
	EncryptionOverhead int64 `json:"encryption_overhead_bytes"`
	CipherSuites       map[string]int64 `json:"cipher_suites"`
}

// NewMetricsCollector 创建指标收集器
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		connectionMetrics: ConnectionMetrics{
			ConnectionsByService: make(map[string]int64),
			ConnectionsByNode:    make(map[string]int64),
			ConnectionHistory:    make([]ConnectionHistoryEntry, 0, 1000),
		},
		serviceMetrics: ServiceMetrics{
			ServiceStatus:    make(map[string]ServiceStatus),
			EndpointCounts:   make(map[string]int),
			LoadBalanceStats: make(map[string]LoadBalanceStats),
		},
		systemMetrics: SystemMetrics{
			StartTime: time.Now(),
		},
		nodeMetrics: NodeMetrics{
			ConnectedNodes: make([]ConnectedNode, 0),
		},
		tunnelMetrics: TunnelMetrics{
			TunnelsByType: make(map[string]int64),
			TunnelDetails: make([]TunnelDetail, 0),
		},
	}
}

// RecordConnection 记录连接事件
func (mc *MetricsCollector) RecordConnection(action, serviceName, clientID string, latency time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// 更新连接计数
	switch action {
	case "connect":
		mc.connectionMetrics.ActiveConnections++
		mc.connectionMetrics.TotalConnections++
		mc.connectionMetrics.ConnectionsByService[serviceName]++
		mc.connectionMetrics.ConnectionsByNode[clientID]++
	case "disconnect":
		mc.connectionMetrics.ActiveConnections--
		if mc.connectionMetrics.ConnectionsByService[serviceName] > 0 {
			mc.connectionMetrics.ConnectionsByService[serviceName]--
		}
	case "failed":
		mc.connectionMetrics.FailedConnections++
	}

	// 记录连接历史
	entry := ConnectionHistoryEntry{
		Timestamp:   time.Now(),
		Action:      action,
		ServiceName: serviceName,
		ClientID:    clientID,
		Latency:     latency.Milliseconds(),
	}
	
	mc.connectionMetrics.ConnectionHistory = append(mc.connectionMetrics.ConnectionHistory, entry)
	
	// 保持历史记录大小
	if len(mc.connectionMetrics.ConnectionHistory) > 1000 {
		mc.connectionMetrics.ConnectionHistory = mc.connectionMetrics.ConnectionHistory[1:]
	}
}

// UpdateServiceStatus 更新服务状态
func (mc *MetricsCollector) UpdateServiceStatus(service types.Service, endpoints []types.Endpoint) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	serviceKey := fmt.Sprintf("%s/%s", service.Metadata.Namespace, service.Metadata.Name)
	
	// 构建端点状态
	endpointStatuses := make([]EndpointStatus, len(endpoints))
	healthyCount := 0
	
	for i, ep := range endpoints {
		status := "offline"
		if ep.Status.Ready {
			status = "online"
			healthyCount++
		}
		
		endpointStatuses[i] = EndpointStatus{
			ID:              ep.ID,
			Address:         ep.IP,
			Port:            ep.Port,
			Status:          status,
			LastHealthCheck: time.Now(),
			Labels:          ep.Labels,
		}
	}
	
	// 确定服务整体状态
	serviceStatus := "unhealthy"
	if healthyCount > 0 {
		serviceStatus = "healthy"
	}
	if len(endpoints) == 0 {
		serviceStatus = "unknown"
	}
	
	mc.serviceMetrics.ServiceStatus[serviceKey] = ServiceStatus{
		Name:      service.Metadata.Name,
		Namespace: service.Metadata.Namespace,
		Status:    serviceStatus,
		LastSeen:  time.Now(),
		Endpoints: endpointStatuses,
		ProxyMode: service.Spec.ProxyMode,
		Labels:    service.Metadata.Labels,
	}
	
	mc.serviceMetrics.EndpointCounts[serviceKey] = len(endpoints)
	
	// 更新服务总数
	mc.updateServiceCounts()
}

// updateServiceCounts 更新服务计数
func (mc *MetricsCollector) updateServiceCounts() {
	total := int64(len(mc.serviceMetrics.ServiceStatus))
	healthy := int64(0)
	unhealthy := int64(0)
	
	for _, status := range mc.serviceMetrics.ServiceStatus {
		switch status.Status {
		case "healthy":
			healthy++
		case "unhealthy":
			unhealthy++
		}
	}
	
	mc.serviceMetrics.TotalServices = total
	mc.serviceMetrics.HealthyServices = healthy
	mc.serviceMetrics.UnhealthyServices = unhealthy
}

// UpdateSystemMetrics 更新系统指标
func (mc *MetricsCollector) UpdateSystemMetrics() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	mc.systemMetrics.MemoryUsage = int64(m.Alloc)
	mc.systemMetrics.GoroutineCount = runtime.NumGoroutine()
	mc.systemMetrics.Uptime = time.Since(mc.systemMetrics.StartTime)
	
	mc.systemMetrics.GCStats = GCStats{
		NumGC:        m.NumGC,
		TotalPause:   time.Duration(m.PauseTotalNs),
		NextGC:       m.NextGC,
		MemAllocated: m.Alloc,
	}
	
	if m.NumGC > 0 {
		mc.systemMetrics.GCStats.LastGC = time.Unix(0, int64(m.LastGC))
	}
}

// RecordTunnel 记录隧道信息
func (mc *MetricsCollector) RecordTunnel(tunnelType, tunnelID, localEP, remoteEP, serviceName string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.tunnelMetrics.ActiveTunnels++
	mc.tunnelMetrics.TotalTunnels++
	mc.tunnelMetrics.TunnelsByType[tunnelType]++
	
	tunnel := TunnelDetail{
		TunnelID:       tunnelID,
		Type:           tunnelType,
		LocalEndpoint:  localEP,
		RemoteEndpoint: remoteEP,
		ServiceName:    serviceName,
		Status:         "active",
		CreatedAt:      time.Now(),
		LastActivity:   time.Now(),
	}
	
	mc.tunnelMetrics.TunnelDetails = append(mc.tunnelMetrics.TunnelDetails, tunnel)
}

// GetMetrics 获取所有指标
func (mc *MetricsCollector) GetMetrics() map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	return map[string]interface{}{
		"connections": mc.connectionMetrics,
		"services":    mc.serviceMetrics,
		"system":      mc.systemMetrics,
		"node":        mc.nodeMetrics,
		"tunnels":     mc.tunnelMetrics,
		"timestamp":   time.Now(),
	}
}

// GetConnectionMetrics 获取连接指标
func (mc *MetricsCollector) GetConnectionMetrics() ConnectionMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.connectionMetrics
}

// GetServiceMetrics 获取服务指标
func (mc *MetricsCollector) GetServiceMetrics() ServiceMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.serviceMetrics
}

// GetSystemMetrics 获取系统指标
func (mc *MetricsCollector) GetSystemMetrics() SystemMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.systemMetrics
}

// ServeHTTP 实现 HTTP 处理器接口
func (mc *MetricsCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	// 更新系统指标
	mc.UpdateSystemMetrics()
	
	// 获取所有指标
	metrics := mc.GetMetrics()
	
	// 编码为 JSON
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metrics); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// StartMetricsServer 启动指标服务器
func (mc *MetricsCollector) StartMetricsServer(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	
	// 注册各种指标端点
	mux.Handle("/metrics", mc)
	mux.HandleFunc("/metrics/connections", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mc.GetConnectionMetrics())
	})
	mux.HandleFunc("/metrics/services", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mc.GetServiceMetrics())
	})
	mux.HandleFunc("/metrics/system", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mc.UpdateSystemMetrics()
		json.NewEncoder(w).Encode(mc.GetSystemMetrics())
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "healthy",
			"timestamp": time.Now(),
		})
	})
	
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()
	
	return server.ListenAndServe()
}