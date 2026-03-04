// 监控系统集成
package server

import (
	"fmt"
	"log"
	"time"

	"github.com/outpostos/OutpostNet/internal/metrics"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// startMetricsServer 启动监控服务器
func (s *Server) startMetricsServer() *metrics.MetricsServer {
	// 获取监控配置
	addr := fmt.Sprintf(":%d", s.config.Monitoring.Port)
	if s.config.Monitoring.Address != "" {
		addr = s.config.Monitoring.Address
	}
	
	// 创建监控服务器
	metricsServer := metrics.NewMetricsServer(addr)
	
	// 集成服务注册表
	metricsServer.IntegrateWithServer(&serverRegistryAdapter{server: s})
	
	// 启动Prometheus导出器
	if s.config.Monitoring.PrometheusEnabled {
		prometheusExporter := metrics.NewPrometheusExporter(metricsServer.GetCollector())
		prometheusExporter.StartExporter()
		log.Printf("Prometheus metrics enabled on %s/metrics", addr)
	}
	
	// 启动告警管理器
	if s.config.Monitoring.AlertsEnabled {
		alertManager := metrics.NewAlertManager(metricsServer.GetCollector())
		alertManager.Start()
		log.Printf("Alert manager started")
	}
	
	log.Printf("Metrics server will start on %s", addr)
	return metricsServer
}

// serverRegistryAdapter 服务注册表适配器
type serverRegistryAdapter struct {
	server *Server
}

// GetServices 获取所有服务
func (sra *serverRegistryAdapter) GetServices() []types.Service {
	services := make([]types.Service, 0)
	
	// 遍历所有注册的服务
	sra.server.clientsMu.RLock()
	defer sra.server.clientsMu.RUnlock()
	
	serviceMap := make(map[string]types.Service)
	
	for _, client := range sra.server.clients {
		if client.Mode == "register" {
			for _, workload := range client.Workloads {
				// 从工作负载ID解析服务信息
				service := sra.parseWorkloadToService(workload, client)
				serviceMap[service.Metadata.Name] = service
			}
		}
	}
	
	for _, service := range serviceMap {
		services = append(services, service)
	}
	
	return services
}

// GetServiceEndpoints 获取服务端点
func (sra *serverRegistryAdapter) GetServiceEndpoints(serviceName string) []types.Endpoint {
	endpoints := make([]types.Endpoint, 0)
	
	sra.server.clientsMu.RLock()
	defer sra.server.clientsMu.RUnlock()
	
	for clientID, client := range sra.server.clients {
		if client.Mode == "register" {
			for _, workload := range client.Workloads {
				if sra.workloadMatchesService(workload, serviceName) {
					endpoint := types.Endpoint{
						ID:       clientID,
						IP:       sra.extractIPFromClient(client),
						Port:     sra.extractPortFromWorkload(workload),
						Protocol: "tcp",
						Status: types.EndpointStatus{
							Ready: sra.isClientHealthy(client),
						},
						Labels:   sra.extractLabelsFromWorkload(workload),
						LastSeen: time.Now(),
					}
					endpoints = append(endpoints, endpoint)
				}
			}
		}
	}
	
	return endpoints
}

// Subscribe 订阅服务变更事件
func (sra *serverRegistryAdapter) Subscribe(callback func(event string, service types.Service)) {
	// 这里应该实现事件订阅机制
	// 由于当前server.go没有事件系统，我们暂时使用定期轮询
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		var lastServices map[string]types.Service
		
		for range ticker.C {
			currentServices := make(map[string]types.Service)
			for _, service := range sra.GetServices() {
				currentServices[service.Metadata.Name] = service
			}
			
			if lastServices != nil {
				// 检查新增服务
				for name, service := range currentServices {
					if _, exists := lastServices[name]; !exists {
						callback("service_added", service)
					}
				}
				
				// 检查删除服务
				for name, service := range lastServices {
					if _, exists := currentServices[name]; !exists {
						callback("service_removed", service)
					}
				}
				
				// 检查更新服务
				for name, service := range currentServices {
					if lastService, exists := lastServices[name]; exists {
						if sra.serviceChanged(lastService, service) {
							callback("service_updated", service)
						}
					}
				}
			}
			
			lastServices = currentServices
		}
	}()
}

// parseWorkloadToService 解析工作负载为服务
func (sra *serverRegistryAdapter) parseWorkloadToService(workload string, client *ClientConn) types.Service {
	// 这里需要根据实际的工作负载格式进行解析
	// 暂时使用简单的实现
	return types.Service{
		Metadata: types.ServiceMetadata{
			Name:      workload, // 简化：直接使用workload作为服务名
			Namespace: "default",
			Labels: map[string]string{
				"client-id":   client.ID,
				"client-mode": client.Mode,
			},
		},
		Spec: types.ServiceSpec{
			ProxyMode: "tcp",
		},
	}
}

// workloadMatchesService 检查工作负载是否匹配服务
func (sra *serverRegistryAdapter) workloadMatchesService(workload, serviceName string) bool {
	// 简化实现：直接比较名称
	return workload == serviceName
}

// extractIPFromClient 从客户端提取IP
func (sra *serverRegistryAdapter) extractIPFromClient(client *ClientConn) string {
	if client.Conn != nil {
		return client.Conn.RemoteAddr().String()
	}
	return "unknown"
}

// extractPortFromWorkload 从工作负载提取端口
func (sra *serverRegistryAdapter) extractPortFromWorkload(workload string) int {
	// 这里需要根据实际格式解析端口
	// 暂时返回默认端口
	return 8080
}

// isClientHealthy 检查客户端是否健康
func (sra *serverRegistryAdapter) isClientHealthy(client *ClientConn) bool {
	// 检查最后心跳时间
	return time.Since(client.LastSeen) < 60*time.Second
}

// extractLabelsFromWorkload 从工作负载提取标签
func (sra *serverRegistryAdapter) extractLabelsFromWorkload(workload string) map[string]string {
	// 简化实现
	return map[string]string{
		"workload": workload,
	}
}

// serviceChanged 检查服务是否发生变化
func (sra *serverRegistryAdapter) serviceChanged(old, new types.Service) bool {
	// 简化实现：检查基本属性
	return old.Spec.ProxyMode != new.Spec.ProxyMode || 
		   len(old.Metadata.Labels) != len(new.Metadata.Labels)
}

// MetricsReporter 监控报告器，实现到Server的监控集成
type MetricsReporter struct {
	collector *metrics.MetricsCollector
}

// NewMetricsReporter 创建监控报告器
func NewMetricsReporter(collector *metrics.MetricsCollector) *MetricsReporter {
	return &MetricsReporter{
		collector: collector,
	}
}

// ReportConnectionEvent 报告连接事件
func (mr *MetricsReporter) ReportConnectionEvent(action, serviceName, clientID string, latency time.Duration) {
	mr.collector.RecordConnection(action, serviceName, clientID, latency)
}

// ReportTunnelMetrics 报告隧道指标
func (mr *MetricsReporter) ReportTunnelMetrics(tunnelType, tunnelID, localEP, remoteEP, serviceName string) {
	mr.collector.RecordTunnel(tunnelType, tunnelID, localEP, remoteEP, serviceName)
}

// 在Server中集成监控报告
func (s *Server) getMetricsReporter() *MetricsReporter {
	// 这个方法可以在Server中使用，用于报告各种事件
	if s.config.Monitoring.Enabled {
		// 这里需要获取MetricsCollector实例
		// 由于架构原因，我们暂时返回nil，实际实现中需要重构
		return nil
	}
	return nil
}