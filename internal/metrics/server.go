// 监控服务器实现
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// MetricsServer 监控服务器
type MetricsServer struct {
	collector *MetricsCollector
	api       *MonitoringAPI
	server    *http.Server
	addr      string
}

// NewMetricsServer 创建监控服务器
func NewMetricsServer(addr string) *MetricsServer {
	collector := NewMetricsCollector()
	api := NewMonitoringAPI(collector)
	
	mux := http.NewServeMux()
	
	// 注册监控API路由
	api.RegisterRoutes(mux)
	
	// 注册静态资源和UI
	registerStaticRoutes(mux)
	
	// 注册WebSocket实时监控
	registerWebSocketRoutes(mux, collector)
	
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	
	return &MetricsServer{
		collector: collector,
		api:       api,
		server:    server,
		addr:      addr,
	}
}

// GetCollector 获取指标收集器
func (ms *MetricsServer) GetCollector() *MetricsCollector {
	return ms.collector
}

// Start 启动监控服务器
func (ms *MetricsServer) Start(ctx context.Context) error {
	// 启动后台任务
	go ms.backgroundTasks(ctx)
	
	// 启动HTTP服务器
	go func() {
		if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Metrics server error: %v\n", err)
		}
	}()
	
	fmt.Printf("Metrics server started on %s\n", ms.addr)
	
	// 等待上下文取消
	<-ctx.Done()
	
	// 优雅关闭
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	return ms.server.Shutdown(shutdownCtx)
}

// backgroundTasks 后台任务
func (ms *MetricsServer) backgroundTasks(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// 更新系统指标
			ms.collector.UpdateSystemMetrics()
			
		case <-ctx.Done():
			return
		}
	}
}

// registerStaticRoutes 注册静态资源路由
func registerStaticRoutes(mux *http.ServeMux) {
	// 监控仪表板首页
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(dashboardHTML))
	})
	
	// 静态资源
	mux.HandleFunc("/static/", func(w http.ResponseWriter, r *http.Request) {
		// 这里可以提供CSS、JS等静态资源
		// 为了简化，现在只提供基本的响应
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("Static resources"))
	})
}

// registerWebSocketRoutes 注册WebSocket路由
func registerWebSocketRoutes(mux *http.ServeMux, collector *MetricsCollector) {
	mux.HandleFunc("/ws/metrics", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocketMetrics(w, r, collector)
	})
}

// handleWebSocketMetrics 处理WebSocket监控连接
func handleWebSocketMetrics(w http.ResponseWriter, r *http.Request, collector *MetricsCollector) {
	// 简化的WebSocket实现
	// 在实际项目中，应该使用gorilla/websocket等库
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	
	// 发送实时指标数据
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			collector.UpdateSystemMetrics()
			metrics := collector.GetMetrics()
			
			// 发送JSON数据
			fmt.Fprintf(w, "data: %v\n\n", metrics)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			
		case <-r.Context().Done():
			return
		}
	}
}

// ServiceMetricsReporter 服务指标报告器接口
type ServiceMetricsReporter interface {
	ReportServiceStatus(service types.Service, endpoints []types.Endpoint)
	ReportConnectionEvent(action, serviceName, clientID string, latency time.Duration)
	ReportTunnelMetrics(tunnelType, tunnelID, localEP, remoteEP, serviceName string)
	ReportLoadBalanceStats(serviceName string, stats LoadBalanceStats)
}

// ServiceRegistry 服务注册表接口
type ServiceRegistry interface {
	GetServices() []types.Service
	GetServiceEndpoints(serviceName string) []types.Endpoint
	Subscribe(callback func(event string, service types.Service))
}

// IntegrateWithServer 与outpostnet服务器集成
func (ms *MetricsServer) IntegrateWithServer(registry ServiceRegistry) {
	// 订阅服务变更事件
	registry.Subscribe(func(event string, service types.Service) {
		endpoints := registry.GetServiceEndpoints(service.Metadata.Name)
		ms.collector.UpdateServiceStatus(service, endpoints)
	})
	
	// 定期同步服务状态
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				services := registry.GetServices()
				for _, service := range services {
					endpoints := registry.GetServiceEndpoints(service.Metadata.Name)
					ms.collector.UpdateServiceStatus(service, endpoints)
				}
			}
		}
	}()
}

// 监控仪表板HTML模板
const dashboardHTML = `
<!DOCTYPE html>
<html>
<head>
    <title>ServiceKeel 监控仪表板</title>
    <meta charset="utf-8">
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background-color: #f5f5f5; 
        }
        .dashboard { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); 
            gap: 20px; 
        }
        .card { 
            background: white; 
            border-radius: 8px; 
            padding: 20px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        }
        .card h3 { 
            margin-top: 0; 
            color: #333; 
        }
        .metric { 
            display: flex; 
            justify-content: space-between; 
            padding: 10px 0; 
            border-bottom: 1px solid #eee; 
        }
        .metric:last-child { 
            border-bottom: none; 
        }
        .metric-value { 
            font-weight: bold; 
            color: #007acc; 
        }
        .status-healthy { 
            color: #28a745; 
        }
        .status-unhealthy { 
            color: #dc3545; 
        }
        .header { 
            text-align: center; 
            margin-bottom: 30px; 
        }
        .refresh-btn {
            background: #007acc;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            margin: 10px;
        }
        .refresh-btn:hover {
            background: #005fa3;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 ServiceKeel 监控仪表板</h1>
        <button class="refresh-btn" onclick="refreshData()">🔄 刷新数据</button>
        <button class="refresh-btn" onclick="toggleAutoRefresh()">⏱️ 自动刷新</button>
    </div>
    
    <div class="dashboard">
        <div class="card">
            <h3>📊 系统概览</h3>
            <div id="system-overview">
                <div class="metric">
                    <span>CPU 使用率</span>
                    <span class="metric-value" id="cpu-usage">加载中...</span>
                </div>
                <div class="metric">
                    <span>内存使用</span>
                    <span class="metric-value" id="memory-usage">加载中...</span>
                </div>
                <div class="metric">
                    <span>运行时间</span>
                    <span class="metric-value" id="uptime">加载中...</span>
                </div>
                <div class="metric">
                    <span>Goroutines</span>
                    <span class="metric-value" id="goroutines">加载中...</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>🌐 服务状态</h3>
            <div id="service-status">
                <div class="metric">
                    <span>服务总数</span>
                    <span class="metric-value" id="total-services">加载中...</span>
                </div>
                <div class="metric">
                    <span>健康服务</span>
                    <span class="metric-value status-healthy" id="healthy-services">加载中...</span>
                </div>
                <div class="metric">
                    <span>异常服务</span>
                    <span class="metric-value status-unhealthy" id="unhealthy-services">加载中...</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>🔗 连接统计</h3>
            <div id="connection-stats">
                <div class="metric">
                    <span>活跃连接</span>
                    <span class="metric-value" id="active-connections">加载中...</span>
                </div>
                <div class="metric">
                    <span>总连接数</span>
                    <span class="metric-value" id="total-connections">加载中...</span>
                </div>
                <div class="metric">
                    <span>失败连接</span>
                    <span class="metric-value status-unhealthy" id="failed-connections">加载中...</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>🚇 隧道统计</h3>
            <div id="tunnel-stats">
                <div class="metric">
                    <span>活跃隧道</span>
                    <span class="metric-value" id="active-tunnels">加载中...</span>
                </div>
                <div class="metric">
                    <span>TCP 隧道</span>
                    <span class="metric-value" id="tcp-tunnels">加载中...</span>
                </div>
                <div class="metric">
                    <span>STCP 隧道</span>
                    <span class="metric-value" id="stcp-tunnels">加载中...</span>
                </div>
                <div class="metric">
                    <span>UDP 隧道</span>
                    <span class="metric-value" id="udp-tunnels">加载中...</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>🏥 集群健康</h3>
            <div id="cluster-health">
                <div class="metric">
                    <span>节点总数</span>
                    <span class="metric-value" id="total-nodes">加载中...</span>
                </div>
                <div class="metric">
                    <span>健康节点</span>
                    <span class="metric-value status-healthy" id="healthy-nodes">加载中...</span>
                </div>
                <div class="metric">
                    <span>集群状态</span>
                    <span class="metric-value" id="cluster-status">加载中...</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>📈 性能指标</h3>
            <div id="performance-metrics">
                <div class="metric">
                    <span>平均延迟</span>
                    <span class="metric-value" id="avg-latency">加载中...</span>
                </div>
                <div class="metric">
                    <span>QPS</span>
                    <span class="metric-value" id="qps">加载中...</span>
                </div>
                <div class="metric">
                    <span>成功率</span>
                    <span class="metric-value" id="success-rate">加载中...</span>
                </div>
            </div>
        </div>
    </div>

    <script>
        let autoRefresh = false;
        let refreshInterval;
        
        function refreshData() {
            // 获取系统概览数据
            fetch('/api/v1/overview')
                .then(response => response.json())
                .then(data => {
                    // 更新系统指标
                    document.getElementById('cpu-usage').textContent = 
                        (data.system.cpu_usage || 0).toFixed(1) + '%';
                    document.getElementById('memory-usage').textContent = 
                        formatBytes(data.system.memory_usage || 0);
                    document.getElementById('uptime').textContent = 
                        data.system.uptime || '未知';
                    document.getElementById('goroutines').textContent = 
                        data.system.goroutines || 0;
                    
                    // 更新服务状态
                    document.getElementById('total-services').textContent = 
                        data.services.total || 0;
                    document.getElementById('healthy-services').textContent = 
                        data.services.healthy || 0;
                    document.getElementById('unhealthy-services').textContent = 
                        data.services.unhealthy || 0;
                    
                    // 更新连接统计
                    document.getElementById('active-connections').textContent = 
                        data.connections.active || 0;
                    document.getElementById('total-connections').textContent = 
                        data.connections.total || 0;
                    document.getElementById('failed-connections').textContent = 
                        data.connections.failed || 0;
                    
                    // 更新隧道统计
                    document.getElementById('active-tunnels').textContent = 
                        data.tunnels.active || 0;
                    document.getElementById('tcp-tunnels').textContent = 
                        (data.tunnels.by_type && data.tunnels.by_type.tcp) || 0;
                    document.getElementById('stcp-tunnels').textContent = 
                        (data.tunnels.by_type && data.tunnels.by_type.stcp) || 0;
                    document.getElementById('udp-tunnels').textContent = 
                        (data.tunnels.by_type && data.tunnels.by_type.udp) || 0;
                })
                .catch(error => {
                    console.error('获取数据失败:', error);
                });
        }
        
        function toggleAutoRefresh() {
            autoRefresh = !autoRefresh;
            const btn = document.querySelector('button[onclick="toggleAutoRefresh()"]');
            
            if (autoRefresh) {
                btn.textContent = '⏹️ 停止自动刷新';
                refreshInterval = setInterval(refreshData, 5000);
            } else {
                btn.textContent = '⏱️ 自动刷新';
                clearInterval(refreshInterval);
            }
        }
        
        function formatBytes(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
        
        // 页面加载时获取一次数据
        document.addEventListener('DOMContentLoaded', function() {
            refreshData();
        });
    </script>
</body>
</html>
`