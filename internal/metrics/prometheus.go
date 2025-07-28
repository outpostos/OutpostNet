// Prometheus指标导出
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

// PrometheusExporter Prometheus指标导出器
type PrometheusExporter struct {
	// 服务指标
	servicesTotal       prometheus.Gauge
	servicesHealthy     prometheus.Gauge
	servicesUnhealthy   prometheus.Gauge
	endpointsTotal      *prometheus.GaugeVec
	
	// 连接指标
	connectionsActive   prometheus.Gauge
	connectionsTotal    prometheus.Counter
	connectionsFailed   prometheus.Counter
	connectionDuration  prometheus.Histogram
	
	// 隧道指标
	tunnelsActive       *prometheus.GaugeVec
	tunnelsTotal        *prometheus.CounterVec
	tunnelBytes         *prometheus.CounterVec
	tunnelLatency       *prometheus.HistogramVec
	
	// 系统指标
	cpuUsage           prometheus.Gauge
	memoryUsage        prometheus.Gauge
	goroutines         prometheus.Gauge
	gcDuration         prometheus.Histogram
	
	// 负载均衡指标
	lbRequests         *prometheus.CounterVec
	lbLatency          *prometheus.HistogramVec
	lbFailovers        *prometheus.CounterVec
	
	// 安全指标
	authAttempts       *prometheus.CounterVec
	authFailures       *prometheus.CounterVec
	certificateExpiry  *prometheus.GaugeVec
	
	collector *MetricsCollector
}

// NewPrometheusExporter 创建Prometheus导出器
func NewPrometheusExporter(collector *MetricsCollector) *PrometheusExporter {
	return &PrometheusExporter{
		// 服务指标
		servicesTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "services",
			Name:      "total",
			Help:      "Total number of services",
		}),
		servicesHealthy: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "services",
			Name:      "healthy",
			Help:      "Number of healthy services",
		}),
		servicesUnhealthy: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "services",
			Name:      "unhealthy",
			Help:      "Number of unhealthy services",
		}),
		endpointsTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "endpoints",
			Name:      "total",
			Help:      "Total number of endpoints per service",
		}, []string{"service", "namespace", "status"}),
		
		// 连接指标
		connectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "connections",
			Name:      "active",
			Help:      "Number of active connections",
		}),
		connectionsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "connections",
			Name:      "total",
			Help:      "Total number of connections",
		}),
		connectionsFailed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "connections",
			Name:      "failed_total",
			Help:      "Total number of failed connections",
		}),
		connectionDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "servicekeel",
			Subsystem: "connections",
			Name:      "duration_seconds",
			Help:      "Connection duration in seconds",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
		}),
		
		// 隧道指标
		tunnelsActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "tunnels",
			Name:      "active",
			Help:      "Number of active tunnels by type",
		}, []string{"type", "service"}),
		tunnelsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "tunnels",
			Name:      "total",
			Help:      "Total number of tunnels by type",
		}, []string{"type", "service"}),
		tunnelBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "tunnels",
			Name:      "bytes_total",
			Help:      "Total bytes transferred through tunnels",
		}, []string{"type", "service", "direction"}),
		tunnelLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "servicekeel",
			Subsystem: "tunnels",
			Name:      "latency_seconds",
			Help:      "Tunnel latency in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}, []string{"type", "service"}),
		
		// 系统指标
		cpuUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "system",
			Name:      "cpu_usage_percent",
			Help:      "CPU usage percentage",
		}),
		memoryUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "system",
			Name:      "memory_usage_bytes",
			Help:      "Memory usage in bytes",
		}),
		goroutines: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "system",
			Name:      "goroutines",
			Help:      "Number of goroutines",
		}),
		gcDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "servicekeel",
			Subsystem: "system",
			Name:      "gc_duration_seconds",
			Help:      "Garbage collection duration in seconds",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		}),
		
		// 负载均衡指标
		lbRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "loadbalancer",
			Name:      "requests_total",
			Help:      "Total number of load balancer requests",
		}, []string{"service", "algorithm", "endpoint"}),
		lbLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "servicekeel",
			Subsystem: "loadbalancer",
			Name:      "latency_seconds",
			Help:      "Load balancer decision latency",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05},
		}, []string{"service", "algorithm"}),
		lbFailovers: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "loadbalancer",
			Name:      "failovers_total",
			Help:      "Total number of failovers",
		}, []string{"service", "from_endpoint", "to_endpoint"}),
		
		// 安全指标
		authAttempts: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "auth",
			Name:      "attempts_total",
			Help:      "Total number of authentication attempts",
		}, []string{"method", "result"}),
		authFailures: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "servicekeel",
			Subsystem: "auth",
			Name:      "failures_total",
			Help:      "Total number of authentication failures",
		}, []string{"method", "reason"}),
		certificateExpiry: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "servicekeel",
			Subsystem: "certificates",
			Name:      "expiry_timestamp",
			Help:      "Certificate expiry timestamp",
		}, []string{"subject", "issuer"}),
		
		collector: collector,
	}
}

// UpdateMetrics 更新Prometheus指标
func (pe *PrometheusExporter) UpdateMetrics() {
	// 获取当前指标数据
	serviceMetrics := pe.collector.GetServiceMetrics()
	connectionMetrics := pe.collector.GetConnectionMetrics()
	systemMetrics := pe.collector.GetSystemMetrics()
	
	// 更新服务指标
	pe.servicesTotal.Set(float64(serviceMetrics.TotalServices))
	pe.servicesHealthy.Set(float64(serviceMetrics.HealthyServices))
	pe.servicesUnhealthy.Set(float64(serviceMetrics.UnhealthyServices))
	
	// 更新端点指标
	for _, status := range serviceMetrics.ServiceStatus {
		for _, endpoint := range status.Endpoints {
			pe.endpointsTotal.WithLabelValues(
				status.Name,
				status.Namespace,
				endpoint.Status,
			).Set(1)
		}
	}
	
	// 更新连接指标
	pe.connectionsActive.Set(float64(connectionMetrics.ActiveConnections))
	pe.connectionsTotal.Add(float64(connectionMetrics.TotalConnections))
	pe.connectionsFailed.Add(float64(connectionMetrics.FailedConnections))
	
	// 更新隧道指标
	tunnelMetrics := pe.collector.tunnelMetrics
	for tunnelType, count := range tunnelMetrics.TunnelsByType {
		pe.tunnelsActive.WithLabelValues(tunnelType, "").Set(float64(count))
	}
	
	// 更新系统指标
	pe.cpuUsage.Set(systemMetrics.CPUUsage)
	pe.memoryUsage.Set(float64(systemMetrics.MemoryUsage))
	pe.goroutines.Set(float64(systemMetrics.GoroutineCount))
}

// RecordConnectionEvent 记录连接事件
func (pe *PrometheusExporter) RecordConnectionEvent(serviceName, result string, duration time.Duration) {
	pe.connectionDuration.Observe(duration.Seconds())
	if result == "failed" {
		pe.connectionsFailed.Inc()
	}
}

// RecordTunnelEvent 记录隧道事件
func (pe *PrometheusExporter) RecordTunnelEvent(tunnelType, serviceName string, latency time.Duration, bytesTransferred int64, direction string) {
	pe.tunnelLatency.WithLabelValues(tunnelType, serviceName).Observe(latency.Seconds())
	pe.tunnelBytes.WithLabelValues(tunnelType, serviceName, direction).Add(float64(bytesTransferred))
}

// RecordLoadBalancerEvent 记录负载均衡事件
func (pe *PrometheusExporter) RecordLoadBalancerEvent(serviceName, algorithm, endpoint string, latency time.Duration) {
	pe.lbRequests.WithLabelValues(serviceName, algorithm, endpoint).Inc()
	pe.lbLatency.WithLabelValues(serviceName, algorithm).Observe(latency.Seconds())
}

// RecordFailover 记录故障转移事件
func (pe *PrometheusExporter) RecordFailover(serviceName, fromEndpoint, toEndpoint string) {
	pe.lbFailovers.WithLabelValues(serviceName, fromEndpoint, toEndpoint).Inc()
}

// RecordAuthEvent 记录认证事件
func (pe *PrometheusExporter) RecordAuthEvent(method, result, reason string) {
	pe.authAttempts.WithLabelValues(method, result).Inc()
	if result == "failure" {
		pe.authFailures.WithLabelValues(method, reason).Inc()
	}
}

// RecordCertificateExpiry 记录证书过期时间
func (pe *PrometheusExporter) RecordCertificateExpiry(subject, issuer string, expiryTime time.Time) {
	pe.certificateExpiry.WithLabelValues(subject, issuer).Set(float64(expiryTime.Unix()))
}

// StartExporter 启动Prometheus导出器
func (pe *PrometheusExporter) StartExporter() {
	// 定期更新指标
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		
		for range ticker.C {
			pe.UpdateMetrics()
		}
	}()
}

// GetPrometheusRegistry 获取Prometheus注册表
func (pe *PrometheusExporter) GetPrometheusRegistry() *prometheus.Registry {
	return prometheus.DefaultRegisterer.(*prometheus.Registry)
}