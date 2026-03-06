package agent

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the Agent.
type Metrics struct {
	TunnelState       *prometheus.GaugeVec
	ActiveStreams      prometheus.Gauge
	BytesSentTotal    *prometheus.CounterVec
	BytesRecvTotal    *prometheus.CounterVec
	ReconnectTotal    *prometheus.CounterVec
	StreamLatency     prometheus.Histogram
	ConfigReloadTotal prometheus.Counter

	registry *prometheus.Registry
}

// NewMetrics creates and registers all Agent metrics.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		TunnelState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outpostnet_tunnel_state",
			Help: "Tunnel connection state (0=disconnected, 1=connected)",
		}, []string{"remote_region"}),

		ActiveStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outpostnet_active_streams",
			Help: "Current number of active QUIC streams",
		}),

		BytesSentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outpostnet_bytes_sent_total",
			Help: "Total bytes sent through tunnels",
		}, []string{"remote_region", "service"}),

		BytesRecvTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outpostnet_bytes_received_total",
			Help: "Total bytes received through tunnels",
		}, []string{"remote_region", "service"}),

		ReconnectTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outpostnet_reconnect_total",
			Help: "Total number of tunnel reconnections",
		}, []string{"remote_region"}),

		StreamLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "outpostnet_stream_latency_seconds",
			Help:    "Stream establishment latency in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
		}),

		ConfigReloadTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outpostnet_config_reload_total",
			Help: "Total number of configuration reloads",
		}),

		registry: reg,
	}

	reg.MustRegister(
		m.TunnelState,
		m.ActiveStreams,
		m.BytesSentTotal,
		m.BytesRecvTotal,
		m.ReconnectTotal,
		m.StreamLatency,
		m.ConfigReloadTotal,
	)

	return m
}

// Handler returns an HTTP handler for the /metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SetTunnelState sets the tunnel state for a region.
func (m *Metrics) SetTunnelState(region string, connected bool) {
	val := 0.0
	if connected {
		val = 1.0
	}
	m.TunnelState.WithLabelValues(region).Set(val)
}

// StreamOpened increments the active stream count.
func (m *Metrics) StreamOpened() {
	m.ActiveStreams.Inc()
}

// StreamClosed decrements the active stream count.
func (m *Metrics) StreamClosed() {
	m.ActiveStreams.Dec()
}

// ObserveStreamLatency records the time taken to establish a stream.
func (m *Metrics) ObserveStreamLatency(d time.Duration) {
	m.StreamLatency.Observe(d.Seconds())
}

// AddBytesSent adds to the bytes sent counter for a region/service.
func (m *Metrics) AddBytesSent(region, service string, n int64) {
	m.BytesSentTotal.WithLabelValues(region, service).Add(float64(n))
}

// AddBytesRecv adds to the bytes received counter for a region/service.
func (m *Metrics) AddBytesRecv(region, service string, n int64) {
	m.BytesRecvTotal.WithLabelValues(region, service).Add(float64(n))
}

// Reconnected increments the reconnection counter for a region.
func (m *Metrics) Reconnected(region string) {
	m.ReconnectTotal.WithLabelValues(region).Inc()
}

// ConfigReloaded increments the config reload counter.
func (m *Metrics) ConfigReloaded() {
	m.ConfigReloadTotal.Inc()
}
