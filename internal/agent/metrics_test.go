package agent

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMetricsRegistration(t *testing.T) {
	m := NewMetrics()

	// Verify all metrics can be used without panic.
	m.SetTunnelState("cloud", true)
	m.SetTunnelState("cloud", false)
	m.StreamOpened()
	m.StreamClosed()
	m.AddBytesSent("cloud", "svc", 1024)
	m.AddBytesRecv("cloud", "svc", 2048)
	m.Reconnected("cloud")
	m.ObserveStreamLatency(100 * time.Millisecond)
	m.ConfigReloaded()
}

func TestMetricsHandler(t *testing.T) {
	m := NewMetrics()

	// Set some values.
	m.SetTunnelState("edge-a", true)
	m.StreamOpened()
	m.StreamOpened()
	m.AddBytesSent("edge-a", "web", 4096)
	m.Reconnected("edge-a")
	m.ObserveStreamLatency(50 * time.Millisecond)
	m.ConfigReloaded()

	// Serve metrics via HTTP.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Close()

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", ln.Addr()))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	// Check key metrics are present.
	checks := []string{
		"outpostnet_tunnel_state",
		"outpostnet_active_streams",
		"outpostnet_bytes_sent_total",
		"outpostnet_reconnect_total",
		"outpostnet_stream_latency_seconds",
		"outpostnet_config_reload_total",
	}

	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("metric %q not found in output", check)
		}
	}

	// Verify specific values.
	if !strings.Contains(output, `outpostnet_tunnel_state{remote_region="edge-a"} 1`) {
		t.Error("tunnel_state for edge-a should be 1 (connected)")
	}
	if !strings.Contains(output, `outpostnet_active_streams 2`) {
		t.Error("active_streams should be 2")
	}
}
