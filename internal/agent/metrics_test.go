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

	// Relay metrics.
	m.RelayStreamStarted("edge-a", "cloud-b", "echo-svc")
	m.RelayStreamFinished()
	m.ObserveRelayLatency(10 * time.Millisecond)
	m.AddRelayBytes("sent", "edge-a", "cloud-b", 4096)
	m.AddRelayBytes("recv", "edge-a", "cloud-b", 2048)
	m.RelayError("no_rule")
	m.RelayError("hop_limit")
	m.RelayError("open_stream")
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

	// Set relay values so CounterVec metrics appear in output.
	m.RelayStreamStarted("edge-a", "cloud-b", "echo-svc")
	m.AddRelayBytes("sent", "edge-a", "cloud-b", 512)
	m.RelayError("no_rule")

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
		"outpostnet_relay_streams_total",
		"outpostnet_relay_bytes_total",
		"outpostnet_relay_active_streams",
		"outpostnet_relay_latency_seconds",
		"outpostnet_relay_errors_total",
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

func TestRelayMetricsValues(t *testing.T) {
	m := NewMetrics()

	// Simulate relay activity.
	m.RelayStreamStarted("edge-a", "cloud-b", "echo-svc")
	m.RelayStreamStarted("edge-a", "cloud-b", "echo-svc")
	m.RelayStreamFinished()
	m.AddRelayBytes("sent", "edge-a", "cloud-b", 1024)
	m.AddRelayBytes("recv", "edge-a", "cloud-b", 512)
	m.ObserveRelayLatency(5 * time.Millisecond)
	m.RelayError("no_rule")

	// Serve and scrape.
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

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	// Relay streams total should be 2.
	if !strings.Contains(output, `outpostnet_relay_streams_total{from_region="edge-a",service="echo-svc",to_region="cloud-b"} 2`) {
		t.Error("relay_streams_total should be 2")
	}

	// Active relay streams: 2 started - 1 finished = 1.
	if !strings.Contains(output, `outpostnet_relay_active_streams 1`) {
		t.Error("relay_active_streams should be 1")
	}

	// Relay bytes sent.
	if !strings.Contains(output, `outpostnet_relay_bytes_total{direction="sent",from_region="edge-a",to_region="cloud-b"} 1024`) {
		t.Error("relay_bytes_total sent should be 1024")
	}

	// Relay errors.
	if !strings.Contains(output, `outpostnet_relay_errors_total{error_type="no_rule"} 1`) {
		t.Error("relay_errors_total no_rule should be 1")
	}
}
