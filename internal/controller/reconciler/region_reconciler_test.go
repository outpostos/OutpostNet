package reconciler

import (
	"testing"

	"github.com/outpostos/OutpostNet/pkg/annotations"
)

func TestBuildAgentConfig(t *testing.T) {
	r := &RegionReconciler{
		DefaultTunnelPort: 4242,
	}

	crossRegionImports := map[string][]annotations.ServiceEntry{
		"cloud": {
			{Name: "cloud-db", Port: 3306, Protocol: "tcp", Region: "cloud"},
			{Name: "cloud-api", Port: 8080, Protocol: "tcp", Region: "cloud"},
		},
	}
	exports := []annotations.ServiceEntry{
		{Name: "edge-sensor", Port: 9000, Protocol: "tcp"},
	}

	cfg := r.buildAgentConfig("edge-a", crossRegionImports, exports, "default")

	if cfg.Region != "edge-a" {
		t.Errorf("region = %q, want edge-a", cfg.Region)
	}

	// Check tunnels.
	if len(cfg.Tunnels) != 1 {
		t.Fatalf("tunnels = %d, want 1", len(cfg.Tunnels))
	}
	if cfg.Tunnels[0].RemoteRegion != "cloud" {
		t.Errorf("tunnel remote_region = %q, want cloud", cfg.Tunnels[0].RemoteRegion)
	}
	if cfg.Tunnels[0].Role != "client" {
		t.Errorf("tunnel role = %q, want client", cfg.Tunnels[0].Role)
	}
	expectedAddr := "outpostnet-agent-cloud.default.svc:4242"
	if cfg.Tunnels[0].RemoteAddr != expectedAddr {
		t.Errorf("tunnel remote_addr = %q, want %q", cfg.Tunnels[0].RemoteAddr, expectedAddr)
	}

	// Check imports.
	if len(cfg.Imports) != 2 {
		t.Fatalf("imports = %d, want 2", len(cfg.Imports))
	}
	if cfg.Imports[0].Name != "cloud-db" {
		t.Errorf("import[0].Name = %q, want cloud-db", cfg.Imports[0].Name)
	}
	if cfg.Imports[0].RemoteRegion != "cloud" {
		t.Errorf("import[0].RemoteRegion = %q, want cloud", cfg.Imports[0].RemoteRegion)
	}

	// Check exports.
	if len(cfg.Exports) != 1 {
		t.Fatalf("exports = %d, want 1", len(cfg.Exports))
	}
	if cfg.Exports[0].Name != "edge-sensor" {
		t.Errorf("export[0].Name = %q, want edge-sensor", cfg.Exports[0].Name)
	}
	if cfg.Exports[0].LocalAddr != "127.0.0.1:9000" {
		t.Errorf("export[0].LocalAddr = %q, want 127.0.0.1:9000", cfg.Exports[0].LocalAddr)
	}
}

func TestBuildAgentConfigMultiRegion(t *testing.T) {
	r := &RegionReconciler{
		DefaultTunnelPort: 4242,
	}

	crossRegionImports := map[string][]annotations.ServiceEntry{
		"cloud": {
			{Name: "cloud-db", Port: 3306, Protocol: "tcp", Region: "cloud"},
		},
		"edge-b": {
			{Name: "sensor-b", Port: 9001, Protocol: "tcp", Region: "edge-b"},
		},
	}

	cfg := r.buildAgentConfig("edge-a", crossRegionImports, nil, "outpostnet")

	if len(cfg.Tunnels) != 2 {
		t.Fatalf("tunnels = %d, want 2", len(cfg.Tunnels))
	}
	if len(cfg.Imports) != 2 {
		t.Fatalf("imports = %d, want 2", len(cfg.Imports))
	}
}

func TestBuildAgentConfigDefaults(t *testing.T) {
	r := &RegionReconciler{} // DefaultTunnelPort = 0, should default to 4242

	crossRegionImports := map[string][]annotations.ServiceEntry{
		"cloud": {
			{Name: "svc", Port: 80, Protocol: "tcp", Region: "cloud"},
		},
	}

	cfg := r.buildAgentConfig("edge", crossRegionImports, nil, "default")

	if cfg.HealthAddr != ":8082" {
		t.Errorf("health_addr = %q, want :8082", cfg.HealthAddr)
	}
	if cfg.Metrics.Addr != ":9090" {
		t.Errorf("metrics addr = %q, want :9090", cfg.Metrics.Addr)
	}

	// Verify default port used.
	expectedAddr := "outpostnet-agent-cloud.default.svc:4242"
	if cfg.Tunnels[0].RemoteAddr != expectedAddr {
		t.Errorf("tunnel remote_addr = %q, want %q", cfg.Tunnels[0].RemoteAddr, expectedAddr)
	}
}

func TestAgentServiceName(t *testing.T) {
	if got := AgentServiceName("edge-a"); got != "outpostnet-agent-edge-a" {
		t.Errorf("AgentServiceName(edge-a) = %q", got)
	}
	if got := AgentServiceName("Cloud"); got != "outpostnet-agent-cloud" {
		t.Errorf("AgentServiceName(Cloud) = %q", got)
	}
}

func TestMapKeys(t *testing.T) {
	m := map[string][]annotations.ServiceEntry{
		"a": nil,
		"b": nil,
	}
	keys := mapKeys(m)
	if len(keys) != 2 {
		t.Errorf("mapKeys returned %d keys, want 2", len(keys))
	}
}
