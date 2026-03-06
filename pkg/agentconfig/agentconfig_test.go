package agentconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
region: edge-a
tunnels:
  - remote_region: cloud
    remote_addr: "cloud-agent.example.com:4242"
    role: client
exports:
  - name: web-api
    local_addr: "127.0.0.1:8080"
    protocol: tcp
imports:
  - name: cloud-db
    listen_addr: "0.0.0.0:13306"
    remote_region: cloud
    protocol: tcp
persistence:
  dir: /tmp/outpostnet-test
metrics:
  addr: ":9091"
health_addr: ":8083"
tls:
  insecure: true
`
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Region != "edge-a" {
		t.Errorf("region = %q, want edge-a", cfg.Region)
	}
	if len(cfg.Tunnels) != 1 {
		t.Fatalf("tunnels len = %d, want 1", len(cfg.Tunnels))
	}
	if cfg.Tunnels[0].RemoteRegion != "cloud" {
		t.Errorf("tunnel remote_region = %q, want cloud", cfg.Tunnels[0].RemoteRegion)
	}
	if cfg.Tunnels[0].Role != "client" {
		t.Errorf("tunnel role = %q, want client", cfg.Tunnels[0].Role)
	}
	if len(cfg.Exports) != 1 {
		t.Fatalf("exports len = %d, want 1", len(cfg.Exports))
	}
	if cfg.Exports[0].Name != "web-api" {
		t.Errorf("export name = %q, want web-api", cfg.Exports[0].Name)
	}
	if len(cfg.Imports) != 1 {
		t.Fatalf("imports len = %d, want 1", len(cfg.Imports))
	}
	if cfg.Imports[0].RemoteRegion != "cloud" {
		t.Errorf("import remote_region = %q, want cloud", cfg.Imports[0].RemoteRegion)
	}
	if cfg.Metrics.Addr != ":9091" {
		t.Errorf("metrics addr = %q, want :9091", cfg.Metrics.Addr)
	}
	if cfg.HealthAddr != ":8083" {
		t.Errorf("health addr = %q, want :8083", cfg.HealthAddr)
	}
}

func TestDefaults(t *testing.T) {
	yaml := `
region: test
tunnels:
  - remote_region: other
    remote_addr: "1.2.3.4:4242"
    role: client
`
	cfg, err := ParseConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}

	if cfg.HealthAddr != ":8082" {
		t.Errorf("default health addr = %q, want :8082", cfg.HealthAddr)
	}
	if cfg.Metrics.Addr != ":9090" {
		t.Errorf("default metrics addr = %q, want :9090", cfg.Metrics.Addr)
	}
	if cfg.Persistence.Dir != "/var/lib/outpostnet" {
		t.Errorf("default persistence dir = %q, want /var/lib/outpostnet", cfg.Persistence.Dir)
	}
}

func TestPersistenceEnabled(t *testing.T) {
	cfg := PersistenceConfig{}
	if !cfg.IsEnabled() {
		t.Error("default persistence should be enabled")
	}

	b := false
	cfg.Enabled = &b
	if cfg.IsEnabled() {
		t.Error("explicitly disabled persistence should report disabled")
	}

	b2 := true
	cfg.Enabled = &b2
	if !cfg.IsEnabled() {
		t.Error("explicitly enabled persistence should report enabled")
	}
}

func TestValidateConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing region",
			yaml: `tunnels: []`,
			want: "region is required",
		},
		{
			name: "tunnel missing remote_region",
			yaml: `
region: test
tunnels:
  - role: client
    remote_addr: "1.2.3.4:4242"`,
			want: "tunnels[0].remote_region is required",
		},
		{
			name: "tunnel invalid role",
			yaml: `
region: test
tunnels:
  - remote_region: other
    role: invalid`,
			want: "tunnels[0].role must be 'server' or 'client'",
		},
		{
			name: "server tunnel missing listen_addr",
			yaml: `
region: test
tunnels:
  - remote_region: other
    role: server`,
			want: "tunnels[0].listen_addr is required",
		},
		{
			name: "client tunnel missing remote_addr",
			yaml: `
region: test
tunnels:
  - remote_region: other
    role: client`,
			want: "tunnels[0].remote_addr is required",
		},
		{
			name: "export missing name",
			yaml: `
region: test
exports:
  - local_addr: "127.0.0.1:8080"
    protocol: tcp`,
			want: "exports[0].name is required",
		},
		{
			name: "export missing local_addr",
			yaml: `
region: test
exports:
  - name: svc
    protocol: tcp`,
			want: "exports[0].local_addr is required",
		},
		{
			name: "export invalid protocol",
			yaml: `
region: test
exports:
  - name: svc
    local_addr: "127.0.0.1:8080"
    protocol: grpc`,
			want: "invalid protocol",
		},
		{
			name: "import missing remote_region",
			yaml: `
region: test
imports:
  - name: svc
    listen_addr: "0.0.0.0:8080"
    protocol: tcp`,
			want: "imports[0].remote_region is required",
		},
		{
			name: "import missing listen_addr",
			yaml: `
region: test
imports:
  - name: svc
    remote_region: cloud
    protocol: tcp`,
			want: "imports[0].listen_addr is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected error")
			}
			if got := err.Error(); !contains(got, tt.want) {
				t.Errorf("error = %q, want to contain %q", got, tt.want)
			}
		})
	}
}

func TestMarshalConfig(t *testing.T) {
	cfg := &Config{
		Region: "edge-a",
		Tunnels: []TunnelConfig{
			{RemoteRegion: "cloud", RemoteAddr: "1.2.3.4:4242", Role: "client"},
		},
		Exports: []ExportConfig{
			{Name: "web", LocalAddr: "127.0.0.1:8080", Protocol: "tcp"},
		},
		HealthAddr: ":8082",
		Metrics:    MetricsConfig{Addr: ":9090"},
		Persistence: PersistenceConfig{Dir: "/var/lib/outpostnet"},
	}

	data, err := MarshalConfig(cfg)
	if err != nil {
		t.Fatalf("MarshalConfig: %v", err)
	}

	// Round-trip.
	cfg2, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("ParseConfig round-trip: %v", err)
	}
	if cfg2.Region != cfg.Region {
		t.Errorf("round-trip region = %q, want %q", cfg2.Region, cfg.Region)
	}
	if len(cfg2.Tunnels) != 1 {
		t.Errorf("round-trip tunnels len = %d, want 1", len(cfg2.Tunnels))
	}
}

func TestServerRoleTunnel(t *testing.T) {
	yaml := `
region: cloud
tunnels:
  - remote_region: edge-a
    role: server
    listen_addr: "0.0.0.0:4242"
exports:
  - name: cloud-db
    local_addr: "127.0.0.1:3306"
    protocol: tcp
`
	cfg, err := ParseConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Tunnels[0].Role != "server" {
		t.Errorf("role = %q, want server", cfg.Tunnels[0].Role)
	}
	if cfg.Tunnels[0].ListenAddr != "0.0.0.0:4242" {
		t.Errorf("listen_addr = %q, want 0.0.0.0:4242", cfg.Tunnels[0].ListenAddr)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstr(s, substr)
}

func searchSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
