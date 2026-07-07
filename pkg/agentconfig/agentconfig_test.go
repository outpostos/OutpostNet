package agentconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
tunnels:
  - remote_addr: "cloud-agent.example.com:4242"
    role: client
exports:
  - name: web-api
    local_addr: "127.0.0.1:8080"
    protocol: tcp
imports:
  - name: cloud-db
    listen_addr: "0.0.0.0:13306"
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

	if len(cfg.Tunnels) != 1 {
		t.Fatalf("tunnels len = %d, want 1", len(cfg.Tunnels))
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
	if cfg.Metrics.Addr != ":9091" {
		t.Errorf("metrics addr = %q, want :9091", cfg.Metrics.Addr)
	}
	if cfg.HealthAddr != ":8083" {
		t.Errorf("health addr = %q, want :8083", cfg.HealthAddr)
	}
}

func TestLoadConfigWithRegion(t *testing.T) {
	// Region is optional but still accepted when present.
	yaml := `
region: edge-a
tunnels:
  - remote_region: cloud
    remote_addr: "cloud-agent.example.com:4242"
    role: client
imports:
  - name: cloud-db
    listen_addr: "0.0.0.0:13306"
    remote_region: cloud
    protocol: tcp
persistence:
  dir: /tmp/outpostnet-test
`
	cfg, err := ParseConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Region != "edge-a" {
		t.Errorf("region = %q, want edge-a", cfg.Region)
	}
	if cfg.Tunnels[0].RemoteRegion != "cloud" {
		t.Errorf("tunnel remote_region = %q, want cloud", cfg.Tunnels[0].RemoteRegion)
	}
	if cfg.Imports[0].RemoteRegion != "cloud" {
		t.Errorf("import remote_region = %q, want cloud", cfg.Imports[0].RemoteRegion)
	}
}

func TestDefaults(t *testing.T) {
	yaml := `
tunnels:
  - remote_addr: "1.2.3.4:4242"
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
			name: "tunnel invalid role",
			yaml: `
tunnels:
  - role: invalid`,
			want: "tunnels[0].role must be 'server' or 'client'",
		},
		{
			name: "server tunnel missing listen_addr",
			yaml: `
tunnels:
  - role: server`,
			want: "tunnels[0].listen_addr is required",
		},
		{
			name: "client tunnel missing remote_addr",
			yaml: `
tunnels:
  - role: client`,
			want: "tunnels[0].remote_addr is required",
		},
		{
			name: "export missing name",
			yaml: `
exports:
  - local_addr: "127.0.0.1:8080"
    protocol: tcp`,
			want: "exports[0].name is required",
		},
		{
			name: "export missing local_addr",
			yaml: `
exports:
  - name: svc
    protocol: tcp`,
			want: "exports[0].local_addr is required",
		},
		{
			name: "export invalid protocol",
			yaml: `
exports:
  - name: svc
    local_addr: "127.0.0.1:8080"
    protocol: grpc`,
			want: "invalid protocol",
		},
		{
			name: "import missing name",
			yaml: `
imports:
  - listen_addr: "0.0.0.0:8080"
    protocol: tcp`,
			want: "imports[0].name or service is required",
		},
		{
			name: "import missing listen_addr",
			yaml: `
imports:
  - name: svc
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

func TestParseServiceRef(t *testing.T) {
	service, nodeID, err := ParseServiceRef("api@node-b")
	if err != nil {
		t.Fatalf("ParseServiceRef: %v", err)
	}
	if service != "api" {
		t.Errorf("service = %q, want api", service)
	}
	if nodeID != "node-b" {
		t.Errorf("nodeID = %q, want node-b", nodeID)
	}

	for _, ref := range []string{"api", "@node-b", "api@", "api@node@extra"} {
		t.Run(ref, func(t *testing.T) {
			if _, _, err := ParseServiceRef(ref); err == nil {
				t.Fatal("expected parse error")
			}
		})
	}
}

func TestServiceImportConfig(t *testing.T) {
	yaml := `
node_id: node-a
imports:
  - service: api@node-b
    listen_addr: 127.0.0.1:9080
    protocol: tcp
management:
  socket_path: /tmp/outpostnet-test.sock
`
	cfg, err := ParseConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.NodeID != "node-a" {
		t.Errorf("node_id = %q, want node-a", cfg.NodeID)
	}
	if cfg.Management.SocketPath != "/tmp/outpostnet-test.sock" {
		t.Errorf("management socket = %q", cfg.Management.SocketPath)
	}
	if cfg.Imports[0].Name != "api" {
		t.Errorf("derived import name = %q, want api", cfg.Imports[0].Name)
	}
	if cfg.Imports[0].RemoteRegion != "node-b" {
		t.Errorf("derived remote region = %q, want node-b", cfg.Imports[0].RemoteRegion)
	}
}

func TestMarshalConfig(t *testing.T) {
	cfg := &Config{
		Tunnels: []TunnelConfig{
			{RemoteAddr: "1.2.3.4:4242", Role: "client"},
		},
		Exports: []ExportConfig{
			{Name: "web", LocalAddr: "127.0.0.1:8080", Protocol: "tcp"},
		},
		HealthAddr:  ":8082",
		Metrics:     MetricsConfig{Addr: ":9090"},
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
	if len(cfg2.Tunnels) != 1 {
		t.Errorf("round-trip tunnels len = %d, want 1", len(cfg2.Tunnels))
	}
}

func TestServerRoleTunnel(t *testing.T) {
	yaml := `
tunnels:
  - role: server
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

// TestFRPStyleConfig verifies the 1.0 target config — no region on either side.
func TestFRPStyleConfig(t *testing.T) {
	serverYAML := `
auth:
  token: changeme
tunnels:
  - role: server
    listen_addr: 0.0.0.0:4242
exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
`
	cfg, err := ParseConfig([]byte(serverYAML))
	if err != nil {
		t.Fatalf("server config ParseConfig: %v", err)
	}
	if cfg.Region != "" {
		t.Errorf("server region should be empty, got %q", cfg.Region)
	}
	if cfg.Tunnels[0].Role != "server" {
		t.Errorf("role = %q, want server", cfg.Tunnels[0].Role)
	}

	clientYAML := `
auth:
  token: changeme
tunnels:
  - role: client
    remote_addr: 10.0.1.1:4242
imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    protocol: tcp
`
	cfgC, err := ParseConfig([]byte(clientYAML))
	if err != nil {
		t.Fatalf("client config ParseConfig: %v", err)
	}
	if cfgC.Region != "" {
		t.Errorf("client region should be empty, got %q", cfgC.Region)
	}
	if cfgC.Tunnels[0].RemoteRegion != "" {
		t.Errorf("tunnel remote_region should be empty, got %q", cfgC.Tunnels[0].RemoteRegion)
	}
	if cfgC.Imports[0].RemoteRegion != "" {
		t.Errorf("import remote_region should be empty, got %q", cfgC.Imports[0].RemoteRegion)
	}
}

func TestRelayConfig(t *testing.T) {
	yamlData := `
tunnels:
  - remote_region: edge-a
    role: server
    listen_addr: "0.0.0.0:4242"
  - remote_region: cloud-b
    role: client
    remote_addr: "cloud-b-gateway:4242"
  - remote_region: edge-a
    role: client
    remote_addr: "edge-a-gateway:4242"
relays:
  - service: svc-b
    from_region: edge-a
    to_region: cloud-b
  - service: svc-a
    from_region: cloud-b
    to_region: edge-a
    max_hops: 2
`
	cfg, err := ParseConfig([]byte(yamlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if len(cfg.Relays) != 2 {
		t.Fatalf("relays len = %d, want 2", len(cfg.Relays))
	}

	r0 := cfg.Relays[0]
	if r0.Service != "svc-b" {
		t.Errorf("relay[0].service = %q, want svc-b", r0.Service)
	}
	if r0.FromRegion != "edge-a" {
		t.Errorf("relay[0].from_region = %q, want edge-a", r0.FromRegion)
	}
	if r0.ToRegion != "cloud-b" {
		t.Errorf("relay[0].to_region = %q, want cloud-b", r0.ToRegion)
	}
	if r0.MaxHops != DefaultMaxHops {
		t.Errorf("relay[0].max_hops = %d, want default %d", r0.MaxHops, DefaultMaxHops)
	}

	r1 := cfg.Relays[1]
	if r1.MaxHops != 2 {
		t.Errorf("relay[1].max_hops = %d, want 2", r1.MaxHops)
	}
}

func TestRelayConfigValidation(t *testing.T) {
	// The second relay rule needs a client tunnel to edge-a to be valid.
	// But we only test individual validation cases below.
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "relay missing service",
			yaml: `
tunnels:
  - remote_region: b
    role: client
    remote_addr: "1.2.3.4:4242"
relays:
  - from_region: a
    to_region: b`,
			want: "relays[0].service is required",
		},
		{
			name: "relay missing from_region",
			yaml: `
tunnels:
  - remote_region: b
    role: client
    remote_addr: "1.2.3.4:4242"
relays:
  - service: svc
    to_region: b`,
			want: "relays[0].from_region is required",
		},
		{
			name: "relay missing to_region",
			yaml: `
tunnels:
  - remote_region: b
    role: client
    remote_addr: "1.2.3.4:4242"
relays:
  - service: svc
    from_region: a`,
			want: "relays[0].to_region is required",
		},
		{
			name: "relay from equals to",
			yaml: `
tunnels:
  - remote_region: a
    role: client
    remote_addr: "1.2.3.4:4242"
relays:
  - service: svc
    from_region: a
    to_region: a`,
			want: "from_region and to_region must differ",
		},
		{
			name: "relay to_region no client tunnel",
			yaml: `
tunnels:
  - remote_region: a
    role: server
    listen_addr: "0.0.0.0:4242"
relays:
  - service: svc
    from_region: a
    to_region: b`,
			want: "to_region \"b\" has no matching client tunnel",
		},
		{
			name: "relay max_hops too high",
			yaml: `
tunnels:
  - remote_region: b
    role: client
    remote_addr: "1.2.3.4:4242"
relays:
  - service: svc
    from_region: a
    to_region: b
    max_hops: 10`,
			want: "max_hops must be 1-5",
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

func TestRelayMarshalRoundTrip(t *testing.T) {
	cfg := &Config{
		Tunnels: []TunnelConfig{
			{RemoteRegion: "edge-a", Role: "server", ListenAddr: "0.0.0.0:4242"},
			{RemoteRegion: "cloud-b", Role: "client", RemoteAddr: "1.2.3.4:4242"},
		},
		Relays: []RelayConfig{
			{Service: "svc-b", FromRegion: "edge-a", ToRegion: "cloud-b", MaxHops: 3},
		},
		HealthAddr:  ":8082",
		Metrics:     MetricsConfig{Addr: ":9090"},
		Persistence: PersistenceConfig{Dir: "/var/lib/outpostnet"},
	}

	data, err := MarshalConfig(cfg)
	if err != nil {
		t.Fatalf("MarshalConfig: %v", err)
	}

	cfg2, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("ParseConfig round-trip: %v", err)
	}
	if len(cfg2.Relays) != 1 {
		t.Fatalf("round-trip relays len = %d, want 1", len(cfg2.Relays))
	}
	r := cfg2.Relays[0]
	if r.Service != "svc-b" || r.FromRegion != "edge-a" || r.ToRegion != "cloud-b" || r.MaxHops != 3 {
		t.Errorf("round-trip relay mismatch: %+v", r)
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
