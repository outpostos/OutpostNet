package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadServerConfig(t *testing.T) {
	yaml := `
listen: "/var/run/outpostnet/server.sock"
log_level: "info"
heartbeat_timeout: "30s"
`
	path := filepath.Join(t.TempDir(), "server.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("LoadServerConfig: %v", err)
	}
	if cfg.Listen != "/var/run/outpostnet/server.sock" {
		t.Errorf("Listen: got %q", cfg.Listen)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q", cfg.LogLevel)
	}
	if cfg.HeartbeatTimeout != "30s" {
		t.Errorf("HeartbeatTimeout: got %q", cfg.HeartbeatTimeout)
	}
}

func TestLoadSidecarConfig(t *testing.T) {
	yaml := `
server: "/var/run/outpostnet/server.sock"
sidecar_id: "pod-abc"
heartbeat_interval: "10s"
exports:
  - name: "web-service"
    local_port: 8080
    protocol: "tcp"
  - name: "dns"
    local_port: 53
    protocol: "udp"
imports:
  - name: "db-service"
    listen_port: 3306
    protocol: "tcp"
`
	path := filepath.Join(t.TempDir(), "sidecar.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := LoadSidecarConfig(path)
	if err != nil {
		t.Fatalf("LoadSidecarConfig: %v", err)
	}
	if cfg.Server != "/var/run/outpostnet/server.sock" {
		t.Errorf("Server: got %q", cfg.Server)
	}
	if len(cfg.Exports) != 2 {
		t.Fatalf("Exports: got %d, want 2", len(cfg.Exports))
	}
	if cfg.Exports[0].Name != "web-service" || cfg.Exports[0].LocalPort != 8080 {
		t.Errorf("Exports[0]: %+v", cfg.Exports[0])
	}
	if cfg.Exports[1].Protocol != "udp" {
		t.Errorf("Exports[1].Protocol: got %q", cfg.Exports[1].Protocol)
	}
	if len(cfg.Imports) != 1 {
		t.Fatalf("Imports: got %d, want 1", len(cfg.Imports))
	}
	if cfg.Imports[0].Name != "db-service" || cfg.Imports[0].ListenPort != 3306 {
		t.Errorf("Imports[0]: %+v", cfg.Imports[0])
	}
}

func TestValidateServerConfig_MissingListen(t *testing.T) {
	yaml := `log_level: "info"`
	path := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	_, err := LoadServerConfig(path)
	if err == nil {
		t.Fatal("expected error for missing listen")
	}
}

func TestValidateServerConfig_BadLogLevel(t *testing.T) {
	yaml := `
listen: "/tmp/test.sock"
log_level: "verbose"
`
	path := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	_, err := LoadServerConfig(path)
	if err == nil {
		t.Fatal("expected error for bad log_level")
	}
}

func TestValidateSidecarConfig_MissingProtocol(t *testing.T) {
	yaml := `
server: "/tmp/test.sock"
exports:
  - name: "svc"
    local_port: 8080
`
	path := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	_, err := LoadSidecarConfig(path)
	if err == nil {
		t.Fatal("expected error for missing protocol")
	}
}

func TestParseProtocol(t *testing.T) {
	if ParseProtocol("tcp") != 0 {
		t.Error("tcp should be 0")
	}
	if ParseProtocol("TCP") != 0 {
		t.Error("TCP should be 0")
	}
	if ParseProtocol("udp") != 1 {
		t.Error("udp should be 1")
	}
	if ParseProtocol("UDP") != 1 {
		t.Error("UDP should be 1")
	}
}

func TestMarshalSidecarConfig(t *testing.T) {
	cfg := &SidecarConfig{
		Server:            "10.0.0.1:9000",
		SidecarID:         "node-east-1",
		HeartbeatInterval: "15s",
		Exports: []ServiceExport{
			{Name: "web", LocalPort: 8080, Protocol: "tcp"},
		},
		Imports: []ServiceImport{
			{Name: "db", ListenPort: 5432, Protocol: "tcp"},
		},
	}

	out, err := MarshalSidecarConfig(cfg)
	if err != nil {
		t.Fatalf("MarshalSidecarConfig: %v", err)
	}

	// Verify the YAML output contains all expected fields.
	checks := []struct {
		field string
		value string
	}{
		{"server", "10.0.0.1:9000"},
		{"sidecar_id", "node-east-1"},
		{"heartbeat_interval", "15s"},
		{"name", "web"},
		{"local_port", "8080"},
		{"protocol", "tcp"},
		{"name", "db"},
		{"listen_port", "5432"},
	}
	for _, c := range checks {
		if !containsField(out, c.value) {
			t.Errorf("YAML output missing value %q for field %q\noutput:\n%s", c.value, c.field, out)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	// Verify zero-value ServerConfig fails validation (listen is required).
	cfg := &ServerConfig{}
	err := validateServerConfig(cfg)
	if err == nil {
		t.Fatal("expected validation error for empty ServerConfig")
	}

	// Verify ServerConfig with only listen passes (log_level is optional).
	cfg = &ServerConfig{Listen: ":9000"}
	err = validateServerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if cfg.LogLevel != "" {
		t.Errorf("expected empty default LogLevel, got %q", cfg.LogLevel)
	}
	if cfg.HeartbeatTimeout != "" {
		t.Errorf("expected empty default HeartbeatTimeout, got %q", cfg.HeartbeatTimeout)
	}
}

// containsField is a helper that checks if the YAML string contains a value.
func containsField(yaml, value string) bool {
	return len(yaml) > 0 && contains(yaml, value)
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestValidateSidecarConfig_MissingServer(t *testing.T) {
	cfg := &SidecarConfig{
		Server: "",
	}
	err := validateSidecarConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing server")
	}
}

func TestValidateSidecarConfig_MissingExportName(t *testing.T) {
	cfg := &SidecarConfig{
		Server: "127.0.0.1:9000",
		Exports: []ServiceExport{
			{Name: "", LocalPort: 8080, Protocol: "tcp"},
		},
	}
	err := validateSidecarConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing export name")
	}
}

func TestValidateSidecarConfig_MissingExportPort(t *testing.T) {
	cfg := &SidecarConfig{
		Server: "127.0.0.1:9000",
		Exports: []ServiceExport{
			{Name: "web", LocalPort: 0, Protocol: "tcp"},
		},
	}
	err := validateSidecarConfig(cfg)
	if err == nil {
		t.Fatal("expected error for export with port=0")
	}
}

func TestValidateSidecarConfig_MissingImportName(t *testing.T) {
	cfg := &SidecarConfig{
		Server: "127.0.0.1:9000",
		Imports: []ServiceImport{
			{Name: "", ListenPort: 3306, Protocol: "tcp"},
		},
	}
	err := validateSidecarConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing import name")
	}
}

func TestValidateSidecarConfig_MissingImportPort(t *testing.T) {
	cfg := &SidecarConfig{
		Server: "127.0.0.1:9000",
		Imports: []ServiceImport{
			{Name: "db", ListenPort: 0, Protocol: "tcp"},
		},
	}
	err := validateSidecarConfig(cfg)
	if err == nil {
		t.Fatal("expected error for import with port=0")
	}
}

func TestValidateSidecarConfig_InvalidProtocol(t *testing.T) {
	cfg := &SidecarConfig{
		Server: "127.0.0.1:9000",
		Exports: []ServiceExport{
			{Name: "api", LocalPort: 8080, Protocol: "grpc"},
		},
	}
	err := validateSidecarConfig(cfg)
	if err == nil {
		t.Fatal("expected error for invalid protocol 'grpc'")
	}
}
