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
