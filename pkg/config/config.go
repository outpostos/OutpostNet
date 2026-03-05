// Package config handles YAML configuration loading and validation for OutpostNet.
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// ServerConfig is the YAML configuration for outpostnet-server.
type ServerConfig struct {
	Listen           string `yaml:"listen"`
	LogLevel         string `yaml:"log_level"`
	HeartbeatTimeout string `yaml:"heartbeat_timeout"` // e.g. "30s"
}

// SidecarConfig is the YAML configuration for outpostnet-sidecar.
type SidecarConfig struct {
	Server            string          `yaml:"server"`
	SidecarID         string          `yaml:"sidecar_id"`
	HeartbeatInterval string          `yaml:"heartbeat_interval"` // e.g. "10s"
	Exports           []ServiceExport `yaml:"exports"`
	Imports           []ServiceImport `yaml:"imports"`
}

// ServiceExport describes an exported service in YAML config.
type ServiceExport struct {
	Name      string `yaml:"name"`
	LocalPort uint16 `yaml:"local_port"`
	Protocol  string `yaml:"protocol"` // "tcp" or "udp"
}

// ServiceImport describes an imported service in YAML config.
type ServiceImport struct {
	Name       string `yaml:"name"`
	ListenPort uint16 `yaml:"listen_port"`
	Protocol   string `yaml:"protocol"` // "tcp" or "udp"
}

// LoadServerConfig loads a ServerConfig from a YAML file.
func LoadServerConfig(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &ServerConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := validateServerConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// LoadSidecarConfig loads a SidecarConfig from a YAML file.
func LoadSidecarConfig(path string) (*SidecarConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &SidecarConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := validateSidecarConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

func validateServerConfig(cfg *ServerConfig) error {
	if cfg.Listen == "" {
		return fmt.Errorf("listen is required")
	}
	if cfg.LogLevel != "" {
		switch cfg.LogLevel {
		case "debug", "info", "warn", "error":
			// ok
		default:
			return fmt.Errorf("invalid log_level %q (must be debug/info/warn/error)", cfg.LogLevel)
		}
	}
	return nil
}

func validateSidecarConfig(cfg *SidecarConfig) error {
	if cfg.Server == "" {
		return fmt.Errorf("server is required")
	}
	for i, exp := range cfg.Exports {
		if exp.Name == "" {
			return fmt.Errorf("exports[%d].name is required", i)
		}
		if exp.LocalPort == 0 {
			return fmt.Errorf("exports[%d].local_port is required", i)
		}
		if err := validateProtocol(exp.Protocol); err != nil {
			return fmt.Errorf("exports[%d].protocol: %w", i, err)
		}
	}
	for i, imp := range cfg.Imports {
		if imp.Name == "" {
			return fmt.Errorf("imports[%d].name is required", i)
		}
		if imp.ListenPort == 0 {
			return fmt.Errorf("imports[%d].listen_port is required", i)
		}
		if err := validateProtocol(imp.Protocol); err != nil {
			return fmt.Errorf("imports[%d].protocol: %w", i, err)
		}
	}
	return nil
}

func validateProtocol(p string) error {
	switch strings.ToLower(p) {
	case "tcp", "udp":
		return nil
	case "":
		return fmt.Errorf("protocol is required (tcp or udp)")
	default:
		return fmt.Errorf("invalid protocol %q (must be tcp or udp)", p)
	}
}

// MarshalSidecarConfig serializes a SidecarConfig to YAML string.
func MarshalSidecarConfig(cfg *SidecarConfig) (string, error) {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal sidecar config: %w", err)
	}
	return string(data), nil
}

// ParseProtocol converts a string protocol to protocol.ServiceProtocol value (0=TCP, 1=UDP).
func ParseProtocol(s string) uint8 {
	if strings.ToLower(s) == "udp" {
		return 1
	}
	return 0
}
