// Package agentconfig defines the Agent configuration format and loading/validation logic.
package agentconfig

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level Agent configuration.
type Config struct {
	// Region identifies which edge region this Agent belongs to.
	Region string `yaml:"region"`

	// Tunnels defines QUIC tunnel connections to other regions.
	Tunnels []TunnelConfig `yaml:"tunnels"`

	// Exports lists services this region makes available to remote regions.
	Exports []ExportConfig `yaml:"exports"`

	// Imports lists services this region imports from remote regions.
	Imports []ImportConfig `yaml:"imports"`

	// Persistence configures local config persistence for offline recovery.
	Persistence PersistenceConfig `yaml:"persistence"`

	// Metrics configures Prometheus metrics endpoint.
	Metrics MetricsConfig `yaml:"metrics"`

	// HealthAddr is the address for health check endpoints (/healthz, /readyz).
	HealthAddr string `yaml:"health_addr"`

	// TLS configures TLS for tunnel connections.
	TLS TLSConfig `yaml:"tls"`
}

// TunnelConfig describes a QUIC tunnel to another region.
type TunnelConfig struct {
	// RemoteRegion is the name of the remote region.
	RemoteRegion string `yaml:"remote_region"`

	// RemoteAddr is the remote Agent's address (host:port).
	RemoteAddr string `yaml:"remote_addr"`

	// Role is either "server" (accept connections) or "client" (dial out).
	Role string `yaml:"role"`

	// ListenAddr is used when role=server; the address to listen on.
	ListenAddr string `yaml:"listen_addr"`
}

// ExportConfig describes a service exported to remote regions.
type ExportConfig struct {
	// Name is the service name.
	Name string `yaml:"name"`

	// LocalAddr is the local address of the service (host:port).
	LocalAddr string `yaml:"local_addr"`

	// Protocol is "tcp" or "udp".
	Protocol string `yaml:"protocol"`
}

// ImportConfig describes a service imported from a remote region.
type ImportConfig struct {
	// Name is the service name to import.
	Name string `yaml:"name"`

	// ListenAddr is the local address to listen on for incoming connections.
	ListenAddr string `yaml:"listen_addr"`

	// RemoteRegion identifies which region exports this service.
	RemoteRegion string `yaml:"remote_region"`

	// Protocol is "tcp" or "udp".
	Protocol string `yaml:"protocol"`
}

// PersistenceConfig configures local config persistence.
type PersistenceConfig struct {
	// Dir is the directory for persisted config files.
	Dir string `yaml:"dir"`

	// Enabled controls whether persistence is active. Default: true.
	Enabled *bool `yaml:"enabled"`
}

// IsEnabled returns whether persistence is enabled (default true).
func (p PersistenceConfig) IsEnabled() bool {
	if p.Enabled == nil {
		return true
	}
	return *p.Enabled
}

// MetricsConfig configures the Prometheus metrics endpoint.
type MetricsConfig struct {
	// Addr is the address to serve metrics on. Default: ":9090".
	Addr string `yaml:"addr"`
}

// TLSConfig configures TLS for tunnel connections.
type TLSConfig struct {
	// CertFile is the path to the TLS certificate.
	CertFile string `yaml:"cert_file"`

	// KeyFile is the path to the TLS private key.
	KeyFile string `yaml:"key_file"`

	// Insecure skips TLS verification (for development only).
	Insecure bool `yaml:"insecure"`
}

// LoadConfig loads an Agent config from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return ParseConfig(data)
}

// ParseConfig parses Agent config from YAML bytes.
func ParseConfig(data []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(cfg)
	if err := ValidateConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.HealthAddr == "" {
		cfg.HealthAddr = ":8082"
	}
	if cfg.Metrics.Addr == "" {
		cfg.Metrics.Addr = ":9090"
	}
	if cfg.Persistence.Dir == "" {
		cfg.Persistence.Dir = "/var/lib/outpostnet"
	}
}

// ValidateConfig checks that the config is well-formed.
func ValidateConfig(cfg *Config) error {
	if cfg.Region == "" {
		return fmt.Errorf("region is required")
	}

	for i, t := range cfg.Tunnels {
		if t.RemoteRegion == "" {
			return fmt.Errorf("tunnels[%d].remote_region is required", i)
		}
		switch strings.ToLower(t.Role) {
		case "server":
			if t.ListenAddr == "" {
				return fmt.Errorf("tunnels[%d].listen_addr is required when role=server", i)
			}
		case "client":
			if t.RemoteAddr == "" {
				return fmt.Errorf("tunnels[%d].remote_addr is required when role=client", i)
			}
		default:
			return fmt.Errorf("tunnels[%d].role must be 'server' or 'client', got %q", i, t.Role)
		}
	}

	for i, e := range cfg.Exports {
		if e.Name == "" {
			return fmt.Errorf("exports[%d].name is required", i)
		}
		if e.LocalAddr == "" {
			return fmt.Errorf("exports[%d].local_addr is required", i)
		}
		if err := validateProtocol(e.Protocol); err != nil {
			return fmt.Errorf("exports[%d].protocol: %w", i, err)
		}
	}

	for i, imp := range cfg.Imports {
		if imp.Name == "" {
			return fmt.Errorf("imports[%d].name is required", i)
		}
		if imp.ListenAddr == "" {
			return fmt.Errorf("imports[%d].listen_addr is required", i)
		}
		if imp.RemoteRegion == "" {
			return fmt.Errorf("imports[%d].remote_region is required", i)
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

// MarshalConfig serializes a Config to YAML bytes.
func MarshalConfig(cfg *Config) ([]byte, error) {
	return yaml.Marshal(cfg)
}
