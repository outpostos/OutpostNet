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
	// Region optionally identifies this agent (used for relay routing).
	Region string `yaml:"region,omitempty"`

	// NodeID identifies this agent for peer-directory role arbitration.
	NodeID string `yaml:"node_id,omitempty"`

	// Tunnels defines QUIC tunnel connections to other regions.
	Tunnels []TunnelConfig `yaml:"tunnels"`

	// Exports lists services this region makes available to remote regions.
	Exports []ExportConfig `yaml:"exports"`

	// Imports lists services this region imports from remote regions.
	Imports []ImportConfig `yaml:"imports"`

	// Relays defines relay forwarding rules for this Agent.
	// When a stream arrives for a service matching a relay rule, the Agent
	// forwards it to the target region instead of handling it locally.
	Relays []RelayConfig `yaml:"relays"`

	// Persistence configures local config persistence for offline recovery.
	Persistence PersistenceConfig `yaml:"persistence"`

	// Metrics configures Prometheus metrics endpoint.
	Metrics MetricsConfig `yaml:"metrics"`

	// HealthAddr is the address for health check endpoints (/healthz, /readyz).
	HealthAddr string `yaml:"health_addr"`

	// TLS configures TLS for tunnel connections.
	TLS TLSConfig `yaml:"tls"`

	// Auth configures authentication for tunnel connections.
	Auth AuthConfig `yaml:"auth"`

	// Management configures the local agent management API.
	Management ManagementConfig `yaml:"management"`
}

// TunnelConfig describes a QUIC tunnel to another region.
type TunnelConfig struct {
	// RemoteRegion optionally names the remote side (used for relay routing).
	RemoteRegion string `yaml:"remote_region,omitempty"`

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

	// Service imports a remote service by peer-directory name in the form
	// "<service>@<node_id>". It is additive with the legacy Name field.
	Service string `yaml:"service,omitempty"`

	// ListenAddr is the local address to listen on for incoming connections.
	ListenAddr string `yaml:"listen_addr"`

	// RemoteRegion optionally identifies which tunnel carries this service.
	// When omitted, the import is served by whichever client tunnel is available.
	RemoteRegion string `yaml:"remote_region,omitempty"`

	// Protocol is "tcp" or "udp".
	Protocol string `yaml:"protocol"`
}

// RelayConfig describes a relay forwarding rule. When the Agent's tunnel server
// receives a stream for Service from FromRegion, it opens a new stream on the
// tunnel client connected to ToRegion and relays data bidirectionally.
type RelayConfig struct {
	// Service is the name of the service to relay.
	Service string `yaml:"service"`

	// FromRegion is the region the incoming stream originates from.
	FromRegion string `yaml:"from_region"`

	// ToRegion is the region to forward the stream to.
	ToRegion string `yaml:"to_region"`

	// MaxHops is the maximum number of relay hops allowed (default 3, max 5).
	MaxHops int `yaml:"max_hops"`
}

// DefaultMaxHops is the default maximum relay hop count.
const DefaultMaxHops = 3

// MaxAllowedHops is the absolute maximum relay hop count.
const MaxAllowedHops = 5

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

// AuthConfig configures authentication for tunnel connections.
type AuthConfig struct {
	// Token is a shared secret used to authenticate QUIC connections.
	// An empty token disables authentication (backward compatible).
	Token string `yaml:"token"`
}

// ManagementConfig configures the agent's local management API.
type ManagementConfig struct {
	// SocketPath is the Unix domain socket path for HTTP management requests.
	SocketPath string `yaml:"socket_path"`
}

const DefaultManagementSocketPath = "/run/outpostnet/outpostnet.sock"

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
	if cfg.Management.SocketPath == "" {
		cfg.Management.SocketPath = DefaultManagementSocketPath
	}
	for i := range cfg.Relays {
		if cfg.Relays[i].MaxHops == 0 {
			cfg.Relays[i].MaxHops = DefaultMaxHops
		}
	}
}

// ValidateConfig checks that the config is well-formed.
func ValidateConfig(cfg *Config) error {
	for i, t := range cfg.Tunnels {
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
		if imp.Name == "" && imp.Service == "" {
			return fmt.Errorf("imports[%d].name or service is required", i)
		}
		if imp.Service != "" {
			service, nodeID, err := ParseServiceRef(imp.Service)
			if err != nil {
				return fmt.Errorf("imports[%d].service: %w", i, err)
			}
			if imp.Name == "" {
				cfg.Imports[i].Name = service
			}
			if imp.RemoteRegion == "" {
				cfg.Imports[i].RemoteRegion = nodeID
			}
		}
		if imp.ListenAddr == "" {
			return fmt.Errorf("imports[%d].listen_addr is required", i)
		}
		if err := validateProtocol(imp.Protocol); err != nil {
			return fmt.Errorf("imports[%d].protocol: %w", i, err)
		}
	}

	// Build set of client-role tunnel regions for relay validation.
	clientRegions := make(map[string]bool)
	for _, t := range cfg.Tunnels {
		if strings.ToLower(t.Role) == "client" {
			clientRegions[t.RemoteRegion] = true
		}
	}

	for i, r := range cfg.Relays {
		if r.Service == "" {
			return fmt.Errorf("relays[%d].service is required", i)
		}
		if r.FromRegion == "" {
			return fmt.Errorf("relays[%d].from_region is required", i)
		}
		if r.ToRegion == "" {
			return fmt.Errorf("relays[%d].to_region is required", i)
		}
		if r.FromRegion == r.ToRegion {
			return fmt.Errorf("relays[%d]: from_region and to_region must differ", i)
		}
		if !clientRegions[r.ToRegion] {
			return fmt.Errorf("relays[%d]: to_region %q has no matching client tunnel", i, r.ToRegion)
		}
		if r.MaxHops < 1 || r.MaxHops > MaxAllowedHops {
			return fmt.Errorf("relays[%d].max_hops must be 1-%d, got %d", i, MaxAllowedHops, r.MaxHops)
		}
	}

	return nil
}

// ParseServiceRef parses "<service>@<node_id>" import references.
func ParseServiceRef(ref string) (serviceName, nodeID string, err error) {
	parts := strings.Split(ref, "@")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("must be in the form <service>@<node_id>")
	}
	return parts[0], parts[1], nil
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
