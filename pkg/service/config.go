package service

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// Defaults for outpostnet-service. They differ from outpostnet-agent so both
// commands can run on one node without colliding.
const (
	DefaultManagementSocketPath = "/run/outpostnet/service.sock"
	DefaultHealthAddr           = ":8083"
)

// Peer is an initial peer-directory entry (node_id -> tunnel address). It is
// fed into the existing peer directory at startup, exactly like one
// PUT /v1/peers call; later changes go through that management API.
type Peer struct {
	NodeID string `yaml:"node_id" json:"node_id"`
	Addr   string `yaml:"addr" json:"addr"`
}

// Config is the outpostnet-service configuration. It reuses the agentconfig
// schema (tunnels, exports, imports, tls, auth, management, node_id) so the
// same validation, peer directory and management API apply, and adds only the
// initial peer list.
//
// In Service terms, imports are stable entries (listen_addr is the stable
// address applications dial) and exports are the instances this node serves.
type Config struct {
	agentconfig.Config `yaml:",inline"`

	// Peers seeds the peer directory at startup.
	Peers []Peer `yaml:"peers,omitempty"`
}

// LoadConfig reads and validates a Service config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return ParseConfig(data)
}

// ParseConfig parses and validates Service config YAML. Unknown fields are
// rejected.
func ParseConfig(data []byte) (*Config, error) {
	cfg := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	ApplyDefaults(cfg)
	if err := ValidateConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// ApplyDefaults fills Service defaults for unset fields.
func ApplyDefaults(cfg *Config) {
	if cfg.HealthAddr == "" {
		cfg.HealthAddr = DefaultHealthAddr
	}
	if cfg.Management.SocketPath == "" {
		cfg.Management.SocketPath = DefaultManagementSocketPath
	}
}

// ValidateConfig applies the shared agentconfig validation and the Service
// rules: node_id is required (it anchors the stable identity and the peer
// role arbitration), only TCP is accepted for entries and exports because
// UDP over the cross-node QUIC tunnel is not implemented, relay rules stay
// with outpostnet-agent, and every entry must form a valid Net target.
func ValidateConfig(cfg *Config) error {
	if cfg.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if len(cfg.Relays) > 0 {
		return fmt.Errorf("relays are not supported by outpostnet-service; configure relay forwarding on outpostnet-agent")
	}
	if err := agentconfig.ValidateConfig(&cfg.Config); err != nil {
		return err
	}
	for i, e := range cfg.Exports {
		if !strings.EqualFold(e.Protocol, "tcp") {
			return fmt.Errorf("exports[%d].protocol %q: only tcp is supported across nodes (udp over the QUIC tunnel is not implemented)", i, e.Protocol)
		}
	}
	for i, imp := range cfg.Imports {
		if !strings.EqualFold(imp.Protocol, "tcp") {
			return fmt.Errorf("imports[%d].protocol %q: only tcp is supported across nodes (udp over the QUIC tunnel is not implemented)", i, imp.Protocol)
		}
		if err := entryTarget(imp).Validate(); err != nil {
			return fmt.Errorf("imports[%d]: %w", i, err)
		}
	}
	for i, p := range cfg.Peers {
		if p.NodeID == "" || p.Addr == "" {
			return fmt.Errorf("peers[%d]: node_id and addr are required", i)
		}
	}
	return nil
}

// EntryTargets returns the Net target of every configured entry, in order.
func (c *Config) EntryTargets() []Target {
	out := make([]Target, 0, len(c.Imports))
	for _, imp := range c.Imports {
		out = append(out, entryTarget(imp))
	}
	return out
}

// entryTarget maps an import to its routing path: "<service>@<node_id>"
// imports use the dynamic peer-directory path; name-only imports use the
// static configured tunnel for remote_region.
func entryTarget(imp agentconfig.ImportConfig) Target {
	if imp.Service != "" {
		if ref, err := ParseRef(imp.Service); err == nil {
			return ref.Target()
		}
	}
	return Target{Service: imp.Name, Region: imp.RemoteRegion}
}
