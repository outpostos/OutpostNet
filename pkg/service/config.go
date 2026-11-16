package service

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
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

	// Mesh connects the Service layer to this node's OutpostMesh gateway
	// for logical service resolution (ADR-0003). Required when any import
	// names a logical service.
	Mesh MeshConfig `yaml:"mesh,omitempty"`
}

// MeshConfig locates the local Mesh gateway and the directory cache.
type MeshConfig struct {
	// GatewayURL is the local Mesh REST base URL, e.g. http://127.0.0.1:8081.
	GatewayURL string `yaml:"gateway_url,omitempty" json:"gateway_url,omitempty"`
	// DirectoryCacheDir holds the cold-start directory cache
	// (service-directory.json); empty disables the cache.
	DirectoryCacheDir string `yaml:"directory_cache_dir,omitempty" json:"directory_cache_dir,omitempty"`
}

// logicalPlaceholderNode stands in for the node of a logical import while
// the shared agentconfig validation (which only knows <service>@<node_id>)
// runs; it never reaches a running host.
const logicalPlaceholderNode = "logical.placeholder"

// isLogicalImport reports whether an import names a logical service.
func isLogicalImport(imp agentconfig.ImportConfig) bool {
	return imp.Service != "" && !strings.Contains(imp.Service, "@")
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
	// Logical imports (service without "@") are a Service feature the
	// shared agentconfig schema does not know. Validate them here, and let
	// agentconfig validate everything else with a placeholder node so that
	// its error indices still match the file.
	var logical []int
	for i, imp := range cfg.Imports {
		if !isLogicalImport(imp) {
			continue
		}
		logical = append(logical, i)
		if err := ValidateLogicalName(imp.Service); err != nil {
			return fmt.Errorf("imports[%d].service: %w", i, err)
		}
		if imp.RemoteRegion != "" {
			return fmt.Errorf("imports[%d] %q: a logical service has no remote_region", i, imp.Service)
		}
		if cfg.Mesh.GatewayURL == "" {
			return fmt.Errorf("imports[%d] %q is a logical service: mesh.gateway_url is required", i, imp.Service)
		}
		cfg.Imports[i].Service = imp.Service + "@" + logicalPlaceholderNode
	}
	err := agentconfig.ValidateConfig(&cfg.Config)
	for _, i := range logical {
		cfg.Imports[i].Service = strings.TrimSuffix(cfg.Imports[i].Service, "@"+logicalPlaceholderNode)
		cfg.Imports[i].RemoteRegion = ""
	}
	if err != nil {
		return err
	}
	if cfg.Mesh.GatewayURL != "" {
		u, err := url.Parse(cfg.Mesh.GatewayURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("mesh.gateway_url %q must be http(s)://host[:port]", cfg.Mesh.GatewayURL)
		}
	}
	for i, e := range cfg.Exports {
		if !strings.EqualFold(e.Protocol, "tcp") {
			return fmt.Errorf("exports[%d].protocol %q: only tcp is supported across nodes (udp over the QUIC tunnel is not implemented)", i, e.Protocol)
		}
	}
	clientRegions := make(map[string]bool)
	for _, t := range cfg.Tunnels {
		if strings.EqualFold(t.Role, "client") {
			clientRegions[t.RemoteRegion] = true
		}
	}
	listenAt := make(map[string]int)
	for i, imp := range cfg.Imports {
		if !strings.EqualFold(imp.Protocol, "tcp") {
			return fmt.Errorf("imports[%d].protocol %q: only tcp is supported across nodes (udp over the QUIC tunnel is not implemented)", i, imp.Protocol)
		}
		tgt := entryTarget(imp)
		if err := tgt.Validate(); err != nil {
			return fmt.Errorf("imports[%d]: %w", i, err)
		}
		// Static entries are bound to a configured client tunnel; without
		// one every connection would fail at runtime.
		if !tgt.Logical && !tgt.Dynamic() && !clientRegions[tgt.Region] {
			return fmt.Errorf("imports[%d] %q: remote_region %q has no client tunnel (add a tunnels entry with role: client and that remote_region, or use service: <name>@<node_id>)", i, imp.Name, tgt.Region)
		}
		// Port 0 asks the OS for a free port, so those never collide.
		if _, port, err := net.SplitHostPort(imp.ListenAddr); err == nil && port != "0" {
			if j, dup := listenAt[imp.ListenAddr]; dup {
				return fmt.Errorf("imports[%d].listen_addr %q is already used by imports[%d]", i, imp.ListenAddr, j)
			}
			listenAt[imp.ListenAddr] = i
		}
	}
	peerAt := make(map[string]int)
	for i, p := range cfg.Peers {
		if p.NodeID == "" || p.Addr == "" {
			return fmt.Errorf("peers[%d]: node_id and addr are required", i)
		}
		if p.NodeID == cfg.NodeID {
			return fmt.Errorf("peers[%d]: node_id %q is this node's own node_id", i, p.NodeID)
		}
		if j, dup := peerAt[p.NodeID]; dup {
			return fmt.Errorf("peers[%d]: duplicate node_id %q (also peers[%d])", i, p.NodeID, j)
		}
		peerAt[p.NodeID] = i
		if err := validatePeerAddr(p.Addr); err != nil {
			return fmt.Errorf("peers[%d] %q: %w", i, p.NodeID, err)
		}
	}
	return nil
}

// validatePeerAddr requires a dialable host:port tunnel address.
func validatePeerAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return fmt.Errorf("addr %q must be host:port", addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("addr %q must be host:port with a port in 1-65535", addr)
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
		if logical, name, node, err := ParseImportService(imp.Service); err == nil {
			if logical {
				return Target{Service: name, Logical: true}
			}
			return Target{Service: name, NodeID: node}
		}
	}
	return Target{Service: imp.Name, Region: imp.RemoteRegion}
}
