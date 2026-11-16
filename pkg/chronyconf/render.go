// Package chronyconf renders the chrony configuration fragment that makes an
// OutpostNet node an NTS time server for its group (ADR-0006 D7).
//
// The fragment is deliberately self-contained: it trusts only the group CA,
// serves NTP to group peers, runs chrony in orphan mode, steps the clock on
// the first large correction, and relaxes NTS certificate time checks until
// the first clock update. It never adds unauthenticated NTP sources unless the
// caller lists them explicitly.
package chronyconf

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// Default values for omitted Config fields.
const (
	DefaultNTPPort         = 123
	DefaultNTSKEPort       = 4460
	DefaultDumpDir         = "/var/lib/chrony"
	DefaultLocalStratum    = 10
	DefaultMakestepThresh  = 1.0
	DefaultMakestepLimit   = 3
	DefaultNocertTimecheck = 1
	DefaultGNSSRefID       = "GPS"
)

// Peer is one group node that this node polls as an NTS server.
type Peer struct {
	NodeID string
	Host   string
}

// GNSS configures the optional gpsd refclock template. Exactly one of
// SHMSegment and SOCKPath must be set; PPS adds a kernel PPS refclock locked
// to the gpsd source. The template is rendered only when a receiver exists.
type GNSS struct {
	SHMSegment *int
	SOCKPath   string
	PPSDevice  string
	RefID      string
}

// Config describes one node's chrony/NTS fragment.
type Config struct {
	// NodeID must match the identity in CertFile.
	NodeID string
	// CertFile and KeyFile are this node's group certificate and key. chrony
	// reads them for the NTS-KE server role.
	CertFile string
	KeyFile  string
	// CAFile is the group CA bundle used to verify NTS peer certificates.
	CAFile string
	// Peers are the group nodes served by this node and polled as NTS
	// servers. The local node is skipped with a warning if present.
	Peers []Peer
	// Allow lists extra client IPv4/IPv6 addresses or CIDRs permitted to use
	// this node as an NTP server, in addition to peer addresses that are IPs.
	Allow []string
	// SourcesDir, when set, moves the server lines into a .sources file for
	// this directory and emits a sourcedir directive. chronyc reload sources
	// then applies peer changes without restarting chronyd. Certificate
	// changes still require a restart.
	SourcesDir string
	// NTPPort is the UDP port this node serves NTP on (default 123).
	NTPPort int
	// NTSKEPort is the TCP port for NTS-KE (default 4460).
	NTSKEPort int
	// DumpDir is chrony's NTS key/cookie directory (default /var/lib/chrony).
	DumpDir string
	// LocalStratum is the orphan-mode stratum (default 10).
	LocalStratum int
	// MakestepThreshold, MakestepLimit configure makestep (defaults 1.0, 3).
	MakestepThreshold float64
	MakestepLimit     int
	// NocertTimecheck disables NTS certificate time checks for this many clock
	// updates; defaults to 1 so a node with a wrong clock can sync first.
	NocertTimecheck int
	// GNSS renders the optional refclock template.
	GNSS *GNSS
	// UnauthenticatedServers are explicit operator additions. They are the
	// only unauthenticated NTP sources the renderer will emit.
	UnauthenticatedServers []string
}

// Result is the rendered configuration. Sources is non-empty only in split
// mode (Config.SourcesDir set).
type Result struct {
	Main     string
	Sources  string
	Warnings []string
}

// Render validates cfg and produces the fragment(s) for chrony.
func Render(cfg Config) (Result, error) {
	cfg = withDefaults(cfg)
	if err := validateScalars(cfg); err != nil {
		return Result{}, err
	}
	leaf, warnings, err := loadNodeCertificate(cfg)
	if err != nil {
		return Result{}, err
	}
	if len(leaf.DNSNames) == 0 && len(leaf.IPAddresses) == 0 {
		warnings = append(warnings, fmt.Sprintf("node certificate %q has no IP/DNS SAN; peers cannot verify this node as an NTS server (issue it with --san)", cfg.CertFile))
	}

	peers, peerWarnings, err := normalizePeers(cfg.Peers, cfg.NodeID)
	if err != nil {
		return Result{}, err
	}
	warnings = append(warnings, peerWarnings...)

	allow, explicit, err := normalizeAllow(cfg.Allow, peers)
	if err != nil {
		return Result{}, err
	}
	if !cfg.split() && len(peers) == 0 {
		warnings = append(warnings, "no group peers configured; this node runs orphan-only until peers are added")
	}
	if cfg.split() {
		for _, p := range peers {
			if ip := net.ParseIP(p.Host); ip != nil && !covered(ip, explicit) {
				warnings = append(warnings, fmt.Sprintf("peer %q address %s is allowed only by a peer-derived rule in the main fragment; pass --allow with the group subnet so peer changes only rewrite the sources file", p.NodeID, p.Host))
				break
			}
		}
	}

	var unauth []string
	for _, raw := range cfg.UnauthenticatedServers {
		host := strings.TrimSpace(raw)
		if err := validateHost("unauthenticated server", host); err != nil {
			return Result{}, err
		}
		unauth = append(unauth, host)
	}
	sort.Strings(unauth)

	sources := renderSources(cfg.NodeID, peers, unauth, cfg.NTPPort)
	main := renderMain(cfg, allow)
	if cfg.split() {
		return Result{Main: main, Sources: strings.TrimPrefix(sources, "\n"), Warnings: warnings}, nil
	}
	return Result{Main: main + sources, Warnings: warnings}, nil
}

func withDefaults(cfg Config) Config {
	if cfg.NTPPort == 0 {
		cfg.NTPPort = DefaultNTPPort
	}
	if cfg.NTSKEPort == 0 {
		cfg.NTSKEPort = DefaultNTSKEPort
	}
	if cfg.DumpDir == "" {
		cfg.DumpDir = DefaultDumpDir
	}
	if cfg.LocalStratum == 0 {
		cfg.LocalStratum = DefaultLocalStratum
	}
	if cfg.MakestepThreshold == 0 {
		cfg.MakestepThreshold = DefaultMakestepThresh
	}
	if cfg.MakestepLimit == 0 {
		cfg.MakestepLimit = DefaultMakestepLimit
	}
	if cfg.NocertTimecheck == 0 {
		cfg.NocertTimecheck = DefaultNocertTimecheck
	}
	if cfg.GNSS != nil {
		g := *cfg.GNSS
		if g.RefID == "" {
			g.RefID = DefaultGNSSRefID
		}
		cfg.GNSS = &g
	}
	return cfg
}

func (c Config) split() bool { return c.SourcesDir != "" }

func validateScalars(cfg Config) error {
	if err := validateDirectiveValue("node_id", cfg.NodeID); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"cert file", cfg.CertFile},
		{"key file", cfg.KeyFile},
		{"CA file", cfg.CAFile},
		{"dump dir", cfg.DumpDir},
	} {
		if err := validateDirectiveValue(field.name, field.value); err != nil {
			return err
		}
	}
	if cfg.SourcesDir != "" {
		if err := validateDirectiveValue("sources dir", cfg.SourcesDir); err != nil {
			return err
		}
	}
	if cfg.NTPPort < 1 || cfg.NTPPort > 65535 {
		return fmt.Errorf("ntp port %d out of range", cfg.NTPPort)
	}
	if cfg.NTSKEPort < 1 || cfg.NTSKEPort > 65535 {
		return fmt.Errorf("nts-ke port %d out of range", cfg.NTSKEPort)
	}
	if cfg.LocalStratum < 1 || cfg.LocalStratum > 15 {
		return fmt.Errorf("local stratum %d out of range 1-15", cfg.LocalStratum)
	}
	if cfg.MakestepThreshold <= 0 {
		return fmt.Errorf("makestep threshold must be positive")
	}
	if cfg.MakestepLimit < -1 || cfg.MakestepLimit == 0 {
		return fmt.Errorf("makestep limit must be -1 or positive")
	}
	if cfg.NocertTimecheck < 0 {
		return fmt.Errorf("nocerttimecheck must not be negative")
	}
	if cfg.GNSS != nil {
		g := cfg.GNSS
		if (g.SHMSegment == nil) == (g.SOCKPath == "") {
			return fmt.Errorf("gnss: set exactly one of SHMSegment or SOCKPath")
		}
		if g.SHMSegment != nil && (*g.SHMSegment < 0 || *g.SHMSegment > 255) {
			return fmt.Errorf("gnss: SHM segment %d out of range", *g.SHMSegment)
		}
		if g.SOCKPath != "" {
			if err := validateDirectiveValue("gnss socket", g.SOCKPath); err != nil {
				return err
			}
		}
		if g.PPSDevice != "" {
			if err := validateDirectiveValue("gnss PPS device", g.PPSDevice); err != nil {
				return err
			}
		}
		if len(g.RefID) == 0 || len(g.RefID) > 4 {
			return fmt.Errorf("gnss: refid %q must be 1-4 characters", g.RefID)
		}
		for _, r := range g.RefID {
			if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
				return fmt.Errorf("gnss: refid %q must be alphanumeric", g.RefID)
			}
		}
	}
	return nil
}

// validateDirectiveValue rejects values that could break out of a chrony
// directive or comment with whitespace, control characters or #.
func validateDirectiveValue(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	for _, r := range value {
		if unicode.IsSpace(r) || r < 0x20 || r == 0x7f || r == '#' {
			return fmt.Errorf("%s must not contain whitespace, control characters or #", name)
		}
	}
	return nil
}

func loadNodeCertificate(cfg Config) (*x509.Certificate, []string, error) {
	pair, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load node certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, fmt.Errorf("parse node certificate: %w", err)
	}
	id, err := tlsutil.NodeID(leaf)
	if err != nil {
		return nil, nil, fmt.Errorf("node certificate identity: %w", err)
	}
	if id != cfg.NodeID {
		return nil, nil, fmt.Errorf("node certificate identity %q does not match node_id %q", id, cfg.NodeID)
	}
	roots, err := loadCABundle(cfg.CAFile)
	if err != nil {
		return nil, nil, err
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: leaf.NotBefore,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return nil, nil, fmt.Errorf("node certificate does not verify against the group CA: %w", err)
	}
	return leaf, nil, nil
}

func loadCABundle(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	added := 0
	for len(data) > 0 {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse CA certificate: %w", err)
		}
		if !cert.IsCA {
			return nil, fmt.Errorf("CA bundle entry %q is not a CA", cert.Subject.String())
		}
		pool.AddCert(cert)
		added++
	}
	if added == 0 {
		return nil, fmt.Errorf("CA bundle %q contains no certificates", path)
	}
	return pool, nil
}

func normalizePeers(in []Peer, self string) ([]Peer, []string, error) {
	seenID := make(map[string]struct{}, len(in))
	seenHost := make(map[string]struct{}, len(in))
	out := make([]Peer, 0, len(in))
	var warnings []string
	for _, raw := range in {
		nodeID := strings.TrimSpace(raw.NodeID)
		host := strings.TrimSpace(raw.Host)
		if err := validateDirectiveValue("peer node_id", nodeID); err != nil {
			return nil, nil, err
		}
		if err := validateHost("peer "+nodeID, host); err != nil {
			return nil, nil, err
		}
		if nodeID == self {
			warnings = append(warnings, fmt.Sprintf("peer %q is the local node; skipped", nodeID))
			continue
		}
		if _, dup := seenID[nodeID]; dup {
			return nil, nil, fmt.Errorf("duplicate peer node_id %q", nodeID)
		}
		key := strings.ToLower(host)
		if _, dup := seenHost[key]; dup {
			return nil, nil, fmt.Errorf("duplicate peer address %q", host)
		}
		seenID[nodeID] = struct{}{}
		seenHost[key] = struct{}{}
		out = append(out, Peer{NodeID: nodeID, Host: host})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out, warnings, nil
}

func validateHost(name, host string) error {
	if err := validateDirectiveValue(name, host); err != nil {
		return err
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if strings.Contains(host, ":") {
		return fmt.Errorf("%s address %q must not include a port", name, host)
	}
	if len(host) > 253 {
		return fmt.Errorf("%s address %q is too long", name, host)
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("%s address %q is not a valid hostname", name, host)
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return fmt.Errorf("%s address %q is not a valid hostname", name, host)
			}
		}
	}
	return nil
}

// normalizeAllow returns the union of peer-derived and explicit access rules,
// plus the explicit rules alone (used to warn when split mode would still need
// a main-fragment rewrite on peer changes).
func normalizeAllow(in []string, peers []Peer) (all, explicit []string, err error) {
	var peerRules []string
	for _, p := range peers {
		if ip := net.ParseIP(p.Host); ip != nil {
			peerRules = append(peerRules, allowRule(ip))
		}
	}
	for _, raw := range in {
		value := strings.TrimSpace(raw)
		if err := validateDirectiveValue("allow", value); err != nil {
			return nil, nil, err
		}
		if _, network, err := net.ParseCIDR(value); err == nil {
			explicit = append(explicit, network.String())
			continue
		}
		ip := net.ParseIP(value)
		if ip == nil {
			return nil, nil, fmt.Errorf("allow %q is not an IP address or CIDR", value)
		}
		explicit = append(explicit, allowRule(ip))
	}
	seen := make(map[string]struct{}, len(peerRules)+len(explicit))
	for _, rule := range append(append([]string(nil), peerRules...), explicit...) {
		if _, dup := seen[rule]; dup {
			continue
		}
		seen[rule] = struct{}{}
		all = append(all, rule)
	}
	sort.Strings(all)
	sort.Strings(explicit)
	return all, explicit, nil
}

func allowRule(ip net.IP) string {
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String() + "/128"
}

func covered(ip net.IP, rules []string) bool {
	for _, rule := range rules {
		if _, network, err := net.ParseCIDR(rule); err == nil {
			if network.Contains(ip) {
				return true
			}
			continue
		}
		if other := net.ParseIP(rule); other != nil && other.Equal(ip) {
			return true
		}
	}
	return false
}

func renderMain(cfg Config, allow []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated by outpostnet-agent chrony render for node %s (ADR-0006 D7).\n", cfg.NodeID)
	b.WriteString("# Re-render when the peer set or node certificate changes; do not edit.\n")

	b.WriteString("\n# NTS server: this node authenticates NTP with its group certificate.\n")
	fmt.Fprintf(&b, "ntsservercert %s\n", cfg.CertFile)
	fmt.Fprintf(&b, "ntsserverkey %s\n", cfg.KeyFile)
	fmt.Fprintf(&b, "ntsdumpdir %s\n", cfg.DumpDir)
	fmt.Fprintf(&b, "ntsport %d\n", cfg.NTSKEPort)

	b.WriteString("\n# Trust only the group CA for NTS peers; the system CA set is disabled.\n")
	b.WriteString("nosystemcert\n")
	fmt.Fprintf(&b, "ntstrustedcerts 1 %s\n", cfg.CAFile)

	b.WriteString("\n# Orphan mode: converge within the group without an external time source.\n")
	fmt.Fprintf(&b, "local stratum %d orphan\n", cfg.LocalStratum)

	b.WriteString("\n# Step the clock on the first large correction (boot with a wrong clock).\n")
	fmt.Fprintf(&b, "makestep %s %d\n", formatFloat(cfg.MakestepThreshold), cfg.MakestepLimit)
	if cfg.NocertTimecheck > 0 {
		b.WriteString("# Accept NTS peer certificates before the first clock update.\n")
		fmt.Fprintf(&b, "nocerttimecheck %d\n", cfg.NocertTimecheck)
	}

	if len(allow) > 0 {
		b.WriteString("\n# Restrict NTP service to group peers and explicit operator networks.\n")
		for _, rule := range allow {
			fmt.Fprintf(&b, "allow %s\n", rule)
		}
	}

	if cfg.split() {
		fmt.Fprintf(&b, "\nsourcedir %s\n", cfg.SourcesDir)
	}

	if cfg.GNSS != nil {
		b.WriteString("\n# GNSS refclock: primary source while a receiver is configured.\n")
		source := cfg.GNSS.RefID
		if cfg.GNSS.SHMSegment != nil {
			extra := ""
			if cfg.GNSS.PPSDevice != "" {
				extra = " noselect"
			}
			fmt.Fprintf(&b, "refclock SHM %d refid %s%s\n", *cfg.GNSS.SHMSegment, source, extra)
		} else {
			extra := ""
			if cfg.GNSS.PPSDevice != "" {
				extra = " noselect"
			}
			fmt.Fprintf(&b, "refclock SOCK %s refid %s%s\n", cfg.GNSS.SOCKPath, source, extra)
		}
		if cfg.GNSS.PPSDevice != "" {
			fmt.Fprintf(&b, "refclock PPS %s lock %s refid PPS prefer\n", cfg.GNSS.PPSDevice, source)
		}
	}
	return b.String()
}

func renderSources(nodeID string, peers []Peer, unauthenticated []string, ntpPort int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n# NTP sources for node %s (reload with `chronyc reload sources`).\n", nodeID)
	b.WriteString("# Group peers are served over NTS with the group CA set defined above.\n")
	if len(peers) == 0 {
		b.WriteString("# No group peers are configured.\n")
	}
	for _, p := range peers {
		port := ""
		if ntpPort != DefaultNTPPort {
			port = fmt.Sprintf(" port %d", ntpPort)
		}
		fmt.Fprintf(&b, "server %s nts iburst certset 1%s\n", p.Host, port)
	}
	if len(unauthenticated) > 0 {
		b.WriteString("\n# Unauthenticated NTP sources explicitly configured by the operator.\n")
		for _, host := range unauthenticated {
			fmt.Fprintf(&b, "server %s\n", host)
		}
	}
	return b.String()
}

func formatFloat(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}
