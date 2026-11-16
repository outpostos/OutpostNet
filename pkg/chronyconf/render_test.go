package chronyconf

import (
	"crypto/tls"
	"flag"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

var updateGolden = flag.Bool("update", false, "update golden files")

// credentials returns a group CA and a node-a certificate valid for
// node-a.edge.local and 10.0.0.5, plus the directory holding them.
func credentials(t *testing.T) (certFile, keyFile, caFile, dir string) {
	t.Helper()
	dir = t.TempDir()
	caPEM, caKeyPEM, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caFile = filepath.Join(dir, "group.pem")
	writeFile(t, caFile, caPEM, 0o644)
	caPath := caFile
	caKeyPath := filepath.Join(dir, "group.key")
	writeFile(t, caKeyPath, caKeyPEM, 0o600)
	caPair, err := tls.LoadX509KeyPair(caPath, caKeyPath)
	if err != nil {
		t.Fatalf("load CA: %v", err)
	}
	certPEM, keyPEM, err := tlsutil.IssueNodeWithSANs(caPair, "node-a", time.Now(),
		[]string{"node-a.edge.local"}, []net.IP{net.ParseIP("10.0.0.5")})
	if err != nil {
		t.Fatalf("IssueNodeWithSANs: %v", err)
	}
	certFile = filepath.Join(dir, "node-a.pem")
	keyFile = filepath.Join(dir, "node-a.key")
	writeFile(t, certFile, certPEM, 0o644)
	writeFile(t, keyFile, keyPEM, 0o600)
	return certFile, keyFile, caFile, dir
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func baseConfig(t *testing.T) (Config, string) {
	t.Helper()
	certFile, keyFile, caFile, dir := credentials(t)
	return Config{
		NodeID:   "node-a",
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}, dir
}

func TestRenderGolden(t *testing.T) {
	intPtr := func(v int) *int { return &v }
	peers := []Peer{
		{NodeID: "node-c", Host: "node-c.edge.local"},
		{NodeID: "node-b", Host: "10.0.0.6"},
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		golden  string
		sources string
	}{
		{
			name:   "single-file",
			mutate: func(c *Config) { c.Peers = peers; c.Allow = []string{"10.0.0.0/24"} },
			golden: "single-file.golden",
		},
		{
			name: "split-sources",
			mutate: func(c *Config) {
				c.Peers = peers
				c.Allow = []string{"10.0.0.0/24"}
				c.SourcesDir = "/etc/chrony/sources.d"
			},
			golden:  "split-main.golden",
			sources: "split-sources.golden",
		},
		{
			name: "gnss-shm-pps",
			mutate: func(c *Config) {
				c.Peers = peers
				c.Allow = []string{"10.0.0.0/24"}
				c.GNSS = &GNSS{SHMSegment: intPtr(0), PPSDevice: "/dev/pps0"}
			},
			golden: "gnss-shm-pps.golden",
		},
		{
			name: "gnss-sock-no-pps",
			mutate: func(c *Config) {
				c.Peers = peers
				c.Allow = []string{"10.0.0.0/24"}
				c.GNSS = &GNSS{SOCKPath: "/run/chrony/gpsd.sock", RefID: "NMEA"}
			},
			golden: "gnss-sock.golden",
		},
		{
			name:   "orphan-only",
			mutate: func(c *Config) {},
			golden: "orphan-only.golden",
		},
		{
			name: "explicit-unauthenticated",
			mutate: func(c *Config) {
				c.Peers = peers[:1]
				c.Allow = []string{"10.0.0.0/24"}
				c.UnauthenticatedServers = []string{"pool.ntp.example"}
			},
			golden: "unauthenticated.golden",
		},
		{
			name: "non-default-ports",
			mutate: func(c *Config) {
				c.Peers = peers[:1]
				c.Allow = []string{"10.0.0.0/24"}
				c.NTPPort = 10123
				c.NTSKEPort = 14460
			},
			golden: "non-default-ports.golden",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, dir := baseConfig(t)
			tc.mutate(&cfg)
			out, err := Render(cfg)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			main := strings.ReplaceAll(out.Main, dir, "/credentials")
			sources := strings.ReplaceAll(out.Sources, dir, "/credentials")
			checkGolden(t, tc.golden, main)
			if tc.sources != "" {
				checkGolden(t, tc.sources, sources)
			} else if out.Sources != "" {
				t.Fatalf("single-file mode returned sources %q", out.Sources)
			}
		})
	}
}

func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("update golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run go test -run TestRenderGolden -update): %v", err)
	}
	if got != string(want) {
		t.Fatalf("rendered config differs from %s:\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
}

func TestRenderWarnsWithoutSAN(t *testing.T) {
	dir := t.TempDir()
	caPEM, caKeyPEM, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "ca.pem"), caPEM, 0o644)
	writeFile(t, filepath.Join(dir, "ca.key"), caKeyPEM, 0o600)
	ca, err := tls.LoadX509KeyPair(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key"))
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := tlsutil.IssueNode(ca, "node-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "node.pem"), certPEM, 0o644)
	writeFile(t, filepath.Join(dir, "node.key"), keyPEM, 0o600)

	out, err := Render(Config{
		NodeID: "node-a", CertFile: filepath.Join(dir, "node.pem"),
		KeyFile: filepath.Join(dir, "node.key"), CAFile: filepath.Join(dir, "ca.pem"),
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !hasWarning(out.Warnings, "no IP/DNS SAN") {
		t.Fatalf("missing SAN warning: %v", out.Warnings)
	}
}

func TestRenderSplitWarnsPeerDerivedAllow(t *testing.T) {
	cfg, _ := baseConfig(t)
	cfg.Peers = []Peer{{NodeID: "node-b", Host: "10.0.0.6"}}
	cfg.SourcesDir = "/etc/chrony/sources.d"
	out, err := Render(cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !hasWarning(out.Warnings, "peer-derived rule") {
		t.Fatalf("missing split-mode allow warning: %v", out.Warnings)
	}
}

func TestRenderSkipsSelfPeer(t *testing.T) {
	cfg, _ := baseConfig(t)
	cfg.Peers = []Peer{{NodeID: "node-a", Host: "10.0.0.5"}}
	out, err := Render(cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !hasWarning(out.Warnings, "local node") {
		t.Fatalf("missing self-peer warning: %v", out.Warnings)
	}
	if strings.Contains(out.Main, "server 10.0.0.5") {
		t.Fatalf("local node rendered as a source:\n%s", out.Main)
	}
}

func TestRenderRejectsInvalid(t *testing.T) {
	good, _ := baseConfig(t)
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty node", func(c *Config) { c.NodeID = "" }},
		{"newline node", func(c *Config) { c.NodeID = "node-a\nserver evil" }},
		{"missing cert", func(c *Config) { c.CertFile = "" }},
		{"cert mismatch", func(c *Config) { c.NodeID = "node-b" }},
		{"missing ca", func(c *Config) { c.CAFile = "" }},
		{"bad ca path", func(c *Config) { c.CAFile = filepath.Join(t.TempDir(), "missing.pem") }},
		{"peer port", func(c *Config) { c.Peers = []Peer{{NodeID: "b", Host: "10.0.0.6:123"}} }},
		{"peer empty host", func(c *Config) { c.Peers = []Peer{{NodeID: "b"}} }},
		{"peer newline", func(c *Config) { c.Peers = []Peer{{NodeID: "b", Host: "10.0.0.6\nallow 0/0"}} }},
		{"duplicate peer id", func(c *Config) { c.Peers = []Peer{{NodeID: "b", Host: "10.0.0.6"}, {NodeID: "b", Host: "10.0.0.7"}} }},
		{"duplicate peer host", func(c *Config) { c.Peers = []Peer{{NodeID: "b", Host: "10.0.0.6"}, {NodeID: "c", Host: "10.0.0.6"}} }},
		{"bad allow", func(c *Config) { c.Allow = []string{"not-a-cidr"} }},
		{"bad unauthenticated", func(c *Config) { c.UnauthenticatedServers = []string{"evil # comment"} }},
		{"gnss both", func(c *Config) { n := 0; c.GNSS = &GNSS{SHMSegment: &n, SOCKPath: "/run/gps.sock"} }},
		{"gnss neither", func(c *Config) { c.GNSS = &GNSS{} }},
		{"gnss bad refid", func(c *Config) { n := 0; c.GNSS = &GNSS{SHMSegment: &n, RefID: "TOOLONG"} }},
		{"gnss bad pps", func(c *Config) { n := 0; c.GNSS = &GNSS{SHMSegment: &n, PPSDevice: "/dev/pps 0"} }},
		{"bad ntp port", func(c *Config) { c.NTPPort = 70000 }},
		{"bad nts-ke port", func(c *Config) { c.NTSKEPort = -1 }},
		{"bad stratum", func(c *Config) { c.LocalStratum = 16 }},
		{"bad makestep threshold", func(c *Config) { c.MakestepThreshold = -1 }},
		{"bad makestep limit", func(c *Config) { c.MakestepLimit = -2 }},
		{"negative nocerttimecheck", func(c *Config) { c.NocertTimecheck = -1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := good
			cfg.Peers = nil
			cfg.Allow = nil
			cfg.GNSS = nil
			cfg.UnauthenticatedServers = nil
			tc.mutate(&cfg)
			if _, err := Render(cfg); err == nil {
				t.Fatalf("Render accepted invalid config")
			}
		})
	}
}

func hasWarning(warnings []string, substr string) bool {
	for _, w := range warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

// TestRenderedConfigParsesWithChronyd runs the real chrony parser on the
// rendered fragments. It skips on hosts without chronyd; Linux CI installs
// chrony so the check runs there (GitHub #76 acceptance 1).
func TestRenderedConfigParsesWithChronyd(t *testing.T) {
	chronyd, err := exec.LookPath("chronyd")
	if err != nil {
		t.Skip("chronyd not installed")
	}
	peers := []Peer{{NodeID: "node-b", Host: "10.0.0.6"}, {NodeID: "node-c", Host: "node-c.edge.local"}}

	t.Run("single-file", func(t *testing.T) {
		cfg, _ := baseConfig(t)
		cfg.Peers = peers
		cfg.Allow = []string{"10.0.0.0/24"}
		cfg.GNSS = &GNSS{SHMSegment: new(int), PPSDevice: "/dev/pps0"}
		out, err := Render(cfg)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		path := filepath.Join(dir, "chrony.conf")
		writeFile(t, path, []byte(out.Main), 0o644)
		runChronydParse(t, chronyd, path, true)
	})

	t.Run("split-sources", func(t *testing.T) {
		sourcesDir := t.TempDir()
		cfg, _ := baseConfig(t)
		cfg.Peers = peers
		cfg.Allow = []string{"10.0.0.0/24"}
		cfg.SourcesDir = sourcesDir
		out, err := Render(cfg)
		if err != nil {
			t.Fatal(err)
		}
		sourcesPath := filepath.Join(sourcesDir, "outpostnet.sources")
		writeFile(t, sourcesPath, []byte(out.Sources), 0o644)
		path := filepath.Join(t.TempDir(), "chrony.conf")
		writeFile(t, path, []byte(out.Main), 0o644)
		// chronyd -p prints the main configuration but not sourcedir
		// contents, so parse the sources through include as well.
		runChronydParse(t, chronyd, path, false)
		wrapper := filepath.Join(t.TempDir(), "sources.conf")
		writeFile(t, wrapper, []byte("include "+sourcesPath+"\n"), 0o644)
		runChronydParse(t, chronyd, wrapper, true)
	})
}

func runChronydParse(t *testing.T, chronyd, path string, wantSource bool) {
	t.Helper()
	cmd := exec.Command(chronyd, "-p", "-f", path)
	output, err := cmd.CombinedOutput()
	// Distributions ship an AppArmor profile that confines chronyd to
	// /etc/chrony, /var/lib/chrony and /run/chrony; a test config in a temp
	// directory is denied. Retry unconfined when the host enforces it.
	if err != nil && strings.Contains(string(output), "Permission denied") {
		if aa, aerr := exec.LookPath("aa-exec"); aerr == nil {
			cmd = exec.Command(aa, "-p", "unconfined", "--", chronyd, "-p", "-f", path)
			output, err = cmd.CombinedOutput()
		}
	}
	if err != nil {
		t.Fatalf("chronyd -p rejected the rendered config: %v\n%s\n--- config ---\n%s", err, output, mustRead(t, path))
	}
	if wantSource && !strings.Contains(string(output), "server 10.0.0.6") {
		t.Fatalf("chronyd -p did not print the parsed sources:\n%s", output)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
