package service_test

import (
	"strings"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/service"
)

func TestParseConfigDefaultsAndEntries(t *testing.T) {
	cfg, err := service.ParseConfig([]byte(`
node_id: node-a
auth:
  token: s3cret
peers:
  - node_id: node-b
    addr: 127.0.0.1:7443
imports:
  - service: echo@node-b
    listen_addr: 127.0.0.1:18080
    protocol: tcp
  - name: legacy
    remote_region: cloud
    listen_addr: 127.0.0.1:18081
    protocol: tcp
tunnels:
  - role: client
    remote_region: cloud
    remote_addr: 10.0.0.1:4242
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Management.SocketPath != service.DefaultManagementSocketPath {
		t.Errorf("socket path = %q, want %q", cfg.Management.SocketPath, service.DefaultManagementSocketPath)
	}
	if cfg.HealthAddr != service.DefaultHealthAddr {
		t.Errorf("health addr = %q, want %q", cfg.HealthAddr, service.DefaultHealthAddr)
	}
	if len(cfg.Peers) != 1 || cfg.Peers[0].NodeID != "node-b" || cfg.Peers[0].Addr != "127.0.0.1:7443" {
		t.Errorf("peers = %+v", cfg.Peers)
	}
	targets := cfg.EntryTargets()
	want := []service.Target{{Service: "echo", NodeID: "node-b"}, {Service: "legacy", Region: "cloud"}}
	if len(targets) != len(want) {
		t.Fatalf("targets = %+v", targets)
	}
	for i := range want {
		if targets[i] != want[i] {
			t.Errorf("target[%d] = %+v, want %+v", i, targets[i], want[i])
		}
	}
}

func TestParseConfigRejects(t *testing.T) {
	cases := map[string]string{
		"missing node_id": `
imports:
  - service: echo@node-b
    listen_addr: 127.0.0.1:1
    protocol: tcp
`,
		"udp entry": `
node_id: a
imports:
  - service: dns@node-b
    listen_addr: 127.0.0.1:53
    protocol: udp
`,
		"udp export": `
node_id: a
exports:
  - name: dns
    local_addr: 127.0.0.1:53
    protocol: udp
`,
		"relays": `
node_id: a
tunnels:
  - role: client
    remote_region: b
    remote_addr: 1.2.3.4:1
relays:
  - service: x
    from_region: c
    to_region: b
`,
		"bad peer": `
node_id: a
peers:
  - node_id: b
`,
		"bad service ref": `
node_id: a
imports:
  - service: echo
    listen_addr: 127.0.0.1:1
    protocol: tcp
`,
		"unknown field": `
node_id: a
entriez: []
`,
	}
	for name, y := range cases {
		if _, err := service.ParseConfig([]byte(y)); err == nil {
			t.Errorf("%s: expected error", name)
		} else if name == "udp entry" && !strings.Contains(err.Error(), "udp") {
			t.Errorf("udp entry error should mention udp: %v", err)
		}
	}
}

// TestParseConfigRejectsInconsistentEntriesAndPeers covers the Service rules
// beyond the shared agentconfig schema; each case must fail for its own
// reason (checked by error substring).
func TestParseConfigRejectsInconsistentEntriesAndPeers(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"duplicate entry listen_addr", `
node_id: a
imports:
  - service: echo@b
    listen_addr: 127.0.0.1:18080
    protocol: tcp
  - service: db@b
    listen_addr: 127.0.0.1:18080
    protocol: tcp
`, "listen_addr"},
		{"duplicate peer node_id", `
node_id: a
peers:
  - node_id: b
    addr: 10.0.0.2:7443
  - node_id: b
    addr: 10.0.0.3:7443
`, "duplicate"},
		{"peer addr without port", `
node_id: a
peers:
  - node_id: b
    addr: 10.0.0.2
`, "host:port"},
		{"peer addr without host", `
node_id: a
peers:
  - node_id: b
    addr: ":7443"
`, "host:port"},
		{"peer addr bad port", `
node_id: a
peers:
  - node_id: b
    addr: 10.0.0.2:http
`, "host:port"},
		{"peer is self", `
node_id: a
peers:
  - node_id: a
    addr: 10.0.0.1:7443
`, "own node_id"},
		{"static entry without client tunnel", `
node_id: a
imports:
  - name: legacy
    remote_region: cloud
    listen_addr: 127.0.0.1:18081
    protocol: tcp
tunnels:
  - role: client
    remote_region: edge
    remote_addr: 10.0.0.1:4242
`, "no client tunnel"},
		{"static entry with only a server tunnel", `
node_id: a
imports:
  - name: legacy
    remote_region: cloud
    listen_addr: 127.0.0.1:18081
    protocol: tcp
tunnels:
  - role: server
    remote_region: cloud
    listen_addr: 127.0.0.1:4242
`, "no client tunnel"},
	}
	for _, c := range cases {
		_, err := service.ParseConfig([]byte(c.yaml))
		if err == nil {
			t.Errorf("%s: expected error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q should mention %q", c.name, err, c.want)
		}
	}
}

// TestParseConfigAcceptsEphemeralEntryPorts: port 0 entries never collide.
func TestParseConfigAcceptsEphemeralEntryPorts(t *testing.T) {
	_, err := service.ParseConfig([]byte(`
node_id: a
imports:
  - service: echo@b
    listen_addr: 127.0.0.1:0
    protocol: tcp
  - service: db@b
    listen_addr: 127.0.0.1:0
    protocol: tcp
`))
	if err != nil {
		t.Fatal(err)
	}
}
