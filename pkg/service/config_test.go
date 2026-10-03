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
