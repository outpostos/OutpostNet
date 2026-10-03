package service_test

import (
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// migrationExample is the config set documented in docs/service-migration.md:
// one node before (combined outpostnet-agent) and after (Net-only agent plus
// outpostnet-service side by side).
const migrationExample = "../../examples/migration"

func loadMigrationExample(t *testing.T) (before, agentAfter *agentconfig.Config, svc *service.Config) {
	t.Helper()
	var err error
	if before, err = agentconfig.LoadConfig(filepath.Join(migrationExample, "node1.agent-combined.yaml")); err != nil {
		t.Fatalf("load combined agent config (before): %v", err)
	}
	if agentAfter, err = agentconfig.LoadConfig(filepath.Join(migrationExample, "node1.agent-net.yaml")); err != nil {
		t.Fatalf("load Net-only agent config (after): %v", err)
	}
	if svc, err = service.LoadConfig(filepath.Join(migrationExample, "node1.service.yaml")); err != nil {
		t.Fatalf("load outpostnet-service config (after): %v", err)
	}
	return before, agentAfter, svc
}

// The Service config must carry exactly the entries and exports the combined
// agent served, under the same node identity and tunnel credentials.
func TestMigrationExampleServiceCarriesCombinedEntriesAndExports(t *testing.T) {
	before, _, svc := loadMigrationExample(t)

	if len(before.Imports) == 0 || len(before.Exports) == 0 {
		t.Fatalf("combined example must have imports and exports, got %d/%d", len(before.Imports), len(before.Exports))
	}
	if svc.NodeID != before.NodeID {
		t.Errorf("node_id: service %q, combined agent %q", svc.NodeID, before.NodeID)
	}
	if !reflect.DeepEqual(svc.Imports, before.Imports) {
		t.Errorf("imports differ:\nservice  %+v\ncombined %+v", svc.Imports, before.Imports)
	}
	if !reflect.DeepEqual(svc.Exports, before.Exports) {
		t.Errorf("exports differ:\nservice  %+v\ncombined %+v", svc.Exports, before.Exports)
	}
	if svc.Auth != before.Auth {
		t.Errorf("auth must match the peers' token")
	}
	if svc.TLS != before.TLS {
		t.Errorf("tls: service %+v, combined %+v", svc.TLS, before.TLS)
	}
}

// After migration the agent keeps only Net duties: same identity, tunnels,
// credentials and management socket (OutpostLet keeps pushing peers to it),
// no entries and no exports.
func TestMigrationExampleAgentKeepsOnlyNetDuties(t *testing.T) {
	before, after, _ := loadMigrationExample(t)

	if len(after.Imports) != 0 || len(after.Exports) != 0 {
		t.Errorf("Net-only agent must have no imports/exports, got %d/%d", len(after.Imports), len(after.Exports))
	}
	stripped := *before
	stripped.Imports, stripped.Exports = nil, nil
	got := *after
	got.Imports, got.Exports = nil, nil
	if !reflect.DeepEqual(got, stripped) {
		t.Errorf("Net-only agent differs from the combined agent beyond imports/exports:\nafter  %+v\nbefore %+v", got, stripped)
	}
}

// Agent and service run on the same node at the same time, so every address
// and socket they own must differ, and the service's peers must point at the
// peers' service tunnel listeners (a service entry reaches exports registered
// on the listener it dials).
func TestMigrationExampleAgentAndServiceCanRunSideBySide(t *testing.T) {
	_, agentCfg, svc := loadMigrationExample(t)

	agentPorts := serverPorts(t, agentCfg.Tunnels)
	svcPorts := serverPorts(t, svc.Tunnels)
	if len(svcPorts) == 0 {
		t.Fatal("service needs its own role: server tunnel to serve exports")
	}
	for p := range svcPorts {
		if agentPorts[p] {
			t.Errorf("tunnel port %s is used by both agent and service", p)
		}
	}
	if svc.Management.SocketPath == agentCfg.Management.SocketPath {
		t.Errorf("management socket %q shared by agent and service", svc.Management.SocketPath)
	}
	taken := map[string]string{
		agentCfg.HealthAddr:   "agent health_addr",
		agentCfg.Metrics.Addr: "agent metrics.addr",
	}
	if what, ok := taken[svc.HealthAddr]; ok {
		t.Errorf("service health_addr %q collides with %s", svc.HealthAddr, what)
	}
	for _, imp := range svc.Imports {
		if what, ok := taken[imp.ListenAddr]; ok {
			t.Errorf("entry %q collides with %s", imp.ListenAddr, what)
		}
	}
	if len(svc.Peers) == 0 {
		t.Fatal("service example must seed peers")
	}
	for _, p := range svc.Peers {
		_, port, err := net.SplitHostPort(p.Addr)
		if err != nil {
			t.Fatalf("peer %s addr %q: %v", p.NodeID, p.Addr, err)
		}
		if !svcPorts[port] || agentPorts[port] {
			t.Errorf("peer %s addr %q must use the service tunnel port, not the agent's", p.NodeID, p.Addr)
		}
	}
}

func serverPorts(t *testing.T, tunnels []agentconfig.TunnelConfig) map[string]bool {
	t.Helper()
	out := make(map[string]bool)
	for _, tc := range tunnels {
		if !strings.EqualFold(tc.Role, "server") {
			continue
		}
		_, port, err := net.SplitHostPort(tc.ListenAddr)
		if err != nil {
			t.Fatalf("tunnel listen_addr %q: %v", tc.ListenAddr, err)
		}
		out[port] = true
	}
	return out
}
