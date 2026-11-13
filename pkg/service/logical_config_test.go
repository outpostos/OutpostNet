package service

import (
	"strings"
	"testing"
)

const logicalBase = `
node_id: node-a
auth: {token: t}
tls: {insecure: true}
`

// Acceptance 5: config validation for logical imports.
func TestParseConfigLogicalImports(t *testing.T) {
	cfg, err := ParseConfig([]byte(logicalBase + `
mesh:
  gateway_url: http://127.0.0.1:8081
  directory_cache_dir: /var/lib/outpostnet-service
imports:
  - {service: echo, listen_addr: "127.0.0.1:19000", protocol: tcp}
  - {service: echo@node-b, listen_addr: "127.0.0.1:19001", protocol: tcp}
`))
	if err != nil {
		t.Fatal(err)
	}
	if imp := cfg.Imports[0]; imp.Service != "echo" || imp.Name != "echo" || imp.RemoteRegion != "" {
		t.Fatalf("logical import after validation = %+v", imp)
	}
	// The node-bound import keeps its previous normalisation.
	if imp := cfg.Imports[1]; imp.Service != "echo@node-b" || imp.Name != "echo" || imp.RemoteRegion != "node-b" {
		t.Fatalf("node-bound import after validation = %+v", imp)
	}
	tg := cfg.EntryTargets()
	if tg[0] != (Target{Service: "echo", Logical: true}) || tg[1] != (Target{Service: "echo", NodeID: "node-b"}) {
		t.Fatalf("targets = %+v", tg)
	}
	if cfg.Mesh.DirectoryCacheDir != "/var/lib/outpostnet-service" {
		t.Fatalf("mesh = %+v", cfg.Mesh)
	}
}

func TestParseConfigLogicalImportErrors(t *testing.T) {
	cases := map[string]struct{ doc, want string }{
		"no gateway": {`
imports:
  - {service: echo, listen_addr: "127.0.0.1:19000", protocol: tcp}
`, "mesh.gateway_url is required"},
		"bad name": {`
mesh: {gateway_url: "http://127.0.0.1:8081"}
imports:
  - {service: Echo_Svc, listen_addr: "127.0.0.1:19000", protocol: tcp}
`, "imports[0].service"},
		"remote region": {`
mesh: {gateway_url: "http://127.0.0.1:8081"}
imports:
  - {service: echo, remote_region: west, listen_addr: "127.0.0.1:19000", protocol: tcp}
`, "no remote_region"},
		"udp": {`
mesh: {gateway_url: "http://127.0.0.1:8081"}
imports:
  - {service: echo, listen_addr: "127.0.0.1:19000", protocol: udp}
`, "only tcp"},
		"bad gateway url": {`
mesh: {gateway_url: "127.0.0.1:8081"}
imports:
  - {service: echo, listen_addr: "127.0.0.1:19000", protocol: tcp}
`, "mesh.gateway_url"},
		"error index preserved": {`
mesh: {gateway_url: "http://127.0.0.1:8081"}
imports:
  - {service: echo, listen_addr: "127.0.0.1:19000", protocol: tcp}
  - {service: other, protocol: tcp}
`, "imports[1].listen_addr"},
		"unknown mesh field": {`
mesh: {gateway: "http://127.0.0.1:8081"}
`, "gateway"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseConfig([]byte(logicalBase + tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}
