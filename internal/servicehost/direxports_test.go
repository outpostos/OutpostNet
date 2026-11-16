package servicehost_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// providerNode is a node with a tunnel listener, no static exports and the
// (fake) local Mesh gateway: its exports come from the directory only.
func providerNode(t *testing.T, nodeID, meshURL string) *service.Config {
	t.Helper()
	return validated(t, &service.Config{
		Config: agentconfig.Config{
			NodeID:     nodeID,
			HealthAddr: "127.0.0.1:0",
			Tunnels:    []agentconfig.TunnelConfig{{Role: "server", ListenAddr: "127.0.0.1:0"}},
			TLS:        agentconfig.TLSConfig{Insecure: true},
			Auth:       agentconfig.AuthConfig{Token: token},
			Management: agentconfig.ManagementConfig{SocketPath: filepath.Join(sockDir(t), nodeID+".sock")},
		},
		Mesh: service.MeshConfig{GatewayURL: meshURL},
	})
}

func routeIs(h *runningHost, name, want string) func() bool {
	return func() bool {
		got, ok := h.h.ExportRoutes()[name]
		if want == "" {
			return !ok
		}
		return ok && got == want
	}
}

// Acceptance 1 + 3: a local registration becomes a route, follows port
// changes, and goes away on DRAINING, is_live=false and expiry; other
// nodes' registrations never become local routes.
func TestDirectoryExportsFollowRegistrations(t *testing.T) {
	mesh := newFakeMesh(t)
	app1, app2 := startCountingApp(t, "v1"), startCountingApp(t, "v2")
	node := startHost(t, providerNode(t, "node-b", mesh.url()))

	mesh.register(reg{service: "other", uid: "x", node: "node-c", port: 9})
	r := reg{service: "echo", uid: "pod-1", node: "node-b", port: portOf(t, app1.addr)}
	mesh.register(r)
	waitFor(t, 5*time.Second, "route appears", routeIs(node, "echo", app1.addr))
	if _, ok := node.h.ExportRoutes()["other"]; ok {
		t.Fatal("another node's registration became a local export")
	}

	r.port = portOf(t, app2.addr)
	mesh.register(r)
	waitFor(t, 5*time.Second, "route re-pointed", routeIs(node, "echo", app2.addr))

	r.health = "SERVICE_HEALTH_STATE_DRAINING"
	mesh.register(r)
	waitFor(t, 5*time.Second, "draining removes route", routeIs(node, "echo", ""))

	r.health = ""
	mesh.register(r)
	waitFor(t, 5*time.Second, "ready again", routeIs(node, "echo", app2.addr))
	mesh.withdraw(r)
	waitFor(t, 5*time.Second, "is_live=false removes route", routeIs(node, "echo", ""))

	r2 := reg{service: "echo", uid: "pod-2", node: "node-b", port: portOf(t, app1.addr), expiry: time.Now().Add(700 * time.Millisecond)}
	mesh.register(r2)
	waitFor(t, 5*time.Second, "short-lived route", routeIs(node, "echo", app1.addr))
	waitFor(t, 5*time.Second, "expiry removes route", routeIs(node, "echo", ""))
}

// Acceptance 2: a static export of the same name wins with a warning; when
// the conflict ends nothing stale is left behind.
func TestStaticExportWinsOverDirectory(t *testing.T) {
	mesh := newFakeMesh(t)
	staticApp, dirApp := startCountingApp(t, "static"), startCountingApp(t, "dir")
	cfg := providerNode(t, "node-b", mesh.url())
	cfg.Exports = []agentconfig.ExportConfig{{Name: "echo", LocalAddr: staticApp.addr, Protocol: "tcp"}}
	var logs logBuffer
	node := startHostWithLogger(t, cfg, logs.logger())

	r := reg{service: "echo", uid: "pod-1", node: "node-b", port: portOf(t, dirApp.addr)}
	mesh.register(r)
	waitFor(t, 5*time.Second, "conflict warning", func() bool {
		return strings.Contains(logs.String(), "static export overrides directory export")
	})
	if got := node.h.ExportRoutes()["echo"]; got != staticApp.addr {
		t.Fatalf("route = %s, want static %s", got, staticApp.addr)
	}
	// Re-syncs do not repeat the warning within the rate limit.
	time.Sleep(1500 * time.Millisecond)
	if n := strings.Count(logs.String(), "static export overrides directory export"); n != 1 {
		t.Fatalf("conflict warned %d times", n)
	}

	node.h.ApplyExports(nil) // static export removed
	waitFor(t, 5*time.Second, "directory export takes over", routeIs(node, "echo", dirApp.addr))
	mesh.withdraw(r)
	waitFor(t, 5*time.Second, "no route left", routeIs(node, "echo", ""))
}

// Acceptance 4: node-a's logical entry reaches node-b's application through
// node-b's directory export; no node has a static export. An established
// connection keeps its backend when the instance moves to another port.
func TestLogicalEntryThroughDirectoryExport(t *testing.T) {
	mesh := newFakeMesh(t) // shared: stands in for replicated Mesh
	app1, app2 := startCountingApp(t, "b1"), startCountingApp(t, "b2")
	nodeB := startHost(t, providerNode(t, "node-b", mesh.url()))
	entry := freeAddr(t)
	startHost(t, validated(t, logicalNode(t, "node-a", entry, mesh.url(), []service.Peer{{NodeID: "node-b", Addr: tunnelAddr(t, nodeB)}})))

	r := reg{service: "echo", uid: "pod-1", node: "node-b", port: portOf(t, app1.addr)}
	mesh.register(r)
	askEventually(t, entry, "b1:hello")
	kept, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer kept.c.Close()
	if got, err := kept.ask("before"); err != nil || got != "b1:before" {
		t.Fatalf("kept = %q %v", got, err)
	}

	r.port = portOf(t, app2.addr)
	mesh.register(r)
	askEventually(t, entry, "b2:hello")
	if got, err := kept.ask("after"); err != nil || got != "b1:after" {
		t.Fatalf("established stream moved: %q %v", got, err)
	}
	if routes := nodeB.h.ExportRoutes(); len(routes) != 1 {
		t.Fatalf("node-b routes = %v", routes)
	}
}
