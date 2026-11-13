package servicehost_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// logicalNode is node-a: a tunnel listener, a logical entry for echo and the
// (fake) local Mesh gateway.
func logicalNode(t *testing.T, nodeID, entry, meshURL string, peers []service.Peer) *service.Config {
	t.Helper()
	return &service.Config{
		Config: agentconfig.Config{
			NodeID:     nodeID,
			HealthAddr: "127.0.0.1:0",
			Tunnels:    []agentconfig.TunnelConfig{{Role: "server", ListenAddr: "127.0.0.1:0"}},
			Imports:    []agentconfig.ImportConfig{{Service: "echo", Name: "echo", ListenAddr: entry, Protocol: "tcp"}},
			TLS:        agentconfig.TLSConfig{Insecure: true},
			Auth:       agentconfig.AuthConfig{Token: token},
			Management: agentconfig.ManagementConfig{SocketPath: filepath.Join(sockDir(t), nodeID+".sock")},
		},
		Peers: peers,
		Mesh:  service.MeshConfig{GatewayURL: meshURL},
	}
}

func validated(t *testing.T, cfg *service.Config) *service.Config {
	t.Helper()
	if err := service.ValidateConfig(cfg); err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

// twoNodes starts node-b (exports echo from appB) and node-a (logical entry
// for echo, dials node-b) against one fake Mesh.
func twoNodes(t *testing.T, mesh *fakeMesh, appB string) (entry string, nodeA, nodeB *runningHost) {
	t.Helper()
	nodeB = startHost(t, exportingNode(t, "node-b", appB, token))
	entry = freeAddr(t)
	nodeA = startHost(t, validated(t, logicalNode(t, "node-a", entry, mesh.url(), []service.Peer{{NodeID: "node-b", Addr: tunnelAddr(t, nodeB)}})))
	return entry, nodeA, nodeB
}

// Acceptance 1 + 2: the logical entry reaches the instance on node-b; when
// the directory gains a node-a instance new connections go local while the
// existing connection stays where it is.
func TestLogicalEntryFollowsDirectory(t *testing.T) {
	mesh := newFakeMesh(t)
	appB := startCountingApp(t, "b")
	regB := reg{service: "echo", uid: "pod-b", node: "node-b", port: portOf(t, appB.addr)}
	mesh.register(regB)
	entry, nodeA, _ := twoNodes(t, mesh, appB.addr)

	askEventually(t, entry, "b:hello")
	kept, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer kept.c.Close()
	if got, err := kept.ask("before"); err != nil || got != "b:before" {
		t.Fatalf("kept warmup = %q %v", got, err)
	}

	appA := startCountingApp(t, "a")
	mesh.register(reg{service: "echo", uid: "pod-a", node: "node-a", port: portOf(t, appA.addr)})
	askEventually(t, entry, "a:hello")
	if got, err := kept.ask("after"); err != nil || got != "b:after" {
		t.Fatalf("existing connection moved: %q %v", got, err)
	}

	// The local instance goes away: new connections return to node-b.
	mesh.withdraw(reg{service: "echo", uid: "pod-a", node: "node-a", port: portOf(t, appA.addr)})
	askEventually(t, entry, "b:hello")

	st, ok := nodeA.h.LogicalStats("echo")
	if !ok || st.Resolved == 0 {
		t.Fatalf("stats = %+v %v", st, ok)
	}
}

// Acceptance 3: the preferred candidate fails before any byte and the same
// connection lands on the next one; once bytes flowed, killing the backend
// closes the connection and nothing is resent to the other candidate.
func TestLogicalFailoverOnlyBeforeBytes(t *testing.T) {
	mesh := newFakeMesh(t)
	appB := startCountingApp(t, "b")
	mesh.register(reg{service: "echo", uid: "pod-b", node: "node-b", port: portOf(t, appB.addr)})
	// A local registration whose backend is not listening: preferred (local
	// first) but unreachable.
	deadA := freeAddr(t)
	mesh.register(reg{service: "echo", uid: "pod-a-dead", node: "node-a", port: portOf(t, deadA)})
	entry, nodeA, _ := twoNodes(t, mesh, appB.addr)

	askEventually(t, entry, "b:hello")
	st, _ := nodeA.h.LogicalStats("echo")
	if st.FailedOver == 0 {
		t.Fatalf("no failover recorded: %+v", st)
	}

	// Now a live local backend (preferred) and node-b as the alternative.
	appA := startCountingApp(t, "a")
	mesh.register(reg{service: "echo", uid: "pod-a-dead", node: "node-a", port: portOf(t, appA.addr)})
	askEventually(t, entry, "a:hello")
	inflight, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer inflight.c.Close()
	if got, err := inflight.ask("request-1"); err != nil || got != "a:request-1" {
		t.Fatalf("in-flight = %q %v", got, err)
	}
	bBefore := appB.count()
	appA.kill()
	expectClosed(t, inflight, "backend killed after bytes were forwarded")
	time.Sleep(200 * time.Millisecond)
	if got := appB.count(); got != bBefore {
		t.Fatalf("the other candidate received %d new connection(s): data was resent", got-bBefore)
	}
}

// Acceptance 4: no_candidate, all_unreachable and directory_stale are each
// counted, and the connection is closed at once.
func TestLogicalFailureReasonsCounted(t *testing.T) {
	t.Run("no_candidate", func(t *testing.T) {
		mesh := newFakeMesh(t)
		appB := startCountingApp(t, "b")
		entry, nodeA, _ := twoNodes(t, mesh, appB.addr)
		waitFor(t, 5*time.Second, "directory synced", func() bool {
			_, fr := nodeA.h.Directory().Instances("echo")
			return fr.Synced && !fr.Stale
		})
		expectRefused(t, entry)
		waitFor(t, 5*time.Second, "no_candidate counted", func() bool {
			st, _ := nodeA.h.LogicalStats("echo")
			return st.Failures[service.ReasonNoCandidate] > 0
		})
	})
	t.Run("all_unreachable", func(t *testing.T) {
		mesh := newFakeMesh(t)
		appB := startCountingApp(t, "b")
		// node-b is reachable but exports no "echo" route for this name:
		// it answers "no route". The local registration has no listener.
		mesh.register(reg{service: "echo", uid: "pod-b", node: "node-b", port: 1})
		mesh.register(reg{service: "echo", uid: "pod-a", node: "node-a", port: portOf(t, freeAddr(t))})
		nodeB := startHost(t, exportingNode(t, "node-b", appB.addr, token))
		nodeB.h.ApplyExports(nil) // no exports at all on node-b
		entry := freeAddr(t)
		cfgA := validated(t, logicalNode(t, "node-a", entry, mesh.url(), []service.Peer{{NodeID: "node-b", Addr: tunnelAddr(t, nodeB)}}))
		nodeA := startHost(t, cfgA)
		waitFor(t, 5*time.Second, "tunnel up", func() bool { return peerStatus(t, cfgA.Management.SocketPath, "node-b") == "connected" })
		// Without this wait the connection can arrive before the first
		// snapshot and is (correctly) counted as directory_stale.
		waitFor(t, 5*time.Second, "directory synced with both registrations", func() bool {
			insts, fr := nodeA.h.Directory().Instances("echo")
			return fr.Synced && !fr.Stale && len(insts) == 2
		})
		expectRefused(t, entry)
		waitFor(t, 5*time.Second, "all_unreachable counted", func() bool {
			st, _ := nodeA.h.LogicalStats("echo")
			return st.Failures[service.ReasonAllUnreachable] > 0
		})
		if appB.count() != 0 {
			t.Fatal("node-b backend was reached without a route")
		}
	})
	t.Run("directory_stale", func(t *testing.T) {
		// Mesh is not reachable at all and there is no cache.
		entry := freeAddr(t)
		nodeA := startHost(t, validated(t, logicalNode(t, "node-a", entry, "http://"+freeAddr(t), nil)))
		expectRefused(t, entry)
		waitFor(t, 5*time.Second, "directory_stale counted", func() bool {
			st, _ := nodeA.h.LogicalStats("echo")
			return st.Failures[service.ReasonDirectoryStale] > 0
		})
	})
}

// A node-bound import next to a logical one keeps working unchanged.
func TestNodeBoundImportUnchangedWithLogical(t *testing.T) {
	mesh := newFakeMesh(t)
	appB := startCountingApp(t, "b")
	nodeB := startHost(t, exportingNode(t, "node-b", appB.addr, token))
	logicalEntry, boundEntry := freeAddr(t), freeAddr(t)
	cfg := logicalNode(t, "node-a", logicalEntry, mesh.url(), []service.Peer{{NodeID: "node-b", Addr: tunnelAddr(t, nodeB)}})
	cfg.Imports = append(cfg.Imports, agentconfig.ImportConfig{Service: "echo@node-b", ListenAddr: boundEntry, Protocol: "tcp"})
	startHost(t, validated(t, cfg))
	askEventually(t, boundEntry, "b:hello")
}

// expectRefused dials the entry and expects the connection to be closed
// without any reply.
func expectRefused(t *testing.T, entry string) {
	t.Helper()
	s, err := dialEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer s.c.Close()
	got, err := s.ask("hello")
	if err == nil {
		t.Fatalf("connection without a backend answered %q", got)
	}
	if strings.Contains(err.Error(), "timeout") {
		t.Fatalf("connection was not closed promptly: %v", err)
	}
}
