package servicehost_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"

	"github.com/outpostos/OutpostNet/internal/observation/observationtest"
	"github.com/outpostos/OutpostNet/internal/servicehost"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

const waitEvent = 15 * time.Second

func startObservedHost(t *testing.T, cfg *service.Config, observationAddr string, logger *logBuffer) *runningHost {
	t.Helper()
	h := servicehost.New(cfg, logger.logger())
	h.EnableObservation(observationAddr)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningHost{h: h, cancel: cancel, done: make(chan struct{})}
	go func() { r.err = h.Run(ctx); close(r.done) }()
	t.Cleanup(func() { r.stop(t) })
	waitFor(t, 5*time.Second, "observed host ready", func() bool {
		select {
		case <-r.done:
			t.Fatalf("host exited: %v", r.err)
		default:
		}
		return h.Ready()
	})
	return r
}

func observationURL(t *testing.T, r *runningHost) string {
	t.Helper()
	addr := r.h.ObservationAddr()
	if addr == nil {
		t.Fatal("observation endpoint is not listening")
	}
	return "http://" + addr.String()
}

func peerIs(nodeID, address string, state observationv1alpha1.PeerLinkState) func(*observationv1alpha1.Event) bool {
	return func(e *observationv1alpha1.Event) bool {
		for _, p := range observationtest.NetPeers(e) {
			if p.GetPeerNodeId() == nodeID && (address == "" || p.GetPeerAddress() == address) && p.State != nil && p.GetState() == state {
				return true
			}
		}
		return false
	}
}

func accessIs(name string, state observationv1alpha1.ServiceAccessState, backendAddr string) func(*observationv1alpha1.Event) bool {
	return func(e *observationv1alpha1.Event) bool {
		if e.GetServiceAccessChanged() == nil {
			return false
		}
		a := e.GetServiceAccessChanged()
		return a.GetServiceName() == name && a.State != nil && a.GetState() == state && a.GetBackendAddress() == backendAddr
	}
}

func mustEvent(t *testing.T, sub *observationtest.Subscription, what string, match func(*observationv1alpha1.Event) bool) *observationv1alpha1.Event {
	t.Helper()
	event, err := sub.WaitFor(waitEvent, what, match)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

// TestObservationReportsNetAndServiceFactsThroughRangeContract runs two real
// service hosts (real QUIC tunnels, token auth, real TCP applications) and
// reads node-a's observation endpoint exactly as range-agent does. It covers
// the Net link and the Service entry through connect, instance replacement,
// a dead backend, and the peer leaving the directory.
func TestObservationReportsNetAndServiceFactsThroughRangeContract(t *testing.T) {
	appV1 := startApp(t, "v1")
	appV2 := startApp(t, "v2")
	nodeB1 := startHost(t, exportingNode(t, "node-b", appV1, token))
	b1Addr := tunnelAddr(t, nodeB1)
	entry := freeAddr(t)
	nodeACfg := consumingNode(t, "node-a", entry, b1Addr, token)
	logs := &logBuffer{}
	nodeA := startObservedHost(t, nodeACfg, "127.0.0.1:0", logs)
	url := observationURL(t, nodeA)
	ctx := context.Background()

	// Before any application connection: the snapshot already carries the
	// Net link and the entry, whose access state is unmeasured (unset).
	waitFor(t, waitEvent, "snapshot shows node-b connected", func() bool {
		snap, err := observationtest.Snapshot(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		return len(snap.GetNetPeers()) == 1 && snap.GetNetPeers()[0].GetState() == observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTED
	})
	sub, err := observationtest.Subscribe(ctx, url, observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	first := sub.Events()
	if len(first) == 0 || first[0].GetSubscriptionSnapshot() == nil {
		t.Fatalf("range-agent would not forward a subscription snapshot: %v", first)
	}
	snap := first[0].GetSubscriptionSnapshot()
	if snap.GetNodeId() != "node-a" || snap.GetComponent() != observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE {
		t.Fatalf("snapshot stream identity = %s/%s", snap.GetNodeId(), snap.GetComponent())
	}
	peer := snap.GetNetPeers()[0]
	if peer.GetPeerNodeId() != "node-b" || peer.GetPeerAddress() != b1Addr || peer.RttMs != nil {
		t.Fatalf("snapshot net peer = %v (rtt must stay unset)", peer)
	}
	if len(snap.GetServiceAccess()) != 1 {
		t.Fatalf("snapshot service access = %v", snap.GetServiceAccess())
	}
	access := snap.GetServiceAccess()[0]
	if access.GetServiceName() != "echo@node-b" || access.GetEntryAddress() != entry || access.State != nil {
		t.Fatalf("snapshot entry before any connection = %v (state must be unset)", access)
	}

	// A real connection through the entry reaches the backend.
	askEventually(t, entry, "v1:hello")
	reached := mustEvent(t, sub, "echo reachable via node-b instance 1", accessIs("echo@node-b", observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_REACHABLE, b1Addr))
	if got := reached.GetServiceAccessChanged(); got.GetBackendNodeId() != "node-b" || got.GetEntryAddress() != entry {
		t.Fatalf("reachable event = %v", got)
	}

	// Instance replacement: node-b moves to a new tunnel address.
	nodeB2 := startHost(t, exportingNode(t, "node-b", appV2, token))
	b2Addr := tunnelAddr(t, nodeB2)
	putPeers(t, nodeACfg.Management.SocketPath, []map[string]string{{"node_id": "node-b", "addr": b2Addr}})
	mustEvent(t, sub, "node-b link connected at the new address", peerIs("node-b", b2Addr, observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTED))
	askEventually(t, entry, "v2:hello")
	mustEvent(t, sub, "echo reachable via the replaced instance", accessIs("echo@node-b", observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_REACHABLE, b2Addr))

	// The provider's local backend is gone: the provider closes the stream
	// without data, which is the only signal the tunnel gives. (While node-a
	// redialled node-b above, connections may already have been reported
	// UNREACHABLE with the tunnel's own error; this looks for the new cause.)
	nodeB2.h.ApplyExports([]agentconfig.ExportConfig{{Name: "echo", LocalAddr: freeAddr(t), Protocol: "tcp"}})
	deadBackend := func(e *observationv1alpha1.Event) bool {
		return accessIs("echo@node-b", observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_UNREACHABLE, b2Addr)(e) &&
			strings.Contains(e.GetServiceAccessChanged().GetLastError(), "before any backend data")
	}
	waitFor(t, waitEvent, "unreachable reported", func() bool {
		if s, err := dialEntry(entry); err == nil {
			_, _ = s.ask("hello")
			s.c.Close()
		}
		_, err := sub.WaitFor(10*time.Millisecond, "unreachable", deadBackend)
		return err == nil
	})
	unreachable := mustEvent(t, sub, "unreachable", deadBackend)
	if got := unreachable.GetServiceAccessChanged(); got.GetBackendNodeId() != "node-b" || got.GetLastErrorTime() == nil {
		t.Fatalf("unreachable event = %v", got)
	}

	// node-b leaves the directory: the link is reported disconnected and the
	// entry has no backend to select.
	putPeers(t, nodeACfg.Management.SocketPath, []map[string]string{})
	mustEvent(t, sub, "node-b link disconnected", peerIs("node-b", b2Addr, observationv1alpha1.PeerLinkState_PEER_LINK_STATE_DISCONNECTED))
	if s, err := dialEntry(entry); err == nil {
		_, _ = s.ask("hello")
		s.c.Close()
	}
	noBackend := mustEvent(t, sub, "no backend", accessIs("echo@node-b", observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_NO_BACKEND, ""))
	if got := noBackend.GetServiceAccessChanged(); got.GetBackendNodeId() != "" || got.GetLastError() == "" {
		t.Fatalf("no-backend event = %v", got)
	}

	// A subscriber that connects now starts from a snapshot holding the
	// current facts, and only changes follow.
	late, err := observationtest.Snapshot(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if len(late.GetNetPeers()) != 0 {
		t.Fatalf("removed peer still in snapshot: %v", late.GetNetPeers())
	}
	if got := late.GetServiceAccess(); len(got) != 1 || got[0].GetState() != observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_NO_BACKEND {
		t.Fatalf("late snapshot service access = %v", got)
	}
	if late.GetSeq() != sub.Events()[len(sub.Events())-1].GetSeq() {
		t.Fatalf("snapshot seq %d is not the last delivered event seq %d", late.GetSeq(), sub.Events()[len(sub.Events())-1].GetSeq())
	}
	if err := sub.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestObservationPortTakenDoesNotStopHost: the observation endpoint failing
// to bind is logged and the product serves as usual.
func TestObservationPortTakenDoesNotStopHost(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()

	app := startApp(t, "v1")
	nodeB := startHost(t, exportingNode(t, "node-b", app, token))
	entry := freeAddr(t)
	logs := &logBuffer{}
	nodeA := startObservedHost(t, consumingNode(t, "node-a", entry, tunnelAddr(t, nodeB), token), taken.Addr().String(), logs)

	askEventually(t, entry, "v1:hello")
	if nodeA.h.ObservationAddr() != nil {
		t.Fatalf("observation endpoint claims %v although the port is taken", nodeA.h.ObservationAddr())
	}
	if !strings.Contains(logs.String(), "observation") || !strings.Contains(logs.String(), "address already in use") {
		t.Fatalf("observation bind failure not logged:\n%s", logs.String())
	}
	select {
	case <-nodeA.done:
		t.Fatalf("host stopped: %v", nodeA.err)
	default:
	}
}
