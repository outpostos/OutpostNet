package agent

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"

	"github.com/outpostos/OutpostNet/internal/observation/observationtest"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startTunnelServer(t *testing.T) (*tunnel.Server, context.CancelFunc) {
	t.Helper()
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := tunnel.NewServer(tunnel.ServerConfig{ListenAddr: "127.0.0.1:0", TLS: tlsutil.ServerTLSConfig(cert), Token: "agent-observation-token"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.ListenAndServe(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("tunnel server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return srv, cancel
}

func observedAgentConfig(t *testing.T, nodeID string, tunnels []agentconfig.TunnelConfig) *agentconfig.Config {
	return &agentconfig.Config{
		NodeID:      nodeID,
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Metrics:     agentconfig.MetricsConfig{Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t))},
		Persistence: agentconfig.PersistenceConfig{Dir: t.TempDir()},
		Tunnels:     tunnels,
		TLS:         agentconfig.TLSConfig{Insecure: true},
		Auth:        agentconfig.AuthConfig{Token: "agent-observation-token"},
		Management:  agentconfig.ManagementConfig{SocketPath: fmt.Sprintf("/tmp/outpostnet-obs-%d.sock", freePort(t))},
	}
}

func runObservedAgent(t *testing.T, cfg *agentconfig.Config, observationAddr string, logs *syncBuffer) (*Agent, chan error) {
	t.Helper()
	a := New(cfg, slog.New(slog.NewTextHandler(logs, nil)))
	a.EnableObservation(observationAddr)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(15 * time.Second):
			t.Error("agent did not stop")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get("http://" + cfg.HealthAddr + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return a, errCh
			}
		}
		select {
		case err := <-errCh:
			t.Fatalf("agent exited: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent not ready; logs:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAgentObservationReportsTunnelPeer runs a real agent with a static
// client tunnel to a real QUIC tunnel server and reads its observation
// endpoint as range-agent does.
func TestAgentObservationReportsTunnelPeer(t *testing.T) {
	srv, stopServer := startTunnelServer(t)
	defer stopServer()
	serverAddr := srv.Addr().String()
	cfg := observedAgentConfig(t, "node-a", []agentconfig.TunnelConfig{{Role: "client", RemoteRegion: "hub", RemoteAddr: serverAddr}})
	logs := &syncBuffer{}
	a, _ := runObservedAgent(t, cfg, "127.0.0.1:0", logs)
	addr := a.ObservationAddr()
	if addr == nil {
		t.Fatalf("observation endpoint not listening; logs:\n%s", logs.String())
	}
	url := "http://" + addr.String()

	sub, err := observationtest.Subscribe(context.Background(), url, observationv1alpha1.Component_COMPONENT_OUTPOSTNET)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	connected := func(e *observationv1alpha1.Event) bool {
		for _, p := range observationtest.NetPeers(e) {
			if p.GetPeerAddress() == serverAddr && p.State != nil && p.GetState() == observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTED {
				return true
			}
		}
		return false
	}
	event, err := sub.WaitFor(10*time.Second, "hub link connected", connected)
	if err != nil {
		t.Fatal(err)
	}
	if event.GetNodeId() != "node-a" || event.GetComponent() != observationv1alpha1.Component_COMPONENT_OUTPOSTNET {
		t.Fatalf("stream identity = %s/%s", event.GetNodeId(), event.GetComponent())
	}
	for _, p := range observationtest.NetPeers(event) {
		if p.GetPeerNodeId() != "" {
			t.Fatalf("a static region tunnel has no node identity, got peer_node_id %q", p.GetPeerNodeId())
		}
	}

	// The server goes away: the link leaves CONNECTED and carries the error.
	stopServer()
	srv.Close()
	lost, err := sub.WaitFor(20*time.Second, "hub link lost", func(e *observationv1alpha1.Event) bool {
		p := e.GetNetPeerChanged()
		return p != nil && p.GetPeerAddress() == serverAddr && p.State != nil &&
			p.GetState() != observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTED && p.GetLastError() != ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if lost.GetNetPeerChanged().GetLastErrorTime() == nil {
		t.Fatalf("lost link without error time: %v", lost)
	}
}

// TestObservationListenFailureDoesNotStopAgent mirrors OutpostLet's
// TestObservationServiceListenFailureDoesNotStopAgent.
func TestObservationListenFailureDoesNotStopAgent(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	logs := &syncBuffer{}
	a, errCh := runObservedAgent(t, observedAgentConfig(t, "node-a", nil), taken.Addr().String(), logs)
	if a.ObservationAddr() != nil {
		t.Fatal("observation endpoint claims to listen on a taken port")
	}
	if !strings.Contains(logs.String(), "observation endpoint unavailable") {
		t.Fatalf("listen failure not logged:\n%s", logs.String())
	}
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("agent stopped because observation failed: %v", err)
	default:
	}
}

// TestObservationWithoutNodeIDIsDisabledNotFatal: a region-only agent has no
// stable node identity for the stream, so observation stays off.
func TestObservationWithoutNodeIDIsDisabledNotFatal(t *testing.T) {
	logs := &syncBuffer{}
	a, _ := runObservedAgent(t, observedAgentConfig(t, "", nil), "127.0.0.1:0", logs)
	if a.ObservationAddr() != nil {
		t.Fatal("observation must not start without node_id")
	}
	if !strings.Contains(logs.String(), "node_id is required") {
		t.Fatalf("disabled observation not explained:\n%s", logs.String())
	}
	// The agent runs as configured; a missing observation stream is a warning.
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "node_id is required") && !strings.Contains(line, "level=WARN") {
			t.Fatalf("observation disabled for lack of node_id must be logged as a warning: %s", line)
		}
	}
}
