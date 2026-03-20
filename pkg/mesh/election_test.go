package mesh

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

// startElectionAcceptLoop starts a goroutine that accepts connections on ln,
// consumes the election magic byte, and passes the connection to le.HandleElectionMessage.
func startElectionAcceptLoop(t *testing.T, ln net.Listener, le *LeaderElection, done chan struct{}) {
	t.Helper()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				// Read and discard the election magic byte.
				var magic [1]byte
				if _, err := io.ReadFull(c, magic[:]); err != nil {
					c.Close()
					return
				}
				if magic[0] != electionMagic {
					c.Close()
					return
				}
				le.HandleElectionMessage(c)
			}(conn)
		}
	}()
}

func TestLeaderElection_SingleNode(t *testing.T) {
	config := DefaultElectionConfig("node-1")
	le := NewLeaderElection(config, slog.Default())

	ctx := context.Background()
	if err := le.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer le.Stop()

	if !le.IsLeader() {
		t.Fatal("single node should be leader")
	}
	if le.CurrentLeader() != "node-1" {
		t.Fatalf("expected leader node-1, got %s", le.CurrentLeader())
	}
	if le.State() != LeaderStateLeader {
		t.Fatalf("expected LeaderStateLeader, got %s", le.State())
	}
}

func TestLeaderElection_Callback(t *testing.T) {
	config := DefaultElectionConfig("node-1")
	le := NewLeaderElection(config, slog.Default())

	var mu sync.Mutex
	var leaderID string
	var isLocal bool
	le.OnLeaderChange(func(id string, local bool) {
		mu.Lock()
		leaderID = id
		isLocal = local
		mu.Unlock()
	})

	ctx := context.Background()
	if err := le.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer le.Stop()

	mu.Lock()
	if leaderID != "node-1" || !isLocal {
		t.Fatalf("callback: expected node-1/true, got %s/%v", leaderID, isLocal)
	}
	mu.Unlock()
}

func TestLeaderElection_TwoNodes_HigherPriorityWins(t *testing.T) {
	logger := slog.Default()

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()

	// Node 1: priority 10
	config1 := DefaultElectionConfig("node-1")
	config1.NodePriority = 10
	config1.Peers = []PeerConfig{{NodeID: "node-2", Address: ln2.Addr().String()}}
	config1.ElectionTimeout = 2 * time.Second
	config1.DialTimeout = 2 * time.Second
	config1.HeartbeatInterval = 100 * time.Millisecond
	config1.LeaderTimeout = 500 * time.Millisecond
	le1 := NewLeaderElection(config1, logger)

	// Node 2: priority 5 (lower)
	config2 := DefaultElectionConfig("node-2")
	config2.NodePriority = 5
	config2.Peers = []PeerConfig{{NodeID: "node-1", Address: ln1.Addr().String()}}
	config2.ElectionTimeout = 2 * time.Second
	config2.DialTimeout = 2 * time.Second
	config2.HeartbeatInterval = 100 * time.Millisecond
	config2.LeaderTimeout = 500 * time.Millisecond
	le2 := NewLeaderElection(config2, logger)

	done := make(chan struct{})
	startElectionAcceptLoop(t, ln1, le1, done)
	startElectionAcceptLoop(t, ln2, le2, done)
	defer close(done)

	ctx := context.Background()
	if err := le1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer le1.Stop()

	if err := le2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer le2.Stop()

	time.Sleep(200 * time.Millisecond)

	// Node 1 (higher priority) should be leader.
	if !le1.IsLeader() {
		t.Fatalf("node-1 (priority 10) should be leader, state=%s, leader=%s",
			le1.State(), le1.CurrentLeader())
	}
	if le1.CurrentLeader() != "node-1" {
		t.Fatalf("node-1 should know it's the leader, got %s", le1.CurrentLeader())
	}
}

func TestLeaderElection_TwoNodes_TieBreakByID(t *testing.T) {
	logger := slog.Default()

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()

	// Both nodes have same priority (0). "node-a" < "node-b" lexicographically.
	configA := DefaultElectionConfig("node-a")
	configA.Peers = []PeerConfig{{NodeID: "node-b", Address: ln2.Addr().String()}}
	configA.ElectionTimeout = 2 * time.Second
	configA.DialTimeout = 2 * time.Second
	configA.HeartbeatInterval = 100 * time.Millisecond
	configA.LeaderTimeout = 500 * time.Millisecond
	leA := NewLeaderElection(configA, logger)

	configB := DefaultElectionConfig("node-b")
	configB.Peers = []PeerConfig{{NodeID: "node-a", Address: ln1.Addr().String()}}
	configB.ElectionTimeout = 2 * time.Second
	configB.DialTimeout = 2 * time.Second
	configB.HeartbeatInterval = 100 * time.Millisecond
	configB.LeaderTimeout = 500 * time.Millisecond
	leB := NewLeaderElection(configB, logger)

	done := make(chan struct{})
	startElectionAcceptLoop(t, ln1, leA, done)
	startElectionAcceptLoop(t, ln2, leB, done)
	defer close(done)

	ctx := context.Background()
	leA.Start(ctx)
	defer leA.Stop()
	leB.Start(ctx)
	defer leB.Stop()

	time.Sleep(200 * time.Millisecond)

	// "node-a" (smaller ID) should win the tie.
	if !leA.IsLeader() {
		t.Fatalf("node-a should win tie-break, state=%s, leader=%s",
			leA.State(), leA.CurrentLeader())
	}
}

func TestLeaderElection_PeerUnreachable(t *testing.T) {
	config := DefaultElectionConfig("node-1")
	config.Peers = []PeerConfig{{NodeID: "node-2", Address: "127.0.0.1:1"}}
	config.DialTimeout = 100 * time.Millisecond
	config.ElectionTimeout = 200 * time.Millisecond
	config.HeartbeatInterval = 100 * time.Millisecond
	config.LeaderTimeout = 500 * time.Millisecond

	le := NewLeaderElection(config, slog.Default())

	ctx := context.Background()
	le.Start(ctx)
	defer le.Stop()

	if !le.IsLeader() {
		t.Fatalf("should become leader when peer unreachable, state=%s", le.State())
	}
}

func TestLeaderElection_StateString(t *testing.T) {
	tests := []struct {
		state    LeaderState
		expected string
	}{
		{LeaderStateFollower, "follower"},
		{LeaderStateCandidate, "candidate"},
		{LeaderStateLeader, "leader"},
		{LeaderState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("LeaderState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestLeaderElection_DefaultConfig(t *testing.T) {
	config := DefaultElectionConfig("test-node")

	if config.NodeID != "test-node" {
		t.Fatalf("expected test-node, got %s", config.NodeID)
	}
	if config.ElectionTimeout != 5*time.Second {
		t.Fatalf("expected 5s election timeout, got %s", config.ElectionTimeout)
	}
	if config.HeartbeatInterval != 10*time.Second {
		t.Fatalf("expected 10s heartbeat interval, got %s", config.HeartbeatInterval)
	}
	if config.LeaderTimeout != 30*time.Second {
		t.Fatalf("expected 30s leader timeout, got %s", config.LeaderTimeout)
	}
}

func TestLeaderElection_Metrics(t *testing.T) {
	config := DefaultElectionConfig("node-1")
	le := NewLeaderElection(config, slog.Default())

	ctx := context.Background()
	le.Start(ctx)
	defer le.Stop()

	if le.Metrics.ElectionsWon.Load() != 1 {
		t.Fatalf("expected 1 election won, got %d", le.Metrics.ElectionsWon.Load())
	}
}

func TestLeaderElection_ShouldAcceptProposal(t *testing.T) {
	config := DefaultElectionConfig("node-b")
	config.NodePriority = 5
	le := NewLeaderElection(config, slog.Default())

	tests := []struct {
		proposerID       string
		proposerPriority int
		expected         bool
	}{
		{"node-a", 10, true},  // Higher priority wins
		{"node-c", 3, false},  // Lower priority loses
		{"node-a", 5, true},   // Same priority, smaller ID wins
		{"node-c", 5, false},  // Same priority, larger ID loses
		{"node-b", 5, false},  // Same ID and priority
	}

	for _, tt := range tests {
		got := le.shouldAcceptProposal(tt.proposerID, tt.proposerPriority)
		if got != tt.expected {
			t.Errorf("shouldAcceptProposal(%s, %d) = %v, want %v",
				tt.proposerID, tt.proposerPriority, got, tt.expected)
		}
	}
}

func TestLeaderElection_ForwarderIntegration(t *testing.T) {
	logger := slog.Default()

	config := DefaultElectionConfig("node-1")
	le := NewLeaderElection(config, logger)
	le.becomeLeader()

	state := NewClusterState("node-1")
	handler := LocalServiceHandlerFunc(func(svc string) (io.ReadWriteCloser, error) {
		return nil, fmt.Errorf("not found")
	})

	fwdConfig := ForwarderConfig{ListenAddr: "127.0.0.1:0", DialTimeout: 2 * time.Second}
	fwd := NewForwarder(fwdConfig, state, handler, logger)
	fwd.ElectionHandler = le.HandleElectionMessage

	go fwd.ListenAndServe()
	defer fwd.Close()

	deadline := time.Now().Add(5 * time.Second)
	for fwd.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("forwarder listener did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Through forwarder: magic byte is consumed by forwarder, so send magic + query.
	conn, err := net.Dial("tcp", fwd.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write([]byte{electionMagic, elecMsgQuery})

	var resp [1]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp[0] != elecMsgLeader {
		t.Fatalf("expected leader announcement, got 0x%02x", resp[0])
	}
}
