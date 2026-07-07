package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
)

func TestPeerDirectoryDiffAddRemoveAddressChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tm := NewTunnelManager(&agentconfig.Config{TLS: agentconfig.TLSConfig{Insecure: true}}, nil, nil)
	connects := make(chan string, 10)
	tm.connectPeerClient = func(ctx context.Context, mc *tunnel.ManagedClient) error {
		connects <- mc.ServerAddr()
		return nil
	}
	tm.connectPeerClientUntil = func(ctx context.Context, mc *tunnel.ManagedClient) {}

	pd := NewPeerDirectory("node-a", tm, nil)

	if err := pd.Replace(ctx, []Peer{{NodeID: "node-b", Addr: "127.0.0.1:4242"}}); err != nil {
		t.Fatalf("Replace add: %v", err)
	}
	if _, ok := tm.DynamicClient("node-b"); !ok {
		t.Fatal("expected dynamic client for added peer")
	}
	if got := <-connects; got != "127.0.0.1:4242" {
		t.Fatalf("connect addr = %q, want 127.0.0.1:4242", got)
	}

	if err := pd.Replace(ctx, []Peer{{NodeID: "node-b", Addr: "127.0.0.1:5252"}}); err != nil {
		t.Fatalf("Replace addr change: %v", err)
	}
	mc, ok := tm.DynamicClient("node-b")
	if !ok {
		t.Fatal("expected dynamic client after addr change")
	}
	if mc.ServerAddr() != "127.0.0.1:5252" {
		t.Fatalf("server addr = %q, want 127.0.0.1:5252", mc.ServerAddr())
	}

	if err := pd.Replace(ctx, nil); err != nil {
		t.Fatalf("Replace remove: %v", err)
	}
	if _, ok := tm.DynamicClient("node-b"); ok {
		t.Fatal("expected dynamic client removed")
	}
}

func TestPeerDirectoryRoleArbitration(t *testing.T) {
	ctx := context.Background()
	tm := NewTunnelManager(&agentconfig.Config{TLS: agentconfig.TLSConfig{Insecure: true}}, nil, nil)
	started := 0
	tm.connectPeerClient = func(ctx context.Context, mc *tunnel.ManagedClient) error {
		started++
		return nil
	}
	tm.connectPeerClientUntil = func(ctx context.Context, mc *tunnel.ManagedClient) {}

	pd := NewPeerDirectory("node-b", tm, nil)
	if err := pd.Replace(ctx, []Peer{
		{NodeID: "node-a", Addr: "127.0.0.1:4242"},
		{NodeID: "node-c", Addr: "127.0.0.1:4243"},
	}); err != nil {
		t.Fatalf("Replace: %v", err)
	}

	if started != 1 {
		t.Fatalf("started clients = %d, want 1", started)
	}
	if _, ok := tm.DynamicClient("node-a"); ok {
		t.Fatal("node-b should not dial lexicographically smaller node-a")
	}
	if _, ok := tm.DynamicClient("node-c"); !ok {
		t.Fatal("node-b should dial lexicographically larger node-c")
	}

	statuses := pd.Statuses()
	for _, st := range statuses {
		if st.NodeID == "node-a" && st.Status != "pending" {
			t.Fatalf("server-role peer status = %q, want pending", st.Status)
		}
	}
}

func TestPeerDirectoryPendingRetryBehavior(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tm := NewTunnelManager(&agentconfig.Config{TLS: agentconfig.TLSConfig{Insecure: true}}, nil, nil)
	tm.connectPeerClient = func(ctx context.Context, mc *tunnel.ManagedClient) error {
		return errors.New("dial failed")
	}
	retry := make(chan string, 1)
	tm.connectPeerClientUntil = func(ctx context.Context, mc *tunnel.ManagedClient) {
		retry <- mc.ServerAddr()
	}

	pd := NewPeerDirectory("node-a", tm, nil)
	if err := pd.Replace(ctx, []Peer{{NodeID: "node-b", Addr: "127.0.0.1:1"}}); err != nil {
		t.Fatalf("Replace should accept pending peer: %v", err)
	}

	select {
	case got := <-retry:
		if got != "127.0.0.1:1" {
			t.Fatalf("retry addr = %q, want 127.0.0.1:1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected pending retry to be scheduled")
	}
}
