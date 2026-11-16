package state_test

import (
	"context"
	"github.com/outpostos/OutpostNet/pkg/state"
	"testing"
)

type permanentRejectSyncer struct{}

func (permanentRejectSyncer) Push(_ context.Context, _ string, req state.PushRequest) (*state.PushResponse, error) {
	return &state.PushResponse{Rejections: []state.PushRejection{{Key: req.Entries[0].Key, Reason: "permanent_policy_rejection"}}}, nil
}
func (permanentRejectSyncer) Pull(context.Context, string, state.PullRequest) ([]state.Entry, error) {
	return nil, nil
}
func TestTimeReferencePermanentRejectionDoesNotPinGC(t *testing.T) {
	ctx := context.Background()
	s, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	g, err := state.NewGossipEngine(state.GossipConfig{NodeID: "source", Store: s, Syncer: permanentRejectSyncer{}, PeerProvider: state.NewStaticPeerProvider([]string{"peer"})})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := g.ReplicationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p := status.Peers[0]; p.LowestRefusedPosition != 0 || p.GCConfirmedPosition != s.CurrentPosition() {
		t.Fatalf("permanent rejection pinned clock refusal gate: %+v", p)
	}
}
