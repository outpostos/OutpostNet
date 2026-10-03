package state

import (
	"context"
	"errors"
	"testing"
)

// Ticket 03d review fixes: the responder's vouching must never be weaker than
// what docs/state-tombstone-gc.md "Requester horizon check" claims.

func newEmptyChurnCluster() *churnCluster {
	return &churnCluster{clock: &fixedPhysicalClock{now: 1_000_000}, net: newLoopNet(), stripNodeID: map[string]bool{}}
}

func pushProbe(engine *GossipEngine, requesterID string, horizon Timestamp) error {
	_, err := engine.HandlePush(context.Background(), PushRequest{
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        engine.policy.ID(),
		SourceNodeID:    requesterID,
		GCHorizon:       timestampPtr(horizon),
	})
	return err
}

// A resync that purges entries records the peer horizon as adopted even when it
// does not raise the local horizon. R (horizon H_R) is refused by P (horizon
// H_P < H_R) for a value P deleted and collected, resyncs and drops it. That
// removal was P's, not R's: a requester that still holds the value and whose
// horizon is below H_P must not be vouched by R, even after it acknowledged
// every later push of R.
func TestResyncPurgeWithoutHorizonChangeIsNotVouched(t *testing.T) {
	ctx := context.Background()
	c := newEmptyChurnCluster()
	r := c.openNode(t, "node-r", []string{"node-r", "node-q"}, "")
	q := c.openNode(t, "node-q", []string{"node-r", "node-q"}, "")

	// Q collects an early tombstone of its own: its horizon is non-zero and the
	// lowest of the three.
	if _, err := q.store.Put(ctx, "q/early", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := q.store.Delete(ctx, "q/early"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	c.exchange(t, q)
	c.clock.now += 2 * retentionMS()
	if result, err := q.engine.CollectTombstones(ctx); err != nil || result.Collected == 0 {
		t.Fatalf("node-q GC: %+v err=%v", result, err)
	}
	horizonQ := q.store.GCHorizon()
	// R still has horizon zero facing Q's floor: its next push is refused and it
	// adopts H_Q through a resync (03d review C). From here on H_Q >= A_R.
	c.exchange(t, r)
	if r.store.GCHorizon() != horizonQ {
		t.Fatalf("setup: node-r did not adopt node-q's horizon: %+v vs %+v", r.store.GCHorizon(), horizonQ)
	}

	// R writes k, replicated to Q.
	if _, err := r.store.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := r.store.Put(ctx, "d", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	c.exchange(t, r)
	assertStoredValue(t, q.store, "k", "v")
	stale := mustGet(t, r.store, "k")

	// P, outside R's membership, received k, deleted it and collected it.
	pStore := mustGCMemoryStore(t, "node-p", c.clock, testRetention)
	if applied, err := pStore.Apply(ctx, stale); !applied || err != nil {
		t.Fatalf("node-p Apply: applied=%v err=%v", applied, err)
	}
	c.clock.now += 1000
	if _, err := pStore.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// R deletes and collects a later key, so its horizon ends above P's.
	c.clock.now += 1000
	if _, err := r.store.Delete(ctx, "d"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	c.exchange(t, r)
	c.clock.now += 2 * retentionMS()
	if result, err := pStore.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("node-p GC: %+v err=%v", result, err)
	}
	if result, err := r.engine.CollectTombstones(ctx); err != nil || result.Collected == 0 {
		t.Fatalf("node-r GC: %+v err=%v", result, err)
	}
	horizonP, horizonR := pStore.GCHorizon(), r.store.GCHorizon()
	if !(horizonQ.Compare(horizonP) < 0 && horizonP.Compare(horizonR) < 0 && stale.Version.HLC.Compare(horizonP) <= 0) {
		t.Fatalf("setup: want H_Q %+v < H_P %+v < H_R %+v and k at %+v <= H_P", horizonQ, horizonP, horizonR, stale.Version.HLC)
	}
	c.attachNode(t, "node-p", []string{"node-r"}, pStore)

	// R pushes k to P, P refuses it as beyond_gc_horizon, R resyncs from P and
	// purges k. Its own horizon does not move.
	c.resyncs.Store(0)
	r.peers.Store([]string{"node-q", "node-p"})
	clearPeerBackoffForTest(r.engine, "node-p")
	if err := r.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-r push: %v", err)
	}
	if got := c.resyncs.Load(); got != 1 {
		t.Fatalf("setup: node-r resynced %d times from node-p, want 1", got)
	}
	assertAbsent(t, r.store, "k")
	if r.store.GCHorizon() != horizonR {
		t.Fatalf("setup: the resync raised node-r's horizon to %+v; the scenario needs it unchanged", r.store.GCHorizon())
	}

	// Q acknowledges a fresh push of R, so its push progress reaches R's floor
	// again, while it still holds the value R purged on P's behalf.
	if _, err := r.store.Put(ctx, "after", []byte("y")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := r.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-r push: %v", err)
	}
	assertStoredValue(t, q.store, "after", "y")
	assertStoredValue(t, q.store, "k", "v")

	var resync *ResyncRequiredError
	if err := pushProbe(r.engine, "node-q", horizonQ); !errors.As(err, &resync) {
		t.Fatalf("node-r vouched for node-q (horizon %+v below the adopted %+v) although node-q holds the value node-r purged for node-p: err=%v", horizonQ, horizonP, err)
	}
}

// A node whose data directory was wiped and that comes back with the same node
// ID at the same address has horizon zero. Its push progress at the responder
// still covers the responder's floor, but that progress was acknowledged by the
// state that was wiped. A zero horizon facing a non-zero floor is not vouched:
// the very first exchange is refused with 410 and the node adopts the
// responder's horizon instead of accepting stale values for collected keys.
func TestWipedRequesterWithSameIdentityIsRefusedOnFirstExchange(t *testing.T) {
	ctx := context.Background()
	cluster := newChurnCluster(t, 2, true)
	runTTLChurn(t, cluster, 24)
	lead, wiped := cluster.nodes[0], cluster.nodes[1]
	if lead.store.(gcStore).GCFloor() == 0 {
		t.Fatalf("setup: node-0 never collected")
	}
	if err := wiped.store.(*DurableStore).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cluster.nodes[1] = cluster.openNode(t, wiped.id, []string{"node-0", "node-1"}, t.TempDir())
	wiped = cluster.nodes[1]
	if !wiped.store.GCHorizon().isZero() {
		t.Fatalf("setup: wiped node has horizon %+v", wiped.store.GCHorizon())
	}

	var resync *ResyncRequiredError
	if err := pushProbe(lead.engine, wiped.id, Timestamp{}); !errors.As(err, &resync) {
		t.Fatalf("node-0 vouched for a requester with horizon zero although its floor is %d: err=%v", lead.store.(gcStore).GCFloor(), err)
	}

	cluster.resyncs.Store(0)
	if _, err := wiped.store.Put(ctx, "node-1/fresh", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	clearPeerBackoffForTest(wiped.engine, lead.id)
	if err := wiped.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-1 push: %v", err)
	}
	if got := cluster.resyncs.Load(); got != 1 {
		t.Fatalf("wiped node resynced %d times on its first push, want 1", got)
	}
	if wiped.store.GCHorizon() != lead.store.GCHorizon() {
		t.Fatalf("wiped node did not adopt node-0's horizon: %+v vs %+v", wiped.store.GCHorizon(), lead.store.GCHorizon())
	}
}
