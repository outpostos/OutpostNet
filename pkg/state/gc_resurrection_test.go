package state

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
)

// Regression tests for review 03b F1/H1: a snapshot resync from a peer must not
// re-introduce a key that the resyncing node deleted and collected, no matter
// how the two horizons compare.

// convergeLoopNodes runs push and digest rounds on every node until a round
// finishes without error and changes nothing, and fails otherwise.
func convergeLoopNodes(t *testing.T, nodes ...*loopNode) {
	t.Helper()
	var last string
	for round := 0; round < 8; round++ {
		var errs []error
		for _, node := range nodes {
			if err := node.sync(t); err != nil {
				errs = append(errs, fmt.Errorf("%s sync: %w", node.id, err))
			}
			if err := node.antiEntropy(t); err != nil {
				errs = append(errs, fmt.Errorf("%s anti-entropy: %w", node.id, err))
			}
		}
		state := liveStateFingerprint(t, nodes...)
		if len(errs) == 0 && state == last {
			return
		}
		last = state
		if round == 7 {
			t.Fatalf("cluster did not converge: errs=%v state=%s", errors.Join(errs...), state)
		}
	}
}

// liveStateFingerprint renders every node's live key/value set.
func liveStateFingerprint(t *testing.T, nodes ...*loopNode) string {
	t.Helper()
	var out string
	for _, node := range nodes {
		out += node.id + "{" + liveKeyValues(t, node.store) + "}"
	}
	return out
}

func liveKeyValues(t *testing.T, store Store) string {
	t.Helper()
	entries, err := store.ListEntries(context.Background())
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	var pairs []string
	for _, entry := range entries {
		if !entry.Deleted {
			pairs = append(pairs, entry.Key+"="+string(entry.Value))
		}
	}
	sort.Strings(pairs)
	return fmt.Sprint(pairs)
}

func assertConvergedLiveState(t *testing.T, nodes ...*loopNode) {
	t.Helper()
	want := liveKeyValues(t, nodes[0].store)
	for _, node := range nodes[1:] {
		if got := liveKeyValues(t, node.store); got != want {
			t.Fatalf("live state diverged: %s has %s, %s has %s", nodes[0].id, want, node.id, got)
		}
	}
}

func assertNotResurrected(t *testing.T, key string, nodes ...*loopNode) {
	t.Helper()
	for _, node := range nodes {
		if entry, ok, err := node.store.Get(context.Background(), key); err != nil || (ok && !entry.Deleted) {
			t.Fatalf("RESURRECTED at %s: %s=%s HLC %d (horizon %d) err=%v",
				node.id, key, entry.Value, entry.Version.HLC.WallMS, node.store.GCHorizon().WallMS, err)
		}
	}
}

// Std H1: node-l is outside node-a's membership while node-a collects "doomed";
// node-l keeps doomed=v1 and collects a key of its own, so HL < HA. node-a's
// push is refused by node-l (beyond_gc_horizon) and node-a resyncs from node-l.
func TestResyncFromLowerHorizonPeerDoesNotResurrectCollectedKey(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock, "node-l")
	b := newLoopNode(t, net, "node-b", clock, "node-a")
	l := newLoopNode(t, net, "node-l", clock, "node-c")
	c := newLoopNode(t, net, "node-c", clock, "node-l")

	if _, err := a.store.Put(ctx, "doomed", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	assertStoredValue(t, l.store, "doomed", "v1")

	// node-l leaves node-a's membership and is partitioned from node-a.
	net.setBlocked("node-a", "node-l", true)
	a.peers.Store([]string{"node-b"})
	clock.now += 50
	aOld, err := a.store.Put(ctx, "a-old", []byte("x"))
	if err != nil {
		t.Fatalf("Put a-old: %v", err)
	}
	clock.now += 50
	if _, err := l.store.Put(ctx, "l-gone", []byte("y")); err != nil {
		t.Fatalf("Put l-gone: %v", err)
	}
	if _, err := l.store.Delete(ctx, "l-gone"); err != nil {
		t.Fatalf("Delete l-gone: %v", err)
	}
	if err := l.sync(t); err != nil {
		t.Fatalf("node-l sync to node-c: %v", err)
	}
	clock.now = 1_002_000
	if _, err := a.store.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete doomed: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("node-a sync to node-b: %v", err)
	}

	clock.now += 2 * retentionMS()
	if result, err := a.engine.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("node-a GC: %+v err=%v", result, err)
	}
	if result, err := l.engine.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("node-l GC: %+v err=%v", result, err)
	}
	ha, hl := a.store.GCHorizon(), l.store.GCHorizon()
	if hl.Compare(ha) >= 0 || aOld.Version.HLC.Compare(hl) > 0 {
		t.Fatalf("setup: want a-old <= HL < HA, got a-old=%+v HL=%+v HA=%+v", aOld.Version.HLC, hl, ha)
	}

	// node-l rejoins. node-a's push of a-old is refused by node-l, so node-a
	// resyncs from node-l, whose snapshot still carries doomed=v1.
	net.setBlocked("node-a", "node-l", false)
	a.peers.Store([]string{"node-b", "node-l"})
	if err := a.sync(t); err != nil {
		t.Fatalf("node-a sync with resync from node-l: %v", err)
	}
	assertNotResurrected(t, "doomed", a)

	l.peers.Store([]string{"node-c", "node-a"})
	convergeLoopNodes(t, a, b, l, c)
	assertNotResurrected(t, "doomed", a, b, l, c)
	assertConvergedLiveState(t, a, b, l, c)
}

type divergentGCCluster struct {
	a, b, l, c *loopNode
}

// newDivergentGCCluster builds Spec F1: node-a and node-b delete and collect
// "doomed" while node-l is partitioned and keeps doomed=v1; node-l collects a
// later key of its own, so its horizon overtakes theirs (HL > HA = HB).
// node-l already pushed doomed=v1 to node-a before the partition, so its push
// cursor no longer carries it.
func newDivergentGCCluster(t *testing.T) *divergentGCCluster {
	t.Helper()
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock, "node-l", "node-b")
	b := newLoopNode(t, net, "node-b", clock, "node-a")
	l := newLoopNode(t, net, "node-l", clock, "node-a", "node-c")
	c := newLoopNode(t, net, "node-c", clock, "node-l")

	if _, err := a.store.Put(ctx, "doomed", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("initial node-a sync: %v", err)
	}
	if err := l.sync(t); err != nil {
		t.Fatalf("initial node-l sync: %v", err)
	}
	assertStoredValue(t, c.store, "doomed", "v1")

	// Partition: node-l (and node-c behind it) are cut off from node-a/node-b,
	// and each side's PeerProvider only lists the reachable side.
	net.setBlocked("node-a", "node-l", true)
	net.setBlocked("node-b", "node-l", true)
	a.peers.Store([]string{"node-b"})
	l.peers.Store([]string{"node-c"})

	clock.now = 1_001_000
	if _, err := a.store.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete doomed: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("node-a sync to node-b: %v", err)
	}
	if err := b.sync(t); err != nil {
		t.Fatalf("node-b sync to node-a: %v", err)
	}
	clock.now = 1_005_000
	if _, err := l.store.Put(ctx, "l-gone", []byte("y")); err != nil {
		t.Fatalf("Put l-gone: %v", err)
	}
	if _, err := l.store.Delete(ctx, "l-gone"); err != nil {
		t.Fatalf("Delete l-gone: %v", err)
	}
	if err := l.sync(t); err != nil {
		t.Fatalf("node-l sync to node-c: %v", err)
	}

	clock.now += 2 * retentionMS()
	for _, node := range []*loopNode{a, b, l} {
		if result, err := node.engine.CollectTombstones(ctx); err != nil || result.Collected != 1 {
			t.Fatalf("%s GC: %+v err=%v", node.id, result, err)
		}
	}
	ha, hb, hl := a.store.GCHorizon(), b.store.GCHorizon(), l.store.GCHorizon()
	if ha != hb || hl.Compare(ha) <= 0 {
		t.Fatalf("setup: want HL > HA = HB, got HL=%+v HA=%+v HB=%+v", hl, ha, hb)
	}

	// Heal. node-a writes something new and pushes it to node-l, which answers
	// 410 because its horizon is higher; node-a resyncs from node-l.
	net.setBlocked("node-a", "node-l", false)
	net.setBlocked("node-b", "node-l", false)
	a.peers.Store([]string{"node-b", "node-l"})
	l.peers.Store([]string{"node-c", "node-a"})
	if _, err := a.store.Put(ctx, "a-new", []byte("z")); err != nil {
		t.Fatalf("Put a-new: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("node-a sync with 410 resync from node-l: %v", err)
	}
	if a.store.GCHorizon() != hl {
		t.Fatalf("node-a did not resync from node-l: horizon %+v, want %+v", a.store.GCHorizon(), hl)
	}
	return &divergentGCCluster{a: a, b: b, l: l, c: c}
}

// Spec F1: both sides collected, node-l's horizon overtook node-a's, node-a's
// push gets 410 and node-a resyncs from node-l.
func TestResyncFromHigherHorizonPeerDoesNotResurrectCollectedKey(t *testing.T) {
	cluster := newDivergentGCCluster(t)
	assertNotResurrected(t, "doomed", cluster.a)

	convergeLoopNodes(t, cluster.a, cluster.b, cluster.l, cluster.c)
	assertNotResurrected(t, "doomed", cluster.a, cluster.b, cluster.l, cluster.c)
	assertConvergedLiveState(t, cluster.a, cluster.b, cluster.l, cluster.c)
	assertStoredValue(t, cluster.l.store, "a-new", "z")
}

// Spec F1 step 3: node-b, which also collected doomed, must not pick it up when
// it resyncs from node-a after node-a's horizon moved past its own.
func TestThirdPeerResyncingFromNodeIsNotInfected(t *testing.T) {
	cluster := newDivergentGCCluster(t)
	if cluster.b.store.GCHorizon().Compare(cluster.a.store.GCHorizon()) >= 0 {
		t.Fatalf("setup: node-b must lag node-a's horizon")
	}
	if err := cluster.b.sync(t); err != nil {
		t.Fatalf("node-b sync (410 resync from node-a): %v", err)
	}
	if cluster.b.store.GCHorizon() != cluster.a.store.GCHorizon() {
		t.Fatalf("node-b did not resync from node-a")
	}
	assertNotResurrected(t, "doomed", cluster.b)

	convergeLoopNodes(t, cluster.a, cluster.b, cluster.l, cluster.c)
	assertNotResurrected(t, "doomed", cluster.a, cluster.b, cluster.l, cluster.c)
	assertConvergedLiveState(t, cluster.a, cluster.b, cluster.l, cluster.c)
}
