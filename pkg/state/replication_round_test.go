package state

import (
	"context"
	"fmt"
	"testing"
)

// Ticket 10-D1: OutboundCaughtUp is an instantaneous "nothing left to push"
// fact. Under sustained writes it is structurally false, because the local
// position also grows with every applied peer entry. OutboundRoundCaughtUp
// judges the latest push round instead: it holds when that round's
// acknowledgement covered the local position read when the round started.

type roundCluster struct {
	clock *fixedPhysicalClock
	net   *loopNet
	nodes []*loopNode
}

func newRoundCluster(t *testing.T, size int) *roundCluster {
	t.Helper()
	c := &roundCluster{clock: &fixedPhysicalClock{now: 1_000_000}, net: newLoopNet()}
	ids := make([]string, size)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		c.nodes = append(c.nodes, newLoopNode(t, c.net, id, c.clock, peers...))
	}
	return c
}

// round runs one staggered push/pull round. With write set, every node writes
// its own heartbeat key right before its exchange, like OutpostLet's
// link-metrics. Peers that are partitioned fail; that is the point.
func (c *roundCluster) round(t *testing.T, round int, write bool) {
	t.Helper()
	c.clock.now += 1_000
	for _, node := range c.nodes {
		if write {
			c.write(t, node, round)
		}
		_ = node.sync(t)
		_ = node.antiEntropy(t)
	}
}

func (c *roundCluster) write(t *testing.T, node *loopNode, round int) {
	t.Helper()
	if _, err := node.store.Put(context.Background(), "heartbeat/"+node.id, []byte(fmt.Sprintf("r%d", round))); err != nil {
		t.Fatalf("%s Put: %v", node.id, err)
	}
}

func statusOf(t *testing.T, node *loopNode) map[string]PeerReplicationStatus {
	t.Helper()
	status, err := node.engine.ReplicationStatus(context.Background())
	if err != nil {
		t.Fatalf("%s ReplicationStatus: %v", node.id, err)
	}
	byPeer := make(map[string]PeerReplicationStatus, len(status.Peers))
	for _, peer := range status.Peers {
		byPeer[peer.Peer] = peer
	}
	return byPeer
}

func TestOutboundRoundCaughtUpHoldsUnderSustainedWrites(t *testing.T) {
	for _, size := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d-replicas", size), func(t *testing.T) {
			c := newRoundCluster(t, size)
			for r := 0; r < 2; r++ {
				c.round(t, r, true)
			}
			for r := 2; r < 8; r++ {
				c.round(t, r, true)
				// Writes keep arriving between rounds.
				for _, node := range c.nodes {
					c.write(t, node, r)
				}
				for _, node := range c.nodes {
					for peer, got := range statusOf(t, node) {
						if got.OutboundCaughtUp {
							t.Fatalf("round %d %s->%s: the instantaneous OutboundCaughtUp holds under sustained writes; the scenario does not exercise the gap: %+v", r, node.id, peer, got)
						}
						if !got.OutboundRoundCaughtUp {
							t.Fatalf("round %d %s->%s: healthy peer under sustained writes not round caught up: %+v", r, node.id, peer, got)
						}
						if got.OutboundRoundTime.IsZero() {
							t.Fatalf("round %d %s->%s: round caught up without a round time: %+v", r, node.id, peer, got)
						}
						if !got.InboundCaughtUp {
							t.Fatalf("round %d %s->%s: healthy peer not inbound caught up: %+v", r, node.id, peer, got)
						}
					}
				}
			}
		})
	}
}

func TestOutboundRoundCaughtUpReflectsPartition(t *testing.T) {
	c := newRoundCluster(t, 3)
	n0, n1, n2 := c.nodes[0], c.nodes[1], c.nodes[2]
	for r := 0; r < 2; r++ {
		c.round(t, r, true)
	}
	behindSince := statusOf(t, n0)[n2.id].OutboundRoundTime
	if behindSince.IsZero() {
		t.Fatalf("setup: %s->%s has no caught-up round before the partition", n0.id, n2.id)
	}
	c.net.setBlocked(n0.id, n2.id, true)
	c.net.setBlocked(n1.id, n2.id, true)
	for r := 2; r < 6; r++ {
		c.round(t, r, true)
		for _, node := range c.nodes {
			c.write(t, node, r)
		}
		for _, node := range []*loopNode{n0, n1} {
			status := statusOf(t, node)
			connected := n1.id
			if node == n1 {
				connected = n0.id
			}
			if got := status[connected]; !got.OutboundRoundCaughtUp || got.OutboundCaughtUp {
				t.Fatalf("round %d %s->%s (connected): want round caught up and not instantaneously caught up: %+v", r, node.id, connected, got)
			}
			if got := status[n2.id]; got.OutboundRoundCaughtUp || got.InboundCaughtUp {
				t.Fatalf("round %d %s->%s (partitioned): must not be caught up: %+v", r, node.id, n2.id, got)
			}
		}
		if got := statusOf(t, n0)[n2.id].OutboundRoundTime; !got.Equal(behindSince) {
			t.Fatalf("round %d: OutboundRoundTime must stay at the last caught-up round before the partition: %v, want %v", r, got, behindSince)
		}
		for peer, got := range statusOf(t, n2) {
			if got.OutboundRoundCaughtUp {
				t.Fatalf("round %d %s->%s (isolated node): must not be round caught up: %+v", r, n2.id, peer, got)
			}
		}
	}

	c.net.setBlocked(n0.id, n2.id, false)
	c.net.setBlocked(n1.id, n2.id, false)
	c.round(t, 6, true)
	for _, node := range c.nodes {
		for peer, got := range statusOf(t, node) {
			if !got.OutboundRoundCaughtUp {
				t.Fatalf("after heal %s->%s: not round caught up: %+v", node.id, peer, got)
			}
		}
	}
}

func TestOutboundRoundCaughtUpWithoutWrites(t *testing.T) {
	c := newRoundCluster(t, 3)
	for r := 0; r < 2; r++ {
		c.round(t, r, true)
	}
	// Nothing changes any more: every push round finds nothing to send.
	for r := 2; r < 4; r++ {
		c.round(t, r, false)
		for _, node := range c.nodes {
			for peer, got := range statusOf(t, node) {
				if !got.OutboundRoundCaughtUp || !got.OutboundCaughtUp {
					t.Fatalf("idle round %d %s->%s: want both outbound facts: %+v", r, node.id, peer, got)
				}
			}
		}
	}
}

// A snapshot resync that purges a value allocates a fresh local position as
// the GC floor without any entry at it (storeCore.finishResync). From then on
// ChangesSince returns nothing past the acknowledged position, so SyncOnce
// skips the push and the acknowledged position never reaches the local
// position: OutboundCaughtUp stays false until the next local write. A push
// round that finds nothing to send still covers everything the store can
// replicate, so it counts as caught up.
func TestOutboundRoundCaughtUpCountsSkippedPushAfterResyncPurge(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock, "node-b", "node-l")
	b := newLoopNode(t, net, "node-b", clock, "node-a")
	l := newLoopNode(t, net, "node-l", clock, "node-a")

	if _, err := a.store.Put(ctx, "doomed", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	assertStoredValue(t, l.store, "doomed", "v1")

	net.setBlocked("node-a", "node-l", true)
	clock.now += 1_000
	if _, err := a.store.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	a.peers.Store([]string{"node-b"})
	if err := a.sync(t); err != nil {
		t.Fatalf("sync tombstone to node-b: %v", err)
	}
	clock.now += 2 * retentionMS()
	if result, err := a.engine.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("node-a GC: %+v err=%v", result, err)
	}
	_ = b

	// node-l comes back holding the collected value and is resynced: the purge
	// moves its local position to a floor no entry carries.
	net.setBlocked("node-a", "node-l", false)
	if err := l.antiEntropy(t); err != nil {
		t.Fatalf("node-l anti-entropy (resync): %v", err)
	}
	assertAbsent(t, l.store, "doomed")

	for round := 0; round < 2; round++ {
		if err := l.sync(t); err != nil {
			t.Fatalf("node-l push round %d: %v", round, err)
		}
		got := statusOf(t, l)["node-a"]
		if got.AcknowledgedPosition >= l.store.CurrentPosition() || got.OutboundCaughtUp {
			t.Fatalf("setup: the purge floor must leave the acknowledged position behind: %+v (current %d)", got, l.store.CurrentPosition())
		}
		if !got.OutboundRoundCaughtUp {
			t.Fatalf("round %d: a push round with nothing to send must count as caught up: %+v", round, got)
		}
	}
}
