package state

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// Ticket 03d: in a healthy cluster (no partition, every member acknowledges
// every push) nodes collect the same tombstones on different maintenance ticks.
// That stagger alone must never force a snapshot resync.

// countingSyncer counts snapshot resync pages pulled through it.
type countingSyncer struct {
	loopSyncer
	snapshots *atomic.Int64
	// pushRefused counts pushes refused with ResyncRequiredError. HandlePush
	// returns that error only from the request-level horizon check, so this is
	// the number of lower-horizon 410s the pusher received.
	pushRefused *atomic.Int64
	strip       func() bool
}

func (s countingSyncer) Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	resp, err := s.loopSyncer.Push(ctx, addr, req)
	var resync *ResyncRequiredError
	if errors.As(err, &resync) {
		s.pushRefused.Add(1)
	}
	if resp != nil && s.strip() {
		resp.NodeID = ""
	}
	return resp, err
}

func (s countingSyncer) PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	if req.SincePosition == 0 && req.StartKey == nil && len(req.Digest) == 0 {
		// Every resync starts with the page at position 0.
		s.snapshots.Add(1)
	}
	return s.loopSyncer.PullSnapshot(ctx, addr, req)
}

type churnNode struct {
	id     string
	store  gcTestStore
	engine *GossipEngine
	peers  *atomicPeerList
}

// gcTestStore is the store surface the churn harness needs from both stores.
type gcTestStore interface {
	Store
	GCHorizon() Timestamp
}

type churnCluster struct {
	nodes   []*churnNode
	clock   *fixedPhysicalClock
	net     *loopNet
	resyncs atomic.Int64
	// pushRefused counts request-level lower-horizon 410s on pushes.
	pushRefused atomic.Int64
	// stripNodeID, when set for a pusher, removes node_id from the push
	// responses it receives, modelling 03b peers that never send it.
	stripNodeID map[string]bool
}

func newChurnCluster(t *testing.T, size int, durable bool) *churnCluster {
	t.Helper()
	cluster := &churnCluster{clock: &fixedPhysicalClock{now: 1_000_000}, net: newLoopNet(), stripNodeID: map[string]bool{}}
	ids := make([]string, size)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	for _, id := range ids {
		dir := ""
		if durable {
			dir = t.TempDir()
		}
		cluster.nodes = append(cluster.nodes, cluster.openNode(t, id, ids, dir))
	}
	t.Cleanup(func() {
		for _, node := range cluster.nodes {
			if closer, ok := node.store.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
	})
	return cluster
}

// openNode opens (or, for a durable dir, reopens) a node and registers its
// engine in the loop network. Every other id is a peer.
func (c *churnCluster) openNode(t *testing.T, id string, ids []string, dir string) *churnNode {
	t.Helper()
	var peers []string
	for _, other := range ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	var store gcTestStore
	if dir != "" {
		store = mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: id, PhysicalClock: c.clock, TombstoneRetention: testRetention})
	} else {
		store = mustGCMemoryStore(t, id, c.clock, testRetention)
	}
	return c.attachNode(t, id, peers, store)
}

// attachNode starts a gossip engine for an existing store with the given peers
// and registers it in the loop network.
func (c *churnCluster) attachNode(t *testing.T, id string, peers []string, store gcTestStore) *churnNode {
	t.Helper()
	peerList := &atomicPeerList{}
	peerList.Store(peers)
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       id,
		Store:        store,
		Syncer:       countingSyncer{loopSyncer: loopSyncer{net: c.net, from: id}, snapshots: &c.resyncs, pushRefused: &c.pushRefused, strip: func() bool { return c.stripNodeID[id] }},
		PeerProvider: PeerProviderFunc(func() []string { return peerList.Load() }),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	c.net.mu.Lock()
	c.net.engines[id] = engine
	c.net.mu.Unlock()
	return &churnNode{id: id, store: store, engine: engine, peers: peerList}
}

func (c *churnCluster) exchange(t *testing.T, node *churnNode) {
	t.Helper()
	for _, peer := range node.engine.peerProvider.Peers() {
		clearPeerBackoffForTest(node.engine, peer)
	}
	if err := node.engine.SyncOnce(context.Background()); err != nil {
		t.Fatalf("%s push: %v", node.id, err)
	}
	if err := node.engine.AntiEntropyOnce(context.Background()); err != nil {
		t.Fatalf("%s anti-entropy: %v", node.id, err)
	}
}

// runTTLChurn drives a TTL workload: every round each node writes fresh keys and
// deletes the keys it wrote ttlRounds earlier, every node exchanges with every
// peer, and node i runs its maintenance (tombstone collection) only on the
// rounds where round % stagger == i, so the nodes collect the same tombstones on
// different ticks. The clock advances so tombstones age past retention.
func runTTLChurn(t *testing.T, cluster *churnCluster, rounds int) (collected int) {
	t.Helper()
	return runTTLChurnRounds(t, cluster, 0, rounds)
}

// runTTLChurnRounds runs rounds [from, to) of the runTTLChurn load, so a test
// can measure a later phase of the same load separately.
func runTTLChurnRounds(t *testing.T, cluster *churnCluster, from, to int) (collected int) {
	t.Helper()
	ctx := context.Background()
	const keysPerRound, ttlRounds = 3, 4
	stagger := len(cluster.nodes) + 1
	for round := from; round < to; round++ {
		cluster.clock.now += retentionMS() / 8
		for index, node := range cluster.nodes {
			for k := 0; k < keysPerRound; k++ {
				if _, err := node.store.Put(ctx, fmt.Sprintf("%s/r%d/k%d", node.id, round, k), []byte("v")); err != nil {
					t.Fatalf("Put: %v", err)
				}
				if round >= ttlRounds {
					if _, err := node.store.Delete(ctx, fmt.Sprintf("%s/r%d/k%d", node.id, round-ttlRounds, k)); err != nil {
						t.Fatalf("Delete: %v", err)
					}
				}
			}
			if round >= ttlRounds {
				// Also expire the neighbour's last key of that round, concurrently
				// with the neighbour deleting it itself.
				neighbour := cluster.nodes[(index+1)%len(cluster.nodes)]
				if _, err := node.store.Delete(ctx, fmt.Sprintf("%s/r%d/k%d", neighbour.id, round-ttlRounds, keysPerRound-1)); err != nil {
					t.Fatalf("Delete: %v", err)
				}
			}
			// Interleave maintenance with exchanges so that a node with a raised
			// horizon talks to peers that have not collected yet.
			if round%stagger == index {
				result, err := node.engine.CollectTombstones(ctx)
				if err != nil {
					t.Fatalf("%s CollectTombstones: %v", node.id, err)
				}
				collected += result.Collected
			}
			cluster.exchange(t, node)
		}
	}
	return collected
}

func assertChurnClusterConverged(t *testing.T, cluster *churnCluster) {
	t.Helper()
	for _, node := range cluster.nodes {
		cluster.exchange(t, node)
	}
	want := liveKeyValues(t, cluster.nodes[0].store)
	for _, node := range cluster.nodes[1:] {
		if got := liveKeyValues(t, node.store); got != want {
			t.Fatalf("live state diverged: %s has %s, %s has %s", cluster.nodes[0].id, want, node.id, got)
		}
		if node.store.GCHorizon().isZero() {
			t.Fatalf("%s never collected anything; the workload did not exercise GC", node.id)
		}
	}
}

// warmUpRounds is long enough for every node of the churn load to age
// tombstones past the retention and collect on its own tick at least once.
const warmUpRounds = 24

// assertFirstCollectionAdoption checks the only resyncs a healthy cluster may
// perform: while a node still has horizon zero it is not vouched by a node that
// collected (03d review C), so the first collector in a fresh cluster resyncs
// each other node once and they adopt its horizon. A horizon never returns to
// zero, so that is at most one resync per other node in the cluster lifetime.
func assertFirstCollectionAdoption(t *testing.T, cluster *churnCluster) int64 {
	t.Helper()
	for _, node := range cluster.nodes {
		if node.store.GCHorizon().isZero() {
			t.Fatalf("%s still has horizon zero after the warm-up", node.id)
		}
	}
	got := cluster.resyncs.Load()
	if limit := int64(len(cluster.nodes) - 1); got > limit {
		t.Fatalf("warm-up performed %d snapshot resyncs, want at most %d (one first-horizon adoption per other node)", got, limit)
	}
	cluster.resyncs.Store(0)
	return got
}

func TestHealthyStaggeredGCNeverForcesResync(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    int
		durable bool
		rounds  int
	}{
		{name: "two-replicas-memory", size: 2, rounds: 600},
		{name: "three-replicas-memory", size: 3, rounds: 600},
		{name: "two-replicas-durable", size: 2, durable: true, rounds: 120},
		{name: "three-replicas-durable", size: 3, durable: true, rounds: 120},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newChurnCluster(t, tc.size, tc.durable)
			collected := runTTLChurnRounds(t, cluster, 0, warmUpRounds)
			adoptions := assertFirstCollectionAdoption(t, cluster)
			collected += runTTLChurnRounds(t, cluster, warmUpRounds, tc.rounds)
			assertChurnClusterConverged(t, cluster)
			if collected == 0 {
				t.Fatalf("no tombstone was collected; the workload did not exercise GC")
			}
			if got := cluster.resyncs.Load(); got != 0 {
				t.Fatalf("healthy cluster with staggered GC performed %d snapshot resyncs after every node had a horizon (collected %d tombstones), want 0", got, collected)
			}
			t.Logf("%s: collected %d tombstones over %d rounds; %d first-horizon adoption resyncs in the first %d rounds, %d resyncs afterwards",
				tc.name, collected, tc.rounds, adoptions, warmUpRounds, cluster.resyncs.Load())
		})
	}
}

// stageLowerHorizon runs a short TTL load and then lets node-0 collect on its
// own, so every other node's horizon is lower than node-0's while all of them
// acknowledged everything node-0 collected.
func stageLowerHorizon(t *testing.T, cluster *churnCluster) {
	t.Helper()
	runTTLChurn(t, cluster, 24)
	ctx := context.Background()
	lead := cluster.nodes[0]
	for _, node := range cluster.nodes {
		if _, err := node.store.Delete(ctx, node.id+"/late"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		cluster.exchange(t, node)
	}
	cluster.clock.now += 2 * retentionMS()
	if result, err := lead.engine.CollectTombstones(ctx); err != nil || result.Collected == 0 {
		t.Fatalf("node-0 GC: %+v err=%v", result, err)
	}
	for _, node := range cluster.nodes[1:] {
		if node.store.GCHorizon().Compare(lead.store.GCHorizon()) >= 0 {
			t.Fatalf("setup: %s horizon %+v must be below node-0's %+v", node.id, node.store.GCHorizon(), lead.store.GCHorizon())
		}
	}
	cluster.resyncs.Store(0)
}

// A durable responder keeps vouching for its acknowledged peers across a
// restart and a compaction: the node ID that answered each peer address is
// persisted with the push progress.
func TestVouchedRequesterSurvivesRestartAndCompaction(t *testing.T) {
	ctx := context.Background()
	cluster := newChurnCluster(t, 2, true)
	stageLowerHorizon(t, cluster)

	lead := cluster.nodes[0]
	durable := lead.store.(*DurableStore)
	dir := durable.dir
	if _, err := durable.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := durable.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cluster.nodes[0] = cluster.openNode(t, lead.id, []string{"node-0", "node-1"}, dir)

	follower := cluster.nodes[1]
	if _, err := follower.store.Put(ctx, "after-restart", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cluster.exchange(t, follower)
	if got := cluster.resyncs.Load(); got != 0 {
		t.Fatalf("restarted responder forced %d resyncs on a peer it had vouched for", got)
	}
	assertStoredValue(t, cluster.nodes[0].store, "after-restart", "x")
}

// A responder never vouches for a requester whose push responses carry no
// node_id (a 03b node): the 03b rule applies and the lower horizon resyncs.
func TestRequesterWithoutNodeIDIsNotVouched(t *testing.T) {
	cluster := &churnCluster{clock: &fixedPhysicalClock{now: 1_000_000}, net: newLoopNet(), stripNodeID: map[string]bool{"node-0": true}}
	ids := []string{"node-0", "node-1"}
	for _, id := range ids {
		cluster.nodes = append(cluster.nodes, cluster.openNode(t, id, ids, ""))
	}
	stageLowerHorizon(t, cluster)

	follower := cluster.nodes[1]
	if _, err := follower.store.Put(context.Background(), "probe", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cluster.exchange(t, follower)
	if got := cluster.resyncs.Load(); got == 0 {
		t.Fatalf("lower-horizon requester without node_id was not refused")
	}
	if follower.store.GCHorizon() != cluster.nodes[0].store.GCHorizon() {
		t.Fatalf("refused requester did not resync to the responder horizon")
	}
}

// exchangeTolerant runs one push and one anti-entropy round and returns the
// errors instead of failing, for rounds with partitioned peers.
func (c *churnCluster) exchangeTolerant(node *churnNode) error {
	for _, peer := range node.engine.peerProvider.Peers() {
		clearPeerBackoffForTest(node.engine, peer)
	}
	ctx := context.Background()
	return errors.Join(node.engine.SyncOnce(ctx), node.engine.AntiEntropyOnce(ctx))
}

// A resync is still forced where it is needed: node-2 is partitioned and dropped
// from the membership for longer than the retention while the others delete and
// collect. After it rejoins it is not vouched for and resyncs, nothing is
// resurrected, and once horizons settle the healthy load runs without resyncs
// again.
func TestPartitionBeyondRetentionStillForcesResync(t *testing.T) {
	ctx := context.Background()
	cluster := newChurnCluster(t, 3, false)
	runTTLChurn(t, cluster, warmUpRounds)
	assertFirstCollectionAdoption(t, cluster)
	a, b, l := cluster.nodes[0], cluster.nodes[1], cluster.nodes[2]
	if _, err := a.store.Put(ctx, "shared", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, node := range cluster.nodes {
		cluster.exchange(t, node)
	}
	assertStoredValue(t, l.store, "shared", "v1")

	cluster.net.setBlocked("node-0", "node-2", true)
	cluster.net.setBlocked("node-1", "node-2", true)
	a.peers.Store([]string{"node-1"})
	b.peers.Store([]string{"node-0"})
	if _, err := a.store.Delete(ctx, "shared"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for round := 0; round < 24; round++ {
		cluster.clock.now += retentionMS() / 8
		for _, node := range []*churnNode{a, b} {
			key := fmt.Sprintf("%s/partition/%d", node.id, round)
			if _, err := node.store.Put(ctx, key, []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if _, err := node.store.Delete(ctx, key); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := node.engine.CollectTombstones(ctx); err != nil {
				t.Fatalf("%s GC: %v", node.id, err)
			}
			cluster.exchange(t, node)
		}
		_ = cluster.exchangeTolerant(l)
	}
	assertAbsent(t, a.store, "shared")
	assertStoredValue(t, l.store, "shared", "v1")

	cluster.net.setBlocked("node-0", "node-2", false)
	cluster.net.setBlocked("node-1", "node-2", false)
	a.peers.Store([]string{"node-1", "node-2"})
	b.peers.Store([]string{"node-0", "node-2"})

	// The rejoin is refused by the request-level lower-horizon check, not only
	// by the digest check or a per-entry rejection: node-2's horizon is non-zero
	// and covers node-0's adopted horizon, so the only reason it is not vouched
	// is that it never acknowledged the tombstones node-0 collected while it was
	// out of the membership. Its first push carries only a fresh key.
	horizonL := l.store.GCHorizon()
	if horizonL.isZero() || horizonL.Compare(a.store.GCHorizon()) >= 0 || horizonL.Compare(a.store.(gcStore).adoptedHorizon()) < 0 {
		t.Fatalf("setup: node-2 horizon %+v must be non-zero, below node-0's %+v and at least its adopted %+v",
			horizonL, a.store.GCHorizon(), a.store.(gcStore).adoptedHorizon())
	}
	if vouched, err := a.engine.requesterVouched(ctx, horizonL, l.id); err != nil || vouched {
		t.Fatalf("node-0 vouches for node-2 after the partition: vouched=%v err=%v", vouched, err)
	}
	if _, err := l.store.Put(ctx, "node-2/rejoin", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cluster.resyncs.Store(0)
	cluster.pushRefused.Store(0)
	for _, peer := range l.engine.peerProvider.Peers() {
		clearPeerBackoffForTest(l.engine, peer)
	}
	if err := l.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-2 rejoin push: %v", err)
	}
	if cluster.pushRefused.Load() == 0 || cluster.resyncs.Load() == 0 {
		t.Fatalf("node-2's rejoin push was not refused with a lower-horizon 410 (refused %d, resyncs %d)",
			cluster.pushRefused.Load(), cluster.resyncs.Load())
	}
	t.Logf("rejoin push: %d lower-horizon 410s, %d resyncs", cluster.pushRefused.Load(), cluster.resyncs.Load())

	var errs error
	for round := 0; round < 4; round++ {
		errs = nil
		for _, node := range cluster.nodes {
			errs = errors.Join(errs, cluster.exchangeTolerant(node))
		}
	}
	if errs != nil {
		t.Fatalf("cluster did not settle after the partition: %v", errs)
	}
	if cluster.resyncs.Load() == 0 {
		t.Fatalf("a node partitioned beyond the retention rejoined without any resync")
	}
	for _, node := range cluster.nodes {
		if entry, ok, err := node.store.Get(ctx, "shared"); err != nil || (ok && !entry.Deleted) {
			t.Fatalf("RESURRECTED at %s: shared=%s err=%v", node.id, entry.Value, err)
		}
	}
	t.Logf("rejoin after a partition beyond retention: %d resyncs", cluster.resyncs.Load())

	cluster.resyncs.Store(0)
	runTTLChurn(t, cluster, 48)
	assertChurnClusterConverged(t, cluster)
	if got := cluster.resyncs.Load(); got != 0 {
		t.Fatalf("healthy load after the partition healed still resynced %d times", got)
	}
}
