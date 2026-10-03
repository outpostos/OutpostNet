package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const testRetention = time.Hour

func retentionMS() int64 { return testRetention.Milliseconds() }

func mustGCMemoryStore(t *testing.T, nodeID string, clock PhysicalClock, retention time.Duration) *MemoryStore {
	t.Helper()
	store, err := NewMemoryStore(MemoryStoreConfig{NodeID: nodeID, PhysicalClock: clock, TombstoneRetention: retention})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	return store
}

func assertAbsent(t *testing.T, store Store, key string) {
	t.Helper()
	if entry, ok, err := store.Get(context.Background(), key); err != nil || ok {
		t.Fatalf("expected %q absent, got %+v ok=%v err=%v", key, entry, ok, err)
	}
}

func TestLocalStoreCollectsTombstonesOnlyAfterRetention(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}

	disabled := mustGCMemoryStore(t, "node-a", clock, 0)
	if _, err := disabled.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	clock.now += 10 * retentionMS()
	if _, err := disabled.CollectTombstones(ctx); !errors.Is(err, ErrTombstoneGCDisabled) {
		t.Fatalf("expected GC to be disabled by default, got %v", err)
	}
	if entry := mustGet(t, disabled, "k"); !entry.Deleted {
		t.Fatalf("default store dropped its tombstone: %+v", entry)
	}

	clock.now = 1_000_000
	store := mustGCMemoryStore(t, "node-a", clock, testRetention)
	old, err := store.Put(ctx, "gone", []byte("old"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	tombstone, err := store.Delete(ctx, "gone")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Put(ctx, "live", []byte("v")); err != nil {
		t.Fatalf("Put live: %v", err)
	}

	clock.now += retentionMS() - 1
	result, err := store.CollectTombstones(ctx)
	if err != nil {
		t.Fatalf("CollectTombstones before retention: %v", err)
	}
	if result.Collected != 0 {
		t.Fatalf("collected a tombstone younger than retention: %+v", result)
	}
	if entry := mustGet(t, store, "gone"); !entry.Deleted {
		t.Fatalf("young tombstone missing: %+v", entry)
	}

	clock.now += 1
	result, err = store.CollectTombstones(ctx)
	if err != nil {
		t.Fatalf("CollectTombstones: %v", err)
	}
	if result.Collected != 1 || result.Horizon != tombstone.Version.HLC || result.Floor != tombstone.Position {
		t.Fatalf("unexpected GC result %+v (tombstone %+v)", result, tombstone)
	}
	assertAbsent(t, store, "gone")
	assertStoredValue(t, store, "live", "v")
	if store.GCHorizon() != tombstone.Version.HLC {
		t.Fatalf("horizon %+v, want %+v", store.GCHorizon(), tombstone.Version.HLC)
	}

	// The deleted value can never come back through replication.
	applied, err := store.Apply(ctx, old)
	var beyond *BeyondGCHorizonError
	if applied || !errors.As(err, &beyond) || beyond.Key != "gone" {
		t.Fatalf("stale value was not refused at the GC horizon: applied=%v err=%v", applied, err)
	}
	assertAbsent(t, store, "gone")
	// An old tombstone for an absent key is a harmless no-op.
	if applied, err := store.Apply(ctx, tombstone); applied || err != nil {
		t.Fatalf("old tombstone should be ignored: applied=%v err=%v", applied, err)
	}
	assertAbsent(t, store, "gone")

	// A newer write for the collected key is ordinary state.
	fresh := newEntryForTest("gone", []byte("new"), "node-b", 99, Timestamp{WallMS: clock.now})
	if applied, err := store.Apply(ctx, fresh); !applied || err != nil {
		t.Fatalf("fresh write after GC refused: applied=%v err=%v", applied, err)
	}
	assertStoredValue(t, store, "gone", "new")

	// A consumer that had not yet consumed the collected tombstone must snapshot.
	if status := store.CheckConsumerPosition(old.Position); status.CanResume || !status.NeedsSnapshot {
		t.Fatalf("position before GC floor must need a snapshot: %+v", status)
	}
	if status := store.CheckConsumerPosition(tombstone.Position); !status.CanResume {
		t.Fatalf("position at GC floor should resume: %+v", status)
	}
}

// loopNet routes in-process gossip between engines with a directional blocklist.
type loopNet struct {
	mu      sync.Mutex
	engines map[string]*GossipEngine
	blocked map[string]bool
}

func newLoopNet() *loopNet {
	return &loopNet{engines: make(map[string]*GossipEngine), blocked: make(map[string]bool)}
}

func (n *loopNet) setBlocked(a, b string, blocked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[a+">"+b] = blocked
	n.blocked[b+">"+a] = blocked
}

func (n *loopNet) target(from, to string) (*GossipEngine, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.blocked[from+">"+to] {
		return nil, fmt.Errorf("loopnet: %s -> %s partitioned", from, to)
	}
	engine := n.engines[to]
	if engine == nil {
		return nil, fmt.Errorf("loopnet: unknown peer %s", to)
	}
	return engine, nil
}

type loopSyncer struct {
	net  *loopNet
	from string
}

func (s loopSyncer) Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	engine, err := s.net.target(s.from, addr)
	if err != nil {
		return nil, err
	}
	return engine.HandlePush(ctx, req)
}

func (s loopSyncer) Pull(ctx context.Context, addr string, req PullRequest) ([]Entry, error) {
	engine, err := s.net.target(s.from, addr)
	if err != nil {
		return nil, err
	}
	return engine.HandlePull(ctx, req)
}

func (s loopSyncer) PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	engine, err := s.net.target(s.from, addr)
	if err != nil {
		return nil, err
	}
	return engine.HandlePullResponse(ctx, req)
}

type loopNode struct {
	id     string
	store  *MemoryStore
	engine *GossipEngine
	peers  atomicPeerList
}

func newLoopNode(t *testing.T, net *loopNet, id string, clock PhysicalClock, peers ...string) *loopNode {
	t.Helper()
	node := &loopNode{id: id, store: mustGCMemoryStore(t, id, clock, testRetention)}
	node.peers.Store(peers)
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       id,
		Store:        node.store,
		Syncer:       loopSyncer{net: net, from: id},
		PeerProvider: PeerProviderFunc(func() []string { return node.peers.Load() }),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	node.engine = engine
	net.mu.Lock()
	net.engines[id] = engine
	net.mu.Unlock()
	return node
}

func (n *loopNode) sync(t *testing.T) error {
	t.Helper()
	for _, peer := range n.peers.Load() {
		clearPeerBackoffForTest(n.engine, peer)
	}
	return n.engine.SyncOnce(context.Background())
}

func (n *loopNode) antiEntropy(t *testing.T) error {
	t.Helper()
	for _, peer := range n.peers.Load() {
		clearPeerBackoffForTest(n.engine, peer)
	}
	return n.engine.AntiEntropyOnce(context.Background())
}

func TestReplicatedGCWaitsUntilEveryPeerAcknowledged(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock, "node-b", "node-c")
	newLoopNode(t, net, "node-b", clock)
	c := newLoopNode(t, net, "node-c", clock)

	if _, err := a.store.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	if _, err := a.store.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	net.setBlocked("node-a", "node-c", true)
	if err := a.sync(t); err == nil {
		t.Fatalf("expected push to partitioned node-c to fail")
	}
	clock.now += 2 * retentionMS()

	if _, err := a.store.CollectTombstones(ctx); !errors.Is(err, ErrTombstoneGCReplicated) {
		t.Fatalf("replicated store must refuse peer-blind GC, got %v", err)
	}
	result, err := a.engine.CollectTombstones(ctx)
	if err != nil {
		t.Fatalf("engine CollectTombstones: %v", err)
	}
	if result.Collected != 0 || result.Blocked != 1 {
		t.Fatalf("collected before node-c acknowledged: %+v", result)
	}
	if entry := mustGet(t, a.store, "k"); !entry.Deleted {
		t.Fatalf("tombstone missing while node-c lags: %+v", entry)
	}

	net.setBlocked("node-a", "node-c", false)
	if err := a.sync(t); err != nil {
		t.Fatalf("sync after heal: %v", err)
	}
	if entry := mustGet(t, c.store, "k"); !entry.Deleted {
		t.Fatalf("node-c did not receive tombstone: %+v", entry)
	}
	result, err = a.engine.CollectTombstones(ctx)
	if err != nil {
		t.Fatalf("engine CollectTombstones: %v", err)
	}
	if result.Collected != 1 || result.Blocked != 0 {
		t.Fatalf("expected collection after every peer acknowledged: %+v", result)
	}
	assertAbsent(t, a.store, "k")
}

func TestLaggingPeerIsForcedToResyncAndCannotResurrect(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock, "node-b", "node-l")
	b := newLoopNode(t, net, "node-b", clock, "node-a")
	l := newLoopNode(t, net, "node-l", clock, "node-a")

	if _, err := a.store.Put(ctx, "doomed", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := a.store.Put(ctx, "kept", []byte("k1")); err != nil {
		t.Fatalf("Put kept: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	assertStoredValue(t, l.store, "doomed", "v1")
	// node-l has already pushed nothing and acknowledged everything; its push
	// cursor towards node-a is current, so only the digest path can reach it.

	// node-l is partitioned and later dropped from node-a's membership.
	net.setBlocked("node-a", "node-l", true)
	net.setBlocked("node-b", "node-l", true)
	// node-l writes something of its own while partitioned, before the horizon.
	lostWrite, err := l.store.Put(ctx, "lagging-own", []byte("old-local"))
	if err != nil {
		t.Fatalf("Put on node-l: %v", err)
	}
	clock.now += 1_000
	tombstone, err := a.store.Delete(ctx, "doomed")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	a.peers.Store([]string{"node-b"})
	if err := a.sync(t); err != nil {
		t.Fatalf("sync tombstone to node-b: %v", err)
	}

	clock.now += 2 * retentionMS()
	result, err := a.engine.CollectTombstones(ctx)
	if err != nil || result.Collected != 1 {
		t.Fatalf("node-a GC: %+v err=%v", result, err)
	}
	// node-b still has a zero horizon, so node-a refuses its next push and
	// node-b adopts the horizon through a snapshot resync, dropping the tombstone.
	if err := b.sync(t); err != nil {
		t.Fatalf("node-b sync: %v", err)
	}
	if b.store.GCHorizon() != a.store.GCHorizon() {
		t.Fatalf("node-b did not adopt node-a's horizon: %+v vs %+v", b.store.GCHorizon(), a.store.GCHorizon())
	}
	assertAbsent(t, a.store, "doomed")
	assertAbsent(t, b.store, "doomed")

	// node-l comes back with the stale value and a fresh write of its own.
	net.setBlocked("node-a", "node-l", false)
	if _, err := l.store.Put(ctx, "fresh-own", []byte("new-local")); err != nil {
		t.Fatalf("Put fresh on node-l: %v", err)
	}

	// node-a pulling from the lagging node refuses the stale value without
	// treating it as a local failure.
	a.peers.Store([]string{"node-b", "node-l"})
	if err := a.antiEntropy(t); err != nil {
		t.Fatalf("node-a anti-entropy against lagging peer: %v", err)
	}
	assertAbsent(t, a.store, "doomed")
	assertStoredValue(t, a.store, "fresh-own", "new-local")

	// node-l's own exchange is refused and turned into a snapshot resync.
	if err := l.antiEntropy(t); err != nil {
		t.Fatalf("node-l anti-entropy (resync): %v", err)
	}
	assertAbsent(t, l.store, "doomed")
	assertAbsent(t, l.store, "lagging-own")
	assertStoredValue(t, l.store, "fresh-own", "new-local")
	assertStoredValue(t, l.store, "kept", "k1")
	if l.store.GCHorizon().Compare(tombstone.Version.HLC) < 0 {
		t.Fatalf("node-l did not adopt the GC horizon: %+v < %+v", l.store.GCHorizon(), tombstone.Version.HLC)
	}
	// After the resync, the purged lagging write cannot be re-applied either.
	if applied, err := l.store.Apply(ctx, lostWrite); applied || err == nil {
		t.Fatalf("purged lagging write re-applied: applied=%v err=%v", applied, err)
	}

	// Normal exchange resumes and nothing is resurrected anywhere.
	if err := l.sync(t); err != nil {
		t.Fatalf("node-l sync after resync: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("node-a sync after resync: %v", err)
	}
	for _, node := range []*loopNode{a, b, l} {
		assertAbsent(t, node.store, "doomed")
	}
}

func TestLaggingPusherIsRefusedWithResyncRequired(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock, "node-b")
	newLoopNode(t, net, "node-b", clock)
	l := newLoopNode(t, net, "node-l", clock, "node-a")

	stale, err := l.store.Put(ctx, "doomed", []byte("stale"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// node-a saw and deleted the key long ago and collected the tombstone once
	// its only listed peer, node-b, acknowledged it.
	if _, err := a.store.Apply(ctx, stale); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	clock.now += 1_000
	if _, err := a.store.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := a.sync(t); err != nil {
		t.Fatalf("node-a sync to node-b: %v", err)
	}
	clock.now += 2 * retentionMS()
	if result, err := a.engine.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("GC: %+v err=%v", result, err)
	}

	// Request-level refusal: the pusher has a lower horizon.
	_, err = a.engine.HandlePush(ctx, PushRequest{
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        a.store.Policy().ID(),
		SourceNodeID:    "node-l",
		Entries:         []Entry{stale},
	})
	var resync *ResyncRequiredError
	if !errors.As(err, &resync) || resync.Horizon != a.store.GCHorizon() {
		t.Fatalf("expected ResyncRequiredError with horizon, got %v", err)
	}
	assertAbsent(t, a.store, "doomed")

	// Entry-level refusal: the pusher claims a horizon as high but still sends a
	// value for a collected key.
	horizon := a.store.GCHorizon()
	resp, err := a.engine.HandlePush(ctx, PushRequest{
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        a.store.Policy().ID(),
		SourceNodeID:    "node-l",
		GCHorizon:       &horizon,
		Entries:         []Entry{stale},
	})
	if err != nil {
		t.Fatalf("HandlePush: %v", err)
	}
	if resp.Accepted != 0 || len(resp.Rejections) != 1 || resp.Rejections[0].Reason != RejectionReasonBeyondGCHorizon || resp.Rejections[0].Transient {
		t.Fatalf("expected a non-transient beyond_gc_horizon rejection, got %+v", resp)
	}
	if resp.GCHorizon == nil || *resp.GCHorizon != horizon {
		t.Fatalf("push response must carry receiver horizon, got %+v", resp.GCHorizon)
	}
	assertAbsent(t, a.store, "doomed")

	// The real push path turns the refusal into a resync and then converges.
	if err := l.sync(t); err != nil {
		t.Fatalf("node-l sync with resync: %v", err)
	}
	assertAbsent(t, l.store, "doomed")
	assertAbsent(t, a.store, "doomed")
	if err := l.sync(t); err != nil {
		t.Fatalf("node-l second sync: %v", err)
	}
}

func TestDurableStorePersistsGCHorizonFloorAndCounters(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &fixedPhysicalClock{now: 1_000_000}
	store := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a", PhysicalClock: clock, TombstoneRetention: testRetention})
	old, err := store.Put(ctx, "gone", []byte("old"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	tombstone, err := store.Delete(ctx, "gone")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	clock.now += 2 * retentionMS()
	if result, err := store.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("CollectTombstones: %+v err=%v", result, err)
	}
	position := store.CurrentPosition()
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The clock moves backwards across the restart.
	clock.now = 1_000_000
	recovered := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a", PhysicalClock: clock, TombstoneRetention: testRetention})
	defer recovered.Close()
	assertAbsent(t, recovered, "gone")
	if recovered.GCHorizon() != tombstone.Version.HLC || recovered.GCFloor() != tombstone.Position {
		t.Fatalf("GC state not recovered: horizon=%+v floor=%d", recovered.GCHorizon(), recovered.GCFloor())
	}
	if recovered.CurrentPosition() < position {
		t.Fatalf("position regressed: %d < %d", recovered.CurrentPosition(), position)
	}
	if applied, err := recovered.Apply(ctx, old); applied || err == nil {
		t.Fatalf("stale value resurrected after restart: applied=%v err=%v", applied, err)
	}
	next, err := recovered.Put(ctx, "after", []byte("x"))
	if err != nil {
		t.Fatalf("Put after restart: %v", err)
	}
	if next.ChangeID.Counter <= tombstone.ChangeID.Counter || next.Version.HLC.Compare(tombstone.Version.HLC) <= 0 {
		t.Fatalf("counters regressed after GC+restart: %+v vs tombstone %+v", next, tombstone)
	}
	if status := recovered.CheckConsumerPosition(old.Position); !status.NeedsSnapshot {
		t.Fatalf("consumer below recovered floor must snapshot: %+v", status)
	}
}

func TestHTTPResyncRequiredIs410AndSnapshotPagesOverHTTP(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	const token = "secret"

	serverStore := mustGCMemoryStore(t, "node-a", clock, testRetention)
	clientStore := mustGCMemoryStore(t, "node-l", clock, testRetention)
	stale, err := clientStore.Put(ctx, "doomed", []byte("stale"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := serverStore.Apply(ctx, stale); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	const live = 2*resyncPageSize + 7
	for i := 0; i < live; i++ {
		if _, err := serverStore.Put(ctx, fmt.Sprintf("live/%04d", i), []byte("v")); err != nil {
			t.Fatalf("Put live: %v", err)
		}
	}
	if _, err := serverStore.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	clock.now += 2 * retentionMS()
	// node-a collects as a local store before it starts replicating.
	if result, err := serverStore.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("GC: %+v err=%v", result, err)
	}
	serverSyncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-a", AuthToken: token})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	serverEngine, err := NewGossipEngine(GossipConfig{NodeID: "node-a", Store: serverStore, Syncer: serverSyncer})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	server := newHTTPServerForNode(t, &httpTestNode{id: "node-a", engine: serverEngine}, token, nil)
	defer server.Close()

	clientSyncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-l", AuthToken: token, Client: server.Client()})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	_, err = clientSyncer.Push(ctx, server.URL, PushRequest{Entries: []Entry{stale}})
	var resync *ResyncRequiredError
	if !errors.As(err, &resync) || resync.Horizon != serverStore.GCHorizon() {
		t.Fatalf("expected 410 to decode as ResyncRequiredError, got %v", err)
	}
	_, err = clientSyncer.Pull(ctx, server.URL, PullRequest{Digest: map[string]DigestRecord{"doomed": newDigestRecord(stale, NewHLCLWWPolicy())}})
	if !errors.As(err, &resync) {
		t.Fatalf("expected lagging digest pull to be refused with 410, got %v", err)
	}
	assertAbsent(t, serverStore, "doomed")

	clientEngine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-l",
		Store:        clientStore,
		Syncer:       clientSyncer,
		PeerProvider: NewStaticPeerProvider([]string{server.URL}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	if err := clientEngine.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce with resync over HTTP: %v", err)
	}
	assertAbsent(t, clientStore, "doomed")
	entries, err := clientStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	if len(entries) != live {
		t.Fatalf("paged snapshot resync applied %d entries, want %d", len(entries), live)
	}
	if clientStore.GCHorizon() != serverStore.GCHorizon() {
		t.Fatalf("client horizon %+v, want %+v", clientStore.GCHorizon(), serverStore.GCHorizon())
	}
	if err := clientEngine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("anti-entropy after resync: %v", err)
	}
	assertAbsent(t, serverStore, "doomed")
}

// Review 03b F3: an engine whose PeerProvider momentarily returns no peers (a
// liveness-based provider during isolation) must not fall back to retention-only
// collection.
func TestReplicatedGCRefusesEmptyPeerList(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	a := newLoopNode(t, net, "node-a", clock)
	if _, err := a.store.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := a.store.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	clock.now += 2 * retentionMS()
	result, err := a.engine.CollectTombstones(ctx)
	if !errors.Is(err, ErrTombstoneGCNoPeers) || result.Collected != 0 {
		t.Fatalf("expected ErrTombstoneGCNoPeers and no collection, got %+v err=%v", result, err)
	}
	if entry := mustGet(t, a.store, "k"); !entry.Deleted {
		t.Fatalf("tombstone must survive a refused collection: %+v", entry)
	}
	if !a.store.GCHorizon().isZero() {
		t.Fatalf("horizon moved on a refused collection: %+v", a.store.GCHorizon())
	}
}
