package state

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreSubmitsReadsObservesAndComparesAtomically(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000}
	store := mustStore(t, "node-a", NewHLCLWWPolicy(), clock)

	observed := make(chan Entry, 3)
	stop := store.Observe(func(entry Entry) {
		observed <- entry
	})
	defer stop()

	created, err := store.Put(ctx, "opaque/entity-1", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if created.ChangeID.OriginNodeID != "node-a" || created.ChangeID.Counter == 0 {
		t.Fatalf("local write did not get an origin identity: %+v", created.ChangeID)
	}
	if created.Position == 0 {
		t.Fatalf("local write did not get a local position: %+v", created)
	}

	got, ok, err := store.Get(ctx, "opaque/entity-1")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("expected v1, got %q", got.Value)
	}
	if event := <-observed; event.Identity() != created.Identity() {
		t.Fatalf("observer saw wrong entry identity: got %+v want %+v", event.Identity(), created.Identity())
	}

	clock.now = 2_000
	updated, swapped, err := store.CompareAndSet(ctx, "opaque/entity-1", created.Identity(), Mutation{Value: []byte("v2")})
	if err != nil {
		t.Fatalf("CompareAndSet update: %v", err)
	}
	if !swapped {
		t.Fatal("expected compare-and-set update to succeed")
	}
	if updated.ChangeID == created.ChangeID {
		t.Fatalf("new local write reused the original change identity: old=%+v new=%+v", created.ChangeID, updated.ChangeID)
	}
	if event := <-observed; string(event.Value) != "v2" {
		t.Fatalf("observer saw wrong update value: %q", event.Value)
	}

	clock.now = 3_000
	_, swapped, err = store.CompareAndSet(ctx, "opaque/entity-1", created.Identity(), Mutation{Value: []byte("stale")})
	if err != nil {
		t.Fatalf("CompareAndSet stale: %v", err)
	}
	if swapped {
		t.Fatal("stale compare-and-set unexpectedly succeeded")
	}

	deleted, swapped, err := store.CompareAndSet(ctx, "opaque/entity-1", updated.Identity(), Mutation{Deleted: true})
	if err != nil {
		t.Fatalf("CompareAndSet delete: %v", err)
	}
	if !swapped || !deleted.Deleted {
		t.Fatalf("expected delete tombstone, swapped=%v entry=%+v", swapped, deleted)
	}
	if event := <-observed; !event.Deleted {
		t.Fatalf("observer did not see delete tombstone: %+v", event)
	}
}

func TestTwoHTTPNodesReplicatePartitionRecoverAndRejectDuplicateOutOfOrderResurrection(t *testing.T) {
	ctx := context.Background()
	token := "test-replication-token"

	clockA := &fixedPhysicalClock{now: 1_000}
	clockB := &fixedPhysicalClock{now: 1_000}
	a := newHTTPTestNode(t, "node-a", token, clockA)
	b := newHTTPTestNode(t, "node-b", token, clockB)
	defer a.server.Close()
	defer b.server.Close()
	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})

	live, err := a.store.Put(ctx, "opaque/target", []byte("live-from-a"))
	if err != nil {
		t.Fatalf("node-a Put: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-a SyncOnce: %v", err)
	}
	gotB := mustGet(t, b.store, "opaque/target")
	if string(gotB.Value) != "live-from-a" {
		t.Fatalf("node-b did not receive live value: %+v", gotB)
	}
	if gotB.ChangeID != live.ChangeID {
		t.Fatalf("forwarded entry changed original change identity: got %+v want %+v", gotB.ChangeID, live.ChangeID)
	}
	if gotB.Position == live.Position {
		t.Fatalf("local consumption position should be node-local, got both %d", gotB.Position)
	}

	a.peers.Store(nil)
	b.peers.Store(nil)
	clockA.now = 3_000
	deleted, err := a.store.Delete(ctx, "opaque/target")
	if err != nil {
		t.Fatalf("node-a Delete: %v", err)
	}
	clockB.now = 2_000
	if _, err := b.store.Put(ctx, "opaque/target", []byte("partition-live-from-b")); err != nil {
		t.Fatalf("node-b partition Put: %v", err)
	}

	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-a recovery SyncOnce: %v", err)
	}
	if err := b.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("node-b recovery SyncOnce: %v", err)
	}
	if err := a.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("node-a recovery AntiEntropyOnce: %v", err)
	}
	if err := b.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("node-b recovery AntiEntropyOnce: %v", err)
	}

	for _, node := range []*httpTestNode{a, b} {
		entry := mustGet(t, node.store, "opaque/target")
		if !entry.Deleted {
			t.Fatalf("%s resurrected deleted state after recovery: %+v", node.id, entry)
		}
		if entry.ChangeID != deleted.ChangeID {
			t.Fatalf("%s did not converge on original delete identity: got %+v want %+v", node.id, entry.ChangeID, deleted.ChangeID)
		}
	}

	resp, err := a.syncer.Push(ctx, b.server.URL, PushRequest{
		SourceNodeID: "node-a",
		Entries: []Entry{
			live,
			deleted,
			live,
			live,
		},
	})
	if err != nil {
		t.Fatalf("duplicate/out-of-order HTTP push: %v", err)
	}
	if resp.Accepted != 0 {
		t.Fatalf("expected duplicate/out-of-order push not to apply new entries, got response %+v", resp)
	}
	entry := mustGet(t, b.store, "opaque/target")
	if !entry.Deleted || entry.ChangeID != deleted.ChangeID {
		t.Fatalf("old duplicate live entry resurrected deleted state: %+v", entry)
	}
}

func TestHTTPAdapterRequiresAuthIdentityProtocolAndSizeLimit(t *testing.T) {
	ctx := context.Background()
	token := "test-auth-token"
	node := newHTTPTestNode(t, "node-a", token, &fixedPhysicalClock{now: 1_000})
	defer node.server.Close()

	entry := newEntryForTest("key", []byte("value"), "node-b", 1, Timestamp{WallMS: 1_000})
	body := mustJSON(t, PushRequest{
		ProtocolVersion: StateProtocolVersion,
		SourceNodeID:    "node-b",
		Entries:         []Entry{entry},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, node.server.URL+PushPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Outpost-Node-ID", "node-b")
	resp, err := node.server.Client().Do(req)
	if err != nil {
		t.Fatalf("unauthenticated request: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without bearer token, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, node.server.URL+PushPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build mismatch request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Outpost-Node-ID", "different-node")
	resp, err = node.server.Client().Do(req)
	if err != nil {
		t.Fatalf("identity mismatch request: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for identity mismatch, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, node.server.URL+PushPath, strings.NewReader(`{"protocol_version":{"major":99,"minor":0},"source":"node-b","entries":[]}`))
	if err != nil {
		t.Fatalf("build protocol request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Outpost-Node-ID", "node-b")
	resp, err = node.server.Client().Do(req)
	if err != nil {
		t.Fatalf("protocol mismatch request: %v", err)
	}
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("expected 426 for protocol mismatch, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	handler, err := NewHTTPHandler(HTTPHandlerConfig{
		NodeID:          "node-small",
		Engine:          node.engine,
		AuthToken:       token,
		MaxRequestBytes: 64,
	})
	if err != nil {
		t.Fatalf("NewHTTPHandler small: %v", err)
	}
	small := httptest.NewServer(handler)
	defer small.Close()
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{
		Client:    small.Client(),
		NodeID:    "node-b",
		AuthToken: token,
	})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	_, err = syncer.Push(ctx, small.URL, PushRequest{SourceNodeID: "node-b", Entries: []Entry{entry}})
	if err == nil || !strings.Contains(err.Error(), "status 413") {
		t.Fatalf("expected 413 size error, got %v", err)
	}

	if _, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "open", Engine: node.engine}); err == nil {
		t.Fatal("expected handler without auth token to be rejected")
	}
	if _, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-b"}); err == nil {
		t.Fatal("expected syncer without auth token to be rejected")
	}
}

func TestPolicyMismatchRejectedAndDigestMetadataStaysGeneric(t *testing.T) {
	ctx := context.Background()
	policyA := epochPolicy{id: "epoch/a"}
	policyB := epochPolicy{id: "epoch/b"}
	store := mustStore(t, "node-a", policyA, &fixedPhysicalClock{now: 1_000})

	if _, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       noopSyncer{},
		PeerProvider: PeerProviderFunc(func() []string { return nil }),
		Policy:       policyB,
	}); err == nil {
		t.Fatal("expected explicit policy mismatch to be rejected")
	}

	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       noopSyncer{},
		PeerProvider: PeerProviderFunc(func() []string { return nil }),
		Policy:       policyA,
	})
	if err != nil {
		t.Fatalf("NewGossipEngine with matching policy: %v", err)
	}
	if _, err := store.Put(ctx, "opaque/epoch", []byte("7")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	digest, err := engine.LocalDigest(ctx)
	if err != nil {
		t.Fatalf("LocalDigest: %v", err)
	}
	record, ok := digest["opaque/epoch"]
	if !ok {
		t.Fatalf("expected digest record, got %#v", digest)
	}
	raw := record.Metadata["epoch"]
	if string(raw) != "7" {
		t.Fatalf("expected generic metadata epoch=7, got %s in %#v", raw, record.Metadata)
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal digest record: %v", err)
	}
	for _, forbidden := range []string{"pod_binding_epoch", "election_term_epoch"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("digest leaked Let-specific field %q: %s", forbidden, data)
		}
	}
}

type httpTestNode struct {
	id     string
	store  *MemoryStore
	engine *GossipEngine
	syncer *HTTPSyncer
	server *httptest.Server
	peers  atomicPeerList
}

func newHTTPTestNode(t *testing.T, id, token string, clock *fixedPhysicalClock) *httpTestNode {
	t.Helper()
	node := &httpTestNode{id: id}
	node.store = mustStore(t, id, NewHLCLWWPolicy(), clock)
	var err error
	node.syncer, err = NewHTTPSyncer(HTTPSyncerConfig{NodeID: id, AuthToken: token})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	node.engine, err = NewGossipEngine(GossipConfig{
		NodeID:       id,
		Store:        node.store,
		Syncer:       node.syncer,
		PeerProvider: PeerProviderFunc(func() []string { return node.peers.Load() }),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{
		NodeID:          id,
		Engine:          node.engine,
		AuthToken:       token,
		MaxRequestBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	node.server = httptest.NewServer(handler)
	node.syncer.client = node.server.Client()
	return node
}

type atomicPeerList struct {
	value atomic.Value
}

func (p *atomicPeerList) Store(peers []string) {
	copied := append([]string(nil), peers...)
	p.value.Store(copied)
}

func (p *atomicPeerList) Load() []string {
	peers, _ := p.value.Load().([]string)
	return append([]string(nil), peers...)
}

func mustStore(t *testing.T, nodeID string, policy MergePolicy, clock PhysicalClock) *MemoryStore {
	t.Helper()
	store, err := NewMemoryStore(MemoryStoreConfig{
		NodeID:        nodeID,
		Policy:        policy,
		PhysicalClock: clock,
	})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	return store
}

func mustGet(t *testing.T, store *MemoryStore, key string) Entry {
	t.Helper()
	entry, ok, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !ok {
		t.Fatalf("missing key %q", key)
	}
	return entry
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

type fixedPhysicalClock struct {
	now int64
}

func (c *fixedPhysicalClock) NowMS() int64 {
	return c.now
}

type noopSyncer struct{}

func (noopSyncer) Push(context.Context, string, PushRequest) (*PushResponse, error) {
	return &PushResponse{ProtocolVersion: StateProtocolVersion}, nil
}

func (noopSyncer) Pull(context.Context, string, PullRequest) ([]Entry, error) {
	return nil, nil
}

type epochPolicy struct {
	id string
}

func (p epochPolicy) ID() string { return p.id }

func (p epochPolicy) ValidateApply(Entry) error { return nil }

func (p epochPolicy) CompareEntries(left, right Entry) int {
	return NewHLCLWWPolicy().CompareEntries(left, right)
}

func (p epochPolicy) DecorateDigest(entry Entry, record *DigestRecord) {
	record.Metadata["epoch"] = json.RawMessage(entry.Value)
}

func (p epochPolicy) CompareEntryToDigest(entry Entry, record DigestRecord) int {
	if raw, ok := record.Metadata["epoch"]; ok {
		cmp := bytes.Compare(entry.Value, raw)
		if cmp != 0 {
			if cmp < 0 {
				return -1
			}
			return 1
		}
	}
	return NewHLCLWWPolicy().CompareEntryToDigest(entry, record)
}

var _ = time.Second
