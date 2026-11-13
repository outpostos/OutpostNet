package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

func TestStoreReturnedValuesCannotMutateCommittedState(t *testing.T) {
	ctx := context.Background()
	store := mustStore(t, "node-a", NewHLCLWWPolicy(), &fixedPhysicalClock{now: 1_000})

	observed := make(chan Entry, 4)
	stop := store.Observe(func(entry Entry) {
		observed <- entry
	})
	defer stop()

	created, err := store.Put(ctx, "opaque/entity-1", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	created.Value[0] = 'X'
	assertStoredValue(t, store, "opaque/entity-1", "v1")
	if event := <-observed; string(event.Value) != "v1" {
		t.Fatalf("observer received aliased Put value: %q", event.Value)
	}
	event := mustGet(t, store, "opaque/entity-1")
	event.Value[0] = 'Y'
	assertStoredValue(t, store, "opaque/entity-1", "v1")

	updated, swapped, err := store.CompareAndSet(ctx, "opaque/entity-1", mustGet(t, store, "opaque/entity-1").Identity(), Mutation{Value: []byte("v2")})
	if err != nil {
		t.Fatalf("CompareAndSet success: %v", err)
	}
	if !swapped {
		t.Fatal("expected CompareAndSet success")
	}
	updated.Value[0] = 'Z'
	assertStoredValue(t, store, "opaque/entity-1", "v2")
	if event := <-observed; string(event.Value) != "v2" {
		t.Fatalf("observer received aliased CAS value: %q", event.Value)
	}

	staleExpected := EntryIdentity{Key: "opaque/entity-1"}
	current, swapped, err := store.CompareAndSet(ctx, "opaque/entity-1", staleExpected, Mutation{Value: []byte("stale")})
	if err != nil {
		t.Fatalf("CompareAndSet stale: %v", err)
	}
	if swapped {
		t.Fatal("stale CompareAndSet unexpectedly succeeded")
	}
	current.Value[0] = 'Q'
	assertStoredValue(t, store, "opaque/entity-1", "v2")
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

func TestHTTPNodesRecoverAfterTransportFailureWithoutSkippingUnconfirmedChanges(t *testing.T) {
	ctx := context.Background()
	token := "test-replication-token"

	clockA := &fixedPhysicalClock{now: 1_000}
	clockB := &fixedPhysicalClock{now: 1_000}
	a := newHTTPTestNode(t, "node-a", token, clockA)
	b := newBreakableHTTPTestNode(t, "node-b", token, clockB)
	defer a.server.Close()
	defer b.server.Close()
	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})

	live, err := a.store.Put(ctx, "opaque/target", []byte("live-from-a"))
	if err != nil {
		t.Fatalf("node-a Put live: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("initial SyncOnce: %v", err)
	}
	assertStoredValue(t, b.store, "opaque/target", "live-from-a")

	b.breaker.Store(true)
	clockA.now = 2_000
	if _, err := a.store.Put(ctx, "opaque/target", []byte("unconfirmed-update")); err != nil {
		t.Fatalf("node-a Put unconfirmed: %v", err)
	}
	clockA.now = 3_000
	deleted, err := a.store.Delete(ctx, "opaque/target")
	if err != nil {
		t.Fatalf("node-a Delete: %v", err)
	}
	if err := a.engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected transport failure during SyncOnce")
	}
	if got := peerLastPushForTest(a.engine, b.server.URL); got != live.Position {
		t.Fatalf("failed push advanced peer cursor: got %d want %d", got, live.Position)
	}
	entry := mustGet(t, b.store, "opaque/target")
	if entry.Deleted || string(entry.Value) != "live-from-a" {
		t.Fatalf("transport failure changed peer state: %+v", entry)
	}

	b.breaker.Store(false)
	clearPeerBackoffForTest(a.engine, b.server.URL)
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("recovery SyncOnce: %v", err)
	}
	entry = mustGet(t, b.store, "opaque/target")
	if !entry.Deleted {
		t.Fatalf("recovered peer did not receive tombstone: %+v", entry)
	}
	if entry.ChangeID != deleted.ChangeID || entry.Version != deleted.Version {
		t.Fatalf("recovered peer did not converge on delete version: got %+v want %+v", entry, deleted)
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
		MaxRequestBytes: (768 << 10) + 1,
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
	oversized := entry
	oversized.Value = make([]byte, 700<<10)
	_, err = syncer.Push(ctx, small.URL, PushRequest{SourceNodeID: "node-b", Entries: []Entry{oversized}})
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

// TestHTTPAdapterRequiresObjectProtocolVersion pins the /state/v1 wire to the
// object form {"major":M,"minor":N}. Every state peer has always encoded the
// version as an object, so a bare integer is malformed input, not a legacy
// peer: it is refused before the request is applied.
func TestHTTPAdapterRequiresObjectProtocolVersion(t *testing.T) {
	ctx := context.Background()
	token := "test-auth-token"
	node := newHTTPTestNode(t, "node-a", token, &fixedPhysicalClock{now: 1_000})
	defer node.server.Close()

	post := func(body string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, node.server.URL+PushPath, strings.NewReader(body))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Outpost-Node-ID", "node-b")
		resp, err := node.server.Client().Do(req)
		if err != nil {
			t.Fatalf("push request: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	policy := NewHLCLWWPolicy().ID()
	if status := post(`{"protocol_version":{"major":1,"minor":0},"policy_id":"` + policy + `","source":"node-b","entries":[]}`); status != http.StatusOK {
		t.Fatalf("object protocol version: status %d, want 200", status)
	}
	if status := post(`{"protocol_version":1,"policy_id":"` + policy + `","source":"node-b","entries":[]}`); status != http.StatusBadRequest {
		t.Fatalf("integer protocol version: status %d, want 400", status)
	}
}

func TestHTTPSyncerRejectsOversizeSuccessAndErrorResponses(t *testing.T) {
	ctx := context.Background()
	oversizeJSON := `{"protocol_version":{"major":1,"minor":0},"policy_id":"outpostnet.state.hlc-lww/v1","accepted":0,"rejected":0,"padding":"` + strings.Repeat("x", 128) + `"}`
	oversizeError := strings.Repeat("e", 128)
	tests := []struct {
		name   string
		status int
		body   string
		length int64
	}{
		{name: "success missing content length", status: http.StatusOK, body: oversizeJSON, length: -1},
		{name: "success declared oversize", status: http.StatusOK, body: oversizeJSON, length: int64(len(oversizeJSON))},
		{name: "error missing content length", status: http.StatusInternalServerError, body: oversizeError, length: -1},
		{name: "error declared oversize", status: http.StatusInternalServerError, body: oversizeError, length: int64(len(oversizeError))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.length >= 0 {
					w.Header().Set("Content-Length", fmt.Sprint(tt.length))
				}
				w.WriteHeader(tt.status)
				if tt.length < 0 {
					w.(http.Flusher).Flush()
				}
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{
				Client:           server.Client(),
				NodeID:           "node-a",
				AuthToken:        "token",
				MaxResponseBytes: 32,
			})
			if err != nil {
				t.Fatalf("NewHTTPSyncer: %v", err)
			}
			_, err = syncer.Push(ctx, server.URL, PushRequest{SourceNodeID: "node-a"})
			if err == nil || !strings.Contains(err.Error(), "response body exceeds 32 bytes") {
				t.Fatalf("expected response size error, got %v", err)
			}
		})
	}

	if _, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-a", AuthToken: "token", MaxResponseBytes: -1}); err == nil {
		t.Fatal("expected negative max response bytes to be rejected")
	}
}

func TestHTTPSyncerRejectsMissingOrMismatchedPolicyResponse(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing policy id",
			body: `{"protocol_version":{"major":1,"minor":0},"accepted":0,"rejected":0}`,
		},
		{
			name: "mismatched policy id",
			body: `{"protocol_version":{"major":1,"minor":0},"policy_id":"other-policy/v1","accepted":0,"rejected":0}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{
				Client:    server.Client(),
				NodeID:    "node-a",
				AuthToken: "token",
				PolicyID:  NewHLCLWWPolicy().ID(),
			})
			if err != nil {
				t.Fatalf("NewHTTPSyncer: %v", err)
			}
			_, err = syncer.Push(ctx, server.URL, PushRequest{SourceNodeID: "node-a"})
			if err == nil || !strings.Contains(err.Error(), "policy") {
				t.Fatalf("expected policy response error, got %v", err)
			}
		})
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

func TestHTTPPolicyMismatchRejectedWithoutChangingState(t *testing.T) {
	ctx := context.Background()
	token := "test-policy-token"
	policyA := epochPolicy{id: "epoch/a"}
	policyB := epochPolicy{id: "epoch/b"}
	a := newHTTPTestNodeWithPolicy(t, "node-a", token, policyA, &fixedPhysicalClock{now: 1_000})
	b := newHTTPTestNodeWithPolicy(t, "node-b", token, policyB, &fixedPhysicalClock{now: 1_000})
	defer a.server.Close()
	defer b.server.Close()

	if _, err := a.store.Put(ctx, "opaque/epoch", []byte("7")); err != nil {
		t.Fatalf("node-a Put: %v", err)
	}
	_, err := a.syncer.Push(ctx, b.server.URL, PushRequest{
		SourceNodeID: "node-a",
		Entries:      []Entry{mustGet(t, a.store, "opaque/epoch")},
	})
	if err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatalf("expected policy mismatch push error, got %v", err)
	}
	if _, ok, err := b.store.Get(ctx, "opaque/epoch"); err != nil || ok {
		t.Fatalf("policy-mismatched push modified receiver: ok=%v err=%v", ok, err)
	}

	_, err = a.syncer.Pull(ctx, b.server.URL, PullRequest{SourceNodeID: "node-a"})
	if err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatalf("expected policy mismatch pull error, got %v", err)
	}

	body := mustJSON(t, PushRequest{
		ProtocolVersion: StateProtocolVersion,
		SourceNodeID:    "node-a",
		Entries:         []Entry{mustGet(t, a.store, "opaque/epoch")},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.server.URL+PushPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build missing policy request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Outpost-Node-ID", "node-a")
	resp, err := b.server.Client().Do(req)
	if err != nil {
		t.Fatalf("missing policy request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing policy id, got %d", resp.StatusCode)
	}
}

func TestAntiEntropyApplyFailureReturnsErrorAndPreservesRetryState(t *testing.T) {
	ctx := context.Background()
	store := mustStore(t, "node-a", NewHLCLWWPolicy(), &fixedPhysicalClock{now: 1_000})
	invalid := newEntryForTest("opaque/bad", []byte("bad"), "node-b", 1, Timestamp{WallMS: 1_000})
	invalid.Key = ""
	valid := newEntryForTest("opaque/good", []byte("good"), "node-b", 2, Timestamp{WallMS: 2_000})
	syncer := &scriptedPullSyncer{
		pulls: [][]Entry{
			{invalid},
			{valid},
		},
	}
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       syncer,
		PeerProvider: NewStaticPeerProvider([]string{"peer-b"}),
		BackoffBase:  time.Hour,
		BackoffMax:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}

	err = engine.AntiEntropyOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "apply pulled entry") {
		t.Fatalf("expected apply error from anti-entropy, got %v", err)
	}
	if failures, blocked := peerFailureForTest(engine, "peer-b"); failures == 0 || !blocked {
		t.Fatalf("apply failure did not preserve retry state: failures=%d blocked=%v", failures, blocked)
	}
	if _, ok, err := store.Get(ctx, "opaque/good"); err != nil || ok {
		t.Fatalf("invalid pull unexpectedly changed store: ok=%v err=%v", ok, err)
	}

	clearPeerBackoffForTest(engine, "peer-b")
	if err := engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("recovery AntiEntropyOnce: %v", err)
	}
	assertStoredValue(t, store, "opaque/good", "good")
}

func TestAntiEntropyOlderOrDuplicateEntriesAreNotApplyFailures(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 2_000}
	store := mustStore(t, "node-a", NewHLCLWWPolicy(), clock)
	current, err := store.Put(ctx, "opaque/current", []byte("current"))
	if err != nil {
		t.Fatalf("Put current: %v", err)
	}
	older := current
	older.Value = []byte("older")
	older.Version.HLC = Timestamp{WallMS: 1_000}
	syncer := &scriptedPullSyncer{pulls: [][]Entry{{current, older}}}
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       syncer,
		PeerProvider: NewStaticPeerProvider([]string{"peer-b"}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	if err := engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("AntiEntropyOnce duplicate/older: %v", err)
	}
	assertStoredValue(t, store, "opaque/current", "current")
}

func TestPeerFailureBackoffSaturatesBeforeOverflow(t *testing.T) {
	store := mustStore(t, "node-a", NewHLCLWWPolicy(), &fixedPhysicalClock{now: 1_000})
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       noopSyncer{},
		PeerProvider: NewStaticPeerProvider([]string{"peer-b"}),
		BackoffBase:  time.Second,
		BackoffMax:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	now := time.Unix(1_000, 0)
	for i := 0; i < 128; i++ {
		engine.recordPeerFailure("peer-b", "push", errors.New("boom"), now)
		state := peerStateForTest(engine, "peer-b")
		if !state.nextAttempt.After(now) {
			t.Fatalf("failure %d produced non-positive backoff: retryAt=%s now=%s", i+1, state.nextAttempt, now)
		}
		if state.nextAttempt.Sub(now) > time.Hour {
			t.Fatalf("failure %d exceeded max backoff: %s", i+1, state.nextAttempt.Sub(now))
		}
	}
}

type httpTestNode struct {
	id      string
	store   Store
	engine  *GossipEngine
	syncer  *HTTPSyncer
	server  *httptest.Server
	peers   atomicPeerList
	breaker atomic.Bool
}

func newHTTPTestNode(t *testing.T, id, token string, clock *fixedPhysicalClock) *httpTestNode {
	return newHTTPTestNodeWithPolicy(t, id, token, NewHLCLWWPolicy(), clock)
}

func newHTTPTestNodeWithPolicy(t *testing.T, id, token string, policy MergePolicy, clock *fixedPhysicalClock) *httpTestNode {
	t.Helper()
	node := &httpTestNode{id: id}
	node.store = mustStore(t, id, policy, clock)
	var err error
	node.syncer, err = NewHTTPSyncer(HTTPSyncerConfig{NodeID: id, AuthToken: token, PolicyID: policy.ID()})
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
	node.server = newHTTPServerForNode(t, node, token, nil)
	node.syncer.client = node.server.Client()
	return node
}

func newBreakableHTTPTestNode(t *testing.T, id, token string, clock *fixedPhysicalClock) *httpTestNode {
	t.Helper()
	node := &httpTestNode{id: id}
	node.store = mustStore(t, id, NewHLCLWWPolicy(), clock)
	var err error
	node.syncer, err = NewHTTPSyncer(HTTPSyncerConfig{NodeID: id, AuthToken: token, PolicyID: node.store.Policy().ID()})
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
	node.server = newHTTPServerForNode(t, node, token, &node.breaker)
	node.syncer.client = node.server.Client()
	return node
}

func newHTTPServerForNode(t *testing.T, node *httpTestNode, token string, breaker *atomic.Bool) *httptest.Server {
	t.Helper()
	handler, err := NewHTTPHandler(HTTPHandlerConfig{
		NodeID:          node.id,
		Engine:          node.engine,
		AuthToken:       token,
		MaxRequestBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	if breaker == nil {
		return httptest.NewServer(handler)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if breaker.Load() {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("httptest response writer does not support hijacking")
				http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack connection: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		handler.ServeHTTP(w, r)
	}))
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

func mustGet(t *testing.T, store Store, key string) Entry {
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

func assertStoredValue(t *testing.T, store Store, key, value string) {
	t.Helper()
	entry := mustGet(t, store, key)
	if string(entry.Value) != value {
		t.Fatalf("stored value for %s = %q, want %q", key, entry.Value, value)
	}
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

type scriptedPullSyncer struct {
	pulls [][]Entry
	calls atomic.Int64
}

func (s *scriptedPullSyncer) Push(context.Context, string, PushRequest) (*PushResponse, error) {
	return &PushResponse{ProtocolVersion: StateProtocolVersion, PolicyID: NewHLCLWWPolicy().ID()}, nil
}

func (s *scriptedPullSyncer) Pull(context.Context, string, PullRequest) ([]Entry, error) {
	index := int(s.calls.Add(1)) - 1
	if index >= len(s.pulls) {
		return nil, nil
	}
	return append([]Entry(nil), s.pulls[index]...), nil
}

func peerStateForTest(engine *GossipEngine, peer string) peerState {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	state := engine.peerStateLocked(peer)
	return *state
}

func peerLastPushForTest(engine *GossipEngine, peer string) uint64 {
	return peerStateForTest(engine, peer).lastPushPosition
}

func peerFailureForTest(engine *GossipEngine, peer string) (int, bool) {
	state := peerStateForTest(engine, peer)
	return state.failures, !state.nextAttempt.IsZero()
}

func clearPeerBackoffForTest(engine *GossipEngine, peer string) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	state := engine.peerStateLocked(peer)
	state.nextAttempt = time.Time{}
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
