package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Review 03b H2: snapshot pages must respect the requester's response limit,
// and resync must apply pages as they arrive instead of buffering the snapshot.

// newCollectedServerStore returns a store that collected "doomed" (so it has a
// GC horizon) and holds the given live entries, plus the stale value of doomed
// for a lagging client to hold.
func newCollectedServerStore(t *testing.T, clock *fixedPhysicalClock, live int, valueBytes int) (*MemoryStore, Entry) {
	t.Helper()
	ctx := context.Background()
	server := mustGCMemoryStore(t, "node-a", clock, testRetention)
	stale, err := server.Put(ctx, "doomed", []byte("stale"))
	if err != nil {
		t.Fatalf("Put doomed: %v", err)
	}
	value := bytes.Repeat([]byte("x"), valueBytes)
	for i := 0; i < live; i++ {
		if _, err := server.Put(ctx, fmt.Sprintf("live/%04d", i), value); err != nil {
			t.Fatalf("Put live: %v", err)
		}
	}
	if _, err := server.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete doomed: %v", err)
	}
	clock.now += 2 * retentionMS()
	if result, err := server.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("server GC: %+v err=%v", result, err)
	}
	return server, stale
}

func newHTTPPair(t *testing.T, server *MemoryStore, clientStore Store, clientMaxResponseBytes int64) (*GossipEngine, func()) {
	t.Helper()
	const token = "secret"
	serverSyncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-a", AuthToken: token})
	if err != nil {
		t.Fatalf("NewHTTPSyncer: %v", err)
	}
	serverEngine, err := NewGossipEngine(GossipConfig{NodeID: "node-a", Store: server, Syncer: serverSyncer})
	if err != nil {
		t.Fatalf("NewGossipEngine server: %v", err)
	}
	httpServer := newHTTPServerForNode(t, &httpTestNode{id: "node-a", engine: serverEngine}, token, nil)
	clientSyncer, err := NewHTTPSyncer(HTTPSyncerConfig{
		NodeID:           "node-l",
		AuthToken:        token,
		Client:           httpServer.Client(),
		MaxResponseBytes: clientMaxResponseBytes,
	})
	if err != nil {
		t.Fatalf("NewHTTPSyncer client: %v", err)
	}
	clientEngine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-l",
		Store:        clientStore,
		Syncer:       clientSyncer,
		PeerProvider: NewStaticPeerProvider([]string{httpServer.URL}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine client: %v", err)
	}
	return clientEngine, httpServer.Close
}

func TestResyncSnapshotWithLargeEntriesStaysWithinResponseLimit(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	// 300 entries of 8 KiB: a 256-entry page is about 2.7 MiB of JSON, far above
	// the default 1 MiB response limit.
	const live = 300
	server, stale := newCollectedServerStore(t, clock, live, 8<<10)
	client := mustGCMemoryStore(t, "node-l", clock, testRetention)
	if _, err := client.Apply(ctx, stale); err != nil {
		t.Fatalf("client Apply stale: %v", err)
	}
	engine, closeServer := newHTTPPair(t, server, client, 0)
	defer closeServer()

	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce with resync of large entries: %v", err)
	}
	assertAbsent(t, client, "doomed")
	entries, err := client.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	if len(entries) != live {
		t.Fatalf("resync applied %d entries, want %d", len(entries), live)
	}
	if client.GCHorizon() != server.GCHorizon() {
		t.Fatalf("client horizon %+v, want %+v", client.GCHorizon(), server.GCHorizon())
	}
}

func TestResyncReportsSingleEntryAboveResponseLimit(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	server, stale := newCollectedServerStore(t, clock, 1, 200<<10)
	client := mustGCMemoryStore(t, "node-l", clock, testRetention)
	if _, err := client.Apply(ctx, stale); err != nil {
		t.Fatalf("client Apply stale: %v", err)
	}
	engine, closeServer := newHTTPPair(t, server, client, 64<<10)
	defer closeServer()

	err := engine.SyncOnce(ctx)
	if err == nil {
		t.Fatalf("expected resync to fail on an entry above the response limit")
	}
	for _, want := range []string{"live/0000", "65536-byte response limit", "MaxResponseBytes"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("resync error is not diagnosable, missing %q: %v", want, err)
		}
	}
	// Nothing was purged and the horizon was not adopted.
	assertStoredValue(t, client, "doomed", "stale")
	if !client.GCHorizon().isZero() {
		t.Fatalf("failed resync adopted a horizon: %+v", client.GCHorizon())
	}

	// The engine-level error is typed.
	serverEngine, err := NewGossipEngine(GossipConfig{NodeID: "node-x", Store: server, Syncer: loopSyncer{}})
	if err != nil {
		t.Fatalf("NewGossipEngine: %v", err)
	}
	_, err = serverEngine.HandlePullResponse(ctx, PullRequest{PolicyID: server.Policy().ID(), Limit: resyncPageSize, MaxBytes: 64 << 10})
	var tooLarge *PullEntryTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Key != "live/0000" || tooLarge.MaxBytes != 64<<10 {
		t.Fatalf("expected PullEntryTooLargeError for live/0000, got %v", err)
	}
}

// pageFailingSyncer serves the first snapshot page and fails every later one.
type pageFailingSyncer struct {
	loopSyncer
}

func (s pageFailingSyncer) PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	if req.SincePosition > 0 {
		return nil, errors.New("injected page failure")
	}
	return s.loopSyncer.PullSnapshot(ctx, addr, req)
}

func TestResyncAppliesPagesAsTheyArriveAndPurgesOnlyAtTheEnd(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	net := newLoopNet()
	server, stale := newCollectedServerStore(t, clock, resyncPageSize+10, 1)
	serverEngine, err := NewGossipEngine(GossipConfig{NodeID: "node-a", Store: server, Syncer: loopSyncer{net: net, from: "node-a"}})
	if err != nil {
		t.Fatalf("NewGossipEngine server: %v", err)
	}
	net.engines["node-a"] = serverEngine

	client := mustGCMemoryStore(t, "node-l", clock, testRetention)
	if _, err := client.Apply(ctx, stale); err != nil {
		t.Fatalf("client Apply stale: %v", err)
	}
	clientEngine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-l",
		Store:        client,
		Syncer:       pageFailingSyncer{loopSyncer{net: net, from: "node-l"}},
		PeerProvider: NewStaticPeerProvider([]string{"node-a"}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine client: %v", err)
	}
	err = clientEngine.resyncFrom(ctx, "node-a", server.GCHorizon())
	if err == nil || !strings.Contains(err.Error(), "injected page failure") {
		t.Fatalf("expected resync to fail on page 1, got %v", err)
	}
	entries, err := client.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	// Page 0 was applied as it arrived (plus the stale entry), and the purge did
	// not run, so the stale value and the zero horizon are untouched.
	if len(entries) != resyncPageSize+1 {
		t.Fatalf("after a failed second page the client holds %d entries, want %d", len(entries), resyncPageSize+1)
	}
	assertStoredValue(t, client, "doomed", "stale")
	if !client.GCHorizon().isZero() {
		t.Fatalf("failed resync adopted a horizon: %+v", client.GCHorizon())
	}
}
