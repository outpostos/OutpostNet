package state

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func peerStatusFor(t *testing.T, engine *GossipEngine, peer string) PeerReplicationStatus {
	t.Helper()
	status, err := engine.ReplicationStatus(context.Background())
	if err != nil {
		t.Fatalf("ReplicationStatus: %v", err)
	}
	for _, p := range status.Peers {
		if p.Peer == peer {
			return p
		}
	}
	t.Fatalf("peer %q missing from status %+v", peer, status)
	return PeerReplicationStatus{}
}

func TestReplicationStatusReportsPerPeerExchangeResults(t *testing.T) {
	ctx := context.Background()
	token := "status-token"
	a := newHTTPTestNode(t, "node-a", token, &fixedPhysicalClock{now: 1_000})
	b := newBreakableHTTPTestNode(t, "node-b", token, &fixedPhysicalClock{now: 1_000})
	defer a.server.Close()
	defer b.server.Close()
	a.peers.Store([]string{b.server.URL})

	if _, err := a.store.Put(ctx, "opaque/k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	status, err := a.engine.ReplicationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Local.Ready || status.Local.CurrentPosition == 0 {
		t.Fatalf("local status = %+v, want ready with a position", status.Local)
	}
	if len(status.Peers) != 1 {
		t.Fatalf("peers = %+v, want exactly the configured peer", status.Peers)
	}
	before := status.Peers[0]
	if before.OutboundCaughtUp || before.InboundCaughtUp || before.CaughtUp {
		t.Fatalf("no exchange yet must not be caught up: %+v", before)
	}
	if !before.LastPullTime.IsZero() || !before.LastSuccessTime.IsZero() || before.LastError != "" {
		t.Fatalf("no exchange yet must not report times or errors: %+v", before)
	}

	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	pushed := peerStatusFor(t, a.engine, b.server.URL)
	if pushed.AcknowledgedPosition != status.Local.CurrentPosition || !pushed.OutboundCaughtUp {
		t.Fatalf("after push: %+v, want acknowledged %d and outbound caught up", pushed, status.Local.CurrentPosition)
	}
	if pushed.InboundCaughtUp || pushed.CaughtUp || !pushed.LastPullTime.IsZero() {
		t.Fatalf("a push alone must not claim inbound catch-up: %+v", pushed)
	}
	if pushed.LastSuccessTime.IsZero() {
		t.Fatalf("acknowledged push must record a success time: %+v", pushed)
	}

	pullStart := time.Now()
	if err := a.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("AntiEntropyOnce: %v", err)
	}
	pulled := peerStatusFor(t, a.engine, b.server.URL)
	if pulled.LastPullTime.Before(pullStart) || !pulled.InboundCaughtUp || !pulled.CaughtUp {
		t.Fatalf("after pull: %+v, want last pull >= %v and caught up", pulled, pullStart)
	}

	b.breaker.Store(true)
	if _, err := a.store.Put(ctx, "opaque/k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := a.engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected push failure while peer is broken")
	}
	failed := peerStatusFor(t, a.engine, b.server.URL)
	if failed.LastError == "" || failed.LastErrorOperation != "push" || failed.LastErrorTime.Before(pulled.LastPullTime) {
		t.Fatalf("failed push not recorded: %+v", failed)
	}
	if failed.ConsecutiveFailures != 1 || failed.NextAttemptTime.IsZero() {
		t.Fatalf("failed push must report backoff: %+v", failed)
	}
	if failed.OutboundCaughtUp || failed.InboundCaughtUp || failed.CaughtUp {
		t.Fatalf("a failure after the last pull must clear catch-up: %+v", failed)
	}
	if failed.AcknowledgedPosition != pushed.AcknowledgedPosition {
		t.Fatalf("failed push moved the acknowledged position: %+v", failed)
	}

	// The engine skips a peer in backoff and returns nil; that is not a pull.
	if err := a.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("AntiEntropyOnce in backoff: %v", err)
	}
	skipped := peerStatusFor(t, a.engine, b.server.URL)
	if !skipped.LastPullTime.Equal(pulled.LastPullTime) || skipped.InboundCaughtUp {
		t.Fatalf("a round skipped by backoff was counted as a pull: %+v", skipped)
	}

	b.breaker.Store(false)
	clearPeerBackoffForTest(a.engine, b.server.URL)
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("recovery SyncOnce: %v", err)
	}
	if err := a.engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatalf("recovery AntiEntropyOnce: %v", err)
	}
	recovered := peerStatusFor(t, a.engine, b.server.URL)
	if !recovered.CaughtUp || recovered.ConsecutiveFailures != 0 || !recovered.NextAttemptTime.IsZero() {
		t.Fatalf("after recovery: %+v, want caught up without backoff", recovered)
	}
	if recovered.LastError != failed.LastError || !recovered.LastErrorTime.Equal(failed.LastErrorTime) {
		t.Fatalf("the latest error must stay reported after recovery: %+v", recovered)
	}
}

func TestReplicationStatusAfterColdStartKeepsAcknowledgedPositionButNoPull(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *DurableStore {
		store, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "node-a"})
		if err != nil {
			t.Fatalf("NewDurableStore: %v", err)
		}
		return store
	}
	first := open()
	if _, err := first.Put(ctx, "opaque/k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := first.SavePeerProgress(ctx, "peer-b", first.CurrentPosition()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := open()
	defer recovered.Close()
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        recovered,
		Syncer:       noopSyncer{},
		PeerProvider: NewStaticPeerProvider([]string{"peer-b"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := peerStatusFor(t, engine, "peer-b")
	if got.AcknowledgedPosition != recovered.CurrentPosition() || !got.OutboundCaughtUp {
		t.Fatalf("recovered acknowledged position not reported: %+v (current %d)", got, recovered.CurrentPosition())
	}
	if !got.LastPullTime.IsZero() || got.InboundCaughtUp || got.CaughtUp {
		t.Fatalf("a fresh process has not pulled yet: %+v", got)
	}
}

func TestReplicationStatusDoesNotLeakCredentials(t *testing.T) {
	ctx := context.Background()
	token := "super-secret-replication-token"
	// A misbehaving peer that echoes the request credentials in its error body.
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rejected "+r.Header.Get("Authorization"), http.StatusForbidden)
	}))
	defer echo.Close()

	store := mustStore(t, "node-a", NewHLCLWWPolicy(), &fixedPhysicalClock{now: 1_000})
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-a", AuthToken: token, Client: echo.Client()})
	if err != nil {
		t.Fatal(err)
	}
	peer := strings.Replace(echo.URL, "http://", "http://ops:url-password@", 1)
	engine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-a",
		Store:        store,
		Syncer:       syncer,
		PeerProvider: NewStaticPeerProvider([]string{peer}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "opaque/k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected the echoing peer to reject the push")
	}
	status, err := engine.ReplicationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Peers) != 1 || status.Peers[0].LastError == "" {
		t.Fatalf("expected one failed peer, got %+v", status.Peers)
	}
	got := status.Peers[0]
	for _, secret := range []string{token, "url-password"} {
		if strings.Contains(got.LastError, secret) || strings.Contains(got.Peer, secret) {
			t.Fatalf("status leaks %q: peer=%q last_error=%q", secret, got.Peer, got.LastError)
		}
	}
	if !strings.Contains(got.LastError, "403") {
		t.Fatalf("redaction must keep the useful part of the error: %q", got.LastError)
	}
}

func TestReplicationStatusIsSafeDuringConcurrentExchanges(t *testing.T) {
	ctx := context.Background()
	token := "status-token"
	a := newHTTPTestNode(t, "node-a", token, &fixedPhysicalClock{now: 1_000})
	b := newHTTPTestNode(t, "node-b", token, &fixedPhysicalClock{now: 1_000})
	defer a.server.Close()
	defer b.server.Close()
	a.peers.Store([]string{b.server.URL})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_, _ = a.store.Put(ctx, "opaque/k", []byte{byte(i)})
			_ = a.engine.SyncOnce(ctx)
			_ = a.engine.AntiEntropyOnce(ctx)
		}
		close(stop)
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			status, err := a.engine.ReplicationStatus(ctx)
			if err != nil {
				t.Errorf("ReplicationStatus: %v", err)
				return
			}
			for _, p := range status.Peers {
				if p.CaughtUp != (p.OutboundCaughtUp && p.InboundCaughtUp) {
					t.Errorf("inconsistent catch-up flags: %+v", p)
					return
				}
			}
		}
	}()
	wg.Wait()
}
