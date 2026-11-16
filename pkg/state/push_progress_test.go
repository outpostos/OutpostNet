package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

type pageSyncer struct {
	push func(string, PushRequest) (*PushResponse, error)
}

func (s pageSyncer) Push(_ context.Context, addr string, req PushRequest) (*PushResponse, error) {
	return s.push(addr, req)
}
func (s pageSyncer) Pull(context.Context, string, PullRequest) ([]Entry, error) { return nil, nil }

func limitedPagingStore(t *testing.T, durable bool, id string) Store {
	t.Helper()
	if !durable {
		s, err := NewMemoryStore(MemoryStoreConfig{NodeID: id, MaxEntryBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: id, MaxEntryBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func seedPages(t *testing.T, store Store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := store.Put(context.Background(), fmt.Sprintf("key/%02d", i), bytes.Repeat([]byte("x"), 2000)); err != nil {
			t.Fatal(err)
		}
	}
}
func pageEngine(t *testing.T, store Store, syncer Syncer, pages int, peers ...string) *GossipEngine {
	t.Helper()
	g, err := NewGossipEngine(GossipConfig{NodeID: "source", Store: store, Syncer: syncer, PeerProvider: NewStaticPeerProvider(peers), MaxPushBytes: 8192, MaxPushPagesPerRound: pages, BackoffBase: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestPushPageFailureResumesAfterConfirmedPage(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			store := limitedPagingStore(t, durable, "source")
			seedPages(t, store, 12)
			calls := 0
			seen := map[string]int{}
			var confirmed uint64
			syncer := pageSyncer{push: func(_ string, req PushRequest) (*PushResponse, error) {
				calls++
				if calls == 3 {
					return nil, errors.New("page 3 disconnected")
				}
				for _, e := range req.Entries {
					seen[e.Key]++
					confirmed = e.Position
				}
				return nil, nil
			}}
			g := pageEngine(t, store, syncer, 16, "target")
			if err := g.SyncOnce(ctx); err == nil {
				t.Fatal("expected page 3 failure")
			}
			status := peerStatusFor(t, g, "target")
			if confirmed == 0 || status.AcknowledgedPosition != confirmed || status.OutboundRoundCaughtUp {
				t.Fatalf("incorrect partial progress: %+v confirmed=%d", status, confirmed)
			}
			if d, ok := store.(*DurableStore); ok {
				dir := d.dir
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "source", MaxEntryBytes: 4096})
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				g = pageEngine(t, reopened, syncer, 16, "target")
			}
			if err := g.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if !peerStatusFor(t, g, "target").OutboundCaughtUp {
				t.Fatal("did not finish")
			}
			if len(seen) != 12 {
				t.Fatalf("sent %d keys, want 12", len(seen))
			}
			for key, n := range seen {
				if n != 1 {
					t.Fatalf("confirmed key %s sent %d times", key, n)
				}
			}
		})
	}
}

func TestPushPageBudgetGivesOtherPeersATurn(t *testing.T) {
	store := limitedPagingStore(t, false, "source")
	seedPages(t, store, 12)
	var order []string
	g := pageEngine(t, store, pageSyncer{push: func(addr string, req PushRequest) (*PushResponse, error) {
		order = append(order, addr)
		return nil, nil
	}}, 2, "behind", "other")
	if err := g.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(order) != "[behind behind other other]" {
		t.Fatalf("peer order = %v", order)
	}
	for _, peer := range []string{"behind", "other"} {
		s := peerStatusFor(t, g, peer)
		if s.AcknowledgedPosition == 0 || s.OutboundRoundCaughtUp {
			t.Fatalf("budget falsely caught up: %+v", s)
		}
	}
	for i := 0; i < 3; i++ {
		if err := g.SyncOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, peer := range []string{"behind", "other"} {
		if !peerStatusFor(t, g, peer).OutboundRoundCaughtUp {
			t.Fatal("later rounds did not catch up")
		}
	}
}

func TestPushLegacyOversizedEntryIsIsolatedAndCounted(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, valueBytes := range []int{4500, 600 << 10} {
			t.Run(fmt.Sprintf("durable=%v/bytes=%d", durable, valueBytes), func(t *testing.T) {
				ctx := context.Background()
				store := limitedPagingStore(t, durable, "source")
				seedPages(t, store, 1)
				large := newEntryForTest("legacy", make([]byte, valueBytes), "old", 20, Timestamp{WallMS: 1})
				if _, err := store.Apply(ctx, large); err != nil {
					t.Fatal(err)
				}
				seedPages(t, store, 2)
				if d, ok := store.(*DurableStore); ok {
					dir := d.dir
					if err := d.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "source", MaxEntryBytes: 4096})
					if err != nil {
						t.Fatal(err)
					}
					defer reopened.Close()
					store = reopened
				}
				target := pagingStore(t, durable, "target")
				receiver, err := NewGossipEngine(GossipConfig{NodeID: "target", Store: target, Syncer: noopSyncer{}})
				if err != nil {
					t.Fatal(err)
				}
				handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "target", Engine: receiver, AuthToken: "legacy-test"})
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(handler)
				defer server.Close()
				httpSyncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "source", AuthToken: "legacy-test"})
				if err != nil {
					t.Fatal(err)
				}
				sawLegacy := false
				g := pageEngine(t, store, pageSyncer{push: func(_ string, req PushRequest) (*PushResponse, error) {
					for _, e := range req.Entries {
						if e.Key == "legacy" {
							if len(req.Entries) != 1 {
								t.Fatal("legacy entry shared a page")
							}
							sawLegacy = true
						}
					}
					return httpSyncer.Push(ctx, server.URL, req)
				}}, 16, "target")
				if err := g.SyncOnce(ctx); err != nil {
					t.Fatal(err)
				}
				if !sawLegacy || peerStatusFor(t, g, "target").OversizedPushEntries != 1 {
					t.Fatal("oversized push not counted")
				}
				got, ok, err := target.Get(ctx, "legacy")
				if err != nil || !ok || !bytes.Equal(got.Value, large.Value) {
					t.Fatal("legacy value did not replicate")
				}
			})
		}
	}
}

func TestPushTransientRejectionStopsAtConfirmedPrefix(t *testing.T) {
	store := limitedPagingStore(t, false, "source")
	seedPages(t, store, 6)
	calls := 0
	g := pageEngine(t, store, pageSyncer{push: func(_ string, req PushRequest) (*PushResponse, error) {
		calls++
		if calls == 1 {
			return &PushResponse{Rejections: []PushRejection{{Key: req.Entries[1].Key, Transient: true}}}, nil
		}
		return nil, nil
	}}, 16, "target")
	if err := g.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || peerStatusFor(t, g, "target").AcknowledgedPosition != 1 {
		t.Fatal("transient rejection skipped or retried within round")
	}
	if err := g.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !peerStatusFor(t, g, "target").OutboundCaughtUp {
		t.Fatal("retry did not finish")
	}
}

func TestStoreBytePagesBoundCopiesAndPreserveOrder(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			store := limitedPagingStore(t, durable, "source")
			seedPages(t, store, 8)
			paged := store.(BytePageStore)
			var since uint64
			count := 0
			for {
				page, err := paged.ChangesSinceBytes(context.Background(), since, 6000)
				if err != nil {
					t.Fatal(err)
				}
				if len(page) == 0 {
					break
				}
				encoded, _ := json.Marshal(page)
				if len(encoded) > 6002 {
					t.Fatalf("page too large: %d", len(encoded))
				}
				for _, e := range page {
					if e.Position <= since {
						t.Fatal("page out of order")
					}
					since = e.Position
					count++
					e.Value[0] = 'y'
				}
			}
			if count != 8 {
				t.Fatalf("got %d entries", count)
			}
			if mustGet(t, store, "key/00").Value[0] != 'x' {
				t.Fatal("page aliases stored values")
			}
		})
	}
}

func TestPushRoundCoversStartPositionWhileWritesContinue(t *testing.T) {
	store := limitedPagingStore(t, false, "source")
	seedPages(t, store, 6)
	calls := 0
	g := pageEngine(t, store, pageSyncer{push: func(_ string, req PushRequest) (*PushResponse, error) {
		calls++
		if calls == 1 {
			if _, err := store.Put(context.Background(), "later", bytes.Repeat([]byte("y"), 2000)); err != nil {
				t.Fatal(err)
			}
		}
		return nil, nil
	}}, 16, "target")
	if err := g.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := peerStatusFor(t, g, "target")
	if !s.OutboundRoundCaughtUp || s.OutboundCaughtUp || s.AcknowledgedPosition != 6 {
		t.Fatalf("round claimed the wrong position: %+v", s)
	}
}
