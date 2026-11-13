package state

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func pagingStore(t *testing.T, durable bool, id string) Store {
	t.Helper()
	if !durable {
		return mustStore(t, id, nil, nil)
	}
	s, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPushPagesCatchUpLargeBacklog(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx := context.Background()
			source := pagingStore(t, durable, "source")
			target := pagingStore(t, durable, "target")
			for i := 0; i < 2000; i++ {
				if _, err := source.Put(ctx, fmt.Sprintf("key/%04d", i), bytes.Repeat([]byte{byte(i)}, 4096)); err != nil {
					t.Fatal(err)
				}
			}
			receiver, err := NewGossipEngine(GossipConfig{NodeID: "target", Store: target, Syncer: noopSyncer{}})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "target", Engine: receiver, AuthToken: "paging-test"})
			if err != nil {
				t.Fatal(err)
			}
			var sizesMu sync.Mutex
			var sizes []int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				sizesMu.Lock()
				sizes = append(sizes, len(body))
				sizesMu.Unlock()
				r.Body = io.NopCloser(bytes.NewReader(body))
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "source", AuthToken: "paging-test"})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := NewGossipEngine(GossipConfig{NodeID: "source", Store: source, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{server.URL}), MaxPushPagesPerRound: 2})
			if err != nil {
				t.Fatal(err)
			}
			rounds := 0
			for round := 0; round < 32; round++ {
				if err := engine.SyncOnce(ctx); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
				rounds++
				if status := peerStatusFor(t, engine, server.URL); status.OutboundCaughtUp {
					break
				}
			}
			if !peerStatusFor(t, engine, server.URL).OutboundCaughtUp {
				t.Fatal("backlog never caught up")
			}
			sizesMu.Lock()
			defer sizesMu.Unlock()
			if rounds < 2 {
				t.Fatal("expected multiple capped rounds")
			}
			t.Logf("8 MiB backlog: %d rounds, %d requests, each <= 768 KiB", rounds, len(sizes))
			if len(sizes) < 2 {
				t.Fatalf("requests = %d, want multiple pages", len(sizes))
			}
			for _, size := range sizes {
				if size > 768<<10 {
					t.Fatalf("request = %d bytes, above 768 KiB", size)
				}
			}
			want, _ := source.ListEntries(ctx)
			got, err := target.ListEntries(ctx)
			if err != nil || len(got) != len(want) {
				t.Fatalf("target entries = %d, want %d: %v", len(got), len(want), err)
			}
			for _, entry := range want {
				other, found, err := target.Get(ctx, entry.Key)
				if err != nil || !found || other.Identity() != entry.Identity() {
					t.Fatalf("mismatch for %s: found=%v err=%v", entry.Key, found, err)
				}
			}
		})
	}
}
