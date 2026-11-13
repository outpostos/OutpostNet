package state

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAntiEntropyPagesLargeDigest(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx := context.Background()
			local, remote := pagingStore(t, durable, "local"), pagingStore(t, durable, "remote")
			// Enough metadata to exceed the old single-request 1 MiB limit.
			for i := 0; i < 9000; i++ {
				key := fmt.Sprintf("key/%05d", i)
				old, err := local.Put(ctx, key, []byte("old"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = remote.Apply(ctx, old); err != nil {
					t.Fatal(err)
				}
				if _, err = remote.Put(ctx, key, []byte("new")); err != nil {
					t.Fatal(err)
				}
			}
			receiver, err := NewGossipEngine(GossipConfig{NodeID: "remote", Store: remote, Syncer: noopSyncer{}})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "remote", Engine: receiver, AuthToken: "digest-test"})
			if err != nil {
				t.Fatal(err)
			}
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				requests++
				if len(body) > 768<<10 {
					t.Errorf("digest request = %d bytes, want <=768 KiB", len(body))
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, r)
				if rec.Body.Len() > 1<<20 {
					t.Errorf("response = %d bytes, want <=1 MiB", rec.Body.Len())
				}
				for k, v := range rec.Header() {
					w.Header()[k] = v
				}
				w.WriteHeader(rec.Code)
				_, _ = w.Write(rec.Body.Bytes())
			}))
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "local", AuthToken: "digest-test"})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: local, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{server.URL})})
			if err != nil {
				t.Fatal(err)
			}
			if err = engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if requests < 2 {
				t.Fatalf("got %d requests, want multiple pages", requests)
			}
			want, err := remote.ListEntries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range want {
				got, found, err := local.Get(ctx, entry.Key)
				if err != nil || !found || got.Identity() != entry.Identity() {
					t.Fatalf("did not converge at %s: %v", entry.Key, err)
				}
			}
			if !peerStatusFor(t, engine, server.URL).InboundCaughtUp {
				t.Fatal("completed round not recorded")
			}
			t.Logf("9000 keys converge in %d bounded HTTP requests", requests)
		})
	}
}
