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
	"testing"
)

func TestAntiEntropyPagesLargeDigest(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx := context.Background()
			local, remote := pagingStore(t, durable, "local"), pagingStore(t, durable, "remote")
			// Enough metadata to exceed the old single-request 1 MiB limit.
			count, budget := 9000, int64(768<<10)
			if durable {
				count, budget = 128, 1024
			}
			for i := 0; i < count; i++ {
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
				if int64(len(body)) > budget {
					t.Errorf("digest request = %d bytes, want <=%d", len(body), budget)
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
			engine, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: local, Syncer: syncer, MaxDigestBytes: budget, PeerProvider: NewStaticPeerProvider([]string{server.URL})})
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
			t.Logf("%d keys converge in %d HTTP requests, each <=%d bytes", count, requests, budget)
		})
	}
}

// The spy observes only public pull requests/responses and injects transport
// failure at that boundary. Snapshots and digest continuations are distinct.
type digestSpy struct {
	target        *GossipEngine
	requests      []PullRequest
	responses     []*PullResponse
	failAt        int
	responseBytes int64
	before        func(PullRequest)
}

func (s *digestSpy) Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	return s.target.HandlePush(ctx, req)
}
func (s *digestSpy) Pull(ctx context.Context, addr string, req PullRequest) ([]Entry, error) {
	resp, err := s.PullSnapshot(ctx, addr, req)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}
func (s *digestSpy) PullSnapshot(ctx context.Context, _ string, req PullRequest) (*PullResponse, error) {
	if req.StartKey != nil {
		start := *req.StartKey
		req.StartKey = &start
		s.requests = append(s.requests, req)
		if s.before != nil {
			s.before(req)
		}
		if s.failAt > 0 && len(s.requests) == s.failAt {
			return nil, fmt.Errorf("injected digest page failure")
		}
	}
	if s.responseBytes > 0 {
		req.MaxBytes = s.responseBytes
	}
	resp, err := s.target.HandlePullResponse(ctx, req)
	if err == nil && req.StartKey != nil {
		s.responses = append(s.responses, resp)
	}
	return resp, err
}
func newDigestSpyPair(t *testing.T, local, remote Store, budget int64) (*GossipEngine, *digestSpy) {
	t.Helper()
	target, err := NewGossipEngine(GossipConfig{NodeID: "remote", Store: remote, Syncer: noopSyncer{}})
	if err != nil {
		t.Fatal(err)
	}
	spy := &digestSpy{target: target}
	engine, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: local, Syncer: spy, MaxDigestBytes: budget, PeerProvider: NewStaticPeerProvider([]string{"remote"})})
	if err != nil {
		t.Fatal(err)
	}
	return engine, spy
}

func TestDigestPageFailureRestartsWholeRound(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			local, remote := pagingStore(t, false, "local"), pagingStore(t, durable, "remote")
			dir := t.TempDir()
			if durable {
				store, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				local = store
			}
			for i := 0; i < 20; i++ {
				if _, err := local.Put(ctx, fmt.Sprintf("key/%02d", i), []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			engine, spy := newDigestSpyPair(t, local, remote, 512)
			spy.failAt = 3
			if err := engine.AntiEntropyOnce(ctx); err == nil {
				t.Fatal("expected failure")
			}
			status := peerStatusFor(t, engine, "remote")
			if !status.LastPullTime.IsZero() || status.InboundCaughtUp {
				t.Fatal("partial round reported pull success")
			}
			if len(spy.requests) != 3 {
				t.Fatalf("requests=%d", len(spy.requests))
			}
			first := *spy.requests[0].StartKey
			spy.failAt = 0
			if durable {
				if err := local.(*DurableStore).Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local"})
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				engine, err = NewGossipEngine(GossipConfig{NodeID: "local", Store: recovered, Syncer: spy, MaxDigestBytes: 512, PeerProvider: NewStaticPeerProvider([]string{"remote"})})
				if err != nil {
					t.Fatal(err)
				}
			}
			clearPeerBackoffForTest(engine, "remote")
			spy.before = func(PullRequest) {
				if !peerStatusFor(t, engine, "remote").LastPullTime.IsZero() {
					t.Error("success recorded before all intervals completed")
				}
			}
			if err := engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if *spy.requests[3].StartKey != first || spy.requests[3].AfterKey != "" {
				t.Fatal("next round resumed partial coverage")
			}
			if !peerStatusFor(t, engine, "remote").InboundCaughtUp {
				t.Fatal("completed retry not recorded")
			}
		})
	}
}

func TestDigestDiffPagesCoverMissingKeysAndEmptyIntervals(t *testing.T) {
	ctx := context.Background()
	local, remote := pagingStore(t, false, "local"), pagingStore(t, false, "remote")
	// Deliberately insert in reverse position order; diff must use key order.
	for i := 39; i >= 0; i-- {
		if _, err := remote.Put(ctx, fmt.Sprintf("key/%02d", i), bytes.Repeat([]byte("x"), 300)); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"key/10", "key/20", "key/30"} {
		if _, err := local.Put(ctx, key, []byte("older")); err != nil {
			t.Fatal(err)
		}
		if _, err := remote.Put(ctx, key, []byte("newer")); err != nil {
			t.Fatal(err)
		}
	}
	engine, spy := newDigestSpyPair(t, local, remote, 512)
	spy.responseBytes = 1100
	if err := engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	continuations := 0
	end := ""
	for i, req := range spy.requests {
		if req.AfterKey == "" {
			if *req.StartKey != end {
				t.Fatal("gap between intervals")
			}
			end = req.EndKey
		} else {
			continuations++
			if i == 0 || *req.StartKey != *spy.requests[i-1].StartKey {
				t.Fatal("continuation changed interval")
			}
		}
		encoded, _ := json.Marshal(req)
		if len(encoded) > 512 {
			t.Fatalf("request bytes=%d", len(encoded))
		}
	}
	if end != "" || continuations == 0 {
		t.Fatal("missing final unbounded interval or diff continuations")
	}
	for _, resp := range spy.responses {
		encoded, _ := json.Marshal(resp)
		if len(encoded)+1 > 1100 {
			t.Fatal("oversized response")
		}
		for _, entry := range resp.Entries {
			seen[entry.Key]++
		}
	}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key/%02d", i)
		if seen[key] != 1 {
			t.Fatalf("%s returned %d times", key, seen[key])
		}
		want := mustGet(t, remote, key)
		got := mustGet(t, local, key)
		if got.Identity() != want.Identity() {
			t.Fatalf("mismatch %s", key)
		}
	}
	// Empty digest is still a digest interval, so a horizon handshake is required.
	empty := pagingStore(t, false, "empty")
	emptyEngine, emptySpy := newDigestSpyPair(t, empty, remote, 512)
	emptySpy.responseBytes = 1100
	if err := emptyEngine.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(emptySpy.requests) < 2 || *emptySpy.requests[0].StartKey != "" || emptySpy.requests[0].EndKey != "" {
		t.Fatal("empty digest did not page the whole key space")
	}
}

func TestCollectedDigestValueAtRangeBoundaryForcesResync(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			remote, stale := newCollectedServerStore(t, clock, 0, 0)
			var local gcTestStore
			if durable {
				store, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", PhysicalClock: clock, TombstoneRetention: testRetention})
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				local = store
			} else {
				local = mustGCMemoryStore(t, "local", clock, testRetention)
			}
			if _, err := local.Apply(ctx, stale); err != nil {
				t.Fatal(err)
			}
			// Keep the stale value while advancing our horizon beyond the responder's.
			// A 410 must come from mechanism 2, not the lower-horizon handshake.
			if _, err := local.Delete(ctx, "unrelated"); err != nil {
				t.Fatal(err)
			}
			clock.now += 2 * retentionMS()
			collector := local.(interface {
				CollectTombstones(context.Context) (TombstoneGCResult, error)
			})
			if _, err := collector.CollectTombstones(ctx); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a", "c", "e", "z"} {
				if _, err := local.Put(ctx, key, []byte("new")); err != nil {
					t.Fatal(err)
				}
			}
			engine, spy := newDigestSpyPair(t, local, remote, 512)
			start := ""
			req := PullRequest{PolicyID: remote.Policy().ID(), SourceNodeID: "local", GCHorizon: timestampPtr(local.GCHorizon()), StartKey: &start, EndKey: "doomed", Digest: map[string]DigestRecord{}}
			if _, err := spy.target.HandlePullResponse(ctx, req); err != nil {
				t.Fatalf("exclusive end checked outside interval: %v", err)
			}
			start = "doomed"
			req.EndKey = "e"
			req.Digest = map[string]DigestRecord{"doomed": newDigestRecord(stale, remote.Policy())}
			for _, cursor := range []string{"", "doomed"} {
				req.AfterKey = cursor
				_, err := spy.target.HandlePullResponse(ctx, req)
				var resync *ResyncRequiredError
				if !errors.As(err, &resync) {
					t.Fatalf("inclusive boundary/cursor %q missed collected key: %v", cursor, err)
				}
			}
			// Exercise the same refusal over actual HTTP, including an empty digest range.
			handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "remote", Engine: spy.target, AuthToken: "digest-test"})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "local", AuthToken: "digest-test"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = syncer.PullSnapshot(ctx, server.URL, req)
			var resync *ResyncRequiredError
			if !errors.As(err, &resync) {
				t.Fatalf("expected HTTP 410: %v", err)
			}
			req.Digest = nil
			req.GCHorizon = nil
			req.AfterKey = ""
			_, err = syncer.PullSnapshot(ctx, server.URL, req)
			if !errors.As(err, &resync) {
				t.Fatalf("empty interval bypassed horizon handshake: %v", err)
			}
			if err := engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			assertAbsent(t, local, "doomed")
			if !peerStatusFor(t, engine, "remote").LastPullTime.IsZero() {
				t.Fatal("resync counted as successful digest round")
			}
			var boundary bool
			for _, request := range spy.requests {
				if _, ok := request.Digest["doomed"]; ok {
					boundary = *request.StartKey == "doomed"
					if len(request.Digest) != 1 {
						t.Fatal("expected one-key extreme paging")
					}
				}
			}
			if !boundary {
				t.Fatal("collected key was not checked at inclusive page boundary")
			}
			before := len(spy.requests)
			if err := engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if len(spy.requests) <= before || *spy.requests[before].StartKey != "" {
				t.Fatal("post-resync round did not restart at first interval")
			}
		})
	}
}

func TestDigestLimitsFailVisibly(t *testing.T) {
	ctx := context.Background()
	for _, limit := range []int64{-1, 1 << 20, 2 << 20} {
		_, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: pagingStore(t, false, "local"), Syncer: noopSyncer{}, MaxDigestBytes: limit})
		if err == nil {
			t.Fatalf("accepted digest budget %d", limit)
		}
	}
	local, remote := pagingStore(t, false, "local"), pagingStore(t, false, "remote")
	if _, err := local.Put(ctx, strings.Repeat("\\\"<", 400), []byte("v")); err != nil {
		t.Fatal(err)
	}
	engine, _ := newDigestSpyPair(t, local, remote, 512)
	err := engine.AntiEntropyOnce(ctx)
	var digestLarge *DigestEntryTooLargeError
	if !errors.As(err, &digestLarge) {
		t.Fatalf("expected digest size error: %v", err)
	}
	if !peerStatusFor(t, engine, "remote").LastPullTime.IsZero() {
		t.Fatal("oversized record counted as success")
	}
	if _, err := remote.Put(ctx, "large", bytes.Repeat([]byte("x"), 2000)); err != nil {
		t.Fatal(err)
	}
	start := ""
	// Test the responder containing the oversized diff, independently of requester metadata.
	target, err := NewGossipEngine(GossipConfig{NodeID: "remote", Store: remote, Syncer: noopSyncer{}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = target.HandlePullResponse(ctx, PullRequest{PolicyID: remote.Policy().ID(), StartKey: &start, MaxBytes: 500})
	var entryLarge *PullEntryTooLargeError
	if !errors.As(err, &entryLarge) || entryLarge.Key != "large" {
		t.Fatalf("expected typed diff size error: %v", err)
	}
}

func TestDigestProtocolRejectsPreviousMajor(t *testing.T) {
	node := newHTTPTestNode(t, "remote", "digest-test", &fixedPhysicalClock{now: 1000})
	defer node.server.Close()
	for _, path := range []string{PushPath, PullPath} {
		body := `{"protocol_version":{"major":1,"minor":0},"source":"local","policy_id":"outpostnet.state.hlc-lww/v1","entries":[]}`
		req, err := http.NewRequest(http.MethodPost, node.server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer digest-test")
		req.Header.Set("X-Outpost-Node-ID", "local")
		resp, err := node.server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUpgradeRequired {
			t.Fatalf("%s: status %d", path, resp.StatusCode)
		}
	}
	// The client must also reject an old responder, before applying its entries.
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"protocol_version":{"major":1,"minor":0},"policy_id":"outpostnet.state.hlc-lww/v1","entries":[]}`)
	}))
	defer old.Close()
	_, err := node.syncer.PullSnapshot(context.Background(), old.URL, PullRequest{})
	if err == nil || !strings.Contains(err.Error(), "version mismatch") {
		t.Fatalf("accepted old responder: %v", err)
	}
}

func TestHTTPSyncerPullDrainsDigestDiff(t *testing.T) {
	ctx := context.Background()
	remote := pagingStore(t, false, "remote")
	for i := 0; i < 10; i++ {
		if _, err := remote.Put(ctx, fmt.Sprintf("key/%02d", i), bytes.Repeat([]byte("x"), 300)); err != nil {
			t.Fatal(err)
		}
	}
	target, err := NewGossipEngine(GossipConfig{NodeID: "remote", Store: remote, Syncer: noopSyncer{}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "remote", Engine: target, AuthToken: "digest-test"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "local", AuthToken: "digest-test", MaxResponseBytes: 1100})
	if err != nil {
		t.Fatal(err)
	}
	start := ""
	entries, err := syncer.Pull(ctx, server.URL, PullRequest{StartKey: &start, MaxBytes: 1100})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("Pull silently lost diff entries: got %d, want 10", len(entries))
	}
}
