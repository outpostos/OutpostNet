package state

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Four real engines and HTTP adapters. Two servers accept pushes but withhold
// responses; neither connection refusal nor a fake Syncer can satisfy this test.
func TestGossipRunPushIsolatesBlackholedPeers(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%t", durable), func(t *testing.T) {
			const interval = 200 * time.Millisecond
			const processing = 100 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clock := &fixedPhysicalClock{now: 1_000_000}
			nodes := make([]*concurrentNode, 4)
			for i := range nodes {
				id := fmt.Sprintf("peer-%d", i)
				nodes[i] = newConcurrentNode(t, id, clockTestStore(t, durable, id, clock))
				nodes[i].engine.interval = interval
				nodes[i].engine.antiEntropyInterval = time.Hour // delivery must be push
			}
			var blackhole atomic.Bool
			release := make(chan struct{})
			var entered [2]atomic.Int32
			started := make(chan struct{}, 8)
			peers := []string{nodes[1].addr} // healthy peer first: test the NEXT tick too
			for i := range entered {
				handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: fmt.Sprintf("peer-%d", i+2), Engine: nodes[i+2].engine, AuthToken: "concurrency-test"})
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == PushPath && blackhole.Load() {
						entered[i].Add(1)
						started <- struct{}{}
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					}
					handler.ServeHTTP(w, r)
				}))
				t.Cleanup(server.Close)
				peers = append(peers, server.URL)
			}
			// Release handlers before closing servers, even if the regression fails.
			t.Cleanup(func() { close(release) })
			nodes[0].peers.Store(peers)
			put := func(key string) Entry {
				t.Helper()
				e, err := nodes[0].store.Put(ctx, key, []byte(key))
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			put("baseline")
			if err := nodes[0].engine.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			blackhole.Store(true)
			put("starts-blackholes")
			done := asyncRound(nodes[0].engine.Run, ctx)
			t.Cleanup(func() { cancel(); awaitRound(t, done) })
			awaitSignal(t, started)
			for n := 0; n < 3; n++ {
				key := fmt.Sprintf("during-blackholes-%d", n)
				before := time.Now()
				e := put(key)
				deadline := time.NewTimer(interval + processing)
				poll := time.NewTicker(time.Millisecond)
				for {
					got, found, err := nodes[1].store.Get(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					if found {
						if string(got.Value) != key || got.ChangeID != e.ChangeID {
							t.Fatalf("wrong replicated entry: %+v", got)
						}
						break
					}
					select {
					case <-deadline.C:
						t.Fatalf("reachable peer did not receive %s within one push interval (%s) plus processing (%s) while other connections hang", key, interval, processing)
					case <-poll.C:
					}
				}
				deadline.Stop()
				poll.Stop()
				t.Logf("%s delivered in %s (interval %s + processing %s)", key, time.Since(before), interval, processing)
			}
			for i := range entered {
				if got := entered[i].Load(); got != 1 {
					t.Fatalf("blackholed peer %d: %d pushes, want exactly one across ticks", i, got)
				}
				p := peerStatusFor(t, nodes[0].engine, peers[i+1])
				if p.AcknowledgedPosition != 1 || p.ConsecutiveFailures != 0 {
					t.Fatalf("unanswered push advanced cursor or failed early: %+v", p)
				}
			}
			// Healthy acknowledgements advance independently while the others hang.
			until := time.Now().Add(processing)
			for peerStatusFor(t, nodes[0].engine, peers[0]).AcknowledgedPosition < nodes[0].store.CurrentPosition() {
				if time.Now().After(until) {
					t.Fatal("healthy push acknowledgement did not advance")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

// Manual rounds must also fan out, join every exchange, report failures, and
// keep failing peers' backoff/cursors separate from healthy acknowledgements.
func TestGossipSyncOncePeersProgressIndependently(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	a := newConcurrentNode(t, "a", clockTestStore(t, true, "a", clock))
	b := newConcurrentNode(t, "b", clockTestStore(t, true, "b", clock))
	a.peers.Store([]string{"blackhole-a", "blackhole-b", b.addr, "blackhole-a"})
	held := make(chan struct{}, 2)
	release := make(chan struct{})
	var requests atomic.Int32
	a.wire.push = func(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
		if addr == b.addr {
			return a.wire.HTTPSyncer.Push(ctx, addr, req)
		}
		requests.Add(1)
		held <- struct{}{}
		select {
		case <-release:
			return nil, fmt.Errorf("unreachable %s", addr)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	e, err := a.store.Put(ctx, "first", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	done := asyncRound(a.engine.SyncOnce, ctx)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	joined := false
	// Always release and join before stores and HTTP servers close.
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-done
		}
	})
	awaitSignal(t, held)
	awaitSignal(t, held)
	deadline := time.Now().Add(time.Second)
	for peerStatusFor(t, a.engine, b.addr).AcknowledgedPosition < e.Position {
		if time.Now().After(deadline) {
			t.Fatal("manual push to healthy peer waited for blackholes")
		}
		time.Sleep(time.Millisecond)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("duplicate peer overlapped: %d requests", got)
	}
	select {
	case <-done:
		joined = true
		t.Fatal("SyncOnce did not join outstanding pushes")
	default:
	}
	// Release the two failures, then test the next round with their backoff.
	unblock()
	select {
	case err := <-done:
		joined = true
		if err == nil || !strings.Contains(err.Error(), "unreachable blackhole-a") || !strings.Contains(err.Error(), "unreachable blackhole-b") {
			t.Fatalf("missing peer errors: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manual round did not join")
	}
	for _, addr := range []string{"blackhole-a", "blackhole-b"} {
		p := peerStatusFor(t, a.engine, addr)
		if p.AcknowledgedPosition != 0 || p.ConsecutiveFailures != 1 || p.NextAttemptTime.IsZero() {
			t.Fatalf("bad failed-peer state: %+v", p)
		}
	}
	second, err := a.store.Put(ctx, "second", []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatal("backoff did not skip failing peers")
	}
	if p := peerStatusFor(t, a.engine, b.addr); p.AcknowledgedPosition != second.Position || p.ConsecutiveFailures != 0 || !p.OutboundRoundCaughtUp {
		t.Fatalf("healthy peer did not progress: %+v", p)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.engine.SyncOnce(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled manual round returned %v", err)
	}
}

type failingPushReadStore struct {
	Store
	reads   atomic.Int32
	started <-chan struct{}
	failure error
}

func (s *failingPushReadStore) ChangesSinceBytes(ctx context.Context, position uint64, limit int64) ([]Entry, error) {
	if s.reads.Add(1) == 1 {
		return s.Store.(BytePageStore).ChangesSinceBytes(ctx, position, limit)
	}
	select {
	case <-s.started:
		return nil, s.failure
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestGossipSyncOnceLocalFailureCancelsAndJoins(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	failure := errors.New("local page read failed")
	store := &failingPushReadStore{Store: clockTestStore(t, false, "a", &fixedPhysicalClock{now: 1_000_000}), started: started, failure: failure}
	a := newConcurrentNode(t, "a", store)
	a.peers.Store([]string{"one", "two"})
	a.wire.push = func(ctx context.Context, _ string, _ PushRequest) (*PushResponse, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release // transport cleanup must finish before SyncOnce returns
		return nil, ctx.Err()
	}
	if _, err := store.Put(context.Background(), "pending", []byte("value")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := asyncRound(a.engine.SyncOnce, ctx)
	joined := false
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-done
		}
	})
	awaitSignal(t, canceled)
	select {
	case <-done:
		joined = true
		t.Fatal("local error returned before canceled peer finished")
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, failure) {
			t.Fatalf("lost local store error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled round did not join")
	}
}
