package state

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Hooks control transport completion, while actual exchanges still use the
// authenticated HTTP adapter and real stores. Install hooks before starting work.
type concurrentWire struct {
	*HTTPSyncer
	push func(context.Context, string, PushRequest) (*PushResponse, error)
	pull func(context.Context, string, PullRequest) (*PullResponse, error)
}

func (w *concurrentWire) Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
	if w.push != nil {
		return w.push(ctx, addr, req)
	}
	return w.HTTPSyncer.Push(ctx, addr, req)
}

func (w *concurrentWire) PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
	if w.pull != nil {
		return w.pull(ctx, addr, req)
	}
	return w.HTTPSyncer.PullSnapshot(ctx, addr, req)
}

type concurrentNode struct {
	store  Store
	engine *GossipEngine
	wire   *concurrentWire
	peers  atomicPeerList
	addr   string
}

func newConcurrentNode(t *testing.T, id string, store Store) *concurrentNode {
	t.Helper()
	wire, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: id, AuthToken: "concurrency-test"})
	if err != nil {
		t.Fatal(err)
	}
	n := &concurrentNode{store: store, wire: &concurrentWire{HTTPSyncer: wire}}
	n.engine, err = NewGossipEngine(GossipConfig{
		NodeID: id, Store: store, Syncer: n.wire,
		PeerProvider: PeerProviderFunc(n.peers.Load),
		Interval:     5 * time.Millisecond, AntiEntropyInterval: 5 * time.Millisecond,
		BackoffBase: time.Hour, BackoffMax: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: id, Engine: n.engine, AuthToken: "concurrency-test"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	n.addr = server.URL
	return n
}

func asyncRound(round func(context.Context) error, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- round(ctx) }()
	return done
}

func awaitRound(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("round did not finish")
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("exchange did not start")
	}
}

func TestGossipRunPushDoesNotWaitForDigest(t *testing.T) {
	testPushDoesNotWaitForDigest(t, true)
}

func TestConcurrentGossipPushDoesNotWaitForDigest(t *testing.T) {
	testPushDoesNotWaitForDigest(t, false)
}

func TestGossipRunPullDoesNotWaitForPushAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := &fixedPhysicalClock{now: 1_000_000}
	a := newConcurrentNode(t, "a", clockTestStore(t, false, "a", clock))
	b := newConcurrentNode(t, "b", clockTestStore(t, false, "b", clock))
	a.peers.Store([]string{b.addr})
	if _, err := a.store.Put(ctx, "pending", []byte("push")); err != nil {
		t.Fatal(err)
	}
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	a.wire.push = func(ctx context.Context, _ string, _ PushRequest) (*PushResponse, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		// Model a transport's cleanup after cancellation. Run must join it.
		<-release
		return nil, ctx.Err()
	}
	done := asyncRound(a.engine.Run, ctx)
	t.Cleanup(func() { cancel(); unblock(); awaitRound(t, done) })
	awaitSignal(t, started)
	if _, err := b.store.Put(ctx, "during-push", []byte("pulled")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		if _, found, err := a.store.Get(ctx, "during-push"); err != nil {
			t.Fatal(err)
		} else if found {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run pull waited for held-open push")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	awaitSignal(t, canceled)
	select {
	case <-done:
		t.Fatal("Run returned before its push loop finished")
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
}

func testPushDoesNotWaitForDigest(t *testing.T, run bool) {
	t.Helper()
	clock := &fixedPhysicalClock{now: 1_000_000}
	a := newConcurrentNode(t, "a", clockTestStore(t, false, "a", clock))
	b := newConcurrentNode(t, "b", clockTestStore(t, false, "b", clock))
	a.peers.Store([]string{b.addr})
	held := make(chan struct{})
	var once sync.Once
	a.wire.pull = func(ctx context.Context, _ string, _ PullRequest) (*PullResponse, error) {
		once.Do(func() { close(held) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := asyncRound(func(ctx context.Context) error {
		if run {
			return a.engine.Run(ctx)
		}
		err := a.engine.AntiEntropyOnce(ctx)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}, ctx)
	t.Cleanup(func() { cancel(); awaitRound(t, done) })
	awaitSignal(t, held)
	if _, err := a.store.Put(ctx, "during-digest", []byte("pushed")); err != nil {
		t.Fatal(err)
	}
	if !run {
		awaitRound(t, asyncRound(a.engine.SyncOnce, ctx))
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		_, found, err := b.store.Get(ctx, "during-digest")
		if err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("Run push waited for held-open digest")
		case <-poll.C:
		}
	}
	status := peerStatusFor(t, a.engine, b.addr)
	if !status.LastPullTime.IsZero() || status.InboundCaughtUp {
		t.Fatalf("unfinished digest claimed inbound catch-up: %+v", status)
	}
}

// Every worker owns one non-reentrant round method. Other workers exercise
// inbound handlers, local writes and status reads on the very same engines.
func concurrentRounds(t *testing.T, ctx context.Context, workers ...func(context.Context) error) {
	t.Helper()
	var pending []<-chan error
	for _, worker := range workers {
		pending = append(pending, asyncRound(func(ctx context.Context) error {
			for i := 0; i < 8; i++ {
				if err := worker(ctx); err != nil {
					return err
				}
			}
			return nil
		}, ctx))
	}
	for _, done := range pending {
		awaitRound(t, done)
	}
}

func progressReader(g *GossipEngine) func(context.Context) error {
	previous := make(map[string]uint64)
	return func(ctx context.Context) error {
		s, err := g.ReplicationStatus(ctx)
		if err != nil {
			return err
		}
		for _, p := range s.Peers {
			if p.AcknowledgedPosition < previous[p.Peer] || p.GCConfirmedPosition > p.AcknowledgedPosition {
				return fmt.Errorf("invalid cursor progression: %+v, previous %d", p, previous[p.Peer])
			}
			if p.RefusedPositions > 0 && (p.OutboundCaughtUp || p.OutboundRoundCaughtUp || p.GCConfirmedPosition >= p.LowestRefusedPosition) {
				return fmt.Errorf("refused position claimed confirmed: %+v", p)
			}
			previous[p.Peer] = p.AcknowledgedPosition
		}
		return nil
	}
}

func assertConverged(t *testing.T, ctx context.Context, nodes ...*concurrentNode) {
	t.Helper()
	// Quiescent rounds settle writes that arrived after a preceding round's
	// start position. Compare identities and payloads, excluding local positions.
	for i := 0; i < 3; i++ {
		for _, n := range nodes {
			if err := n.engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if err := n.engine.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	var want map[string]Entry
	for _, n := range nodes {
		entries, err := n.store.ListEntries(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := make(map[string]Entry)
		for _, e := range entries {
			e.Position = 0
			got[e.Key] = e
		}
		if want == nil {
			want = got
		} else if !reflect.DeepEqual(want, got) {
			t.Fatalf("replicas diverged: want %v, got %v", want, got)
		}
		s, err := n.engine.ReplicationStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range s.Peers {
			// Resync can allocate a floor with no entry to push. The round
			// claim still holds even if the raw cursor cannot cover that floor.
			if !p.InboundCaughtUp || !p.OutboundRoundCaughtUp || p.CaughtUp != (p.OutboundCaughtUp && p.InboundCaughtUp) || p.LastPullTime.IsZero() || p.LastSuccessTime.IsZero() || p.ConsecutiveFailures != 0 || !p.NextAttemptTime.IsZero() {
				t.Fatalf("converged replica has incorrect status: %+v", p)
			}
		}
	}
}

func TestConcurrentGossipResyncUnderWrites(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			clock := &fixedPhysicalClock{now: 1_000_000}
			as := clockTestStore(t, durable, "a", clock)
			bs := clockTestStore(t, durable, "b", clock)
			stale := newEntryForTest("collected", []byte("stale"), "a", 1, Timestamp{WallMS: 999_000})
			if _, err := as.Apply(ctx, stale); err != nil {
				t.Fatal(err)
			}
			if _, err := bs.Delete(ctx, stale.Key); err != nil {
				t.Fatal(err)
			}
			clock.now += 61_000
			if gc, err := bs.(interface {
				CollectTombstones(context.Context) (TombstoneGCResult, error)
			}).CollectTombstones(ctx); err != nil || gc.Collected != 1 {
				t.Fatalf("GC setup: %+v %v", gc, err)
			}
			a := newConcurrentNode(t, "a", as)
			b := newConcurrentNode(t, "b", bs)
			c := newConcurrentNode(t, "c", clockTestStore(t, durable, "c", clock))
			a.peers.Store([]string{b.addr, c.addr})
			b.peers.Store([]string{a.addr, c.addr})
			c.peers.Store([]string{a.addr, b.addr})
			snapshot, release := make(chan struct{}), make(chan struct{})
			var held atomic.Bool
			var gone atomic.Int32
			a.wire.push = func(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
				resp, err := a.wire.HTTPSyncer.Push(ctx, addr, req)
				var resync *ResyncRequiredError
				if errors.As(err, &resync) {
					gone.Add(1)
				}
				return resp, err
			}
			a.wire.pull = func(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
				if req.StartKey == nil && held.CompareAndSwap(false, true) {
					close(snapshot)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return a.wire.HTTPSyncer.PullSnapshot(ctx, addr, req)
			}
			push := asyncRound(a.engine.SyncOnce, ctx)
			awaitSignal(t, snapshot)
			// The pull must complete its own 410 resync while the push's
			// snapshot remains held, sharing the same store and horizon.
			awaitRound(t, asyncRound(a.engine.AntiEntropyOnce, ctx))
			if p := peerStatusFor(t, a.engine, b.addr); !p.LastPullTime.IsZero() || p.InboundCaughtUp || p.LastSuccessTime.IsZero() {
				t.Fatalf("resync alone claimed a completed digest: %+v", p)
			}
			write := 0
			writer := func(ctx context.Context) error {
				for _, n := range []*concurrentNode{a, b, c} {
					if _, err := n.store.Put(ctx, fmt.Sprintf("%s/write/%d", n.addr, write), []byte("live")); err != nil {
						return err
					}
				}
				write++
				return nil
			}
			concurrentRounds(t, ctx, b.engine.SyncOnce, b.engine.AntiEntropyOnce, c.engine.SyncOnce, c.engine.AntiEntropyOnce, writer, progressReader(a.engine))
			close(release)
			awaitRound(t, push)
			concurrentRounds(t, ctx, a.engine.SyncOnce, a.engine.AntiEntropyOnce, b.engine.SyncOnce, b.engine.AntiEntropyOnce, c.engine.SyncOnce, c.engine.AntiEntropyOnce, writer, progressReader(a.engine))
			assertConverged(t, ctx, a, b, c)
			for _, n := range []*concurrentNode{a, b, c} {
				assertAbsent(t, n.store, stale.Key)
				for _, origin := range []*concurrentNode{a, b, c} {
					for i := 0; i < 16; i++ {
						assertStoredValue(t, n.store, fmt.Sprintf("%s/write/%d", origin.addr, i), "live")
					}
				}
			}
			if gone.Load() == 0 || write != 16 {
				t.Fatal("410 or concurrent writes were not exercised")
			}
		})
	}
}

type concurrentClock struct{ now atomic.Int64 }

func (c *concurrentClock) NowMS() int64 { return c.now.Load() }

func TestConcurrentGossipTimeReference(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var nodes []*concurrentNode
			var refs []*TimeReference
			for i, id := range []string{"a", "b"} {
				ref, err := NewTimeReference(TimeReferenceConfig{SystemClock: &fixedPhysicalClock{now: 1_000_000 + int64(i)*600_000}})
				if err != nil {
					t.Fatal(err)
				}
				if id == "b" {
					if err := ref.UpdateSource(TimeSourceStatus{Synchronized: true, Uncertainty: time.Millisecond}); err != nil {
						t.Fatal(err)
					}
				}
				var store Store
				if durable {
					d, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: id, TimeReference: ref})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = d.Close() })
					store = d
				} else {
					store, err = NewMemoryStore(MemoryStoreConfig{NodeID: id, TimeReference: ref})
					if err != nil {
						t.Fatal(err)
					}
				}
				nodes = append(nodes, newConcurrentNode(t, id, store))
				refs = append(refs, ref)
			}
			a, b := nodes[0], nodes[1]
			a.peers.Store([]string{b.addr})
			b.peers.Store([]string{a.addr})
			write := 0
			writer := func(ctx context.Context) error {
				for i, n := range nodes {
					if _, err := n.store.Put(ctx, fmt.Sprintf("node%d/%d", i, write), []byte("ok")); err != nil {
						return err
					}
				}
				write++
				return nil
			}
			concurrentRounds(t, ctx, a.engine.SyncOnce, a.engine.AntiEntropyOnce, b.engine.SyncOnce, b.engine.AntiEntropyOnce, writer, progressReader(a.engine))
			assertConverged(t, ctx, a, b)
			if s := refs[0].TimeStatus(); !s.UsingPeerReference || !s.ClockSuspect || s.AcceptedSamples == 0 {
				t.Fatalf("concurrent exchanges lost the healthy peer reference: %+v", s)
			}
			if s := refs[1].TimeStatus(); s.Quality != TimeSynced || s.UsingPeerReference || s.ClockSuspect {
				t.Fatalf("healthy peer incorrectly self-suspected: %+v", s)
			}
		})
	}
}

func TestConcurrentGossipRefusalProgressRecovery(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			aClock := &fixedPhysicalClock{now: 2_320_000}
			bClock := &concurrentClock{}
			bClock.now.Store(1_000_000)
			var source Store
			cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "a", PhysicalClock: aClock, TombstoneRetention: time.Minute}
			if durable {
				d, err := NewDurableStore(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				source = d
			} else {
				source = clockTestStore(t, false, "a", aClock)
			}
			for i, wall := range []int64{1_600_000, 2_200_000} {
				e := newEntryForTest(fmt.Sprintf("refused/%d", i), nil, "ahead-writer", uint64(i+1), Timestamp{WallMS: wall})
				e.Deleted = true
				if _, err := source.Apply(ctx, e); err != nil {
					t.Fatal(err)
				}
			}
			first := mustGet(t, source, "refused/0")
			second := mustGet(t, source, "refused/1")
			a := newConcurrentNode(t, "a", source)
			b := newConcurrentNode(t, "b", clockTestStore(t, durable, "b", bClock))
			a.peers.Store([]string{b.addr})
			b.peers.Store([]string{a.addr})
			if err := a.engine.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if p := peerStatusFor(t, a.engine, b.addr); p.AcknowledgedPosition != second.Position || p.RefusedPositions != 2 || p.LowestRefusedPosition != first.Position || p.GCConfirmedPosition != first.Position-1 {
				t.Fatalf("initial refusal set: %+v", p)
			}
			// The first tombstone becomes admissible; the second remains refused.
			// B's next digest confirms the first while A saves new push progress.
			bClock.now.Store(1_600_000)
			write := 0
			writer := func(ctx context.Context) error {
				e := newEntryForTest(fmt.Sprintf("healthy/%d", write), []byte("ok"), "healthy-writer", uint64(write+1), Timestamp{WallMS: 1_600_000})
				write++
				if _, err := a.store.Apply(ctx, e); err != nil {
					return err
				}
				_, err := b.store.Put(ctx, fmt.Sprintf("b/%d", write), []byte("ok"))
				return err
			}
			concurrentRounds(t, ctx, a.engine.SyncOnce, a.engine.AntiEntropyOnce, b.engine.SyncOnce, b.engine.AntiEntropyOnce, writer, progressReader(a.engine))
			for i := 0; i < 2; i++ {
				if err := b.engine.AntiEntropyOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.engine.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			before := peerStatusFor(t, a.engine, b.addr)
			check := func(n *concurrentNode) {
				t.Helper()
				p := peerStatusFor(t, n.engine, b.addr)
				if p.AcknowledgedPosition != before.AcknowledgedPosition || p.RefusedPositions != 1 || p.LowestRefusedPosition != second.Position || p.GCConfirmedPosition != first.Position || p.OutboundCaughtUp || p.OutboundRoundCaughtUp || p.ConsecutiveFailures != 0 || !p.NextAttemptTime.IsZero() {
					t.Fatalf("lost refusal or progress during concurrent confirmation/recovery: %+v", p)
				}
			}
			check(a)
			assertAbsent(t, b.store, "refused/1")
			if gc, err := a.engine.CollectTombstones(ctx); err != nil || gc.Collected != 1 || gc.Blocked != 1 {
				t.Fatalf("GC did not honor the remaining refusal: %+v %v", gc, err)
			}
			if durable {
				d := a.store.(*DurableStore)
				if _, err := d.Compact(ctx); err != nil {
					t.Fatal(err)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := NewDurableStore(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = recovered.Close() })
				a = newConcurrentNode(t, "a", recovered)
				a.peers.Store([]string{b.addr})
				b.peers.Store([]string{a.addr})
				check(a)
				if p := peerStatusFor(t, a.engine, b.addr); !p.LastPullTime.IsZero() || !p.LastSuccessTime.IsZero() || p.InboundCaughtUp {
					t.Fatalf("process-local exchange times survived restart: %+v", p)
				}
			}
			// Digest acceptance releases the final refusal without decreasing the
			// persisted push cursor. Concurrent handlers must not restore it.
			bClock.now.Store(2_200_000)
			concurrentRounds(t, ctx, a.engine.SyncOnce, a.engine.AntiEntropyOnce, b.engine.SyncOnce, b.engine.AntiEntropyOnce, progressReader(a.engine))
			assertConverged(t, ctx, a, b)
			if p := peerStatusFor(t, a.engine, b.addr); p.RefusedPositions != 0 || p.GCConfirmedPosition != p.AcknowledgedPosition || p.AcknowledgedPosition < before.AcknowledgedPosition {
				t.Fatalf("accepted refusal did not release the safe prefix: %+v", p)
			}
			if gc, err := a.engine.CollectTombstones(ctx); err != nil || gc.Collected != 1 || gc.Blocked != 0 {
				t.Fatalf("accepted tombstone still blocked: %+v %v", gc, err)
			}
		})
	}
}

func TestConcurrentGossipFailureAndBackoff(t *testing.T) {
	for _, failPush := range []bool{false, true} {
		for _, failureLast := range []bool{false, true} {
			t.Run(fmt.Sprintf("failPush=%v/failureLast=%v", failPush, failureLast), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				clock := &fixedPhysicalClock{now: 1_000_000}
				a := newConcurrentNode(t, "a", clockTestStore(t, false, "a", clock))
				b := newConcurrentNode(t, "b", clockTestStore(t, false, "b", clock))
				a.peers.Store([]string{b.addr})
				if err := a.engine.AntiEntropyOnce(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := a.store.Put(ctx, "pending", []byte("push")); err != nil {
					t.Fatal(err)
				}
				pushStarted, pullStarted := make(chan struct{}), make(chan struct{})
				pushRelease, pullRelease := make(chan struct{}), make(chan struct{})
				var calls atomic.Int32
				failure := errors.New("injected disconnect")
				a.wire.push = func(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
					calls.Add(1)
					close(pushStarted)
					select {
					case <-pushRelease:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if failPush {
						return nil, failure
					}
					return a.wire.HTTPSyncer.Push(ctx, addr, req)
				}
				a.wire.pull = func(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
					calls.Add(1)
					close(pullStarted)
					select {
					case <-pullRelease:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if !failPush {
						return nil, failure
					}
					return a.wire.HTTPSyncer.PullSnapshot(ctx, addr, req)
				}
				push := asyncRound(a.engine.SyncOnce, ctx)
				pull := asyncRound(a.engine.AntiEntropyOnce, ctx)
				awaitSignal(t, pushStarted)
				awaitSignal(t, pullStarted)
				failed, succeeded := pull, push
				failRelease, successRelease := pullRelease, pushRelease
				operation := "pull"
				if failPush {
					failed, succeeded = push, pull
					failRelease, successRelease = pushRelease, pullRelease
					operation = "push"
				}
				finishFailure := func() {
					close(failRelease)
					select {
					case err := <-failed:
						if !errors.Is(err, failure) {
							t.Fatalf("lost exchange failure: %v", err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if failureLast {
					close(successRelease)
					awaitRound(t, succeeded)
					finishFailure()
				} else {
					finishFailure()
					close(successRelease)
					awaitRound(t, succeeded)
				}
				p := peerStatusFor(t, a.engine, b.addr)
				if p.LastErrorOperation != operation || p.LastError == "" || p.LastErrorTime.IsZero() || p.InboundCaughtUp || p.CaughtUp {
					t.Fatalf("failure racing a pull lost conservative status: %+v", p)
				}
				if failPush && (p.AcknowledgedPosition != 0 || p.OutboundRoundCaughtUp || p.LastPullTime.IsZero()) {
					t.Fatalf("failed push advanced cursor or lost successful pull: %+v", p)
				}
				if !failPush && (!p.OutboundCaughtUp || !p.OutboundRoundCaughtUp || p.OutboundRoundTime.IsZero()) {
					t.Fatalf("successful push progress was lost: %+v", p)
				}
				if failureLast {
					if p.ConsecutiveFailures != 1 || p.NextAttemptTime.IsZero() || !p.LastSuccessTime.Before(p.LastErrorTime) {
						t.Fatalf("latest failure did not retain backoff: %+v", p)
					}
					// Both methods share the backoff. Neither may reach the
					// transport or move pull/round times when skipped.
					awaitRound(t, asyncRound(a.engine.SyncOnce, ctx))
					awaitRound(t, asyncRound(a.engine.AntiEntropyOnce, ctx))
					if got := peerStatusFor(t, a.engine, b.addr); calls.Load() != 2 || !reflect.DeepEqual(got, p) {
						t.Fatalf("backoff attempted an exchange or changed status: %+v", got)
					}
				} else if p.ConsecutiveFailures != 0 || !p.NextAttemptTime.IsZero() || !p.LastSuccessTime.After(p.LastErrorTime) {
					t.Fatalf("later success did not clear backoff and retain error history: %+v", p)
				}
			})
		}
	}
}
