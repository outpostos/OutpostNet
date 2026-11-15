package state

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplicationStatusClockTimestamps(t *testing.T) {
	ctx := context.Background()
	physical := &fixedPhysicalClock{now: 1_000_000}
	store := clockTestStore(t, false, "a", physical)
	remote := newConcurrentNode(t, "b", clockTestStore(t, false, "b", physical))
	transport, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "a", AuthToken: "concurrency-test"})
	if err != nil {
		t.Fatal(err)
	}
	wire := &concurrentWire{HTTPSyncer: transport}
	// A wall-only reference a year away from the system clock.
	reference := time.Now().AddDate(1, 0, 0).Round(0)
	engine, err := NewGossipEngine(GossipConfig{
		NodeID: "a", Store: store, Syncer: wire,
		PeerProvider: NewStaticPeerProvider([]string{remote.addr}),
		Clock:        func() time.Time { return reference },
		BackoffBase:  time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := peerStatusFor(t, engine, remote.addr)
	if !initial.LastSuccessTime.IsZero() || !initial.LastPullTime.IsZero() || !initial.LastErrorTime.IsZero() || !initial.OutboundRoundTime.IsZero() || !initial.NextAttemptTime.IsZero() {
		t.Fatalf("clock hook stamped events that never happened: %+v", initial)
	}
	if _, err := store.Put(ctx, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	pushStart := reference
	pushEnd := reference.Add(time.Second)
	wire.push = func(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
		if req.TimeSample.SentPhysicalMS != physical.NowMS() {
			t.Fatalf("status clock changed physical handshake time: %+v", req.TimeSample)
		}
		reference = pushEnd
		return wire.HTTPSyncer.Push(ctx, addr, req)
	}
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	pushed := peerStatusFor(t, engine, remote.addr)
	if !pushed.OutboundRoundTime.Equal(pushStart) || !pushed.LastSuccessTime.Equal(pushEnd) || !pushed.LastPullTime.IsZero() {
		t.Fatalf("push status must use reference at start/completion: %+v", pushed)
	}
	// An empty push still records a caught-up round, but no exchange success.
	reference = reference.Add(time.Second)
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	empty := peerStatusFor(t, engine, remote.addr)
	if !empty.OutboundRoundTime.Equal(reference) || !empty.LastSuccessTime.Equal(pushEnd) {
		t.Fatalf("empty round status: %+v", empty)
	}
	pullStart := reference.Add(time.Second)
	pullEnd := pullStart.Add(time.Second)
	reference = pullStart
	wire.pull = func(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
		reference = pullEnd
		return wire.HTTPSyncer.PullSnapshot(ctx, addr, req)
	}
	if err := engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	pulled := peerStatusFor(t, engine, remote.addr)
	if !pulled.LastPullTime.Equal(pullStart) || !pulled.LastSuccessTime.Equal(pullEnd) || !pulled.InboundCaughtUp {
		t.Fatalf("pull status must use reference at start/completion: %+v", pulled)
	}
	failure := errors.New("injected disconnect")
	reference = reference.Add(time.Second)
	wire.pull = func(context.Context, string, PullRequest) (*PullResponse, error) { return nil, failure }
	if err := engine.AntiEntropyOnce(ctx); !errors.Is(err, failure) {
		t.Fatalf("lost failure: %v", err)
	}
	failed := peerStatusFor(t, engine, remote.addr)
	if !failed.LastErrorTime.Equal(reference) || !failed.LastPullTime.Equal(pullStart) || !failed.LastSuccessTime.Equal(pullEnd) || failed.InboundCaughtUp {
		t.Fatalf("failed exchange status must retain reference history: %+v", failed)
	}
	// The published deadline uses the reference; the actual backoff remains local.
	remaining := failed.NextAttemptTime.Sub(reference)
	if remaining < 59*time.Minute || remaining > time.Hour {
		t.Fatalf("backoff status deadline is not reference based: %v", remaining)
	}
}

func TestReplicationStatusClockResync(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, push := range []bool{false, true} {
			t.Run(fmt.Sprintf("durable=%v/push=%v", durable, push), func(t *testing.T) {
				ctx := context.Background()
				physical := &fixedPhysicalClock{now: 1_000_000}
				server, stale := newCollectedServerStore(t, physical, 1, 8)
				remote := newConcurrentNode(t, "node-a", server)
				store := clockTestStore(t, durable, "node-l", physical)
				if _, err := store.Apply(ctx, stale); err != nil {
					t.Fatal(err)
				}
				wall := time.Now().AddDate(-1, 0, 0).Truncate(time.Millisecond)
				reference, err := NewTimeReference(TimeReferenceConfig{
					SystemClock: PhysicalClockFunc(func() int64 { return wall.UnixMilli() }),
				})
				if err != nil {
					t.Fatal(err)
				}
				transport, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "node-l", AuthToken: "concurrency-test"})
				if err != nil {
					t.Fatal(err)
				}
				engine, err := NewGossipEngine(GossipConfig{
					NodeID: "node-l", Store: store, Syncer: transport, Clock: reference.Now,
					PeerProvider: NewStaticPeerProvider([]string{remote.addr}),
				})
				if err != nil {
					t.Fatal(err)
				}
				round := engine.AntiEntropyOnce
				if push {
					round = engine.SyncOnce
				}
				if err := round(ctx); err != nil {
					t.Fatal(err)
				}
				assertAbsent(t, store, "doomed")
				assertStoredValue(t, store, "live/0000", "xxxxxxxx")
				p := peerStatusFor(t, engine, remote.addr)
				if !p.LastSuccessTime.Equal(wall) || !p.LastPullTime.IsZero() || p.InboundCaughtUp || p.ConsecutiveFailures != 0 || !p.NextAttemptTime.IsZero() {
					t.Fatalf("resync status must use reference without claiming a pull: %+v", p)
				}
				if err := engine.AntiEntropyOnce(ctx); err != nil {
					t.Fatal(err)
				}
				p = peerStatusFor(t, engine, remote.addr)
				if !p.LastPullTime.Equal(wall) || !p.InboundCaughtUp {
					t.Fatalf("post-resync pull status: %+v", p)
				}
			})
		}
	}
}

func TestReplicationStatusClockRunAndBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	physical := &fixedPhysicalClock{now: 1_000_000}
	store := clockTestStore(t, false, "a", physical)
	remote := newConcurrentNode(t, "b", clockTestStore(t, false, "b", physical))
	transport, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "a", AuthToken: "concurrency-test"})
	if err != nil {
		t.Fatal(err)
	}
	wire := &concurrentWire{HTTPSyncer: transport}
	var wall atomic.Int64
	wall.Store(time.Now().AddDate(1, 0, 0).UnixMilli())
	clock := func() time.Time { return time.UnixMilli(wall.Load()) }
	const backoff = 150 * time.Millisecond
	engine, err := NewGossipEngine(GossipConfig{
		NodeID: "a", Store: store, Syncer: wire, Clock: clock,
		PeerProvider: NewStaticPeerProvider([]string{remote.addr}),
		Interval:     10 * time.Millisecond, AntiEntropyInterval: 15 * time.Millisecond,
		BackoffBase: backoff, BackoffMax: backoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "outbound", []byte("push")); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.store.Put(ctx, "inbound", []byte("pull")); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected disconnect")
	var pushes, pulls atomic.Int32
	tooEarly := make(chan struct{}, 1)
	started := time.Now()
	wire.push = func(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
		if pushes.Add(1) == 1 {
			return nil, failure
		}
		if time.Since(started) < backoff {
			select {
			case tooEarly <- struct{}{}:
			default:
			}
		}
		return transport.Push(ctx, addr, req)
	}
	wire.pull = func(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
		pulls.Add(1)
		if time.Since(started) < backoff {
			select {
			case tooEarly <- struct{}{}:
			default:
			}
		}
		return transport.PullSnapshot(ctx, addr, req)
	}
	if err := engine.SyncOnce(ctx); !errors.Is(err, failure) {
		t.Fatalf("expected failure, got %v", err)
	}
	failed := peerStatusFor(t, engine, remote.addr)
	// Jump forward: neither method may bypass the shared monotonic backoff.
	wall.Add(int64(365 * 24 * time.Hour / time.Millisecond))
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engine.AntiEntropyOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if pushes.Load() != 1 || pulls.Load() != 0 {
		t.Fatal("reference jump bypassed backoff")
	}
	if got := peerStatusFor(t, engine, remote.addr); got != failed {
		t.Fatalf("skipped rounds changed status: %+v", got)
	}
	// Jump backwards, then freeze: real intervals must still retry and pull.
	wall.Add(-int64(2 * 365 * 24 * time.Hour / time.Millisecond))
	done := asyncRound(engine.Run, ctx)
	t.Cleanup(func() { cancel(); awaitRound(t, done) })
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		p := peerStatusFor(t, engine, remote.addr)
		if p.CaughtUp {
			if !p.LastPullTime.Equal(clock()) || !p.LastSuccessTime.Equal(clock()) || !p.OutboundRoundTime.Equal(clock()) || !p.LastErrorTime.Equal(failed.LastErrorTime) || !p.NextAttemptTime.IsZero() {
				t.Fatalf("recovered periodic status lost reference/history: %+v", p)
			}
			break
		}
		select {
		case <-tooEarly:
			t.Fatal("retry occurred before monotonic backoff elapsed")
		case <-ctx.Done():
			t.Fatal("frozen/backwards reference stopped periodic recovery")
		case <-ticker.C:
		}
	}
	select {
	case <-tooEarly:
		t.Fatal("retry occurred too early")
	default:
	}
	if pushes.Load() < 2 || pulls.Load() < 1 {
		t.Fatal("periodic loops did not exchange")
	}
	assertStoredValue(t, store, "inbound", "pull")
	assertStoredValue(t, remote.store, "outbound", "push")
}

func TestReplicationStatusClockConcurrentFailureOrdering(t *testing.T) {
	for _, failPush := range []bool{false, true} {
		t.Run(fmt.Sprintf("failPush=%v", failPush), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			physical := &fixedPhysicalClock{now: 1_000_000}
			store := clockTestStore(t, false, "a", physical)
			remote := newConcurrentNode(t, "b", clockTestStore(t, false, "b", physical))
			transport, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "a", AuthToken: "concurrency-test"})
			if err != nil {
				t.Fatal(err)
			}
			wire := &concurrentWire{HTTPSyncer: transport}
			var wall atomic.Int64
			wall.Store(time.Now().AddDate(1, 0, 0).UnixMilli())
			engine, err := NewGossipEngine(GossipConfig{
				NodeID: "a", Store: store, Syncer: wire,
				PeerProvider: NewStaticPeerProvider([]string{remote.addr}),
				Clock:        func() time.Time { return time.UnixMilli(wall.Load()) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Put(ctx, "first", []byte("push")); err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			failure := errors.New("injected concurrent failure")
			if failPush {
				wire.pull = func(ctx context.Context, addr string, req PullRequest) (*PullResponse, error) {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return transport.PullSnapshot(ctx, addr, req)
				}
				wire.push = func(context.Context, string, PushRequest) (*PushResponse, error) { return nil, failure }
			} else {
				wire.push = func(ctx context.Context, addr string, req PushRequest) (*PushResponse, error) {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return transport.Push(ctx, addr, req)
				}
				wire.pull = func(context.Context, string, PullRequest) (*PullResponse, error) { return nil, failure }
			}
			succeed, fail := engine.SyncOnce, engine.AntiEntropyOnce
			if failPush {
				succeed, fail = engine.AntiEntropyOnce, engine.SyncOnce
			}
			done := asyncRound(succeed, ctx)
			awaitSignal(t, started)
			// A later failure has an earlier wall timestamp than the held round.
			wall.Add(-int64(365 * 24 * time.Hour / time.Millisecond))
			if err := fail(ctx); !errors.Is(err, failure) {
				t.Fatalf("failure: %v", err)
			}
			close(release)
			awaitRound(t, done)
			// Keep a write beyond the successful round's cutoff, so the raw
			// outbound cursor cannot mask a false round-caught-up claim.
			if _, err := store.Put(ctx, "later", []byte("pending")); err != nil {
				t.Fatal(err)
			}
			p := peerStatusFor(t, engine, remote.addr)
			if p.InboundCaughtUp || p.OutboundRoundCaughtUp || p.CaughtUp || p.ConsecutiveFailures != 0 || p.LastError == "" || !p.NextAttemptTime.IsZero() {
				t.Fatalf("wall ordering overrode the later monotonic failure: %+v", p)
			}
			// A fresh pull at exactly the failure's wall time must recover.
			wire.pull = nil
			if err := engine.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			p = peerStatusFor(t, engine, remote.addr)
			if !p.InboundCaughtUp || !p.LastPullTime.Equal(p.LastErrorTime) {
				t.Fatalf("frozen wall clock prevented fresh pull recovery: %+v", p)
			}
		})
	}
}
