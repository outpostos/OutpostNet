package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func clockTestStore(t *testing.T, durable bool, id string, clock PhysicalClock) Store {
	t.Helper()
	if durable {
		s, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: id, PhysicalClock: clock, TombstoneRetention: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	return mustGCMemoryStore(t, id, clock, time.Minute)
}

func clockHTTPPair(t *testing.T, source, target Store) (*GossipEngine, *GossipEngine, string) {
	t.Helper()
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "target", AuthToken: "clock-test"})
	if err != nil {
		t.Fatal(err)
	}
	serving, err := NewGossipEngine(GossipConfig{NodeID: "source", Store: source, Syncer: syncer})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "source", Engine: serving, AuthToken: "clock-test"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	pulling, err := NewGossipEngine(GossipConfig{NodeID: "target", Store: target, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{server.URL})})
	if err != nil {
		t.Fatal(err)
	}
	return serving, pulling, server.URL
}

func seedClockAhead(t *testing.T, source Store, now int64, key string) Entry {
	t.Helper()
	e := newEntryForTest(key, []byte("future"), "bad-writer", 1, Timestamp{WallMS: now + int64(10*time.Minute/time.Millisecond)})
	if _, err := source.Apply(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestClockAheadDigestRoundKeepsHealthyEntries(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			source := clockTestStore(t, durable, "source", &fixedPhysicalClock{now: clock.now + 600_000})
			target := clockTestStore(t, durable, "target", clock)
			seedClockAhead(t, source, clock.now, "a-future")
			healthy := newEntryForTest("z-healthy", []byte("ok"), "good-writer", 2, Timestamp{WallMS: clock.now})
			if _, err := source.Apply(ctx, healthy); err != nil {
				t.Fatal(err)
			}
			_, pulling, addr := clockHTTPPair(t, source, target)
			for round := 0; round < 2; round++ {
				if err := pulling.AntiEntropyOnce(ctx); err != nil {
					t.Fatalf("clock refusal voided round: %v", err)
				}
				s := peerStatusFor(t, pulling, addr)
				if s.LastPullTime.IsZero() || s.ConsecutiveFailures != 0 || !s.NextAttemptTime.IsZero() {
					t.Fatalf("refusal charged to forwarding peer: %+v", s)
				}
			}
			assertStoredValue(t, target, "z-healthy", "ok")
			assertAbsent(t, target, "a-future")
			assertClockRefusals(t, pulling, "bad-writer", 2)
			clock.now += 600_000
			if err := pulling.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			assertStoredValue(t, target, "a-future", "future")
		})
	}
}

func TestClockAheadSnapshotKeepsHeldOlderValue(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			source := clockTestStore(t, durable, "source", clock)
			target := clockTestStore(t, durable, "target", clock)
			old := newEntryForTest("kept", []byte("older-valid"), "good-writer", 1, Timestamp{WallMS: clock.now})
			if _, err := target.Apply(ctx, old); err != nil {
				t.Fatal(err)
			}
			if _, err := source.Delete(ctx, "collected"); err != nil {
				t.Fatal(err)
			}
			clock.now += 60_001
			if _, err := source.(interface {
				CollectTombstones(context.Context) (TombstoneGCResult, error)
			}).CollectTombstones(ctx); err != nil {
				t.Fatal(err)
			}
			healthyNow := clock.now
			clock.now += 600_000
			seedClockAhead(t, source, healthyNow, "kept")
			clock.now = healthyNow
			if _, err := source.Apply(ctx, newEntryForTest("healthy", []byte("ok"), "good-writer", 2, Timestamp{WallMS: clock.now})); err != nil {
				t.Fatal(err)
			}
			_, pulling, addr := clockHTTPPair(t, source, target)
			// Its zero horizon is refused with 410; SyncOnce must finish the snapshot.
			if err := pulling.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			assertStoredValue(t, target, "kept", "older-valid")
			assertStoredValue(t, target, "healthy", "ok")
			assertClockRefusals(t, pulling, "bad-writer", 1)
			if target.(interface{ GCHorizon() Timestamp }).GCHorizon() != source.(interface{ GCHorizon() Timestamp }).GCHorizon() {
				t.Fatal("snapshot horizon not adopted")
			}
			s := peerStatusFor(t, pulling, addr)
			if s.LastSuccessTime.IsZero() || s.ConsecutiveFailures != 0 || !s.NextAttemptTime.IsZero() {
				t.Fatalf("snapshot refusal caused backoff: %+v", s)
			}
		})
	}
}

func TestClockAheadPushSkipsAndGatesGC(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			healthyClock := &fixedPhysicalClock{now: 1_000_000}
			badClock := &fixedPhysicalClock{now: healthyClock.now + 365*24*60*60*1000}
			sender := clockTestStore(t, durable, "target", badClock)
			receiver := clockTestStore(t, durable, "source", healthyClock)
			tomb, err := sender.Delete(ctx, "refused-delete")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sender.Apply(ctx, newEntryForTest("later-good", []byte("ok"), "good-writer", 2, Timestamp{WallMS: healthyClock.now})); err != nil {
				t.Fatal(err)
			}
			_, pusher, addr := clockHTTPPair(t, receiver, sender)
			if err := pusher.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			s := peerStatusFor(t, pusher, addr)
			if s.AcknowledgedPosition != sender.CurrentPosition() {
				t.Fatalf("cursor blocked by bad writer: %+v", s)
			}
			assertStoredValue(t, receiver, "later-good", "ok")
			badClock.now += 120_000
			check := func() {
				t.Helper()
				gc, err := pusher.CollectTombstones(ctx)
				if err != nil || gc.Collected != 0 || gc.Blocked != 1 {
					t.Fatalf("refused tombstone collected: %+v err=%v", gc, err)
				}
				if got := mustGet(t, sender, tomb.Key); !got.Deleted {
					t.Fatal("delete lost")
				}
			}
			check()
			if d, ok := sender.(*DurableStore); ok {
				dir := d.dir
				if _, err := d.Compact(ctx); err != nil {
					t.Fatal(err)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "target", PhysicalClock: badClock, TombstoneRetention: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				sender = reopened
				syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "target", AuthToken: "clock-test"})
				if err != nil {
					t.Fatal(err)
				}
				pusher, err = NewGossipEngine(GossipConfig{NodeID: "target", Store: sender, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{addr})})
				if err != nil {
					t.Fatal(err)
				}
				check()
			}
		})
	}
}

func TestFutureHorizonCannotDeleteOrAdvanceHealthyReplica(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			for _, operation := range []string{"push", "pull"} {
				t.Run(operation, func(t *testing.T) {
					ctx := context.Background()
					healthy := &fixedPhysicalClock{now: 1_000_000}
					future := &fixedPhysicalClock{now: healthy.now + 365*24*60*60*1000}
					source := clockTestStore(t, durable, "source", future)
					if _, err := source.Delete(ctx, "old-fault-delete"); err != nil {
						t.Fatal(err)
					}
					future.now += 120_000
					if r, err := source.(interface {
						CollectTombstones(context.Context) (TombstoneGCResult, error)
					}).CollectTombstones(ctx); err != nil || r.Collected != 1 {
						t.Fatalf("legacy horizon setup: %+v %v", r, err)
					}
					target := clockTestStore(t, durable, "target", healthy)
					if _, err := target.Put(ctx, "healthy-only", []byte("keep")); err != nil {
						t.Fatal(err)
					}
					_, engine, _ := clockHTTPPair(t, source, target)
					var err error
					if operation == "push" {
						err = engine.SyncOnce(ctx)
					} else {
						err = engine.AntiEntropyOnce(ctx)
					}
					if err == nil {
						t.Fatal("future horizon was trusted")
					}
					assertStoredValue(t, target, "healthy-only", "keep")
					if !target.(interface{ GCHorizon() Timestamp }).GCHorizon().isZero() {
						t.Fatal("adopted future horizon")
					}
					e, err := target.Put(ctx, "after", []byte("healthy"))
					if err != nil {
						t.Fatal(err)
					}
					if e.Version.HLC.WallMS > healthy.now+MaxClockOffsetMS {
						t.Fatal("healthy HLC infected")
					}
				})
			}
		})
	}
}

func TestClockAheadTombstoneCollectionCap(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	store := clockTestStore(t, false, "target", clock)
	clock.now += 365 * 24 * 60 * 60 * 1000
	if _, err := store.Delete(ctx, "future"); err != nil {
		t.Fatal(err)
	}
	clock.now = 1_000_000
	// A malicious/overflowing HLC must remain ineligible independently of age.
	// Negative wall times near the int64 boundary must not wrap the cutoff.
	clock.now = -9223372036854775807
	r, err := store.(interface {
		CollectTombstones(context.Context) (TombstoneGCResult, error)
	}).CollectTombstones(ctx)
	if err != nil || r.Collected != 0 {
		t.Fatalf("future tombstone collected: %+v err=%v", r, err)
	}
}

func TestRefusalSetReleasesOnlyAcceptedKeys(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			healthy := &fixedPhysicalClock{now: 1_000_000}
			bad := &fixedPhysicalClock{now: 1_600_000}
			sender := clockTestStore(t, durable, "target", bad)
			receiver := clockTestStore(t, durable, "source", healthy)
			if _, err := sender.Delete(ctx, "first"); err != nil {
				t.Fatal(err)
			}
			bad.now += 600_000
			if _, err := sender.Delete(ctx, "second"); err != nil {
				t.Fatal(err)
			}
			_, pusher, _ := clockHTTPPair(t, receiver, sender)
			if err := pusher.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			handler, err := NewHTTPHandler(HTTPHandlerConfig{NodeID: "target", Engine: pusher, AuthToken: "clock-test"})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "source", AuthToken: "clock-test"})
			if err != nil {
				t.Fatal(err)
			}
			puller, err := NewGossipEngine(GossipConfig{NodeID: "source", Store: receiver, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{server.URL})})
			if err != nil {
				t.Fatal(err)
			}
			healthy.now = 1_600_000
			bad.now += 120_000
			for i := 0; i < 2; i++ {
				if err := puller.AntiEntropyOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			r, err := pusher.CollectTombstones(ctx)
			if err != nil || r.Collected != 1 || r.Blocked != 1 {
				t.Fatalf("digest did not release just first refusal: %+v %v", r, err)
			}
			assertAbsent(t, sender, "first")
			if !mustGet(t, sender, "second").Deleted {
				t.Fatal("unaccepted second delete lost")
			}
			healthy.now = bad.now
			// A newer version accepted by a push releases the older refused position.
			if _, err := sender.Delete(ctx, "second"); err != nil {
				t.Fatal(err)
			}
			if err := pusher.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			bad.now += 120_000
			r, err = pusher.CollectTombstones(ctx)
			if err != nil || r.Collected != 1 || r.Blocked != 0 {
				t.Fatalf("replacement did not release refusal: %+v %v", r, err)
			}
		})
	}
}

func assertClockRefusals(t *testing.T, g *GossipEngine, writer string, want uint64) {
	t.Helper()
	status, err := g.ReplicationStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range status.Clocks {
		if s.NodeID == writer {
			if !s.ClockSuspect || s.RefusedEntries != want {
				t.Fatalf("wrong writer observation: %+v", s)
			}
			return
		}
	}
	t.Fatalf("no refusal observation for writer %s: %+v", writer, status.Clocks)
}

func TestPeerHorizonGuardOnAllRequests(t *testing.T) {
	for _, path := range []string{"push", "digest", "snapshot"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			serving, _, addr := clockHTTPPair(t, clockTestStore(t, false, "source", clock), clockTestStore(t, false, "target", clock))
			client, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "target", AuthToken: "clock-test"})
			if err != nil {
				t.Fatal(err)
			}
			future := Timestamp{WallMS: clock.now + 600_000}
			if path == "push" {
				_, err = client.Push(ctx, addr, PushRequest{GCHorizon: &future})
			} else {
				req := PullRequest{GCHorizon: &future}
				if path == "digest" {
					start := ""
					req.StartKey = &start
				}
				_, err = client.PullSnapshot(ctx, addr, req)
			}
			if err == nil {
				t.Fatal("accepted future request horizon")
			}
			clocks := serving.ClockStatus()
			if len(clocks) != 1 || clocks[0].NodeID != "target" || !clocks[0].ClockSuspect || clocks[0].RefusedHorizons != 1 {
				t.Fatalf("missing peer observation: %+v", clocks)
			}
		})
	}
}

func TestPeerHorizonGuardOnAllResponses(t *testing.T) {
	for _, path := range []string{"push", "digest", "snapshot-first", "snapshot-later"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			store := clockTestStore(t, false, "target", clock)
			if _, err := store.Put(ctx, "healthy-only", []byte("keep")); err != nil {
				t.Fatal(err)
			}
			future := Timestamp{WallMS: clock.now + 600_000}
			pages := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "push") {
					if strings.HasPrefix(path, "snapshot") {
						w.WriteHeader(http.StatusGone)
						_ = json.NewEncoder(w).Encode(map[string]any{"error": "resync", "gc_horizon": Timestamp{WallMS: clock.now}})
					} else {
						_ = json.NewEncoder(w).Encode(PushResponse{ProtocolVersion: StateProtocolVersion, PolicyID: store.Policy().ID(), GCHorizon: &future})
					}
					return
				}
				response := PullResponse{ProtocolVersion: StateProtocolVersion, PolicyID: store.Policy().ID(), GCHorizon: &future}
				if path == "snapshot-later" && pages == 0 {
					response.GCHorizon = &Timestamp{WallMS: clock.now}
					response.More = true
					response.NextSincePosition = 1
					response.Entries = []Entry{newEntryForTest("page-one", []byte("ok"), "source", 1, Timestamp{WallMS: clock.now})}
				}
				pages++
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "target", AuthToken: "clock-test"})
			if err != nil {
				t.Fatal(err)
			}
			g, err := NewGossipEngine(GossipConfig{NodeID: "target", Store: store, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{server.URL})})
			if err != nil {
				t.Fatal(err)
			}
			if path == "digest" {
				err = g.AntiEntropyOnce(ctx)
			} else {
				err = g.SyncOnce(ctx)
			}
			if err == nil {
				t.Fatal("accepted future response horizon")
			}
			assertStoredValue(t, store, "healthy-only", "keep")
			if !store.(interface{ GCHorizon() Timestamp }).GCHorizon().isZero() {
				t.Fatal("future horizon adopted")
			}
			statuses := g.ClockStatus()
			if len(statuses) != 1 || statuses[0].RefusedHorizons != 1 || !statuses[0].ClockSuspect {
				t.Fatalf("missing horizon observation: %+v", statuses)
			}
		})
	}
}

func TestRefusedTombstonePartitionBeyondRetention(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			healthy := &fixedPhysicalClock{now: 1_000_000}
			bad := &fixedPhysicalClock{now: healthy.now}
			sender := clockTestStore(t, durable, "target", bad)
			receiver := clockTestStore(t, durable, "source", healthy)
			old, err := sender.Put(ctx, "deleted", []byte("old"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := receiver.Apply(ctx, old); err != nil {
				t.Fatal(err)
			}
			bad.now += 365 * 24 * 60 * 60 * 1000
			tomb, err := sender.Delete(ctx, "deleted")
			if err != nil {
				t.Fatal(err)
			}
			serving, pusher, _ := clockHTTPPair(t, receiver, sender)
			if err := pusher.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			assertClockRefusals(t, serving, "target", 1)
			// No exchanges during the partition. Membership retains the refusing peer.
			bad.now += 120_000
			healthy.now += 120_000
			r, err := pusher.CollectTombstones(ctx)
			if err != nil || r.Collected != 0 || r.Blocked != 1 {
				t.Fatalf("partition dropped refusal: %+v %v", r, err)
			}
			// On reconnection the stale peer's live value cannot displace the tombstone.
			if _, err := pusher.HandlePush(ctx, PushRequest{PolicyID: sender.Policy().ID(), SourceNodeID: "source", Entries: []Entry{old}}); err != nil {
				t.Fatal(err)
			}
			if got := mustGet(t, sender, "deleted"); !got.Deleted || got.Version != tomb.Version {
				t.Fatal("stale live value resurrected")
			}
			if !receiver.(interface{ GCHorizon() Timestamp }).GCHorizon().isZero() {
				t.Fatal("healthy horizon infected")
			}
			e, err := receiver.Put(ctx, "healthy-after", []byte("ok"))
			if err != nil || e.Version.HLC.WallMS > healthy.now+MaxClockOffsetMS {
				t.Fatalf("healthy HLC infected: %+v %v", e, err)
			}
		})
	}
}

func TestRefusedPositionCannotVouchPastGCFloor(t *testing.T) {
	ctx := context.Background()
	ahead := &fixedPhysicalClock{now: 1_600_000}
	sender := clockTestStore(t, false, "target", ahead)
	if _, err := sender.Put(ctx, "future-live", []byte("future")); err != nil {
		t.Fatal(err)
	}
	tomb := newEntryForTest("collected", nil, "good-writer", 2, Timestamp{WallMS: 1_000_000})
	tomb.Deleted = true
	if _, err := sender.Apply(ctx, tomb); err != nil {
		t.Fatal(err)
	}
	if r, err := sender.(interface {
		CollectTombstones(context.Context) (TombstoneGCResult, error)
	}).CollectTombstones(ctx); err != nil || r.Collected != 1 {
		t.Fatalf("sender GC setup: %+v %v", r, err)
	}
	healthy := &fixedPhysicalClock{now: 999_000}
	receiver := clockTestStore(t, false, "source", healthy)
	if _, err := receiver.Delete(ctx, "own-collected"); err != nil {
		t.Fatal(err)
	}
	healthy.now += 61_000
	if _, err := receiver.(interface {
		CollectTombstones(context.Context) (TombstoneGCResult, error)
	}).CollectTombstones(ctx); err != nil {
		t.Fatal(err)
	}
	_, pusher, addr := clockHTTPPair(t, receiver, sender)
	if err := pusher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status := peerStatusFor(t, pusher, addr)
	if status.RefusedPositions != 1 || status.GCConfirmedPosition != 0 || status.OutboundCaughtUp || status.OutboundRoundCaughtUp {
		t.Fatalf("refused entry reported confirmed: %+v", status)
	}
	horizon := receiver.(interface{ GCHorizon() Timestamp }).GCHorizon()
	_, err := pusher.HandlePush(ctx, PushRequest{PolicyID: sender.Policy().ID(), SourceNodeID: "source", GCHorizon: &horizon})
	var resync *ResyncRequiredError
	if !errors.As(err, &resync) {
		t.Fatalf("requester vouched past refusal: %v", err)
	}
}

func TestRefusalAndCursorCommitTogetherOnDiskFailure(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	bad := &fixedPhysicalClock{now: 1_600_000}
	fault := &logFault{}
	cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "target", PhysicalClock: bad, TombstoneRetention: time.Minute, wrapLog: fault.wrap}
	sender := mustDurableStore(t, cfg)
	defer sender.Close()
	if _, err := sender.Delete(ctx, "future-delete"); err != nil {
		t.Fatal(err)
	}
	receiver := clockTestStore(t, false, "source", clock)
	_, pusher, addr := clockHTTPPair(t, receiver, sender)
	fault.arm(func(f *logFault) { f.failSyncs = 1 })
	if err := pusher.SyncOnce(ctx); err == nil {
		t.Fatal("cursor save falsely succeeded")
	}
	if status := peerStatusFor(t, pusher, addr); status.AcknowledgedPosition != 0 || status.RefusedPositions != 0 {
		t.Fatalf("failed save published partial acknowledgement: %+v", status)
	}
	if err := sender.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.wrapLog = nil
	recovered := mustDurableStore(t, cfg)
	defer recovered.Close()
	if progress, err := recovered.PeerProgress(ctx, addr); err != nil || progress != 0 {
		t.Fatalf("failed cursor survived restart: %d %v", progress, err)
	}
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "target", AuthToken: "clock-test"})
	if err != nil {
		t.Fatal(err)
	}
	pusher, err = NewGossipEngine(GossipConfig{NodeID: "target", Store: recovered, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{addr})})
	if err != nil {
		t.Fatal(err)
	}
	if err := pusher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	bad.now += 120_000
	if r, err := pusher.CollectTombstones(ctx); err != nil || r.Collected != 0 || r.Blocked != 1 {
		t.Fatalf("retry lost GC refusal: %+v %v", r, err)
	}
}
