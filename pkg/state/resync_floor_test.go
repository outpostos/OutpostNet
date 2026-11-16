package state

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// Exercise the public store and authenticated gossip boundaries: a reader
// resumes at the purge notice, and the next push resumes from its saved cursor.
func TestResyncPurgeImmediateWriteReachesReadersAndPeer(t *testing.T) {
	for _, kind := range []string{"memory", "durable"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			var local interface {
				Store
				GCFloor() uint64
			}
			if kind == "memory" {
				s, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local", PhysicalClock: clock, ArchiveDir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				local = s
			} else {
				s, err := NewDurableStore(DurableStoreConfig{NodeID: "local", PhysicalClock: clock, Dir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				local = s
			}
			if _, err := local.Put(ctx, "lost", []byte("unreplicated")); err != nil {
				t.Fatal(err)
			}
			before := local.CurrentPosition()
			peer := mustGCMemoryStore(t, "peer", clock, time.Second)
			clock.now += 100
			if _, err := peer.Delete(ctx, "gc"); err != nil {
				t.Fatal(err)
			}
			clock.now += 2000
			if _, err := peer.CollectTombstones(ctx); err != nil {
				t.Fatal(err)
			}
			var observed []Entry
			unobserve := local.Observe(func(e Entry) { observed = append(observed, e) })
			defer unobserve()
			engine := resyncArchiveEngine(t, local, peer)
			if err := engine.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			// Make the first mutation after the purge, before any further exchange.
			fresh, err := local.Put(ctx, "fresh", []byte{0, 255, '\n', 0})
			if err != nil {
				t.Fatal(err)
			}
			assertAbsent(t, local, "lost")
			floor := local.GCFloor()
			if floor <= before || fresh.Position <= floor {
				t.Errorf("write must follow purge floor: before=%d floor=%d write=%d", before, floor, fresh.Position)
			}
			if status := local.CheckConsumerPosition(before); !status.NeedsSnapshot {
				t.Errorf("pre-purge consumer must snapshot: %+v", status)
			}
			if status := local.CheckConsumerPosition(floor); !status.CanResume {
				t.Errorf("consumer at purge floor must resume: %+v", status)
			}
			if len(observed) != 2 || observed[0].Key != "lost" || !observed[0].Deleted || observed[0].Position != floor || !reflect.DeepEqual(observed[1], fresh) {
				t.Errorf("observer must see purge notice then immediate write: %+v", observed)
			}
			changes, err := local.ChangesSince(ctx, floor, 0)
			if err != nil || !reflect.DeepEqual(changes, []Entry{fresh}) {
				t.Errorf("ChangesSince at purge floor skipped write: %+v, %v", changes, err)
			}
			// This is the byte-paged read used by the peer push cursor, checked
			// at the floor even when the engine's prior acknowledgement is older.
			page, err := local.(BytePageStore).ChangesSinceBytes(ctx, floor, 4096)
			if err != nil || !reflect.DeepEqual(page, []Entry{fresh}) {
				t.Errorf("push page at purge floor skipped write: %+v, %v", page, err)
			}
			if err := engine.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			got := mustGet(t, peer, fresh.Key)
			if got.Identity() != fresh.Identity() || !reflect.DeepEqual(got.Value, fresh.Value) {
				t.Errorf("peer push lost immediate write: %+v; want %+v", got, fresh)
			}
			status, err := engine.ReplicationStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(status.Peers) != 1 || status.Peers[0].AcknowledgedPosition != fresh.Position || !status.Peers[0].OutboundCaughtUp || status.Peers[0].RefusedPositions != 0 {
				t.Errorf("push cursor did not acknowledge immediate write: %+v", status.Peers)
			}
		})
	}
}
