package state

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
)

// Ticket 03d (03b F4): a resync commits each snapshot page as one durable
// record with one fsync, and a crash in the middle of that record recovers
// whole pages only.

const batchTestLive = 2*resyncPageSize + 10 // three snapshot pages

// newBatchResyncPair returns a server that collected "doomed" and holds
// batchTestLive live entries, and an engine for a durable client that still
// holds the stale doomed value.
func newBatchResyncPair(t *testing.T, cfg DurableStoreConfig) (*MemoryStore, *DurableStore, *GossipEngine) {
	t.Helper()
	ctx := context.Background()
	clock := cfg.PhysicalClock.(*fixedPhysicalClock)
	net := newLoopNet()
	server, stale := newCollectedServerStore(t, clock, batchTestLive, 1)
	serverEngine, err := NewGossipEngine(GossipConfig{NodeID: "node-a", Store: server, Syncer: loopSyncer{net: net, from: "node-a"}})
	if err != nil {
		t.Fatalf("NewGossipEngine server: %v", err)
	}
	net.engines["node-a"] = serverEngine

	client := mustDurableStore(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Apply(ctx, stale); err != nil {
		t.Fatalf("client Apply stale: %v", err)
	}
	clientEngine, err := NewGossipEngine(GossipConfig{
		NodeID:       "node-l",
		Store:        client,
		Syncer:       loopSyncer{net: net, from: "node-l"},
		PeerProvider: NewStaticPeerProvider([]string{"node-a"}),
	})
	if err != nil {
		t.Fatalf("NewGossipEngine client: %v", err)
	}
	return server, client, clientEngine
}

func countLiveEntries(t *testing.T, store Store) int {
	t.Helper()
	entries, err := store.ListEntries(context.Background())
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	var live int
	for _, entry := range entries {
		if strings.HasPrefix(entry.Key, "live/") && !entry.Deleted {
			live++
		}
	}
	return live
}

func TestDurableResyncSyncsOncePerPage(t *testing.T) {
	ctx := context.Background()
	fault := &logFault{}
	cfg := DurableStoreConfig{
		Dir:                t.TempDir(),
		NodeID:             "node-l",
		PhysicalClock:      &fixedPhysicalClock{now: 1_000_000},
		TombstoneRetention: testRetention,
		wrapLog:            fault.wrap,
	}
	server, client, engine := newBatchResyncPair(t, cfg)

	fault.mu.Lock()
	before := fault.syncs
	fault.mu.Unlock()
	if err := engine.resyncFrom(ctx, "node-a", server.GCHorizon()); err != nil {
		t.Fatalf("resync: %v", err)
	}
	fault.mu.Lock()
	syncs := fault.syncs - before
	fault.mu.Unlock()
	// Three pages and the final purge record.
	if want := 3 + 1; syncs != want {
		t.Fatalf("resync of %d entries issued %d fsyncs, want %d (one per page plus the purge)", batchTestLive, syncs, want)
	}
	assertAbsent(t, client, "doomed")
	if got := countLiveEntries(t, client); got != batchTestLive {
		t.Fatalf("client holds %d live entries, want %d", got, batchTestLive)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cfg.wrapLog = nil
	recovered := mustDurableStore(t, cfg)
	defer recovered.Close()
	assertAbsent(t, recovered, "doomed")
	if got := countLiveEntries(t, recovered); got != batchTestLive {
		t.Fatalf("recovered client holds %d live entries, want %d", got, batchTestLive)
	}
	if recovered.GCHorizon() != server.GCHorizon() {
		t.Fatalf("recovered horizon %+v, want %+v", recovered.GCHorizon(), server.GCHorizon())
	}
}

// batchCrash captures a crash image of the store directory while the target
// batch record is being written: either after half of it reached the file
// (torn) or after all of it did but before its fsync.
type batchCrash struct {
	t      *testing.T
	dir    string
	target int
	torn   bool

	mu    sync.Mutex
	seen  int
	image string
}

func (c *batchCrash) wrap(handle durableLog) durableLog {
	return &batchCrashLog{durableLog: handle, crash: c}
}

type batchCrashLog struct {
	durableLog
	crash *batchCrash
}

func (l *batchCrashLog) Write(data []byte) (int, error) {
	c := l.crash
	if !bytes.Contains(data, []byte(`"type":"batch"`)) {
		return l.durableLog.Write(data)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen++
	if c.seen != c.target {
		return l.durableLog.Write(data)
	}
	if c.torn {
		written, err := l.durableLog.Write(data[:len(data)/2])
		c.image = crashImage(c.t, c.dir)
		if err != nil {
			return written, err
		}
		return written, io.ErrShortWrite
	}
	written, err := l.durableLog.Write(data)
	c.image = crashImage(c.t, c.dir)
	return written, err
}

func TestDurableResyncCrashInsideBatchRecoversWholePagesOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		torn bool
		// wantLive is what the crash image recovers: page 0 alone when the
		// second page's record is torn, both pages when it was complete.
		wantLive int
	}{
		{name: "torn-second-page", torn: true, wantLive: resyncPageSize},
		{name: "unsynced-complete-second-page", torn: false, wantLive: 2 * resyncPageSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			crash := &batchCrash{t: t, dir: dir, target: 2, torn: tc.torn}
			cfg := DurableStoreConfig{
				Dir:                dir,
				NodeID:             "node-l",
				PhysicalClock:      &fixedPhysicalClock{now: 1_000_000},
				TombstoneRetention: testRetention,
				wrapLog:            crash.wrap,
			}
			server, client, engine := newBatchResyncPair(t, cfg)
			err := engine.resyncFrom(ctx, "node-a", server.GCHorizon())
			if crash.image == "" {
				t.Fatalf("the second batch record was never written")
			}
			if tc.torn {
				if err == nil {
					t.Fatalf("resync succeeded although its second page could not be written")
				}
				// The live store rolled the torn record back: page 1 is not visible,
				// nothing was purged, and a retry completes.
				if got := countLiveEntries(t, client); got != resyncPageSize {
					t.Fatalf("after the failed page the live store shows %d entries, want %d", got, resyncPageSize)
				}
				assertStoredValue(t, client, "doomed", "stale")
				if err := engine.resyncFrom(ctx, "node-a", server.GCHorizon()); err != nil {
					t.Fatalf("retry resync: %v", err)
				}
				assertAbsent(t, client, "doomed")
				if got := countLiveEntries(t, client); got != batchTestLive {
					t.Fatalf("after retry the store holds %d live entries, want %d", got, batchTestLive)
				}
			} else if err != nil {
				t.Fatalf("resync: %v", err)
			}

			// Cold start from the crash image: whole pages only, no purge, the
			// stale value still present and the horizon not adopted.
			cfg.Dir = crash.image
			cfg.wrapLog = nil
			recovered := mustDurableStore(t, cfg)
			defer recovered.Close()
			if got := countLiveEntries(t, recovered); got != tc.wantLive {
				t.Fatalf("crash image recovered %d live entries, want exactly %d (whole pages)", got, tc.wantLive)
			}
			assertStoredValue(t, recovered, "doomed", "stale")
			if !recovered.GCHorizon().isZero() {
				t.Fatalf("crash image adopted a horizon before the purge: %+v", recovered.GCHorizon())
			}
			assertIncreasingSequences(t, readDurableLogRecords(t, crash.image))
			if _, err := recovered.Put(ctx, "after-crash", []byte("x")); err != nil {
				t.Fatalf("Put after crash recovery: %v", err)
			}
		})
	}
}
