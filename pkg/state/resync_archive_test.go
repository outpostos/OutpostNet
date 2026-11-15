package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Exercise the public store + authenticated HTTP gossip seam, including a
// forwarded write, a tombstone, a held key and a post-horizon local write.
func TestResyncArchivesOnlyPurgedEntriesAndPersistsCounts(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	dir := t.TempDir()
	local, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local", PhysicalClock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = local.Close() }()
	peer := mustGCMemoryStore(t, "peer", clock, time.Second)
	value := []byte{0, 255, 128, '\n', '"', 0}
	lost, err := local.Put(ctx, "lost", value)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := local.Delete(ctx, "deleted")
	if err != nil {
		t.Fatal(err)
	}
	forwarded := lost
	forwarded.Key = "forwarded"
	forwarded.Version.WriterNodeID = "writer"
	forwarded.ChangeID.OriginNodeID = "writer"
	if _, err := local.Apply(ctx, forwarded); err != nil {
		t.Fatal(err)
	}
	forwarded = mustGet(t, local, "forwarded")
	held, err := local.Put(ctx, "held", []byte("held"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Apply(ctx, held); err != nil {
		t.Fatal(err)
	}
	clock.now += 100
	if _, err := peer.Delete(ctx, "gc"); err != nil {
		t.Fatal(err)
	}
	horizon := mustGet(t, peer, "gc").Version.HLC
	clock.now += 2000
	if _, err := peer.CollectTombstones(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Put(ctx, "new", []byte("new")); err != nil {
		t.Fatal(err)
	}
	engine := resyncArchiveEngine(t, local, peer)
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status := local.LocalStatus().Resync
	if status.PurgedLocalOrigin != 2 || status.PurgedForwarded != 1 {
		t.Fatalf("counts: %+v", status)
	}
	if len(status.ByWriter) != 2 {
		t.Fatalf("writer counters: %+v", status)
	}
	for _, c := range status.ByWriter {
		if c.Peer == "" || (c.WriterNodeID != "local" && c.WriterNodeID != "writer") || (c.WriterNodeID == "local" && (c.Entries != 2 || !c.LocalOrigin)) || (c.WriterNodeID == "writer" && (c.Entries != 1 || c.LocalOrigin)) {
			t.Fatalf("missing attribution: %+v", c)
		}
	}
	path := filepath.Join(dir, status.LatestArchivePath)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("archive permissions: %v", info.Mode())
	}
	records := readResyncArchive(t, path)
	want := map[string]Entry{lost.Key: lost, deleted.Key: deleted, forwarded.Key: forwarded}
	if len(records) != len(want) {
		t.Fatalf("archive contains %d entries: %+v", len(records), records)
	}
	for _, r := range records {
		expected, ok := want[r.Key]
		if !ok {
			t.Fatalf("unexpected archived key: %s", r.Key)
		}
		if !reflect.DeepEqual(r.Entry, expected) || r.Position != expected.Position || r.Peer == "" || r.Horizon != horizon {
			t.Fatalf("archive mismatch: %+v want %+v", r, expected)
		}
		delete(want, r.Key)
		assertAbsent(t, local, r.Key)
	}
	assertStoredValue(t, local, "held", "held")
	assertStoredValue(t, local, "new", "new")
	if _, err := local.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	local, err = NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local", PhysicalClock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if got := local.LocalStatus().Resync; !reflect.DeepEqual(got, status) {
		t.Fatalf("status lost after compact/restart: %+v want %+v", got, status)
	}
	if !bytes.Equal(readResyncArchive(t, path)[0].Value, records[0].Value) {
		t.Fatal("archive changed on restart")
	}
}

type archivedResyncEntry struct {
	Entry
	Position uint64    `json:"position"`
	Peer     string    `json:"peer"`
	Horizon  Timestamp `json:"horizon"`
}

func readResyncArchive(t *testing.T, path string) []archivedResyncEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []archivedResyncEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	for scanner.Scan() {
		var r archivedResyncEntry
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		r.Entry.Position = r.Position
		out = append(out, r)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
func resyncArchiveEngine(t *testing.T, local Store, peer Store, configure ...func(*GossipConfig)) *GossipEngine {
	t.Helper()
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{AuthToken: "test", NodeID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewGossipEngine(GossipConfig{NodeID: "peer", Store: peer, Syncer: syncer})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPHandlerConfig{Engine: p, AuthToken: "test", NodeID: "peer"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := GossipConfig{NodeID: "local", Store: local, Syncer: syncer, PeerProvider: NewStaticPeerProvider([]string{server.URL})}
	for _, f := range configure {
		f(&cfg)
	}
	engine, err := NewGossipEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// A real archive failure must leave all observable live state unchanged, even
// if the failure happens after bytes were written, at file or directory fsync.
func TestResyncArchiveFailureLeavesStoreUnchanged(t *testing.T) {
	for _, stage := range []string{"create", "write", "short-write", "sync", "close", "directory-sync", "parent-sync", "purge-sync"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			clock := &fixedPhysicalClock{now: 1_000_000}
			dir := t.TempDir()
			cfg := DurableStoreConfig{Dir: dir, NodeID: "local", PhysicalClock: clock}
			fault := errors.New("archive unavailable")
			cfg.wrapResyncArchive = func(log durableLog) durableLog { return &resyncArchiveFault{durableLog: log, stage: stage, err: fault} }
			if stage == "directory-sync" {
				cfg.syncResyncArchiveDir = func(string) error { return fault }
			}
			if stage == "parent-sync" {
				cfg.syncResyncArchiveDir = func(p string) error {
					if filepath.Base(p) != "archive" {
						return fault
					}
					return syncDir(p)
				}
			}
			if stage == "purge-sync" {
				cfg.preAppendHook = func(r durableRecord) error {
					if r.Type == durableRecordPurge {
						return fault
					}
					return nil
				}
			}
			local, err := NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			if _, err := local.Put(ctx, "lost", []byte{0, 255, 0}); err != nil {
				t.Fatal(err)
			}
			before := local.LocalStatus()
			entries, _ := local.ListEntries(ctx)
			if stage == "create" {
				if err := os.WriteFile(filepath.Join(dir, "archive"), []byte("blocked"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			peer := mustGCMemoryStore(t, "peer", clock, time.Second)
			clock.now += 100
			_, _ = peer.Delete(ctx, "gc")
			clock.now += 2000
			if _, err := peer.CollectTombstones(ctx); err != nil {
				t.Fatal(err)
			}
			engine := resyncArchiveEngine(t, local, peer)
			if err := engine.SyncOnce(ctx); err == nil {
				t.Fatal("resync succeeded despite archive/purge failure")
			}
			after, _ := local.ListEntries(ctx)
			if !reflect.DeepEqual(entries, after) || !reflect.DeepEqual(before, local.LocalStatus()) || !local.GCHorizon().isZero() || local.GCFloor() != 0 {
				t.Fatalf("failed purge changed live state: %+v", local.LocalStatus())
			}
			if err := local.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local", PhysicalClock: clock})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if !reflect.DeepEqual(before, reopened.LocalStatus()) {
				t.Fatalf("failure changed durable state: %+v", reopened.LocalStatus())
			}
		})
	}
}

type resyncArchiveFault struct {
	durableLog
	stage string
	err   error
}

func (f *resyncArchiveFault) Write(p []byte) (int, error) {
	if f.stage == "write" {
		return 0, f.err
	}
	if f.stage == "short-write" {
		return len(p) - 1, nil
	}
	return f.durableLog.Write(p)
}
func (f *resyncArchiveFault) Sync() error {
	if f.stage == "sync" {
		return f.err
	}
	return f.durableLog.Sync()
}
func (f *resyncArchiveFault) Close() error {
	err := f.durableLog.Close()
	if f.stage == "close" {
		return f.err
	}
	return err
}

func TestResyncLossWarningBeforeRetentionWithFrozenWallClock(t *testing.T) {
	ctx := context.Background()
	local, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local", TombstoneRetention: 400 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := NewMemoryStore(MemoryStoreConfig{NodeID: "peer"})
	if err != nil {
		t.Fatal(err)
	}
	engine := resyncArchiveEngine(t, local, peer, func(c *GossipConfig) { c.ResyncLossWarningFraction = .5 })
	if _, err := local.Put(ctx, "offline", []byte("write")); err != nil {
		t.Fatal(err)
	}
	engine.clock = func() time.Time { return time.Unix(0, 0) }
	// Close the real HTTP server through a failing transport boundary.
	original := engine.syncer
	engine.syncer = &offlineResyncSyncer{Syncer: original}
	started := time.Now()
	if err := engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected disconnected peer")
	}
	time.Sleep(250 * time.Millisecond)
	_ = engine.SyncOnce(ctx) // shared backoff must not suppress the early warning
	status, err := engine.ReplicationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := status.Peers[0]
	if !p.ResyncLossLimitApproaching || p.ResyncLossWarnings != 1 || p.UnreachableFor < 200*time.Millisecond || time.Since(started) >= 400*time.Millisecond {
		t.Fatalf("missing pre-retention warning: %+v", p)
	}
	_ = engine.SyncOnce(ctx)
	status, _ = engine.ReplicationStatus(ctx)
	if status.Peers[0].ResyncLossWarnings != 1 {
		t.Fatal("warning repeated during the same outage")
	}
	engine.syncer = original
	clearPeerBackoffForTest(engine, engine.peerProvider.Peers()[0])
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status, _ = engine.ReplicationStatus(ctx)
	if status.Peers[0].ResyncLossLimitApproaching || status.Peers[0].UnreachableFor != 0 {
		t.Fatal("warning not cleared after recovery")
	}
}

type offlineResyncSyncer struct{ Syncer }

func (s *offlineResyncSyncer) Push(context.Context, string, PushRequest) (*PushResponse, error) {
	return nil, errors.New("partition")
}

func TestResyncWarningsRunDuringLongBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local", TombstoneRetention: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := NewMemoryStore(MemoryStoreConfig{NodeID: "peer"})
	if err != nil {
		t.Fatal(err)
	}
	engine := resyncArchiveEngine(t, local, peer, func(c *GossipConfig) {
		c.ResyncLossWarningFraction = .1
		c.Interval = time.Hour
		c.AntiEntropyInterval = time.Hour
		c.BackoffBase = time.Hour
	})
	if _, err := local.Put(ctx, "offline", []byte("write")); err != nil {
		t.Fatal(err)
	}
	engine.syncer = &offlineResyncSyncer{Syncer: engine.syncer}
	if err := engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected failure")
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.ReplicationStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Peers[0].ResyncLossLimitApproaching {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Run did not warn while both replication loops were idle in backoff")
}

func TestResyncWarningFractionValidation(t *testing.T) {
	store, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	syncer, err := NewHTTPSyncer(HTTPSyncerConfig{NodeID: "local", AuthToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	for _, fraction := range []float64{-1, 1, 2, math.NaN(), math.Inf(1)} {
		if _, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: store, Syncer: syncer, ResyncLossWarningFraction: fraction}); err == nil {
			t.Fatalf("accepted fraction %v", fraction)
		}
	}
}

func TestMemoryReplicaCanRetainResyncArchives(t *testing.T) {
	ctx := context.Background()
	clock := &fixedPhysicalClock{now: 1_000_000}
	dir := t.TempDir()
	local, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local", PhysicalClock: clock, ArchiveDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	lost, err := local.Put(ctx, "lost", []byte{255, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	peer := mustGCMemoryStore(t, "peer", clock, time.Second)
	clock.now += 100
	if _, err := peer.Delete(ctx, "gc"); err != nil {
		t.Fatal(err)
	}
	clock.now += 2000
	if _, err := peer.CollectTombstones(ctx); err != nil {
		t.Fatal(err)
	}
	engine := resyncArchiveEngine(t, local, peer)
	if err := engine.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status := local.LocalStatus().Resync
	archived := readResyncArchive(t, filepath.Join(dir, status.LatestArchivePath))
	if len(archived) != 1 || !reflect.DeepEqual(archived[0].Entry, lost) || status.PurgedLocalOrigin != 1 {
		t.Fatalf("memory archive mismatch: %+v %+v", archived, status)
	}
	status.ByWriter[0].Entries = 99
	if local.LocalStatus().Resync.ByWriter[0].Entries != 1 {
		t.Fatal("status caller mutated store counters")
	}
}
