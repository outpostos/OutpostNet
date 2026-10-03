package state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var errInjectedCompaction = errors.New("injected compaction failure")

// durableSnapshotForTest captures everything a cold start must restore.
type durableSnapshotForTest struct {
	Entries      []Entry
	Positions    []uint64
	PeerProgress map[string]uint64
	Horizon      Timestamp
	Floor        uint64
	Current      uint64
}

func captureDurable(t *testing.T, store *DurableStore, peers ...string) durableSnapshotForTest {
	t.Helper()
	ctx := context.Background()
	entries, err := store.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	snapshot := durableSnapshotForTest{
		Entries:      entries,
		PeerProgress: make(map[string]uint64),
		Horizon:      store.GCHorizon(),
		Floor:        store.GCFloor(),
		Current:      store.CurrentPosition(),
	}
	for _, entry := range entries {
		snapshot.Positions = append(snapshot.Positions, entry.Position)
	}
	for _, peer := range peers {
		progress, err := store.PeerProgress(ctx, peer)
		if err != nil {
			t.Fatalf("PeerProgress: %v", err)
		}
		snapshot.PeerProgress[peer] = progress
	}
	return snapshot
}

func assertSameDurableState(t *testing.T, got, want durableSnapshotForTest) {
	t.Helper()
	if !reflect.DeepEqual(got.Entries, want.Entries) || !reflect.DeepEqual(got.Positions, want.Positions) {
		t.Fatalf("entries differ after recovery:\n got  %+v\n want %+v", got.Entries, want.Entries)
	}
	if !reflect.DeepEqual(got.PeerProgress, want.PeerProgress) {
		t.Fatalf("peer progress differs: got %+v want %+v", got.PeerProgress, want.PeerProgress)
	}
	if got.Horizon != want.Horizon || got.Floor != want.Floor {
		t.Fatalf("GC state differs: got %+v/%d want %+v/%d", got.Horizon, got.Floor, want.Horizon, want.Floor)
	}
	if got.Current < want.Current {
		t.Fatalf("position regressed: got %d want >= %d", got.Current, want.Current)
	}
}

// seedCompactionStore builds a store with live values, an uncollected
// tombstone, a collected tombstone and saved peer progress.
func seedCompactionStore(t *testing.T, cfg DurableStoreConfig, clock *fixedPhysicalClock) (*DurableStore, Entry) {
	t.Helper()
	ctx := context.Background()
	store := mustDurableStore(t, cfg)
	if _, err := store.Put(ctx, "collected", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	collected, err := store.Delete(ctx, "collected")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for i := 0; i < 5; i++ {
		for round := 0; round < 3; round++ {
			if _, err := store.Put(ctx, fmt.Sprintf("live/%d", i), []byte(fmt.Sprintf("v%d", round))); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	clock.now += 2 * retentionMS()
	if result, err := store.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("CollectTombstones: %+v err=%v", result, err)
	}
	if _, err := store.Delete(ctx, "recent-tombstone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.SavePeerProgress(ctx, "peer-b", 7); err != nil {
		t.Fatalf("SavePeerProgress: %v", err)
	}
	return store, collected
}

func durableLogLines(t *testing.T, dir string) int {
	t.Helper()
	return len(readDurableLogRecords(t, dir))
}

func maxSequence(records []durableRecord) uint64 {
	var max uint64
	for _, record := range records {
		if record.Sequence > max {
			max = record.Sequence
		}
	}
	return max
}

func TestDurableCompactionPreservesStateCountersAndProgress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &fixedPhysicalClock{now: 1_000_000}
	cfg := DurableStoreConfig{Dir: dir, NodeID: "node-a", PhysicalClock: clock, TombstoneRetention: testRetention}
	store, collected := seedCompactionStore(t, cfg, clock)
	before := captureDurable(t, store, "peer-b")
	recordsBefore := readDurableLogRecords(t, dir)

	result, err := store.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.RecordsBefore != len(recordsBefore) || result.RecordsAfter >= result.RecordsBefore {
		t.Fatalf("unexpected compaction result %+v (before %d)", result, len(recordsBefore))
	}
	records := readDurableLogRecords(t, dir)
	if len(records) != result.RecordsAfter {
		t.Fatalf("log has %d records, result says %d", len(records), result.RecordsAfter)
	}
	if records[0].Type != durableRecordSnapshot {
		t.Fatalf("compacted log must start with a snapshot header, got %+v", records[0])
	}
	// One header, one record per current entry (live values and the uncollected
	// tombstone) and one per peer.
	if want := 1 + len(before.Entries) + 1; len(records) != want {
		t.Fatalf("compacted log has %d records, want %d", len(records), want)
	}
	assertIncreasingSequences(t, records)
	if records[0].Sequence <= maxSequence(recordsBefore) {
		t.Fatalf("compaction regressed sequence: %d after %d", records[0].Sequence, maxSequence(recordsBefore))
	}
	assertSameDurableState(t, captureDurable(t, store, "peer-b"), before)

	// The store keeps appending to the compacted log.
	afterCompact, err := store.Put(ctx, "after-compact", []byte("y"))
	if err != nil {
		t.Fatalf("Put after Compact: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clock.now = 1 // physical clock far behind every recovered HLC
	recovered := mustDurableStore(t, cfg)
	defer recovered.Close()
	got := captureDurable(t, recovered, "peer-b")
	if mustGet(t, recovered, "after-compact").ChangeID != afterCompact.ChangeID {
		t.Fatalf("append after compaction lost")
	}
	assertAbsent(t, recovered, "collected")
	if entry := mustGet(t, recovered, "recent-tombstone"); !entry.Deleted {
		t.Fatalf("uncollected tombstone lost by compaction: %+v", entry)
	}
	if got.Horizon != before.Horizon || got.Floor != before.Floor || got.PeerProgress["peer-b"] != 7 {
		t.Fatalf("GC state or progress lost by compaction: %+v", got)
	}
	next, err := recovered.Put(ctx, "next", []byte("z"))
	if err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if next.ChangeID.Counter <= afterCompact.ChangeID.Counter || next.Position <= afterCompact.Position ||
		next.Version.HLC.Compare(afterCompact.Version.HLC) <= 0 || next.Version.HLC.Compare(collected.Version.HLC) <= 0 {
		t.Fatalf("counters regressed after compaction+restart: next %+v, last %+v", next, afterCompact)
	}
	if applied, err := recovered.Apply(ctx, Entry{Key: "collected", Value: []byte("x"), Version: StateVersion{WriterNodeID: "node-a", HLC: Timestamp{WallMS: 1_000_000}}, ChangeID: ChangeID{OriginNodeID: "node-a", Counter: 1}}); applied || err == nil {
		t.Fatalf("collected key resurrected after compaction: applied=%v err=%v", applied, err)
	}
}

func TestDurableLogAndColdReplayStayBoundedUnderChurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &fixedPhysicalClock{now: 1_000_000}
	cfg := DurableStoreConfig{
		Dir:                  dir,
		NodeID:               "node-a",
		PhysicalClock:        clock,
		TombstoneRetention:   testRetention,
		CompactionMinRecords: 32,
	}
	store := mustDurableStore(t, cfg)
	const live = 5
	const perRound = 10
	bound := 2 * (1 + live + perRound*2)
	if bound < cfg.CompactionMinRecords {
		bound = cfg.CompactionMinRecords
	}
	for i := 0; i < live; i++ {
		if _, err := store.Put(ctx, fmt.Sprintf("live/%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	const rounds = 8
	maxLines := 0
	compactions := 0
	for round := 0; round < rounds; round++ {
		// TTL-like churn: every round uses fresh IDs that are written and deleted.
		for i := 0; i < perRound; i++ {
			key := fmt.Sprintf("ttl/%d/%d", round, i)
			if _, err := store.Put(ctx, key, []byte("x")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if _, err := store.Delete(ctx, key); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		}
		clock.now += retentionMS() + 1
		if _, err := store.CollectTombstones(ctx); err != nil {
			t.Fatalf("CollectTombstones: %v", err)
		}
		// The peak log size is right before compaction.
		if lines := durableLogLines(t, dir); lines > maxLines {
			maxLines = lines
		}
		if lines := durableLogLines(t, dir); lines > bound {
			t.Fatalf("round %d: log has %d records before compaction, exceeds bound %d", round, lines, bound)
		}
		compacted, err := store.CompactIfNeeded(ctx)
		if err != nil {
			t.Fatalf("CompactIfNeeded: %v", err)
		}
		if compacted {
			compactions++
		}
		entries, err := store.ListEntries(ctx)
		if err != nil {
			t.Fatalf("ListEntries: %v", err)
		}
		if len(entries) != live {
			t.Fatalf("round %d: memory index holds %d entries, want %d", round, len(entries), live)
		}
		lines := durableLogLines(t, dir)
		if lines > maxLines {
			maxLines = lines
		}
		if lines > bound {
			t.Fatalf("round %d: log has %d records, exceeds bound %d", round, lines, bound)
		}
	}
	if compactions == 0 {
		t.Fatalf("CompactIfNeeded never compacted")
	}
	before := captureDurable(t, store)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Cold start replays exactly the records on disk, which stay bounded by the
	// live set instead of the rounds*perRound*2 writes of history.
	if history := rounds * perRound * 2; history <= bound {
		t.Fatalf("test workload too small: history %d within bound %d", history, bound)
	}
	replayed := durableLogLines(t, dir)
	if replayed > bound {
		t.Fatalf("cold start would replay %d records, bound %d", replayed, bound)
	}
	recovered := mustDurableStore(t, cfg)
	defer recovered.Close()
	assertSameDurableState(t, captureDurable(t, recovered), before)
	t.Logf("%d rounds x %d TTL keys: %d compactions, max log %d records, cold replay %d records, bound %d", rounds, perRound, compactions, maxLines, replayed, bound)
}

// crashImage copies the durable directory as it is on disk at this instant,
// which is what a process crash at this point leaves behind.
func crashImage(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() == durableLockFile {
			continue
		}
		in, err := os.Open(filepath.Join(src, entry.Name()))
		if err != nil {
			t.Fatalf("open %s: %v", entry.Name(), err)
		}
		out, err := os.Create(filepath.Join(dst, entry.Name()))
		if err != nil {
			t.Fatalf("create %s: %v", entry.Name(), err)
		}
		if _, err := io.Copy(out, in); err != nil {
			t.Fatalf("copy %s: %v", entry.Name(), err)
		}
		_ = in.Close()
		if err := out.Close(); err != nil {
			t.Fatalf("close %s: %v", entry.Name(), err)
		}
	}
	return dst
}

func TestDurableCompactionCrashAtEveryStepRecovers(t *testing.T) {
	steps := []string{compactStepWriteTmp, compactStepSyncTmp, compactStepRename, compactStepSyncDir, compactStepReopen}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			clock := &fixedPhysicalClock{now: 1_000_000}
			var image string
			cfg := DurableStoreConfig{
				Dir:                dir,
				NodeID:             "node-a",
				PhysicalClock:      clock,
				TombstoneRetention: testRetention,
				compactionHook: func(current string) error {
					if current == step {
						image = crashImage(t, dir)
					}
					return nil
				},
			}
			store, _ := seedCompactionStore(t, cfg, clock)
			before := captureDurable(t, store, "peer-b")
			if _, err := store.Compact(ctx); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if image == "" {
				t.Fatalf("hook for step %s never ran", step)
			}

			cfg.Dir = image
			cfg.compactionHook = nil
			recovered := mustDurableStore(t, cfg)
			defer recovered.Close()
			assertSameDurableState(t, captureDurable(t, recovered, "peer-b"), before)
			if _, err := os.Stat(filepath.Join(image, durableCompactTmpFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovery left the compaction temp file behind: %v", err)
			}
			next, err := recovered.Put(ctx, "after-crash", []byte("x"))
			if err != nil {
				t.Fatalf("Put after crash recovery: %v", err)
			}
			if next.Position <= before.Current {
				t.Fatalf("position reused after crash: %d <= %d", next.Position, before.Current)
			}
			assertIncreasingSequences(t, readDurableLogRecords(t, image))
		})
	}
}

func TestDurableCompactionFailuresKeepStoreConsistent(t *testing.T) {
	cases := []struct {
		name       string
		arm        func(fault *logFault)
		hookStep   string
		failClosed bool
	}{
		{name: "short-write-temp", arm: func(f *logFault) { f.shortWrites = 1 }},
		{name: "write-temp", arm: func(f *logFault) { f.failWrites = 1 }},
		{name: "sync-temp", arm: func(f *logFault) { f.failSyncs = 1 }},
		{name: "rename", hookStep: compactStepRename},
		{name: "sync-dir", hookStep: compactStepSyncDir, failClosed: true},
		{name: "reopen", hookStep: compactStepReopen, failClosed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			clock := &fixedPhysicalClock{now: 1_000_000}
			fault := &logFault{}
			cfg := DurableStoreConfig{
				Dir:                dir,
				NodeID:             "node-a",
				PhysicalClock:      clock,
				TombstoneRetention: testRetention,
				wrapLog:            fault.wrap,
				compactionHook: func(step string) error {
					if tc.hookStep != "" && step == tc.hookStep {
						return errInjectedCompaction
					}
					return nil
				},
			}
			store, _ := seedCompactionStore(t, cfg, clock)
			before := captureDurable(t, store, "peer-b")
			if tc.arm != nil {
				fault.arm(tc.arm)
			}
			_, err := store.Compact(ctx)
			if err == nil {
				t.Fatalf("expected compaction failure")
			}
			if tc.failClosed != errors.Is(err, ErrDurableStoreUnwritable) {
				t.Fatalf("fail-closed=%v but error was %v", tc.failClosed, err)
			}
			if _, statErr := os.Stat(filepath.Join(dir, durableCompactTmpFile)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed compaction left the temp file: %v", statErr)
			}
			assertSameDurableState(t, captureDurable(t, store, "peer-b"), before)
			_, putErr := store.Put(ctx, "after-failure", []byte("x"))
			if tc.failClosed {
				if !errors.Is(putErr, ErrDurableStoreUnwritable) {
					t.Fatalf("store must be fail-closed after %s, Put returned %v", tc.name, putErr)
				}
				assertAbsent(t, store, "after-failure")
			} else if putErr != nil {
				t.Fatalf("store must stay writable after %s: %v", tc.name, putErr)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			cfg.wrapLog = nil
			cfg.compactionHook = nil
			recovered := mustDurableStore(t, cfg)
			defer recovered.Close()
			got := captureDurable(t, recovered, "peer-b")
			if _, ok, _ := recovered.Get(ctx, "after-failure"); ok == tc.failClosed {
				t.Fatalf("after-failure presence=%v, fail-closed=%v", ok, tc.failClosed)
			}
			if !tc.failClosed {
				// Drop the post-failure write before comparing.
				got.Entries = got.Entries[:len(got.Entries)-1]
				got.Positions = got.Positions[:len(got.Positions)-1]
			}
			assertSameDurableState(t, got, before)
			assertIncreasingSequences(t, readDurableLogRecords(t, dir))
		})
	}
}

func TestDurableRecoveryRejectsMisplacedSnapshotHeader(t *testing.T) {
	dir := makeDurableDir(t, "node-a")
	header := durableRecordLine(t, durableRecord{Type: durableRecordSnapshot, Sequence: 2, Position: 1})
	data := append(durableEntryRecordLine(t, 1, "a", 1), header...)
	if err := os.WriteFile(filepath.Join(dir, durableLogFile), data, 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}
	_, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	if err == nil || !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("expected misplaced snapshot header to refuse opening, got %v", err)
	}
}

func TestDurableCompactionHeaderAloneRestoresCounters(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &fixedPhysicalClock{now: 1_000_000}
	cfg := DurableStoreConfig{Dir: dir, NodeID: "node-a", PhysicalClock: clock, TombstoneRetention: testRetention}
	store := mustDurableStore(t, cfg)
	if _, err := store.Put(ctx, "only", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	tombstone, err := store.Delete(ctx, "only")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	clock.now += 2 * retentionMS()
	if result, err := store.CollectTombstones(ctx); err != nil || result.Collected != 1 {
		t.Fatalf("CollectTombstones: %+v err=%v", result, err)
	}
	if _, err := store.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if records := readDurableLogRecords(t, dir); len(records) != 1 || records[0].Type != durableRecordSnapshot {
		t.Fatalf("expected a header-only log, got %+v", records)
	}

	clock.now = 1 // the physical clock went far backwards
	recovered := mustDurableStore(t, cfg)
	defer recovered.Close()
	next, err := recovered.Put(ctx, "next", []byte("y"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if next.ChangeID.Counter <= tombstone.ChangeID.Counter || next.Position <= tombstone.Position {
		t.Fatalf("change counter or position reused: next %+v tombstone %+v", next, tombstone)
	}
	if next.Version.HLC.Compare(tombstone.Version.HLC) <= 0 {
		t.Fatalf("HLC regressed below a collected tombstone: %+v <= %+v", next.Version.HLC, tombstone.Version.HLC)
	}
}
