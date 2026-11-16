package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestClockResetArchivesPurgedEntries(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(map[bool]string{false: "log", true: "snapshot"}[compact], func(t *testing.T) {
			ctx := context.Background()
			now := int64(1_800_000_000_000)
			clock := PhysicalClockFunc(func() int64 { return now })
			cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", PhysicalClock: clock}
			s, err := NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := s.Put(ctx, "healthy", []byte("keep")); err != nil {
				t.Fatal(err)
			}
			now += 2 * MaxClockOffsetMS
			if _, err := s.Put(ctx, "binary", []byte("superseded")); err != nil {
				t.Fatal(err)
			}
			value := make([]byte, 256)
			for i := range value {
				value[i] = byte(i)
			}
			if _, err := s.Put(ctx, "binary", value); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Delete(ctx, "tombstone"); err != nil {
				t.Fatal(err)
			}
			remote := Entry{Key: "forwarded", Value: []byte{0xff, 0, '\n'}, Version: StateVersion{WriterNodeID: "peer", HLC: Timestamp{WallMS: now}}, ChangeID: ChangeID{OriginNodeID: "peer", Counter: 90}}
			if applied, err := s.Apply(ctx, remote); err != nil || !applied {
				t.Fatalf("forwarded write: %v %v", applied, err)
			}
			want, err := s.ListEntries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if compact {
				if _, err := s.Compact(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			now -= 2 * MaxClockOffsetMS
			result, err := ResetDurableClock(ctx, ClockResetConfig{Dir: cfg.Dir, Reference: clock})
			if err != nil || result.Purged != 3 {
				t.Fatalf("reset: %+v %v", result, err)
			}
			paths, err := filepath.Glob(filepath.Join(cfg.Dir, "archive", "clock-reset-*.jsonl"))
			if err != nil || len(paths) != 1 {
				t.Fatalf("expected one purge archive, got %v: %v", paths, err)
			}
			if result.ArchivePath != paths[0] || result.Archived != 3 {
				t.Fatalf("archive result: %+v", result)
			}
			file, err := os.Open(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			decoder := json.NewDecoder(file)
			archived := make(map[string]Entry)
			for {
				var record struct {
					Entry
					Position uint64 `json:"position"`
				}
				if err := decoder.Decode(&record); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				record.Entry.Position = record.Position
				if _, duplicate := archived[record.Key]; duplicate {
					t.Fatalf("duplicate archive key %q", record.Key)
				}
				archived[record.Key] = record.Entry
			}
			if len(archived) != 3 {
				t.Fatalf("archive count: %d", len(archived))
			}
			for _, entry := range want {
				if entry.Key == "healthy" {
					if _, ok := archived[entry.Key]; ok {
						t.Fatal("healthy entry archived")
					}
					continue
				}
				if !reflect.DeepEqual(archived[entry.Key], entry) {
					t.Fatalf("archive changed %q value or metadata", entry.Key)
				}
			}
			// Make the archived clocks admissible: exclusion must come from the
			// recovery/replication boundary, not from future-clock rejection.
			now += 2 * MaxClockOffsetMS
			s, err = NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			peer, err := NewMemoryStore(MemoryStoreConfig{NodeID: "peer", PhysicalClock: clock})
			if err != nil {
				t.Fatal(err)
			}
			_, pulling, _ := clockHTTPPair(t, s, peer)
			if err := pulling.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			for _, store := range []Store{s, peer} {
				entries, err := store.ListEntries(ctx)
				if err != nil || len(entries) != 1 || entries[0].Key != "healthy" {
					t.Fatalf("archive leaked into recovery/replication: %+v %v", entries, err)
				}
			}
		})
	}
}

func TestClockResetRebuildFailureRetainsArchiveAndRefusesRecovery(t *testing.T) {
	for _, step := range []string{compactStepWriteTmp, compactStepSyncTmp, compactStepRename, compactStepSyncDir, compactStepReopen} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			now := int64(1_800_000_000_000)
			clock := PhysicalClockFunc(func() int64 { return now })
			cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", PhysicalClock: clock}
			s, err := NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, "old", []byte("recover manually")); err != nil {
				t.Fatal(err)
			}
			if err := s.appendPurge(purgeSet{Horizon: Timestamp{WallMS: now + 2*MaxClockOffsetMS}, Floor: 50}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			oldLog, err := os.ReadFile(filepath.Join(cfg.Dir, durableLogFile))
			if err != nil {
				t.Fatal(err)
			}
			reset := ClockResetConfig{Dir: cfg.Dir, Reference: clock, Rebuild: true, compactionHook: func(at string) error {
				if at == step {
					return errInjectedCompaction
				}
				return nil
			}}
			if result, err := ResetDurableClock(ctx, reset); err == nil || result.Purged != 0 {
				t.Fatalf("incomplete rebuild reported success: %+v %v", result, err)
			}
			paths, err := filepath.Glob(filepath.Join(cfg.Dir, "archive", "rebuild-*", durableLogFile))
			if err != nil || len(paths) != 1 {
				t.Fatalf("missing rebuild archive: %v %v", paths, err)
			}
			archived, err := os.ReadFile(paths[0])
			if err != nil || !bytes.Equal(archived, oldLog) {
				t.Fatalf("incomplete rebuild damaged old log: %v", err)
			}
			if opened, err := NewDurableStore(cfg); !errors.Is(err, ErrClockRebuildIncomplete) {
				if opened != nil {
					opened.Close()
				}
				t.Fatalf("incomplete rebuild allowed startup: %v", err)
			}
			if _, err := ResetDurableClock(ctx, reset); err == nil {
				t.Fatal("incomplete rebuild allowed automatic retry")
			}
		})
	}
}

func TestClockResetArchiveFailurePreservesStore(t *testing.T) {
	for _, step := range []string{"create-directory", "short-write", "sync-file", "sync-archive-directory", "sync-parent-directory"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			now := int64(1_800_000_000_000)
			clock := PhysicalClockFunc(func() int64 { return now })
			cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", PhysicalClock: clock}
			s, err := NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, "future", []byte("preserve on failure")); err != nil {
				t.Fatal(err)
			}
			want, err := s.ListEntries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			position := s.CurrentPosition()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			// Include unfinished state artifacts: an unsuccessful reset must not even
			// repair the log tail or discard the uninstalled compaction.
			logPath := filepath.Join(cfg.Dir, durableLogFile)
			log, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(logPath, append(log, []byte("{unconfirmed")...), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.Dir, durableCompactTmpFile), []byte("uninstalled snapshot"), 0o600); err != nil {
				t.Fatal(err)
			}
			before := make(map[string][]byte)
			for _, name := range []string{durableManifestFile, durableLogFile, durableCompactTmpFile} {
				before[name], err = os.ReadFile(filepath.Join(cfg.Dir, name))
				if err != nil {
					t.Fatal(err)
				}
			}
			// A file in place of the archive directory gives a real filesystem error,
			// including when the test runs with root privileges.
			if step == "create-directory" {
				if err := os.WriteFile(filepath.Join(cfg.Dir, "archive"), []byte("blocked"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			now -= 2 * MaxClockOffsetMS
			fault := &logFault{}
			if step == "short-write" {
				fault.arm(func(f *logFault) { f.shortWrites = 1 })
			}
			if step == "sync-file" {
				fault.arm(func(f *logFault) { f.failSyncs = 1 })
			}
			reset := ClockResetConfig{Dir: cfg.Dir, Reference: clock, wrapArchive: fault.wrap, archiveSyncDir: func(dir string) error {
				if step == "sync-parent-directory" && dir == cfg.Dir || step == "sync-archive-directory" && dir == filepath.Join(cfg.Dir, "archive") {
					return errInjectedSync
				}
				return syncDir(dir)
			}}
			result, err := ResetDurableClock(ctx, reset)
			if err == nil || result.Purged != 0 {
				t.Fatalf("archive failure reported success: %+v %v", result, err)
			}
			for name, data := range before {
				after, err := os.ReadFile(filepath.Join(cfg.Dir, name))
				if err != nil || !bytes.Equal(after, data) {
					t.Fatalf("archive failure changed %s: %v", name, err)
				}
			}
			s, err = NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			got, err := s.ListEntries(ctx)
			if err != nil || !reflect.DeepEqual(got, want) || s.CurrentPosition() != position {
				t.Fatalf("archive failure changed recovered store: %+v %v", got, err)
			}
		})
	}
}
