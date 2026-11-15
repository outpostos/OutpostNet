package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestClockResetRecovery(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprint(compact), func(t *testing.T) {
			ctx := context.Background()
			now := int64(1_800_000_000_000)
			clock := PhysicalClockFunc(func() int64 { return now })
			cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", PhysicalClock: clock}
			s, err := NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, "healthy", []byte("keep")); err != nil {
				t.Fatal(err)
			}
			now += int64(365 * 24 * time.Hour / time.Millisecond)
			if _, err := s.Put(ctx, "future", []byte("wrong")); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Delete(ctx, "deleted"); err != nil {
				t.Fatal(err)
			}
			remote := Entry{Key: "forwarded", Version: StateVersion{HLC: Timestamp{WallMS: now}, WriterNodeID: "other"}, ChangeID: ChangeID{OriginNodeID: "other", Counter: 90}}
			if _, err := s.Apply(ctx, remote); err != nil {
				t.Fatal(err)
			}
			if compact {
				if _, err := s.Compact(ctx); err != nil {
					t.Fatal(err)
				}
			}
			position := s.CurrentPosition()
			if _, err := ResetDurableClock(ctx, ClockResetConfig{Dir: cfg.Dir, Reference: clock}); !errors.Is(err, ErrDurableStoreLocked) {
				t.Fatalf("live reset: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			now -= int64(365 * 24 * time.Hour / time.Millisecond)
			s, err = NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if statuses := s.ClockStatus(); len(statuses) == 0 || !statuses[0].ClockSuspect {
				t.Fatalf("missing local alarm: %+v", statuses)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			result, err := ResetDurableClock(ctx, ClockResetConfig{Dir: cfg.Dir, Reference: clock})
			if err != nil {
				t.Fatal(err)
			}
			if result.Purged != 3 || result.Rebuilt || result.Floor <= position {
				t.Fatalf("reset result: %+v", result)
			}
			for i := 0; i < 2; i++ {
				s, err = NewDurableStore(cfg)
				if err != nil {
					t.Fatal(err)
				}
				entries, err := s.ListEntries(ctx)
				if err != nil || len(entries) != 1 || entries[0].Key != "healthy" {
					t.Fatalf("remaining: %+v %v", entries, err)
				}
				if !s.CheckConsumerPosition(position).NeedsSnapshot {
					t.Fatal("old cursor survived purge")
				}
				if err := checkFutureTimestamp(s.hlc.Current(), now); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					if _, err := s.Compact(ctx); err != nil {
						t.Fatal(err)
					}
					s.Close()
				}
			}
			defer s.Close()
			fresh, err := s.Put(ctx, "future", []byte("correct"))
			if err != nil || fresh.ChangeID.Counter <= position {
				t.Fatalf("fresh write: %+v %v", fresh, err)
			}
			peer, err := NewMemoryStore(MemoryStoreConfig{NodeID: "healthy-peer", PhysicalClock: clock})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Put(ctx, "deleted", []byte("older healthy value")); err != nil {
				t.Fatal(err)
			}
			if applied, err := peer.Apply(ctx, fresh); err != nil || !applied {
				t.Fatalf("peer refused reset writer: %v", err)
			}
			_, pulling, _ := clockHTTPPair(t, peer, s)
			if err := pulling.AntiEntropyOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if restored, found, err := s.Get(ctx, "deleted"); err != nil || !found || restored.Deleted || string(restored.Value) != "older healthy value" {
				t.Fatalf("purged future tombstone still hides healthy peer value: %+v %v", restored, err)
			}
			if _, err := s.Apply(ctx, remote); err == nil {
				t.Fatal("old future write accepted")
			}
			got, _, _ := s.Get(ctx, "future")
			if string(got.Value) != "correct" {
				t.Fatal("old future write won")
			}
		})
	}
}

func TestClockResetFutureHorizonUpgrade(t *testing.T) {
	ctx := context.Background()
	now := int64(1_800_000_000_000)
	clock := PhysicalClockFunc(func() int64 { return now })
	cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "retained-identity", PhysicalClock: clock}
	s, err := NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "keep-until-rebuild", nil); err != nil {
		t.Fatal(err)
	}
	// Simulate pre-D4 persisted purge; normal GC can no longer generate it.
	future := Timestamp{WallMS: now + int64(365*24*time.Hour/time.Millisecond)}
	if err := s.appendPurge(purgeSet{Horizon: future, Floor: 50, Adopted: future}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	manifestPath := filepath.Join(cfg.Dir, durableManifestFile)
	if err := writeManifest(manifestPath, durableManifest{FormatVersion: durableFormatVersion, NodeID: cfg.NodeID, PolicyID: NewHLCLWWPolicy().ID()}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(cfg.Dir, durableLogFile))
	if opened, err := NewDurableStore(cfg); !errors.Is(err, ErrClockRebuildRequired) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("upgrade allowed future horizon: %v", err)
	}
	if _, err := ResetDurableClock(ctx, ClockResetConfig{Dir: cfg.Dir, Reference: clock}); !errors.Is(err, ErrClockRebuildRequired) {
		t.Fatalf("partial reset allowed: %v", err)
	}
	after, _ := os.ReadFile(filepath.Join(cfg.Dir, durableLogFile))
	if string(before) != string(after) {
		t.Fatal("refused reset changed log")
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, durableLogFile), append(before, []byte("{unconfirmed")...), 0o600); err != nil {
		t.Fatal(err)
	}
	// Rebuild must preserve even unfinished artifacts byte-for-byte by rename.
	for name, data := range map[string]string{durableCompactTmpFile: "uninstalled compaction", durableManifestFile + ".tmp": "uninstalled manifest"} {
		if err := os.WriteFile(filepath.Join(cfg.Dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldFiles := make(map[string][]byte)
	oldInfo := make(map[string]os.FileInfo)
	for _, name := range []string{durableLogFile, durableManifestFile, durableCompactTmpFile, durableManifestFile + ".tmp"} {
		oldFiles[name], err = os.ReadFile(filepath.Join(cfg.Dir, name))
		if err != nil {
			t.Fatal(err)
		}
		oldInfo[name], err = os.Stat(filepath.Join(cfg.Dir, name))
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := ResetDurableClock(ctx, ClockResetConfig{Dir: cfg.Dir, Reference: clock, Rebuild: true})
	if err != nil || !result.Rebuilt || result.Purged != 1 || result.Floor <= 50 {
		t.Fatalf("rebuild: %+v %v", result, err)
	}
	if filepath.Dir(result.ArchivePath) != filepath.Join(cfg.Dir, "archive") || !strings.HasPrefix(filepath.Base(result.ArchivePath), "rebuild-") || result.Archived != 1 {
		t.Fatalf("rebuild archive result: %+v", result)
	}
	for name, data := range oldFiles {
		path := filepath.Join(result.ArchivePath, name)
		got, err := os.ReadFile(path)
		if err != nil || !reflect.DeepEqual(got, data) {
			t.Fatalf("rebuild changed archived %s: %v", name, err)
		}
		info, err := os.Stat(path)
		if err != nil || !os.SameFile(oldInfo[name], info) {
			t.Fatalf("rebuild did not rename %s: %v", name, err)
		}
	}
	s, err = NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if entries, err := s.ListEntries(ctx); err != nil || len(entries) != 0 || !s.CheckConsumerPosition(50).NeedsSnapshot {
		t.Fatalf("rebuild recovered archived entries or old cursor: %+v %v", entries, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if entries, err := s.ListEntries(ctx); err != nil || len(entries) != 0 {
		t.Fatalf("second startup recovered archived state: %+v %v", entries, err)
	}
	if s.nodeID != cfg.NodeID || !s.GCHorizon().isZero() || !s.adoptedHorizon().isZero() {
		t.Fatal("identity or horizon incorrect")
	}
	fresh, err := s.Put(ctx, "new", nil)
	if err != nil || fresh.ChangeID.Counter <= 50 || checkFutureTimestamp(fresh.Version.HLC, now) != nil {
		t.Fatalf("rebuild write: %+v %v", fresh, err)
	}
}

func TestClockStepDetection(t *testing.T) {
	system := int64(1_800_000_000_000)
	mono := time.Unix(0, 0)
	ref, err := NewTimeReference(TimeReferenceConfig{SystemClock: PhysicalClockFunc(func() int64 { return system }), MonotonicNow: func() time.Time { return mono }})
	if err != nil {
		t.Fatal(err)
	}
	baseline := ref.NowMS()
	mono = mono.Add(time.Second)
	system += MaxClockOffsetMS + 1001
	if got := ref.NowMS(); got != baseline+1000 || !ref.TimeStatus().ClockSuspect {
		t.Fatalf("step adopted: %d %+v", got, ref.TimeStatus())
	}
	mono = mono.Add(time.Second)
	if got := ref.NowMS(); got != baseline+2000 {
		t.Fatalf("rejected time not extrapolated: %d", got)
	}
	if err := ref.UpdateSource(TimeSourceStatus{Synchronized: true}); err != nil {
		t.Fatal(err)
	}
	if s := ref.TimeStatus(); s.ReferenceMS != system || s.ClockSuspect {
		t.Fatalf("leaving UNSYNCED baseline: %+v", s)
	}
	mono = mono.Add(time.Second)
	system += 1000
	if ref.NowMS() != system || ref.TimeStatus().ClockSuspect {
		t.Fatal("baseline was not reset")
	}
	system += 2 * MaxClockOffsetMS
	if ref.NowMS() == system {
		t.Fatal("second step accepted")
	}
	ref.ConfirmSystemClock()
	if ref.NowMS() != system || ref.TimeStatus().ClockSuspect {
		t.Fatal("operator confirmation ignored")
	}
}

func TestClockResetBoundaryAndRetry(t *testing.T) {
	ctx := context.Background()
	now := int64(1_800_000_000_000)
	clock := PhysicalClockFunc(func() int64 { return now })
	dir := t.TempDir()
	cfg := DurableStoreConfig{Dir: dir, NodeID: "local", PhysicalClock: clock}
	s, err := NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now += MaxClockOffsetMS
	boundary, err := s.Put(ctx, "at-valve", nil)
	if err != nil {
		t.Fatal(err)
	}
	now++
	if _, err := s.Put(ctx, "over-valve", nil); err != nil {
		t.Fatal(err)
	}
	s.Close()
	now -= MaxClockOffsetMS + 1
	result, err := ResetDurableClock(ctx, ClockResetConfig{Dir: dir, Reference: clock})
	if err != nil || result.Purged != 1 {
		t.Fatalf("boundary purge: %+v %v", result, err)
	}
	result, err = ResetDurableClock(ctx, ClockResetConfig{Dir: dir, Reference: clock})
	if err != nil || result.Purged != 0 {
		t.Fatalf("retry: %+v %v", result, err)
	}
	s, err = NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, found, _ := s.Get(ctx, boundary.Key)
	if !found || got.Identity() != boundary.Identity() {
		t.Fatal("within-valve state changed")
	}
	fresh, err := s.Put(ctx, "fresh", nil)
	if err != nil || fresh.Version.HLC.Compare(boundary.Version.HLC) <= 0 {
		t.Fatalf("retained boundary outranks new write: %+v %v", fresh, err)
	}
	// Cancellation and exhausted counters must fail without confirming a purge.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ResetDurableClock(cancelled, ClockResetConfig{Dir: dir, Reference: clock}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.clock.SetAtLeast(math.MaxUint64)
	if _, err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := ResetDurableClock(ctx, ClockResetConfig{Dir: dir, Reference: clock}); err == nil {
		t.Fatal("counter wrapped")
	}
}

func TestClockStepBoundaryAndElapsedTime(t *testing.T) {
	system := int64(1_800_000_000_000)
	mono := time.Unix(0, 0)
	ref, err := NewTimeReference(TimeReferenceConfig{SystemClock: PhysicalClockFunc(func() int64 { return system }), MonotonicNow: func() time.Time { return mono }})
	if err != nil {
		t.Fatal(err)
	}
	mono = mono.Add(24 * time.Hour)
	system += int64(24 * time.Hour / time.Millisecond)
	if ref.NowMS() != system || ref.TimeStatus().ClockSuspect {
		t.Fatal("ordinary elapsed time rejected")
	}
	system += MaxClockOffsetMS
	if ref.NowMS() != system || ref.TimeStatus().ClockSuspect {
		t.Fatal("valve boundary rejected")
	}
	system -= 1000
	if ref.NowMS() != system || ref.TimeStatus().ClockSuspect {
		t.Fatal("backward step rejected")
	}
}

func TestClockResetFailureRecovery(t *testing.T) {
	for _, step := range []string{"short-write", "sync-temp", "rename", "sync-dir", "reopen"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			now := int64(1_800_000_000_000)
			clock := PhysicalClockFunc(func() int64 { return now })
			cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", PhysicalClock: clock}
			s, err := NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, "healthy", nil); err != nil {
				t.Fatal(err)
			}
			now += 2 * MaxClockOffsetMS
			if _, err := s.Put(ctx, "future", nil); err != nil {
				t.Fatal(err)
			}
			s.Close()
			now -= 2 * MaxClockOffsetMS
			fault := &logFault{}
			reset := ClockResetConfig{Dir: cfg.Dir, Reference: clock, wrapLog: fault.wrap, compactionHook: func(at string) error {
				if at == step {
					return errInjectedCompaction
				}
				return nil
			}}
			if step == "short-write" {
				fault.arm(func(f *logFault) { f.shortWrites = 1 })
			}
			result, err := ResetDurableClock(ctx, reset)
			if err == nil || result.Purged != 0 {
				t.Fatalf("failure reported success: %+v %v", result, err)
			}
			s, err = NewDurableStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, future, _ := s.Get(ctx, "future")
			_, healthy, _ := s.Get(ctx, "healthy")
			installed := step == "sync-dir" || step == "reopen"
			if !healthy || future == installed {
				t.Fatalf("partial state after failure: healthy=%v future=%v installed=%v", healthy, future, installed)
			}
		})
	}
}

type valueOrderedResetPolicy struct{ MergePolicy }

func (valueOrderedResetPolicy) ID() string { return "test.value-order/v1" }
func (valueOrderedResetPolicy) CompareEntries(a, b Entry) int {
	return strings.Compare(string(a.Value), string(b.Value))
}

func TestClockResetPreservesOpaqueCommittedPolicy(t *testing.T) {
	ctx := context.Background()
	now := int64(1_800_000_000_000)
	clock := PhysicalClockFunc(func() int64 { return now })
	cfg := DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", Policy: valueOrderedResetPolicy{NewHLCLWWPolicy()}, PhysicalClock: clock}
	s, err := NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range []string{"a", "z"} {
		e := Entry{Key: "opaque", Value: []byte(value), Version: StateVersion{WriterNodeID: "other", HLC: Timestamp{WallMS: now - int64(i)}}, ChangeID: ChangeID{OriginNodeID: "other", Counter: uint64(i + 1)}}
		if applied, err := s.Apply(ctx, e); err != nil || !applied {
			t.Fatalf("seed: %v %v", applied, err)
		}
	}
	s.Close()
	if _, err := ResetDurableClock(ctx, ClockResetConfig{Dir: cfg.Dir, Reference: clock}); err != nil {
		t.Fatal(err)
	}
	s, err = NewDurableStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, found, err := s.Get(ctx, "opaque")
	if err != nil || !found || string(e.Value) != "z" {
		t.Fatalf("reset reinterpreted domain ordering: %+v %v", e, err)
	}
}

func TestClockStepProtectsStoreWrites(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			system := int64(1_800_000_000_000)
			mono := time.Unix(0, 0)
			ref, err := NewTimeReference(TimeReferenceConfig{SystemClock: PhysicalClockFunc(func() int64 { return system }), MonotonicNow: func() time.Time { return mono }})
			if err != nil {
				t.Fatal(err)
			}
			var s interface {
				Store
				ClockStatus() []NodeClockStatus
			}
			if durable {
				disk, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: "local", TimeReference: ref})
				if err != nil {
					t.Fatal(err)
				}
				defer disk.Close()
				s = disk
			} else {
				memory, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local", TimeReference: ref})
				if err != nil {
					t.Fatal(err)
				}
				s = memory
			}
			if _, err := s.Put(ctx, "before", nil); err != nil {
				t.Fatal(err)
			}
			system += int64(365 * 24 * time.Hour / time.Millisecond)
			mono = mono.Add(time.Second)
			e, err := s.Put(ctx, "during", nil)
			if err != nil || e.Version.HLC.WallMS != 1_800_000_001_000 {
				t.Fatalf("step poisoned HLC: %+v %v", e, err)
			}
			if status := s.ClockStatus(); len(status) != 1 || !status[0].ClockSuspect {
				t.Fatalf("step alarm missing: %+v", status)
			}
		})
	}
}
