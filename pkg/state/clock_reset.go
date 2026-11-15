package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// ErrClockRebuildRequired requires an explicit offline full-state rebuild;
// deleting future entries alone cannot undo an already adopted future horizon.
var ErrClockRebuildRequired = errors.New("state GC horizon is in the future; stop the host and rebuild state from healthy peers with outpostnet-state-reset --rebuild")

// ClockResetConfig configures an offline reset of an existing state directory.
// Reference must be a corrected clock (or a host's established TimeReference).
// Nil uses system time. Rebuild explicitly permits clearing ALL state when the
// horizon is ahead, while retaining the manifest identity and monotonic counters.
type ClockResetConfig struct {
	Dir       string
	Reference PhysicalClock
	Rebuild   bool
	// Test hooks exercise the same disk operations and commit boundary as Compact.
	wrapLog        func(durableLog) durableLog
	compactionHook func(string) error
}

// ClockResetResult contains counts only, never entry payloads or credentials.
type ClockResetResult struct {
	Purged      int
	Rebuilt     bool
	ReferenceMS int64
	Floor       uint64
}

// committedPolicy replays accepted log commits by their local position. Reset
// must not reinterpret opaque domain policies or import their owning packages.
// Normal recovery applies merge policy ordering; this offline view follows the
// authoritative committed replacements, including non-HLC domain ordering.
type committedPolicy struct {
	MergePolicy
	id string
}

func (p committedPolicy) ID() string { return p.id }
func (p committedPolicy) CompareEntries(a, b Entry) int {
	if a.Position < b.Position {
		return -1
	}
	if a.Position > b.Position {
		return 1
	}
	return 0
}

func readClockManifest(dir string) (durableManifest, error) {
	var manifest durableManifest
	data, err := os.ReadFile(filepath.Join(dir, durableManifestFile))
	if err != nil {
		return manifest, fmt.Errorf("read existing state manifest: %w", err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, fmt.Errorf("decode state manifest: %w", err)
	}
	return manifest, nil
}

// checkClockUpgrade runs once per directory on the first D5-aware open. A
// refused check leaves its marker unset; offline reset always checks regardless
// of that marker. Normal D4 guards prevent new future horizons after upgrade.
func (s *DurableStore) checkClockUpgrade() error {
	manifest, err := readClockManifest(s.dir)
	if err != nil {
		return err
	}
	if manifest.ClockRecoveryVersion >= 1 {
		return nil
	}
	if checkFutureTimestamp(s.GCHorizon(), s.TimeReferenceMS()) != nil || checkFutureTimestamp(s.adoptedHorizon(), s.TimeReferenceMS()) != nil {
		return ErrClockRebuildRequired
	}
	manifest.ClockRecoveryVersion = 1
	return writeManifest(filepath.Join(s.dir, durableManifestFile), manifest)
}

// ResetDurableClock takes the same exclusive directory lock as normal recovery.
// The host must be stopped. An atomic, synced compaction replaces ALL historical
// HLC sources (entries, batches, snapshot headers and purge records) together.
// A failure is never reported as a successful purge. Before rename the old log
// survives; after rename the complete replacement is authoritative on restart.
func ResetDurableClock(ctx context.Context, cfg ClockResetConfig) (result ClockResetResult, err error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if strings.TrimSpace(cfg.Dir) == "" {
		return result, fmt.Errorf("clock reset requires an existing state directory")
	}
	manifest, err := readClockManifest(cfg.Dir)
	if err != nil {
		return result, err
	}
	clock := cfg.Reference
	if clock == nil {
		clock = SystemPhysicalClock{}
	}
	now := clock.NowMS()
	if now <= 0 {
		return result, fmt.Errorf("clock reset requires a positive reference time")
	}
	s, err := openDurableStore(DurableStoreConfig{Dir: cfg.Dir, NodeID: manifest.NodeID, Policy: committedPolicy{MergePolicy: NewHLCLWWPolicy(), id: manifest.PolicyID}, PhysicalClock: clock, wrapLog: cfg.wrapLog, compactionHook: cfg.compactionHook}, true)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	futureHorizon := checkFutureTimestamp(s.horizon, now) != nil || checkFutureTimestamp(s.adopted, now) != nil
	if futureHorizon && !cfg.Rebuild {
		return result, ErrClockRebuildRequired
	}
	if s.CurrentPosition() == math.MaxUint64 {
		return result, fmt.Errorf("clock reset position counter exhausted")
	}
	result.ReferenceMS, result.Rebuilt = now, futureHorizon
	// Bump past even the latest pre-reset cursor. A floor equal to a removed
	// position would leave a consumer at that position incorrectly resumable.
	result.Floor = s.clock.Next()
	s.floor = result.Floor
	resetHLC := Timestamp{WallMS: now}
	for _, entry := range s.entries {
		if futureHorizon || checkFutureTimestamp(entry.Version.HLC, now) != nil {
			s.removeEntryLocked(entry)
			result.Purged++
		} else {
			resetHLC = maxTimestamp(resetHLC, entry.Version.HLC)
		}
	}
	if futureHorizon {
		s.horizon, s.adopted = Timestamp{}, Timestamp{}
		s.peerProgress = make(map[string]uint64)
		s.peerNodes = make(map[string]string)
		s.peerRefused = make(map[string][]refusedPosition)
	} else {
		resetHLC = maxTimestamp(resetHLC, s.horizon)
		for peer, refused := range s.peerRefused {
			var retained []refusedPosition
			for _, r := range refused {
				if _, ok := s.entries[r.Key]; ok {
					retained = append(retained, r)
				}
			}
			s.peerRefused[peer] = retained
		}
	}
	s.hlc = newHLCClock(clock)
	s.hlc.Restore(resetHLC)
	if _, err := s.Compact(ctx); err != nil {
		return ClockResetResult{}, err
	}
	manifest.ClockRecoveryVersion = 1
	if err := writeManifest(filepath.Join(s.dir, durableManifestFile), manifest); err != nil {
		return ClockResetResult{}, err
	}
	return result, nil
}
