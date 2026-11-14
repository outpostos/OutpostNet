package state

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
)

// Tombstone GC and horizon resync. The design and its trade-offs are described
// in docs/state-tombstone-gc.md.

var (
	// ErrTombstoneGCDisabled reports that the store was configured without a
	// tombstone retention, which is the default.
	ErrTombstoneGCDisabled = errors.New("state tombstone GC is disabled (TombstoneRetention is 0)")
	// ErrTombstoneGCReplicated reports a peer-blind GC request on a store that is
	// replicated by a GossipEngine. Use GossipEngine.CollectTombstones instead.
	ErrTombstoneGCReplicated = errors.New("state store is replicated; collect tombstones through GossipEngine.CollectTombstones")
	// ErrTombstoneGCNoPeers reports a collection request on a replicated store
	// whose PeerProvider returned no peers. An empty list cannot be told apart
	// from a transient isolation of a liveness-based provider, so nothing is
	// collected; the PeerProvider must return the configured membership.
	ErrTombstoneGCNoPeers = errors.New("state store is replicated but the PeerProvider returned no peers; refusing to collect tombstones without peer acknowledgement")
	// ErrTombstoneGCUnsupported reports that a store or syncer does not implement
	// the tombstone GC / snapshot resync surface.
	ErrTombstoneGCUnsupported = errors.New("state tombstone GC or snapshot resync is not supported by this store or syncer")
)

// RejectionReasonBeyondGCHorizon is the PushRejection reason for an entry whose
// key the receiver no longer tracks and whose version is at or below the
// receiver's GC horizon. The sender must resync from the receiver.
const RejectionReasonBeyondGCHorizon = "beyond_gc_horizon"

// BeyondGCHorizonError reports that an incoming entry targets an absent key at or
// below the store's GC horizon. The store cannot tell whether the key was deleted
// and collected, so it refuses the entry instead of possibly resurrecting it.
type BeyondGCHorizonError struct {
	Key      string
	EntryHLC Timestamp
	Horizon  Timestamp
}

func (e *BeyondGCHorizonError) Error() string {
	return fmt.Sprintf("state entry %q at HLC %d.%d is at or below the GC horizon %d.%d of an absent key; sender must resync",
		e.Key, e.EntryHLC.WallMS, e.EntryHLC.Logical, e.Horizon.WallMS, e.Horizon.Logical)
}

// ResyncRequiredError reports that a peer refused an incremental exchange
// because the requester's GC horizon is lower than the peer's. The requester
// must take a snapshot resync from that peer.
type ResyncRequiredError struct {
	NodeID string
	TimeSample
	Peer    string
	Horizon Timestamp
}

func (e *ResyncRequiredError) Error() string {
	return fmt.Sprintf("state peer %q requires snapshot resync: its GC horizon is %d.%d", e.Peer, e.Horizon.WallMS, e.Horizon.Logical)
}

// TombstoneGCResult reports one collection pass.
type TombstoneGCResult struct {
	// Collected tombstones were removed in this pass.
	Collected int
	// Blocked tombstones were old enough but not yet acknowledged by every peer.
	Blocked int
	// Horizon and Floor are the store's GC horizon and floor after the pass.
	Horizon Timestamp
	Floor   uint64
}

// removedEntry identifies an entry removed by GC or resync. Replay removes the
// key only if its current entry still has this exact identity.
type removedEntry struct {
	Key      string       `json:"key"`
	ChangeID ChangeID     `json:"change_id"`
	Version  StateVersion `json:"version"`
}

// purgeSet is committed atomically: removals plus the horizon and floor they
// imply.
type purgeSet struct {
	Horizon Timestamp
	Floor   uint64
	Removed []removedEntry
	// Adopted is the peer horizon adopted by a resync; zero for collection.
	Adopted Timestamp
}

func (t Timestamp) isZero() bool { return t == Timestamp{} }

func maxTimestamp(a, b Timestamp) Timestamp {
	if a.Compare(b) >= 0 {
		return a
	}
	return b
}

func timestampPtr(ts Timestamp) *Timestamp {
	if ts.isZero() {
		return nil
	}
	return &ts
}

func timestampValue(ts *Timestamp) Timestamp {
	if ts == nil {
		return Timestamp{}
	}
	return *ts
}

// gcStore is the internal surface GossipEngine uses for gated collection and
// snapshot resync. MemoryStore and DurableStore implement it.
type gcStore interface {
	GCHorizon() Timestamp
	GCFloor() uint64
	adoptedHorizon() Timestamp
	tombstoneGCEnabled() bool
	markReplicated()
	collectTombstones(ctx context.Context, safePosition uint64, gated bool) (TombstoneGCResult, error)
	applySnapshotPage(ctx context.Context, page []Entry) (snapshotPageResult, error)
	finishResync(ctx context.Context, held map[string]struct{}, peerHorizon Timestamp) (resyncResult, error)
}

// snapshotPageResult reports how one resync snapshot page was merged.
type snapshotPageResult struct {
	// Applied entries changed local state.
	Applied int
	// Refused entries exceed the clock safety valve or are values for keys
	// this store collected (absent, at or below the local horizon).
	Refused int
}

// peerAckStore is implemented by stores that persist, with each acknowledged
// push position, the node ID that answered at that peer address. DurableStore
// implements it; GossipEngine uses it to vouch for lower-horizon requesters
// across restarts.
type peerAckStore interface {
	savePeerAck(ctx context.Context, peer, nodeID string, position uint64, refused []refusedPosition) error
	peersAnsweringAs(nodeID string) []string
}

type resyncResult struct {
	Purged  int
	Horizon Timestamp
}

// GCHorizon returns the highest HLC this store removed or adopted from a peer.
func (s *storeCore) GCHorizon() Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.horizon
}

// GCFloor returns the highest local position of any removed entry. Consumer
// positions below it require a snapshot.
func (s *storeCore) GCFloor() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.floor
}

// adoptedHorizon returns the highest peer horizon adopted through a resync.
func (s *storeCore) adoptedHorizon() Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adopted
}

func (s *storeCore) tombstoneGCEnabled() bool { return s.retention > 0 }

func (s *storeCore) markReplicated() { s.replicated.Store(true) }

// CollectTombstones removes tombstones older than the configured retention from
// a store that is not replicated. Replicated stores must use
// GossipEngine.CollectTombstones, which also waits for peer acknowledgement.
func (s *storeCore) CollectTombstones(ctx context.Context) (TombstoneGCResult, error) {
	if s.replicated.Load() {
		return TombstoneGCResult{}, ErrTombstoneGCReplicated
	}
	return s.collectTombstones(ctx, 0, false)
}

func (s *storeCore) collectTombstones(ctx context.Context, safePosition uint64, gated bool) (TombstoneGCResult, error) {
	if err := ctx.Err(); err != nil {
		return TombstoneGCResult{}, err
	}
	if s.retention <= 0 {
		return TombstoneGCResult{}, ErrTombstoneGCDisabled
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	now := s.TimeReferenceMS()
	retentionNow := s.retentionNowMS()
	cutoff := int64(math.MinInt64)
	if retentionNow >= math.MinInt64+s.retention.Milliseconds() {
		cutoff = retentionNow - s.retention.Milliseconds()
	}
	s.mu.RLock()
	set := purgeSet{Horizon: s.horizon, Floor: s.floor}
	var blocked int
	for _, entry := range s.entries {
		if !entry.Deleted || entry.Version.HLC.WallMS > cutoff {
			continue
		}
		if checkFutureTimestamp(entry.Version.HLC, now) != nil || (gated && entry.Position > safePosition) {
			blocked++
			continue
		}
		set.Removed = append(set.Removed, removedEntry{Key: entry.Key, ChangeID: entry.ChangeID, Version: entry.Version})
		set.Horizon = maxTimestamp(set.Horizon, entry.Version.HLC)
		set.Floor = maxUint64(set.Floor, entry.Position)
	}
	s.mu.RUnlock()
	if len(set.Removed) == 0 {
		return TombstoneGCResult{Blocked: blocked, Horizon: set.Horizon, Floor: set.Floor}, nil
	}
	if s.purgeHook != nil {
		if err := s.purgeHook(set); err != nil {
			return TombstoneGCResult{}, err
		}
	}
	s.mu.Lock()
	s.applyPurgeLocked(set)
	s.mu.Unlock()
	return TombstoneGCResult{Collected: len(set.Removed), Blocked: blocked, Horizon: set.Horizon, Floor: set.Floor}, nil
}

// applyPurgeLocked removes entries whose identity still matches and raises the
// horizon, the floor and the HLC lower bound. It is shared by live collection,
// resync and replay, so an adopted horizon raises the HLC at runtime exactly as
// replaying its purge record does after a restart.
func (s *storeCore) applyPurgeLocked(set purgeSet) []Entry {
	var removed []Entry
	for _, ref := range set.Removed {
		current, ok := s.entries[ref.Key]
		if !ok || current.ChangeID != ref.ChangeID || current.Version != ref.Version {
			continue
		}
		s.removeEntryLocked(current)
		removed = append(removed, current)
	}
	s.horizon = maxTimestamp(s.horizon, set.Horizon)
	s.floor = maxUint64(s.floor, set.Floor)
	s.adopted = maxTimestamp(s.adopted, set.Adopted)
	s.clock.SetAtLeast(s.floor)
	// Local writes must order after every removed or adopted entry; otherwise a
	// store whose physical clock lags the horizon would issue writes at or below
	// it, which peers refuse as beyond_gc_horizon.
	s.hlc.Restore(s.horizon)
	return removed
}

// finishResync completes a snapshot resync after every snapshot page was
// applied through Apply: in one atomic purge it drops every local entry at or
// below the peer horizon whose key the peer does not hold, and adopts that
// horizon. held is the set of keys the peer's snapshot carried.
//
// Snapshot entries go through the same local horizon check as any Apply: a
// snapshot value for a key this store collected (absent, HLC <= local horizon)
// is refused, because the peer's horizon is a single maximum timestamp and does
// not prove the peer saw that delete. The peer still holding such a value is
// cleaned up by its own resync from us (beyond_gc_horizon push rejection or a
// digest pull answered with 410).
func (s *storeCore) finishResync(ctx context.Context, held map[string]struct{}, peerHorizon Timestamp) (resyncResult, error) {
	var result resyncResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if err := checkFutureTimestamp(peerHorizon, s.TimeReferenceMS()); err != nil {
		return result, err
	}
	s.mu.RLock()
	set := purgeSet{Horizon: maxTimestamp(s.horizon, peerHorizon), Floor: s.floor}
	horizonChanged := set.Horizon != s.horizon
	for _, entry := range s.entries {
		if _, ok := held[entry.Key]; ok {
			continue
		}
		if entry.Version.HLC.Compare(peerHorizon) > 0 {
			continue
		}
		set.Removed = append(set.Removed, removedEntry{Key: entry.Key, ChangeID: entry.ChangeID, Version: entry.Version})
	}
	s.mu.RUnlock()
	if len(set.Removed) == 0 && !horizonChanged {
		result.Horizon = set.Horizon
		return result, nil
	}
	// Every removal of a resync, and every part of the horizon it raises, was
	// decided by the peer: this store cannot vouch that any requester
	// acknowledged it. That holds even when the local horizon does not move
	// (the peer's horizon is lower but it collected a key we still held), so the
	// peer horizon is recorded as adopted whenever the resync changes anything.
	set.Adopted = peerHorizon
	if len(set.Removed) > 0 {
		// A purged value is a visible change. It gets a fresh local position that
		// becomes the floor, so every consumer that may hold the value re-snapshots.
		set.Floor = s.clock.Next()
	}
	if s.purgeHook != nil {
		if err := s.purgeHook(set); err != nil {
			return result, err
		}
	}
	s.mu.Lock()
	removed := s.applyPurgeLocked(set)
	s.mu.Unlock()
	for _, entry := range removed {
		slog.Warn("state: resync purged entry the peer refuses (deleted cluster-side or lagging beyond retention)",
			"key", entry.Key,
			"writer", entry.Version.WriterNodeID,
			"hlcWallMS", entry.Version.HLC.WallMS,
		)
		notice := Entry{Key: entry.Key, Deleted: true, Version: entry.Version, ChangeID: entry.ChangeID, Position: set.Floor}
		s.notify(notice)
	}
	result.Purged = len(removed)
	result.Horizon = set.Horizon
	return result, nil
}

// applySnapshotPage merges one resync snapshot page with the same rules as
// Apply (validation, HLC offset, LWW, local horizon refusal), but commits every
// accepted entry of the page together: DurableStore writes them as one record
// with one fsync instead of one per entry, and the page becomes visible only
// after that record is durable. A page is all or nothing: an entry that fails
// validation aborts the page before anything is written. Clock-ahead entries
// are refused individually; all remaining accepted entries commit atomically.
// A crash mid-write leaves an unterminated record that recovery
// discards.
func (s *storeCore) applySnapshotPage(ctx context.Context, page []Entry) (snapshotPageResult, error) {
	var result snapshotPageResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	eligible := make([]Entry, 0, len(page))
	for _, incoming := range page {
		if err := s.policy.ValidateApply(incoming); err != nil {
			return result, fmt.Errorf("validate snapshot entry %q: %w", incoming.Key, err)
		}
		if _, err := s.hlc.Update(incoming.Version.HLC); err != nil {
			var offset *ClockOffsetError
			if errors.As(err, &offset) {
				result.Refused++
				s.observeClockEntry(incoming, true)
				continue
			}
			return result, fmt.Errorf("reject remote HLC for key %q from %q: %w", incoming.Key, incoming.Version.WriterNodeID, err)
		}
		s.observeClockEntry(incoming, false)
		eligible = append(eligible, incoming)
	}
	// commitMu excludes every other mutation, so the decisions taken under the
	// read lock still hold when the batch is installed.
	accepted := make([]Entry, 0, len(page))
	pending := make(map[string]int, len(page))
	s.mu.RLock()
	for _, incoming := range eligible {
		existing, found := s.entries[incoming.Key]
		if index, ok := pending[incoming.Key]; ok {
			existing, found = accepted[index], true
		}
		if found && s.policy.CompareEntries(incoming, existing) <= 0 {
			continue
		}
		if !found && s.beyondHorizonLocked(incoming) {
			if !incoming.Deleted {
				result.Refused++
			}
			continue
		}
		entry := copyEntry(incoming)
		entry.Position = s.clock.Merge(maxUint64(entry.Position, entry.ChangeID.Counter))
		if index, ok := pending[entry.Key]; ok {
			accepted[index] = entry
			continue
		}
		pending[entry.Key] = len(accepted)
		accepted = append(accepted, entry)
	}
	s.mu.RUnlock()
	if len(accepted) == 0 {
		return result, nil
	}
	if s.batchHook != nil {
		if err := s.batchHook(accepted); err != nil {
			return result, err
		}
	}
	s.mu.Lock()
	for _, entry := range accepted {
		s.setEntryLocked(entry)
	}
	s.mu.Unlock()
	for _, entry := range accepted {
		s.notify(entry)
	}
	result.Applied = len(accepted)
	return result, nil
}

// beyondHorizonLocked reports whether an entry for an absent key must be refused.
func (s *storeCore) beyondHorizonLocked(entry Entry) bool {
	return !s.horizon.isZero() && entry.Version.HLC.Compare(s.horizon) <= 0
}

// Both stores must keep the GC surface; a regression would silently disable
// gated collection and resync in GossipEngine.
var (
	_ gcStore      = (*MemoryStore)(nil)
	_ gcStore      = (*DurableStore)(nil)
	_ peerAckStore = (*DurableStore)(nil)
	_ interface {
		CompactIfNeeded(context.Context) (bool, error)
	} = (*DurableStore)(nil)
	_ SnapshotSyncer = (*HTTPSyncer)(nil)
)
