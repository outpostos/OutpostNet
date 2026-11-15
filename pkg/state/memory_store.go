package state

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
)

// MemoryStoreConfig configures an in-memory state replica.
type MemoryStoreConfig struct {
	NodeID string
	Policy MergePolicy
	// MaxEntryBytes bounds a local entry's JSON encoding (default 512 KiB).
	// Remote Apply and recovery preserve larger legacy entries.
	MaxEntryBytes int64
	PhysicalClock PhysicalClock
	// TimeReference supplies HLC, admission/horizon guards and conservative retention.
	// Configure it instead of PhysicalClock; share one reference across host stores.
	TimeReference *TimeReference
	// TombstoneRetention enables tombstone GC: CollectTombstones removes
	// tombstones whose HLC wall time is at least this old. Zero (the default)
	// keeps every tombstone. See docs/state-tombstone-gc.md.
	TombstoneRetention time.Duration
}

// MemoryStore is an in-memory state replica. It keeps tombstones in memory
// until they are collected (opt-in, TombstoneRetention).
type MemoryStore struct {
	*storeCore
}

// MemoryStore implements the same replica seam as DurableStore.
var _ Store = (*MemoryStore)(nil)

// NewMemoryStore creates a state replica.
func NewMemoryStore(cfg MemoryStoreConfig) (*MemoryStore, error) {
	if cfg.TimeReference != nil && cfg.PhysicalClock != nil {
		return nil, fmt.Errorf("configure TimeReference or PhysicalClock, not both")
	}
	physical := cfg.PhysicalClock
	if cfg.TimeReference != nil {
		physical = cfg.TimeReference
	}
	core, err := newStoreCore(cfg.NodeID, cfg.Policy, physical, cfg.TombstoneRetention, cfg.MaxEntryBytes)
	if err != nil {
		return nil, err
	}
	return &MemoryStore{storeCore: core}, nil
}

type storeCore struct {
	nodeID     string
	policy     MergePolicy
	entries    map[string]Entry
	byKey      *btree.BTreeG[entryRef]
	byPosition *btree.BTreeG[entryRef]
	clock      localClock
	hlc        *hlcClock
	// ready reports whether this replica can serve local reads and resume a
	// consumer position. Recovery runs to completion before a store constructor
	// returns, so a published store starts ready; a lifecycle owner clears it when
	// the replica stops serving (DurableStore.Close).
	ready atomic.Bool

	// retention is the minimum tombstone age before collection; 0 disables GC.
	retention     time.Duration
	maxEntryBytes int64
	// replicated is set once a GossipEngine owns replication of this store, after
	// which peer-blind collection is refused.
	replicated atomic.Bool
	// horizon and floor are guarded by mu. See GCHorizon and GCFloor.
	clockStatus map[string]NodeClockStatus
	horizon     Timestamp
	floor       uint64
	// adopted is the highest peer horizon adopted through a snapshot resync. A
	// requester below it is not vouched for (removals done elsewhere). Guarded by
	// mu.
	adopted Timestamp
	// purgeHook persists purge sets. It is set by DurableStore before the store
	// is published and is nil for MemoryStore.
	purgeHook func(purgeSet) error
	// batchHook persists the accepted entries of one resync snapshot page as a
	// single durable record. Set by DurableStore; nil for MemoryStore.
	batchHook func([]Entry) error

	commitMu    sync.Mutex
	mu          sync.RWMutex
	observersMu sync.RWMutex
	observers   map[uint64]func(Entry)
	nextObsID   uint64
}

func newStoreCore(nodeID string, policy MergePolicy, physical PhysicalClock, retention time.Duration, maxEntryBytes int64) (*storeCore, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("state store node id is required")
	}
	if retention < 0 {
		return nil, fmt.Errorf("state tombstone retention must not be negative")
	}
	if maxEntryBytes == 0 {
		maxEntryBytes = defaultMaxEntryBytes
	}
	if maxEntryBytes < 0 {
		return nil, fmt.Errorf("state max entry bytes must be positive")
	}
	core := &storeCore{
		maxEntryBytes: maxEntryBytes,
		nodeID:        nodeID,
		policy:        normalizePolicy(policy),
		entries:       make(map[string]Entry),
		byKey:         newKeyIndex(),
		byPosition:    newPositionIndex(),
		hlc:           newHLCClock(physical),
		observers:     make(map[uint64]func(Entry)),
		retention:     retention,
	}
	core.ready.Store(true)
	return core, nil
}

// markNotReady records that this replica no longer serves local state.
func (s *storeCore) markNotReady() { s.ready.Store(false) }

// Policy returns the merge policy used by local writes, Apply and digests.
func (s *storeCore) Policy() MergePolicy { return s.policy }

// Put commits a local opaque value.
func (s *storeCore) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	return s.put(ctx, key, Mutation{Value: value}, nil)
}

// Delete commits a local tombstone.
func (s *storeCore) Delete(ctx context.Context, key string) (Entry, error) {
	return s.put(ctx, key, Mutation{Deleted: true}, nil)
}

func (s *storeCore) put(ctx context.Context, key string, mutation Mutation, persist func(Entry) error) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	entry := s.newLocalEntry(key, mutation)
	if err := s.checkEntrySize(entry); err != nil {
		return Entry{}, err
	}
	if err := s.policy.ValidateApply(entry); err != nil {
		return Entry{}, err
	}
	if persist != nil {
		if err := persist(entry); err != nil {
			return Entry{}, err
		}
	}
	s.mu.Lock()
	s.setEntryLocked(entry)
	s.mu.Unlock()
	s.notify(entry)
	return copyEntry(entry), nil
}

// CompareAndSet atomically applies a local mutation if the current entry still
// has the expected identity.
func (s *storeCore) CompareAndSet(ctx context.Context, key string, expected EntryIdentity, mutation Mutation) (Entry, bool, error) {
	return s.compareAndSet(ctx, key, expected, mutation, nil)
}

func (s *storeCore) compareAndSet(ctx context.Context, key string, expected EntryIdentity, mutation Mutation, persist func(Entry) error) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.mu.Lock()
	current, found := s.entries[key]
	if !found || current.Identity() != expected {
		s.mu.Unlock()
		return copyEntry(current), false, nil
	}
	entry := s.newLocalEntry(key, mutation)
	if err := s.checkEntrySize(entry); err != nil {
		s.mu.Unlock()
		return Entry{}, false, err
	}
	if err := s.policy.ValidateApply(entry); err != nil {
		s.mu.Unlock()
		return Entry{}, false, err
	}
	if persist != nil {
		// The durable append, including its fsync and any rollback, runs while mu is
		// held here, so a slow disk delays concurrent readers of this store.
		// Put/Delete append outside mu; Apply also appends under mu. This lock scope
		// is pre-existing; 03b evaluated it and left narrowing it open.
		if err := persist(entry); err != nil {
			s.mu.Unlock()
			return Entry{}, false, err
		}
	}
	s.setEntryLocked(entry)
	s.mu.Unlock()
	s.notify(entry)
	return copyEntry(entry), true, nil
}

// Get reads the current entry for a key, including tombstones.
func (s *storeCore) Get(ctx context.Context, key string) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[key]
	return copyEntry(entry), ok, nil
}

// ListEntries returns all latest entries ordered by local position.
func (s *storeCore) ListEntries(ctx context.Context) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positionEntriesLocked(0, 0), nil
}

// ChangesSince returns entries with local position greater than since.
func (s *storeCore) ChangesSince(ctx context.Context, since uint64, limit int) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positionEntriesLocked(since, limit), nil
}

// Apply merges a remote entry. It preserves the original ChangeID and assigns a
// fresh local Position for downstream consumption.
func (s *storeCore) Apply(ctx context.Context, incoming Entry) (bool, error) {
	return s.apply(ctx, incoming, nil)
}

// apply merges a remote entry. Every path, snapshot resync included, refuses an
// absent key at or below the local GC horizon: a peer's horizon says nothing
// about whether that peer saw the deletes this store collected.
func (s *storeCore) apply(ctx context.Context, incoming Entry, persist func(Entry) error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	if err := s.policy.ValidateApply(incoming); err != nil {
		return false, err
	}
	if _, err := s.hlc.Update(incoming.Version.HLC); err != nil {
		var offsetErr *ClockOffsetError
		if errors.As(err, &offsetErr) {
			s.observeClockEntry(incoming, true)
			slog.Warn("state: remote HLC exceeds max clock offset",
				"key", incoming.Key,
				"peer", incoming.Version.WriterNodeID,
				"offsetMS", offsetErr.OffsetMS,
				"maxOffsetMS", offsetErr.MaxClockOffset,
			)
		}
		return false, fmt.Errorf("reject remote HLC for key %q from %q: %w", incoming.Key, incoming.Version.WriterNodeID, err)
	}

	s.observeClockEntry(incoming, false)
	s.mu.Lock()
	existing, found := s.entries[incoming.Key]
	if found && s.policy.CompareEntries(incoming, existing) <= 0 {
		s.mu.Unlock()
		return false, nil
	}
	if !found && s.beyondHorizonLocked(incoming) {
		horizon := s.horizon
		s.mu.Unlock()
		if incoming.Deleted {
			// An absent key already reads as deleted; an old tombstone changes nothing.
			return false, nil
		}
		return false, &BeyondGCHorizonError{Key: incoming.Key, EntryHLC: incoming.Version.HLC, Horizon: horizon}
	}
	entry := copyEntry(incoming)
	entry.Position = s.clock.Merge(maxUint64(entry.Position, entry.ChangeID.Counter))
	if persist != nil {
		if err := persist(entry); err != nil {
			s.mu.Unlock()
			return false, err
		}
	}
	s.setEntryLocked(entry)
	s.mu.Unlock()
	s.notify(entry)
	return true, nil
}

// CurrentPosition returns the latest node-local consumption position.
func (s *storeCore) CurrentPosition() uint64 { return s.clock.Current() }

// LocalStatus reports local recovery readiness and the latest local position.
// Ready is false once the replica has stopped serving local state.
func (s *storeCore) LocalStatus() LocalStatus {
	return LocalStatus{Ready: s.ready.Load(), CurrentPosition: s.CurrentPosition()}
}

// CheckConsumerPosition reports whether a stored consumer position can resume.
func (s *storeCore) CheckConsumerPosition(position uint64) ConsumerPositionStatus {
	current := s.CurrentPosition()
	ready := s.ready.Load()
	floor := s.GCFloor()
	status := ConsumerPositionStatus{
		Ready:           ready,
		CurrentPosition: current,
		Requested:       position,
	}
	switch {
	case !ready:
		status.NeedsSnapshot = true
		status.Reason = "local store is not ready"
	case position == 0:
		status.NeedsSnapshot = true
		status.Reason = "no prior position; start from snapshot"
	case position < floor:
		status.NeedsSnapshot = true
		status.Reason = "consumer position precedes removed tombstones (GC floor)"
	case position > current:
		status.NeedsSnapshot = true
		status.Reason = "consumer position is ahead of local state"
	default:
		status.CanResume = true
	}
	return status
}

// Observe registers a non-consuming write observer and returns an unsubscribe
// function.
func (s *storeCore) Observe(fn func(Entry)) func() {
	if fn == nil {
		return func() {}
	}
	s.observersMu.Lock()
	s.nextObsID++
	id := s.nextObsID
	s.observers[id] = fn
	s.observersMu.Unlock()
	return func() {
		s.observersMu.Lock()
		delete(s.observers, id)
		s.observersMu.Unlock()
	}
}

func (s *storeCore) restoreEntry(entry Entry) error {
	if err := s.policy.ValidateApply(entry); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, found := s.entries[entry.Key]
	if !found || s.policy.CompareEntries(entry, existing) > 0 {
		s.setEntryLocked(copyEntry(entry))
	}
	s.clock.SetAtLeast(maxUint64(entry.Position, entry.ChangeID.Counter))
	s.hlc.Restore(entry.Version.HLC)
	return nil
}

func (s *storeCore) newLocalEntry(key string, mutation Mutation) Entry {
	position := s.clock.Next()
	ts := s.hlc.Now()
	entry := Entry{
		Key:      key,
		Value:    append([]byte(nil), mutation.Value...),
		Deleted:  mutation.Deleted,
		Version:  StateVersion{WriterNodeID: s.nodeID, HLC: ts},
		ChangeID: ChangeID{OriginNodeID: s.nodeID, Counter: position},
		Position: position,
	}
	if mutation.Deleted {
		entry.Value = append([]byte(nil), mutation.Value...)
	}
	return entry
}

func (s *storeCore) notify(entry Entry) {
	s.observersMu.RLock()
	observers := make([]func(Entry), 0, len(s.observers))
	for _, observer := range s.observers {
		observers = append(observers, observer)
	}
	s.observersMu.RUnlock()
	for _, observer := range observers {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.Warn("state: observer panicked", "key", entry.Key, "panic", recovered)
				}
			}()
			observer(copyEntry(entry))
		}()
	}
}

func (s *storeCore) positionEntriesLocked(since uint64, limit int) []Entry {
	result := make([]Entry, 0)
	s.byPosition.AscendGreaterOrEqual(entryRef{position: since}, func(ref entryRef) bool {
		if ref.position <= since {
			return true
		}
		result = append(result, copyEntry(s.entries[ref.key]))
		return limit <= 0 || len(result) < limit
	})
	return result
}

func copyEntry(entry Entry) Entry {
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
