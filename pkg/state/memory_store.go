package state

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
)

// MemoryStoreConfig configures an in-memory state replica.
type MemoryStoreConfig struct {
	NodeID        string
	Policy        MergePolicy
	PhysicalClock PhysicalClock
}

// MemoryStore is an in-memory state replica. It keeps tombstones in memory; disk
// durability is intentionally left to the persistence ticket.
type MemoryStore struct {
	nodeID  string
	policy  MergePolicy
	entries map[string]Entry
	clock   localClock
	hlc     *hlcClock

	commitMu    sync.Mutex
	mu          sync.RWMutex
	observersMu sync.RWMutex
	observers   map[uint64]func(Entry)
	nextObsID   uint64
}

// NewMemoryStore creates a state replica.
func NewMemoryStore(cfg MemoryStoreConfig) (*MemoryStore, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("state store node id is required")
	}
	return &MemoryStore{
		nodeID:    cfg.NodeID,
		policy:    normalizePolicy(cfg.Policy),
		entries:   make(map[string]Entry),
		hlc:       newHLCClock(cfg.PhysicalClock),
		observers: make(map[uint64]func(Entry)),
	}, nil
}

// Policy returns the merge policy used by local writes, Apply and digests.
func (s *MemoryStore) Policy() MergePolicy { return s.policy }

// Put commits a local opaque value.
func (s *MemoryStore) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	return s.put(ctx, key, Mutation{Value: value})
}

// Delete commits a local tombstone.
func (s *MemoryStore) Delete(ctx context.Context, key string) (Entry, error) {
	return s.put(ctx, key, Mutation{Deleted: true})
}

func (s *MemoryStore) put(ctx context.Context, key string, mutation Mutation) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	entry := s.newLocalEntry(key, mutation)
	if err := s.policy.ValidateApply(entry); err != nil {
		return Entry{}, err
	}
	s.mu.Lock()
	s.entries[key] = entry
	s.mu.Unlock()
	s.notify(entry)
	return entry, nil
}

// CompareAndSet atomically applies a local mutation if the current entry still
// has the expected identity.
func (s *MemoryStore) CompareAndSet(ctx context.Context, key string, expected EntryIdentity, mutation Mutation) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.mu.Lock()
	current, found := s.entries[key]
	if !found || current.Identity() != expected {
		s.mu.Unlock()
		return current, false, nil
	}
	entry := s.newLocalEntry(key, mutation)
	if err := s.policy.ValidateApply(entry); err != nil {
		s.mu.Unlock()
		return Entry{}, false, err
	}
	s.entries[key] = entry
	s.mu.Unlock()
	s.notify(entry)
	return entry, true, nil
}

// Get reads the current entry for a key, including tombstones.
func (s *MemoryStore) Get(ctx context.Context, key string) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[key]
	return copyEntry(entry), ok, nil
}

// ListEntries returns all latest entries ordered by local position.
func (s *MemoryStore) ListEntries(ctx context.Context) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return filterAndSortEntries(s.entries, 0, 0), nil
}

// ChangesSince returns entries with local position greater than since.
func (s *MemoryStore) ChangesSince(ctx context.Context, since uint64, limit int) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return filterAndSortEntries(s.entries, since, limit), nil
}

// Apply merges a remote entry. It preserves the original ChangeID and assigns a
// fresh local Position for downstream consumption.
func (s *MemoryStore) Apply(ctx context.Context, incoming Entry) (bool, error) {
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
			slog.Warn("state: remote HLC exceeds max clock offset",
				"key", incoming.Key,
				"peer", incoming.Version.WriterNodeID,
				"offsetMS", offsetErr.OffsetMS,
				"maxOffsetMS", offsetErr.MaxClockOffset,
			)
		}
		return false, fmt.Errorf("reject remote HLC for key %q from %q: %w", incoming.Key, incoming.Version.WriterNodeID, err)
	}

	s.mu.Lock()
	existing, found := s.entries[incoming.Key]
	if found && s.policy.CompareEntries(incoming, existing) <= 0 {
		s.mu.Unlock()
		return false, nil
	}
	entry := copyEntry(incoming)
	entry.Position = s.clock.Merge(maxUint64(entry.Position, entry.ChangeID.Counter))
	s.entries[entry.Key] = entry
	s.mu.Unlock()
	s.notify(entry)
	return true, nil
}

// CurrentPosition returns the latest node-local consumption position.
func (s *MemoryStore) CurrentPosition() uint64 { return s.clock.Current() }

// Observe registers a non-consuming write observer and returns an unsubscribe
// function.
func (s *MemoryStore) Observe(fn func(Entry)) func() {
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

func (s *MemoryStore) newLocalEntry(key string, mutation Mutation) Entry {
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

func (s *MemoryStore) notify(entry Entry) {
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

func filterAndSortEntries(entries map[string]Entry, since uint64, limit int) []Entry {
	result := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Position > since {
			result = append(result, copyEntry(entry))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Position != result[j].Position {
			return result[i].Position < result[j].Position
		}
		return result[i].Key < result[j].Key
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
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
