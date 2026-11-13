package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// BytePageStore optionally provides bounded incremental reads without copying
// the entire backlog. The budget counts JSON entries and separating commas,
// excluding the surrounding array. A first entry exceeding the budget is
// returned alone so callers can attempt to replicate legacy data.
type BytePageStore interface {
	ChangesSinceBytes(context.Context, uint64, int64) ([]Entry, error)
}

// ChangesSinceBytes returns a position-ordered page, copying only its values.
// Sorting uses shallow references under the read lock, not backlog payload copies.
func (s *storeCore) ChangesSinceBytes(ctx context.Context, since uint64, maxBytes int64) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("state page byte budget must be positive")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ordered := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		if e.Position > since {
			ordered = append(ordered, e)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Position < ordered[j].Position })
	return fitEntryPage(ctx, ordered, maxBytes, s.maxEntryBytes)
}

func fitEntryPage(ctx context.Context, entries []Entry, maxBytes, maxEntryBytes int64) ([]Entry, error) {
	var result []Entry
	var used int64
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		size := int64(len(encoded))
		if len(result) > 0 {
			size++
		}
		if len(result) > 0 && (used+size > maxBytes || int64(len(encoded)) > maxEntryBytes) {
			break
		}
		result = append(result, copyEntry(e))
		used += size
		if used >= maxBytes || int64(len(encoded)) > maxEntryBytes {
			break
		}
	}
	return result, nil
}

func pushPage(ctx context.Context, store Store, since uint64, maxBytes int64) ([]Entry, error) {
	if paged, ok := store.(BytePageStore); ok {
		return paged.ChangesSinceBytes(ctx, since, maxBytes)
	}
	// Keep existing Store implementations source-compatible; request one entry
	// at a time rather than materializing their entire backlog.
	var result []Entry
	var used int64
	for {
		entries, err := store.ChangesSince(ctx, since, 1)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return result, nil
		}
		e := entries[0]
		if e.Position <= since {
			return nil, fmt.Errorf("state page did not advance past %d", since)
		}
		encoded, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		size := int64(len(encoded))
		if len(result) > 0 {
			size++
		}
		if len(result) > 0 && (used+size > maxBytes || int64(len(encoded)) > entryLimit(store)) {
			return result, nil
		}
		result = append(result, e)
		used += size
		if used >= maxBytes || int64(len(encoded)) > entryLimit(store) {
			return result, nil
		}
		since = e.Position
	}
}

const defaultMaxEntryBytes int64 = 512 << 10

// ErrEntryTooLarge reports a local entry exceeding MaxEntryBytes. Nothing is
// persisted or published for the rejected write.
var ErrEntryTooLarge = errors.New("state entry exceeds MaxEntryBytes")

// MaxEntryBytes returns the encoded local entry limit used by this store.
func (s *storeCore) MaxEntryBytes() int64 { return s.maxEntryBytes }

func entryLimit(store Store) int64 {
	if limits, ok := store.(interface{ MaxEntryBytes() int64 }); ok {
		return limits.MaxEntryBytes()
	}
	return defaultMaxEntryBytes
}

func (s *storeCore) checkEntrySize(entry Entry) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if int64(len(encoded)) > s.maxEntryBytes {
		return fmt.Errorf("%w: encoded entry needs %d bytes, limit %d", ErrEntryTooLarge, len(encoded), s.maxEntryBytes)
	}
	return nil
}

func validatePushRequestLimit(pushBytes, requestBytes int64) error {
	if pushBytes >= requestBytes {
		return fmt.Errorf("gossip MaxPushBytes (%d) must be less than HTTP MaxRequestBytes (%d)", pushBytes, requestBytes)
	}
	return nil
}
