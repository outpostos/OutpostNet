package state

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/btree"
)

// KeyDigest is value-free replication metadata for one key.
type KeyDigest struct {
	Key    string
	Record DigestRecord
}

// ReplicationReadStore is an optional replication read seam. Store remains
// source-compatible. Bounds are [start, end); empty end means unbounded.
// Digests are in key order and include tombstones. Returned metadata and values
// are owned by the caller. DigestDiffPage uses the store policy and copies only
// the values that fit the response (including its envelope and continuation).
type ReplicationReadStore interface {
	DigestRange(context.Context, string, string) ([]KeyDigest, error)
	HasKey(context.Context, string) (bool, error)
	DigestDiffPage(context.Context, PullRequest, PullResponse) (*PullResponse, error)
}

var _ ReplicationReadStore = (*MemoryStore)(nil)
var _ ReplicationReadStore = (*DurableStore)(nil)

type entryRef struct {
	key      string
	position uint64
}

func newKeyIndex() *btree.BTreeG[entryRef] {
	return btree.NewG(32, func(a, b entryRef) bool { return a.key < b.key })
}
func newPositionIndex() *btree.BTreeG[entryRef] {
	return btree.NewG(32, func(a, b entryRef) bool {
		if a.position != b.position {
			return a.position < b.position
		}
		return a.key < b.key
	})
}

// Entry values are immutable after publication. Indexes contain only key and
// position, and are updated with the map under mu after persistence succeeds.
func (s *storeCore) setEntryLocked(entry Entry) {
	if old, ok := s.entries[entry.Key]; ok {
		s.byPosition.Delete(entryRef{old.Key, old.Position})
	}
	ref := entryRef{entry.Key, entry.Position}
	s.byKey.ReplaceOrInsert(ref)
	s.byPosition.ReplaceOrInsert(ref)
	s.entries[entry.Key] = entry
}
func (s *storeCore) removeEntryLocked(entry Entry) {
	ref := entryRef{entry.Key, entry.Position}
	s.byKey.Delete(ref)
	s.byPosition.Delete(ref)
	delete(s.entries, entry.Key)
}

// Snapshot shallow, immutable references under the lock; policy decoding,
// comparisons and response encoding happen after unlocking. These references
// never cross the public read seam. Only selected response values are copied.
func (s *storeCore) keyRange(ctx context.Context, start, end, after string) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var entries []Entry
	if start == "" && end == "" && after == "" {
		entries = make([]Entry, 0, s.byKey.Len())
	}
	lower := start
	if after > lower {
		lower = after
	}
	s.byKey.AscendGreaterOrEqual(entryRef{key: lower}, func(ref entryRef) bool {
		if end != "" && ref.key >= end {
			return false
		}
		if after != "" && ref.key <= after {
			return true
		}
		entries = append(entries, s.entries[ref.key])
		return ctx.Err() == nil
	})
	return entries, ctx.Err()
}

func (s *storeCore) DigestRange(ctx context.Context, start, end string) ([]KeyDigest, error) {
	entries, err := s.keyRange(ctx, start, end, "")
	if err != nil {
		return nil, err
	}
	result := make([]KeyDigest, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record := newDigestRecord(entry, s.policy)
		// Policies may derive RawMessage directly from Value. Detach it too.
		for k, v := range record.Metadata {
			record.Metadata[k] = append([]byte(nil), v...)
		}
		result = append(result, KeyDigest{entry.Key, record})
	}
	return result, nil
}
func (s *storeCore) HasKey(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[key]
	return ok, nil
}
func (s *storeCore) DigestDiffPage(ctx context.Context, req PullRequest, resp PullResponse) (*PullResponse, error) {
	start := ""
	if req.StartKey != nil {
		start = *req.StartKey
	}
	entries, err := s.keyRange(ctx, start, req.EndKey, req.AfterKey)
	if err != nil {
		return nil, err
	}
	return digestDiffPage(ctx, req, &resp, entries, s.policy)
}

func (g *GossipEngine) localDigestEntries(ctx context.Context) ([]KeyDigest, error) {
	if reader, ok := g.store.(ReplicationReadStore); ok {
		return reader.DigestRange(ctx, "", "")
	}
	entries, err := g.store.ListEntries(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]KeyDigest, 0, len(entries))
	for _, e := range entries {
		result = append(result, KeyDigest{e.Key, newDigestRecord(e, g.policy)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func digestDiffPage(ctx context.Context, req PullRequest, resp *PullResponse, entries []Entry, policy MergePolicy) (*PullResponse, error) {
	limit := req.Limit
	if limit == 0 {
		limit = resyncPageSize
	}
	if limit < 0 {
		return nil, fmt.Errorf("negative digest limit")
	}
	var candidates []Entry
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !digestKeyInRange(entry.Key, req) || (req.AfterKey != "" && entry.Key <= req.AfterKey) {
			continue
		}
		record, exists := req.Digest[entry.Key]
		if !exists || policy.CompareEntryToDigest(entry, record) > 0 {
			candidates = append(candidates, entry)
			if len(candidates) > limit {
				break
			}
		}
	}
	more := len(candidates) > limit
	if more {
		candidates = candidates[:limit]
	}
	resp.Entries = candidates
	maxBytes := req.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxResponseBytes
	}
	fit, err := fitDigestPage(resp, maxBytes)
	if err != nil {
		return nil, err
	}
	if fit < len(candidates) {
		resp.Entries = candidates[:fit]
		more = true
	}
	// Never expose the immutable store values through a response.
	var owned []Entry
	if len(resp.Entries) > 0 {
		owned = make([]Entry, len(resp.Entries))
	}
	for i, entry := range resp.Entries {
		owned[i] = copyEntry(entry)
	}
	resp.Entries = owned
	if more {
		resp.More = true
		resp.NextKey = resp.Entries[len(resp.Entries)-1].Key
	}
	return resp, nil
}
