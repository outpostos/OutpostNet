package state

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

// DigestEntryTooLargeError means one digest record and its range envelope cannot
// fit. Never truncate it: doing so would defeat the collected-value check.
type DigestEntryTooLargeError struct {
	Key             string
	Bytes, MaxBytes int64
}

func (e *DigestEntryTooLargeError) Error() string {
	return fmt.Sprintf("state digest key %q needs %d request bytes, above MaxDigestBytes %d; raise the digest and peer request limits", e.Key, e.Bytes, e.MaxBytes)
}

func (g *GossipEngine) pullDigestRound(ctx context.Context, addr string, digest []KeyDigest) error {
	_, ok := g.syncer.(SnapshotSyncer)
	if !ok {
		return fmt.Errorf("digest paging requires a syncer implementing SnapshotSyncer")
	}
	longest := ""
	longestBytes := 0
	for _, item := range digest {
		encoded, _ := json.Marshal(item.Key)
		if len(encoded) > longestBytes {
			longest = item.Key
			longestBytes = len(encoded)
		}
	}
	start := ""
	for offset := 0; ; {
		if err := ctx.Err(); err != nil {
			return err
		}
		req := PullRequest{
			TimeSample:      g.timeSample(),
			ProtocolVersion: StateProtocolVersion, PolicyID: g.policy.ID(),
			SourceNodeID: g.nodeID, GCHorizon: g.localHorizon(),
			StartKey: &start, MaxBytes: defaultMaxResponseBytes,
			Digest: make(map[string]DigestRecord),
		}
		// Reserve the largest local boundary/cursor and horizon, including metadata
		// used by the transport. Every actual request is checked again before send.
		budget := req
		budget.TimeSample = TimeSample{SentPhysicalMS: math.MinInt64, TimeQuality: TimeUnsynced}
		budget.EndKey = longest
		budget.AfterKey = longest
		budget.MaxBytes = math.MaxInt64
		budget.GCHorizon = &Timestamp{WallMS: math.MinInt64, Logical: math.MaxUint16}
		budget.Digest = map[string]DigestRecord{"": {}}
		envelope, err := json.Marshal(budget)
		if err != nil {
			return err
		}
		emptyRecord, _ := json.Marshal(DigestRecord{})
		used := int64(len(envelope) - len(emptyRecord) - 3) // remove "":record; retain {}
		end := offset
		for end < len(digest) {
			key := digest[end].Key
			k, _ := json.Marshal(key)
			record, err := json.Marshal(digest[end].Record)
			if err != nil {
				return err
			}
			size := int64(len(k) + 1 + len(record))
			if end > offset {
				size++
			}
			if used+size > g.maxDigestBytes {
				if end == offset {
					return &DigestEntryTooLargeError{Key: key, Bytes: used + size, MaxBytes: g.maxDigestBytes}
				}
				break
			}
			used += size
			req.Digest[key] = digest[end].Record
			end++
		}
		if end < len(digest) {
			req.EndKey = digest[end].Key
		}
		for {
			encoded, err := json.Marshal(req)
			if err != nil {
				return err
			}
			if int64(len(encoded)) > g.maxDigestBytes {
				return &DigestEntryTooLargeError{Key: req.AfterKey, Bytes: int64(len(encoded)), MaxBytes: g.maxDigestBytes}
			}
			resp, err := g.pullExchange(ctx, addr, req)
			if err != nil {
				return err
			}
			if err := g.checkPeerHorizon(addr, timestampValue(resp.GCHorizon)); err != nil {
				return err
			}
			if err := validateDigestResponse(req, resp); err != nil {
				return err
			}
			if err := g.applyDigestPage(ctx, addr, resp.Entries); err != nil {
				return err
			}
			if !resp.More {
				break
			}
			req.AfterKey = resp.NextKey
		}
		if end == len(digest) {
			return nil
		}
		start = req.EndKey
		offset = end
	}
}

func digestKeyInRange(key string, req PullRequest) bool {
	return (req.StartKey == nil || key >= *req.StartKey) && (req.EndKey == "" || key < req.EndKey)
}

func (g *GossipEngine) handleDigestResponse(ctx context.Context, req PullRequest, resp *PullResponse, horizon Timestamp) (*PullResponse, error) {
	if req.MaxBytes < 0 || req.Limit < 0 || req.SincePosition != 0 {
		return nil, fmt.Errorf("invalid digest paging limits or position cursor")
	}
	if req.StartKey != nil && req.EndKey != "" && req.EndKey < *req.StartKey {
		return nil, fmt.Errorf("digest end_key precedes start_key")
	}
	if req.AfterKey != "" && !digestKeyInRange(req.AfterKey, req) {
		return nil, fmt.Errorf("digest cursor outside interval")
	}
	for key := range req.Digest {
		if !digestKeyInRange(key, req) {
			return nil, fmt.Errorf("digest key %q outside declared interval", key)
		}
	}
	if err := g.confirmDigestRefusals(ctx, req.SourceNodeID, req.Digest); err != nil {
		return nil, err
	}
	if err := g.checkRequesterHorizon(ctx, horizon, req.GCHorizon, req.SourceNodeID); err != nil {
		return nil, err
	}
	// Check the COMPLETE interval on every continuation, not just keys after the
	// diff cursor. The validated digest contains only keys in that interval.
	if err := g.checkRequesterHoldsCollected(ctx, horizon, req.Digest); err != nil {
		return nil, err
	}
	if reader, ok := g.store.(ReplicationReadStore); ok {
		return reader.DigestDiffPage(ctx, req, *resp)
	}
	entries, err := g.store.ListEntries(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return digestDiffPage(ctx, req, resp, entries, g.policy)
}

// Like fitPullPage, but budget the actual exclusive key cursor instead of a
// position. Count JSON escaping and the HTTP encoder's trailing newline.
func fitDigestPage(resp *PullResponse, maxBytes int64) (int, error) {
	envelope := *resp
	envelope.Entries = []Entry{}
	envelope.More = true
	envelope.NextKey = "."
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return 0, err
	}
	used := int64(len(encoded) + 1 - 3) // remove encoded "."; each candidate supplies its key
	for i, entry := range resp.Entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return 0, err
		}
		size := int64(len(data))
		if i > 0 {
			size++
		}
		cursor, _ := json.Marshal(entry.Key)
		total := used + size + int64(len(cursor))
		if total > maxBytes {
			if i == 0 {
				return 0, &PullEntryTooLargeError{Key: entry.Key, Bytes: total, MaxBytes: maxBytes}
			}
			return i, nil
		}
		used += size
	}
	if len(resp.Entries) == 0 && used+3 > maxBytes {
		return 0, fmt.Errorf("pull response envelope exceeds %d bytes", maxBytes)
	}
	return len(resp.Entries), nil
}

func validateDigestResponse(req PullRequest, resp *PullResponse) error {
	if resp == nil {
		return fmt.Errorf("empty digest response")
	}
	// Validate the continuation before applying anything; never accept a cursor
	// that skips entries, leaves this interval, or cannot make progress.
	last := req.AfterKey
	for _, entry := range resp.Entries {
		if !digestKeyInRange(entry.Key, req) || ((resp.More || req.AfterKey != "") && last != "" && entry.Key <= last) {
			return fmt.Errorf("digest response key %q outside interval or out of order", entry.Key)
		}
		last = entry.Key
	}
	if resp.More && (len(resp.Entries) == 0 || resp.NextKey != last || resp.NextKey <= req.AfterKey) {
		return fmt.Errorf("digest response did not advance within interval")
	}
	return nil
}
