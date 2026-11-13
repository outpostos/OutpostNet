package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// MergePolicy owns conflict ordering and digest metadata for one keyspace.
// Policy callbacks must treat entries as immutable and support concurrent reads;
// replication may evaluate immutable snapshots after releasing the store lock.
type MergePolicy interface {
	ID() string
	ValidateApply(entry Entry) error
	CompareEntries(left, right Entry) int
	DecorateDigest(entry Entry, record *DigestRecord)
	CompareEntryToDigest(entry Entry, record DigestRecord) int
}

type hlcLWWPolicy struct{}

// NewHLCLWWPolicy returns the domain-agnostic HLC last-writer-wins policy.
func NewHLCLWWPolicy() MergePolicy { return hlcLWWPolicy{} }

func (hlcLWWPolicy) ID() string { return "outpostnet.state.hlc-lww/v1" }

func (hlcLWWPolicy) ValidateApply(entry Entry) error {
	if strings.TrimSpace(entry.Key) == "" {
		return fmt.Errorf("state entry key is required")
	}
	if strings.TrimSpace(entry.Version.WriterNodeID) == "" {
		return fmt.Errorf("state entry writer node is required for key %q", entry.Key)
	}
	if strings.TrimSpace(entry.ChangeID.OriginNodeID) == "" || entry.ChangeID.Counter == 0 {
		return fmt.Errorf("state entry change identity is required for key %q", entry.Key)
	}
	if entry.Version.HLC.WallMS <= 0 {
		return fmt.Errorf("state entry HLC wall time must be > 0 for key %q", entry.Key)
	}
	return nil
}

func (hlcLWWPolicy) CompareEntries(left, right Entry) int {
	if result := left.Version.HLC.Compare(right.Version.HLC); result != 0 {
		return result
	}
	if result := strings.Compare(left.Version.WriterNodeID, right.Version.WriterNodeID); result != 0 {
		return result
	}
	if result := strings.Compare(left.ChangeID.OriginNodeID, right.ChangeID.OriginNodeID); result != 0 {
		return result
	}
	if left.ChangeID.Counter < right.ChangeID.Counter {
		return -1
	}
	if left.ChangeID.Counter > right.ChangeID.Counter {
		return 1
	}
	return 0
}

func (hlcLWWPolicy) DecorateDigest(Entry, *DigestRecord) {}

func (p hlcLWWPolicy) CompareEntryToDigest(entry Entry, record DigestRecord) int {
	proxy := Entry{
		Key:      entry.Key,
		Deleted:  record.Deleted,
		Version:  record.Version,
		ChangeID: record.ChangeID,
	}
	return p.CompareEntries(entry, proxy)
}

func normalizePolicy(policy MergePolicy) MergePolicy {
	if policy == nil {
		return NewHLCLWWPolicy()
	}
	return policy
}

func policiesCompatible(left, right MergePolicy) bool {
	left = normalizePolicy(left)
	right = normalizePolicy(right)
	return left.ID() == right.ID()
}

var errPolicyIDRequired = errors.New("state policy id is required")

func validatePolicyID(local MergePolicy, peerPolicyID string) error {
	local = normalizePolicy(local)
	if strings.TrimSpace(peerPolicyID) == "" {
		return errPolicyIDRequired
	}
	if peerPolicyID != local.ID() {
		return fmt.Errorf("state policy id mismatch: got %q want %q", peerPolicyID, local.ID())
	}
	return nil
}

func newDigestRecord(entry Entry, policy MergePolicy) DigestRecord {
	record := DigestRecord{
		Version:  entry.Version,
		ChangeID: entry.ChangeID,
		Deleted:  entry.Deleted,
		Metadata: make(map[string]json.RawMessage),
	}
	normalizePolicy(policy).DecorateDigest(entry, &record)
	if len(record.Metadata) == 0 {
		record.Metadata = nil
	}
	return record
}
