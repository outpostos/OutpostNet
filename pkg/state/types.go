package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

// ProtocolVersion is the major.minor wire version for state replication.
type ProtocolVersion struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
}

// StateProtocolVersion requires bounded handshake time samples (cluster-wide upgrade).
var StateProtocolVersion = ProtocolVersion{Major: 3, Minor: 0}

func (v ProtocolVersion) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// CompatibleWith reports whether a peer can sync with the local protocol.
func (v ProtocolVersion) CompatibleWith(local ProtocolVersion) bool {
	return v.Major != 0 && v.Major == local.Major
}

// ChangeID identifies the original write event. Forwarding preserves it.
type ChangeID struct {
	OriginNodeID string `json:"origin_node_id"`
	Counter      uint64 `json:"counter"`
}

// StateVersion orders competing values for a key.
type StateVersion struct {
	WriterNodeID string    `json:"writer_node_id"`
	HLC          Timestamp `json:"hlc"`
}

// EntryIdentity is an opaque stable identity for atomic conditions.
type EntryIdentity struct {
	Key       string       `json:"key"`
	Deleted   bool         `json:"deleted"`
	Version   StateVersion `json:"version"`
	ChangeID  ChangeID     `json:"change_id"`
	ValueHash string       `json:"value_hash"`
}

// Entry is one opaque replicated state record. Position is node-local and is
// not part of the wire representation or original write identity.
type Entry struct {
	Key      string       `json:"key"`
	Value    []byte       `json:"value,omitempty"`
	Deleted  bool         `json:"deleted,omitempty"`
	Version  StateVersion `json:"version"`
	ChangeID ChangeID     `json:"change_id"`
	Position uint64       `json:"-"`
}

// Identity returns the atomic comparison identity for the entry.
func (e Entry) Identity() EntryIdentity {
	return EntryIdentity{
		Key:       e.Key,
		Deleted:   e.Deleted,
		Version:   e.Version,
		ChangeID:  e.ChangeID,
		ValueHash: valueHash(e.Value),
	}
}

// Mutation describes a conditional value replacement or tombstone.
type Mutation struct {
	Value   []byte
	Deleted bool
}

// PushRequest is a batch of entries pushed from one node to a peer.
type PushRequest struct {
	TimeSample
	ProtocolVersion ProtocolVersion `json:"protocol_version"`
	PolicyID        string          `json:"policy_id"`
	SourceNodeID    string          `json:"source"`
	// GCHorizon is the sender's GC horizon; omitted when zero.
	GCHorizon *Timestamp `json:"gc_horizon,omitempty"`
	Entries   []Entry    `json:"entries"`
}

// PushResponse reports how many pushed entries changed receiver state.
type PushResponse struct {
	TimeSample
	ProtocolVersion ProtocolVersion `json:"protocol_version"`
	PolicyID        string          `json:"policy_id"`
	Accepted        int             `json:"accepted"`
	Rejected        int             `json:"rejected"`
	Rejections      []PushRejection `json:"rejections,omitempty"`
	// GCHorizon is the receiver's GC horizon; omitted when zero.
	GCHorizon *Timestamp `json:"gc_horizon,omitempty"`
	// NodeID is the receiver's node ID. The pusher records it next to its push
	// progress so that, as a responder, it can tell whether a requester
	// acknowledged its removed tombstones. Omitted by 03b nodes.
	NodeID string `json:"node_id,omitempty"`
}

// PushRejection explains an entry rejection.
type PushRejection struct {
	Key         string `json:"key"`
	Reason      string `json:"reason"`
	Transient   bool   `json:"transient,omitempty"`
	OffsetMS    int64  `json:"offset_ms,omitempty"`
	MaxOffsetMS int64  `json:"max_offset_ms,omitempty"`
}

// PullRequest asks for entries newer than a cursor or a digest.
type PullRequest struct {
	TimeSample
	ProtocolVersion ProtocolVersion         `json:"protocol_version"`
	PolicyID        string                  `json:"policy_id"`
	SourceNodeID    string                  `json:"source,omitempty"`
	SincePosition   uint64                  `json:"since_position,omitempty"`
	Limit           int                     `json:"limit,omitempty"`
	Digest          map[string]DigestRecord `json:"digest,omitempty"`
	// GCHorizon is the requester's GC horizon; omitted when zero.
	GCHorizon *Timestamp `json:"gc_horizon,omitempty"`
	// StartKey selects digest mode even for an empty digest. Intervals are
	// [StartKey, EndKey); empty EndKey means unbounded. Nil selects a position
	// pull (or a full-space digest for direct callers supplying Digest).
	StartKey *string `json:"start_key,omitempty"`
	EndKey   string  `json:"end_key,omitempty"`
	// AfterKey is an exclusive diff cursor within the SAME complete interval.
	AfterKey string `json:"after_key,omitempty"`
	// MaxBytes bounds the encoded size of a pull response: the
	// responder ends the page (More) before it would exceed it. Omitted when
	// zero (digest pulls use the default 1 MiB; position pulls are unbounded).
	MaxBytes int64 `json:"max_bytes,omitempty"`
}

// PullResponse is returned by the HTTP pull endpoint.
type PullResponse struct {
	// NodeID identifies the responder, so address aliases cannot count as two peers.
	NodeID string `json:"node_id"`
	TimeSample
	ProtocolVersion ProtocolVersion `json:"protocol_version"`
	PolicyID        string          `json:"policy_id"`
	Entries         []Entry         `json:"entries"`
	// GCHorizon is the responder's GC horizon, read before Entries; omitted when
	// zero.
	GCHorizon *Timestamp `json:"gc_horizon,omitempty"`
	// More and NextSincePosition page a position pull with a Limit.
	More              bool   `json:"more,omitempty"`
	NextSincePosition uint64 `json:"next_since_position,omitempty"`
	// NextKey is the exclusive last-key cursor of a digest diff page.
	NextKey string `json:"next_key,omitempty"`
}

// DigestRecord captures latest known state for a key plus policy metadata.
type DigestRecord struct {
	Version  StateVersion               `json:"version"`
	ChangeID ChangeID                   `json:"change_id"`
	Deleted  bool                       `json:"deleted,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}

// Syncer exchanges state with a peer adapter.
type Syncer interface {
	Push(ctx context.Context, addr string, req PushRequest) (*PushResponse, error)
	Pull(ctx context.Context, addr string, req PullRequest) ([]Entry, error)
}

// Store is the public state replica seam used by consumers and gossip.
type Store interface {
	Policy() MergePolicy
	Put(ctx context.Context, key string, value []byte) (Entry, error)
	Delete(ctx context.Context, key string) (Entry, error)
	CompareAndSet(ctx context.Context, key string, expected EntryIdentity, mutation Mutation) (Entry, bool, error)
	Get(ctx context.Context, key string) (Entry, bool, error)
	ListEntries(ctx context.Context) ([]Entry, error)
	ChangesSince(ctx context.Context, since uint64, limit int) ([]Entry, error)
	Apply(ctx context.Context, incoming Entry) (bool, error)
	CurrentPosition() uint64
	Observe(fn func(Entry)) func()
	LocalStatus() LocalStatus
	CheckConsumerPosition(position uint64) ConsumerPositionStatus
}

// LocalStatus reports whether the store has completed local recovery and the
// latest node-local state position. It does not claim global convergence.
type LocalStatus struct {
	Ready           bool
	CurrentPosition uint64
}

// ConsumerPositionStatus explains whether a consumer position can be used for
// incremental reads or must restart from a snapshot.
type ConsumerPositionStatus struct {
	Ready           bool
	CurrentPosition uint64
	Requested       uint64
	CanResume       bool
	NeedsSnapshot   bool
	Reason          string
}

// PeerProgressStore is implemented by durable stores that persist gossip
// progress after a peer has acknowledged a push.
type PeerProgressStore interface {
	PeerProgress(ctx context.Context, peer string) (uint64, error)
	SavePeerProgress(ctx context.Context, peer string, position uint64) error
}

// PeerProvider returns current peer addresses.
type PeerProvider interface {
	Peers() []string
}

// PeerProviderFunc adapts a function into PeerProvider.
type PeerProviderFunc func() []string

// Peers returns current peers.
func (f PeerProviderFunc) Peers() []string { return f() }

// StaticPeerProvider returns a fixed peer list.
type StaticPeerProvider struct {
	peers []string
}

// NewStaticPeerProvider creates a fixed peer provider.
func NewStaticPeerProvider(peers []string) *StaticPeerProvider {
	return &StaticPeerProvider{peers: append([]string(nil), peers...)}
}

// Peers returns a copy of peers.
func (p *StaticPeerProvider) Peers() []string {
	return append([]string(nil), p.peers...)
}

type localClock struct {
	value atomic.Uint64
}

func (c *localClock) Next() uint64    { return c.value.Add(1) }
func (c *localClock) Current() uint64 { return c.value.Load() }
func (c *localClock) SetAtLeast(remote uint64) {
	for {
		local := c.value.Load()
		if local >= remote {
			return
		}
		if c.value.CompareAndSwap(local, remote) {
			return
		}
	}
}
func (c *localClock) Merge(remote uint64) uint64 {
	for {
		local := c.value.Load()
		next := local
		if remote > next {
			next = remote
		}
		next++
		if c.value.CompareAndSwap(local, next) {
			return next
		}
	}
}

func valueHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func newEntryForTest(key string, value []byte, nodeID string, counter uint64, ts Timestamp) Entry {
	return Entry{
		Key:     key,
		Value:   append([]byte(nil), value...),
		Version: StateVersion{WriterNodeID: nodeID, HLC: ts},
		ChangeID: ChangeID{
			OriginNodeID: nodeID,
			Counter:      counter,
		},
		Position: counter,
	}
}
