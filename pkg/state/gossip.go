package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

const rejectionReasonClockOffset = "clock_offset"

// GossipConfig configures incremental push and digest pull exchange.
type GossipConfig struct {
	NodeID              string
	Store               Store
	Syncer              Syncer
	PeerProvider        PeerProvider
	Policy              MergePolicy
	Interval            time.Duration
	AntiEntropyInterval time.Duration
	BackoffBase         time.Duration
	BackoffMax          time.Duration
}

// GossipEngine exchanges state with peers through a Syncer.
type GossipEngine struct {
	nodeID              string
	store               Store
	syncer              Syncer
	peerProvider        PeerProvider
	policy              MergePolicy
	interval            time.Duration
	antiEntropyInterval time.Duration
	backoffBase         time.Duration
	backoffMax          time.Duration

	mu         sync.Mutex
	peerStates map[string]*peerState
}

type peerState struct {
	lastPushPosition uint64
	// nodeID is the node that answered the latest acknowledged push here.
	nodeID      string
	failures    int
	nextAttempt time.Time

	// Exchange results reported by ReplicationStatus. Times are the local
	// clock; zero means "not in this process".
	lastSuccess   time.Time // latest acknowledged push or fully applied pull
	lastPull      time.Time // start of the latest fully applied pull
	lastError     string    // redacted text of the latest failed exchange
	lastErrorTime time.Time
	lastErrorOp   string
}

// NewGossipEngine creates a replication engine. If Policy is provided it must
// match the store policy; otherwise the store is the only strategy source.
func NewGossipEngine(cfg GossipConfig) (*GossipEngine, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("gossip node id is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("gossip store is required")
	}
	if cfg.Syncer == nil {
		return nil, fmt.Errorf("gossip syncer is required")
	}
	if cfg.PeerProvider == nil {
		cfg.PeerProvider = NewStaticPeerProvider(nil)
	}
	storePolicy := cfg.Store.Policy()
	if cfg.Policy != nil && !policiesCompatible(storePolicy, cfg.Policy) {
		return nil, fmt.Errorf("gossip policy %q is incompatible with store policy %q", cfg.Policy.ID(), storePolicy.ID())
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = 10 * time.Second
	}
	antiEntropyInterval := cfg.AntiEntropyInterval
	if antiEntropyInterval == 0 {
		antiEntropyInterval = 15 * time.Second
	}
	backoffBase := cfg.BackoffBase
	if backoffBase == 0 {
		backoffBase = minDuration(interval, antiEntropyInterval)
	}
	backoffMax := cfg.BackoffMax
	if backoffMax == 0 {
		backoffMax = 5 * backoffBase
	}
	if backoffMax < backoffBase {
		backoffMax = backoffBase
	}
	if gs, ok := cfg.Store.(gcStore); ok {
		// From now on tombstones may only be collected through the engine, which
		// knows the peers that must acknowledge them first.
		gs.markReplicated()
	}
	return &GossipEngine{
		nodeID:              cfg.NodeID,
		store:               cfg.Store,
		syncer:              cfg.Syncer,
		peerProvider:        cfg.PeerProvider,
		policy:              storePolicy,
		interval:            interval,
		antiEntropyInterval: antiEntropyInterval,
		backoffBase:         backoffBase,
		backoffMax:          backoffMax,
		peerStates:          make(map[string]*peerState),
	}, nil
}

// Run starts periodic sync until ctx is canceled.
func (g *GossipEngine) Run(ctx context.Context) error {
	pushTicker := time.NewTicker(g.interval)
	defer pushTicker.Stop()
	antiEntropyTicker := time.NewTicker(g.antiEntropyInterval)
	defer antiEntropyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pushTicker.C:
			_ = g.SyncOnce(ctx)
		case <-antiEntropyTicker.C:
			_ = g.AntiEntropyOnce(ctx)
			g.maintain(ctx)
		}
	}
}

// SyncOnce performs one incremental push round.
func (g *GossipEngine) SyncOnce(ctx context.Context) error {
	var errs []error
	now := time.Now()
	for _, addr := range g.peerProvider.Peers() {
		if ok, until := g.allowAttempt(addr, now); !ok {
			slog.Debug("state gossip: peer in backoff", "peer", redactedPeerURL(addr), "until", until)
			continue
		}
		since, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			errs = append(errs, redactPeerErr(addr, err))
			continue
		}
		changes, err := g.store.ChangesSince(ctx, since, 0)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			continue
		}
		resp, err := g.syncer.Push(ctx, addr, PushRequest{
			ProtocolVersion: StateProtocolVersion,
			PolicyID:        g.policy.ID(),
			SourceNodeID:    g.nodeID,
			GCHorizon:       g.localHorizon(),
			Entries:         changes,
		})
		if err != nil {
			var resync *ResyncRequiredError
			if errors.As(err, &resync) {
				// The peer refused our increments because we lag behind its GC
				// horizon. Resync from it; the cursor stays put and the next round
				// pushes whatever survived the resync.
				if err := g.resyncFrom(ctx, addr, resync.Horizon); err != nil {
					errs = append(errs, g.recordPeerFailure(addr, "push-resync", err, now))
					continue
				}
				g.recordPeerSuccess(addr)
				continue
			}
			errs = append(errs, g.recordPeerFailure(addr, "push", err, now))
			continue
		}
		pushed := g.pushSuccessPosition(addr, since, changes, resp)
		if err := g.recordPushSuccess(ctx, addr, pushed, resp); err != nil {
			errs = append(errs, g.recordPeerFailure(addr, "push-progress", err, now))
			continue
		}
		if hasBeyondHorizonRejection(resp) {
			// The peer refused entries for keys it collected. We hold stale state for
			// them, so we resync from the peer, which purges those entries locally.
			if err := g.resyncFrom(ctx, addr, timestampValue(resp.GCHorizon)); err != nil {
				errs = append(errs, g.recordPeerFailure(addr, "push-resync", err, now))
				continue
			}
		}
	}
	return errors.Join(errs...)
}

// AntiEntropyOnce performs one digest pull round.
func (g *GossipEngine) AntiEntropyOnce(ctx context.Context) error {
	var errs []error
	now := time.Now()
	for _, addr := range g.peerProvider.Peers() {
		if ok, until := g.allowAttempt(addr, now); !ok {
			slog.Debug("state gossip: peer in backoff", "peer", redactedPeerURL(addr), "until", until)
			continue
		}
		digest, err := g.LocalDigest(ctx)
		if err != nil {
			return err
		}
		// A pull reports the peer's state as of when it was asked, so its start
		// time is what "inbound caught up" can claim.
		started := time.Now()
		entries, err := g.syncer.Pull(ctx, addr, PullRequest{
			ProtocolVersion: StateProtocolVersion,
			PolicyID:        g.policy.ID(),
			SourceNodeID:    g.nodeID,
			GCHorizon:       g.localHorizon(),
			Digest:          digest,
		})
		if err != nil {
			var resync *ResyncRequiredError
			if errors.As(err, &resync) {
				if err := g.resyncFrom(ctx, addr, resync.Horizon); err != nil {
					errs = append(errs, g.recordPeerFailure(addr, "pull-resync", err, now))
					continue
				}
				g.recordPeerSuccess(addr)
				continue
			}
			errs = append(errs, g.recordPeerFailure(addr, "pull", err, now))
			continue
		}
		var applyErrs []error
		var beyondHorizon int
		for _, entry := range entries {
			if _, err := g.store.Apply(ctx, entry); err != nil {
				var beyond *BeyondGCHorizonError
				if errors.As(err, &beyond) {
					// The peer lags behind our GC horizon. Refusing its stale entry is
					// the intended outcome, not a local failure; the peer resyncs on
					// its own next exchange with us.
					beyondHorizon++
					continue
				}
				applyErrs = append(applyErrs, fmt.Errorf("apply pulled entry from %s key %q: %w", redactedPeerURL(addr), entry.Key, err))
			}
		}
		if beyondHorizon > 0 {
			slog.Warn("state gossip: refused pulled entries below the local GC horizon; peer must resync",
				"peer", redactedPeerURL(addr),
				"refused", beyondHorizon,
			)
		}
		if err := errors.Join(applyErrs...); err != nil {
			errs = append(errs, g.recordPeerFailure(addr, "pull-apply", err, now))
			continue
		}
		g.recordPullSuccess(addr, started)
	}
	return errors.Join(errs...)
}

// HandlePush applies entries from a peer.
func (g *GossipEngine) HandlePush(ctx context.Context, req PushRequest) (*PushResponse, error) {
	if err := validatePolicyID(g.policy, req.PolicyID); err != nil {
		return nil, err
	}
	horizon := g.storeHorizon()
	if err := g.checkRequesterHorizon(ctx, horizon, req.GCHorizon, req.SourceNodeID); err != nil {
		return nil, err
	}
	var accepted, rejected int
	var rejections []PushRejection
	for _, entry := range req.Entries {
		applied, err := g.store.Apply(ctx, entry)
		if err != nil {
			rejected++
			var beyond *BeyondGCHorizonError
			if errors.As(err, &beyond) {
				rejections = append(rejections, PushRejection{Key: entry.Key, Reason: RejectionReasonBeyondGCHorizon})
				continue
			}
			if rejection, ok := rejectionFromApplyError(entry, err); ok {
				rejections = append(rejections, rejection)
				continue
			}
			return nil, fmt.Errorf("apply pushed entry from %q key %q: %w", req.SourceNodeID, entry.Key, err)
		}
		if applied {
			accepted++
		} else {
			rejected++
		}
	}
	return &PushResponse{
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        g.policy.ID(),
		Accepted:        accepted,
		Rejected:        rejected,
		Rejections:      rejections,
		GCHorizon:       timestampPtr(g.storeHorizon()),
		NodeID:          g.nodeID,
	}, nil
}

// HandlePull returns entries newer than the caller's cursor or digest.
func (g *GossipEngine) HandlePull(ctx context.Context, req PullRequest) ([]Entry, error) {
	resp, err := g.HandlePullResponse(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// HandlePullResponse answers a pull with paging and GC horizon metadata. A
// digest pull from a requester whose GC horizon is lower than ours and that we
// cannot vouch for, or whose digest still holds a value we collected, is
// refused with ResyncRequiredError. Position pulls (no digest) are always
// served: they are how a lagging requester pages through our snapshot during
// resync.
func (g *GossipEngine) HandlePullResponse(ctx context.Context, req PullRequest) (*PullResponse, error) {
	if err := validatePolicyID(g.policy, req.PolicyID); err != nil {
		return nil, err
	}
	// The horizon is read before the entries, so it never claims more than the
	// returned state reflects.
	horizon := g.storeHorizon()
	resp := &PullResponse{
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        g.policy.ID(),
		GCHorizon:       timestampPtr(horizon),
	}
	if len(req.Digest) > 0 {
		if err := g.checkRequesterHorizon(ctx, horizon, req.GCHorizon, req.SourceNodeID); err != nil {
			return nil, err
		}
		if err := g.checkRequesterHoldsCollected(ctx, horizon, req.Digest); err != nil {
			return nil, err
		}
		entries, err := g.diffAgainstDigest(ctx, req.Digest, req.Limit)
		if err != nil {
			return nil, err
		}
		resp.Entries = entries
		return resp, nil
	}
	entries, err := g.store.ChangesSince(ctx, req.SincePosition, req.Limit)
	if err != nil {
		return nil, err
	}
	resp.Entries = entries
	more := req.Limit > 0 && len(entries) == req.Limit
	if req.MaxBytes > 0 {
		fit, err := fitPullPage(resp, req.MaxBytes)
		if err != nil {
			return nil, err
		}
		if fit < len(entries) {
			resp.Entries = entries[:fit]
			more = true
		}
	}
	if more {
		resp.More = true
		resp.NextSincePosition = resp.Entries[len(resp.Entries)-1].Position
	}
	return resp, nil
}

// fitPullPage returns how many leading entries of resp fit, encoded, into
// maxBytes together with the response envelope (paging fields included). A
// first entry that cannot fit on its own is an error: paging cannot make
// progress past it.
func fitPullPage(resp *PullResponse, maxBytes int64) (int, error) {
	envelope := *resp
	envelope.Entries = []Entry{}
	envelope.More = true
	envelope.NextSincePosition = math.MaxUint64
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return 0, fmt.Errorf("encode pull response envelope: %w", err)
	}
	// +1 for the newline the HTTP encoder appends.
	used := int64(len(encoded)) + 1
	for index, entry := range resp.Entries {
		encoded, err := json.Marshal(entry)
		if err != nil {
			return 0, fmt.Errorf("encode pull entry %q: %w", entry.Key, err)
		}
		size := int64(len(encoded))
		if index > 0 {
			size++ // separating comma
		}
		if used+size > maxBytes {
			if index == 0 {
				return 0, &PullEntryTooLargeError{Key: entry.Key, Bytes: used + size, MaxBytes: maxBytes}
			}
			return index, nil
		}
		used += size
	}
	return len(resp.Entries), nil
}

// PullEntryTooLargeError reports that a single entry cannot be returned within
// the requester's response limit, so a byte-bounded page cannot advance.
type PullEntryTooLargeError struct {
	Key      string
	Bytes    int64
	MaxBytes int64
}

func (e *PullEntryTooLargeError) Error() string {
	return fmt.Sprintf("state entry %q needs a %d-byte pull response, above the requester's %d-byte response limit; raise HTTPSyncerConfig.MaxResponseBytes on the requester",
		e.Key, e.Bytes, e.MaxBytes)
}

// LocalDigest returns the local summary using the engine policy.
func (g *GossipEngine) LocalDigest(ctx context.Context) (map[string]DigestRecord, error) {
	entries, err := g.store.ListEntries(ctx)
	if err != nil {
		return nil, err
	}
	digest := make(map[string]DigestRecord, len(entries))
	for _, entry := range entries {
		digest[entry.Key] = newDigestRecord(entry, g.policy)
	}
	return digest, nil
}

func (g *GossipEngine) diffAgainstDigest(ctx context.Context, digest map[string]DigestRecord, limit int) ([]Entry, error) {
	entries, err := g.store.ListEntries(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		record, ok := digest[entry.Key]
		if !ok || g.policy.CompareEntryToDigest(entry, record) > 0 {
			result = append(result, entry)
		}
	}
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (g *GossipEngine) allowAttempt(addr string, now time.Time) (bool, time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	if state.nextAttempt.After(now) {
		return false, state.nextAttempt
	}
	return true, time.Time{}
}

func (g *GossipEngine) peerLastPushPosition(ctx context.Context, addr string) (uint64, error) {
	if progress, ok := g.store.(PeerProgressStore); ok {
		position, err := progress.PeerProgress(ctx, addr)
		if err != nil {
			return 0, err
		}
		return position, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peerStateLocked(addr).lastPushPosition, nil
}

func (g *GossipEngine) recordPushSuccess(ctx context.Context, addr string, pushed uint64, resp *PushResponse) error {
	var nodeID string
	if resp != nil {
		nodeID = resp.NodeID
	}
	if acks, ok := g.store.(peerAckStore); ok {
		if err := acks.savePeerAck(ctx, addr, nodeID, pushed); err != nil {
			return err
		}
	} else if progress, ok := g.store.(PeerProgressStore); ok {
		if err := progress.SavePeerProgress(ctx, addr, pushed); err != nil {
			return err
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	if pushed > state.lastPushPosition {
		state.lastPushPosition = pushed
	}
	// The node that answered this push replaces any earlier one; a response
	// without node_id (a 03b node, or another node that took over the address)
	// clears the mapping, so that address no longer vouches for anyone.
	state.nodeID = nodeID
	state.failures = 0
	state.nextAttempt = time.Time{}
	state.lastSuccess = time.Now()
	return nil
}

// recordPeerSuccess clears backoff after an exchange that is neither an
// acknowledged push nor a fully applied pull (a snapshot resync). It does not
// move lastPull: the conservative inbound claim waits for the next pull.
func (g *GossipEngine) recordPeerSuccess(addr string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures = 0
	state.nextAttempt = time.Time{}
	state.lastSuccess = time.Now()
}

func (g *GossipEngine) recordPullSuccess(addr string, started time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures = 0
	state.nextAttempt = time.Time{}
	state.lastSuccess = time.Now()
	state.lastPull = started
}

// recordPeerFailure records a failed exchange and returns err with the peer
// password removed, for the caller to return.
func (g *GossipEngine) recordPeerFailure(addr, operation string, err error, now time.Time) error {
	err = redactPeerErr(addr, err)
	message := err.Error()
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures++
	backoff := saturatedBackoff(g.backoffBase, g.backoffMax, state.failures)
	state.nextAttempt = now.Add(backoff)
	state.lastError = message
	state.lastErrorTime = time.Now()
	state.lastErrorOp = operation
	slog.Warn("state gossip: peer sync failed", "peer", redactedPeerURL(addr), "operation", operation, "err", message, "retryAt", state.nextAttempt)
	return err
}

func saturatedBackoff(base, max time.Duration, failures int) time.Duration {
	if failures <= 0 {
		return base
	}
	backoff := base
	for i := 1; i < failures; i++ {
		if backoff >= max || backoff > max/2 {
			return max
		}
		backoff *= 2
	}
	if backoff > max {
		return max
	}
	return backoff
}

func (g *GossipEngine) peerStateLocked(addr string) *peerState {
	state := g.peerStates[addr]
	if state == nil {
		state = &peerState{}
		g.peerStates[addr] = state
	}
	return state
}

func (g *GossipEngine) pushSuccessPosition(peer string, since uint64, changes []Entry, resp *PushResponse) uint64 {
	if resp == nil {
		return maxPosition(changes)
	}
	firstTransient := uint64(0)
	for _, rejection := range resp.Rejections {
		if !rejection.Transient {
			continue
		}
		if rejectedPosition, ok := positionForKey(changes, rejection.Key); ok && (firstTransient == 0 || rejectedPosition < firstTransient) {
			firstTransient = rejectedPosition
		}
		slog.Warn("state gossip: transient push rejection",
			"peer", redactedPeerURL(peer),
			"key", rejection.Key,
			"reason", rejection.Reason,
			"offsetMS", rejection.OffsetMS,
			"maxOffsetMS", rejection.MaxOffsetMS,
		)
	}
	if firstTransient == 0 {
		return maxPosition(changes)
	}
	pushed := since
	for _, entry := range changes {
		if entry.Position < firstTransient && entry.Position > pushed {
			pushed = entry.Position
		}
	}
	return pushed
}

func rejectionFromApplyError(entry Entry, err error) (PushRejection, bool) {
	var offsetErr *ClockOffsetError
	if !errors.As(err, &offsetErr) {
		return PushRejection{}, false
	}
	return PushRejection{
		Key:         entry.Key,
		Reason:      rejectionReasonClockOffset,
		Transient:   true,
		OffsetMS:    offsetErr.OffsetMS,
		MaxOffsetMS: offsetErr.MaxClockOffset,
	}, true
}

func positionForKey(entries []Entry, key string) (uint64, bool) {
	for _, entry := range entries {
		if entry.Key == key {
			return entry.Position, true
		}
	}
	return 0, false
}

func maxPosition(entries []Entry) uint64 {
	var max uint64
	for _, entry := range entries {
		if entry.Position > max {
			max = entry.Position
		}
	}
	return max
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// SnapshotSyncer is implemented by syncers that can page through a peer's full
// state with GC horizon metadata. Snapshot resync requires it.
type SnapshotSyncer interface {
	PullSnapshot(ctx context.Context, addr string, req PullRequest) (*PullResponse, error)
}

// resyncPageSize bounds the entry count of one snapshot page. The byte size of a
// page is bounded separately by PullRequest.MaxBytes, which HTTPSyncer sets to
// its response limit.
const resyncPageSize = 256

// CollectTombstones removes tombstones that are older than the store retention
// and acknowledged by every peer the PeerProvider currently returns. The
// PeerProvider must return the configured membership, not the currently live
// peers. An empty peer list is refused with ErrTombstoneGCNoPeers instead of
// falling back to retention alone.
func (g *GossipEngine) CollectTombstones(ctx context.Context) (TombstoneGCResult, error) {
	gs, ok := g.store.(gcStore)
	if !ok {
		return TombstoneGCResult{}, ErrTombstoneGCUnsupported
	}
	peers := g.peerProvider.Peers()
	if len(peers) == 0 {
		return TombstoneGCResult{}, ErrTombstoneGCNoPeers
	}
	var safe uint64
	for index, addr := range peers {
		position, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			return TombstoneGCResult{}, err
		}
		if index == 0 || position < safe {
			safe = position
		}
	}
	return gs.collectTombstones(ctx, safe, true)
}

// maintain runs opt-in GC and compaction from Run.
func (g *GossipEngine) maintain(ctx context.Context) {
	if gs, ok := g.store.(gcStore); ok && gs.tombstoneGCEnabled() {
		if _, err := g.CollectTombstones(ctx); err != nil {
			slog.Warn("state gossip: tombstone collection failed", "err", err)
		}
	}
	if compactor, ok := g.store.(interface {
		CompactIfNeeded(context.Context) (bool, error)
	}); ok {
		if _, err := compactor.CompactIfNeeded(ctx); err != nil {
			slog.Warn("state gossip: durable log compaction failed", "err", err)
		}
	}
}

// resyncFrom pages through the peer's full state, applying each page through
// the store's normal Apply as it arrives, and then lets the store purge what the
// peer refuses and adopt the peer horizon. Only the set of snapshot keys is kept
// across pages, not the entries.
func (g *GossipEngine) resyncFrom(ctx context.Context, addr string, knownHorizon Timestamp) error {
	shown := redactedPeerURL(addr)
	gs, ok := g.store.(gcStore)
	if !ok {
		return fmt.Errorf("peer %s requires snapshot resync: %w", shown, ErrTombstoneGCUnsupported)
	}
	snapshotter, ok := g.syncer.(SnapshotSyncer)
	if !ok {
		return fmt.Errorf("peer %s requires snapshot resync, syncer cannot pull snapshots: %w", shown, ErrTombstoneGCUnsupported)
	}
	var (
		held             = make(map[string]struct{})
		since            uint64
		horizon          Timestamp
		applied, refused int
	)
	for page := 0; ; page++ {
		resp, err := snapshotter.PullSnapshot(ctx, addr, PullRequest{
			ProtocolVersion: StateProtocolVersion,
			PolicyID:        g.policy.ID(),
			SourceNodeID:    g.nodeID,
			GCHorizon:       g.localHorizon(),
			SincePosition:   since,
			Limit:           resyncPageSize,
		})
		if err != nil {
			return fmt.Errorf("pull resync snapshot page %d from %s: %w", page, shown, err)
		}
		if page == 0 {
			// The first page's horizon was read before any snapshot entry, so it is
			// covered by everything the peer returns afterwards.
			horizon = timestampValue(resp.GCHorizon)
			if horizon.Compare(knownHorizon) < 0 {
				return fmt.Errorf("resync snapshot from %s reports horizon %+v below the refused horizon %+v", shown, horizon, knownHorizon)
			}
		}
		for _, entry := range resp.Entries {
			held[entry.Key] = struct{}{}
		}
		// The page is merged like Apply and committed as one batch. Values for
		// keys we deleted and collected are refused (counted): the peer still
		// holds them and is cleaned up by its own resync.
		merged, err := gs.applySnapshotPage(ctx, resp.Entries)
		if err != nil {
			return fmt.Errorf("apply resync snapshot page %d from %s: %w", page, shown, err)
		}
		applied += merged.Applied
		refused += merged.Refused
		if !resp.More {
			break
		}
		if resp.NextSincePosition <= since {
			return fmt.Errorf("resync snapshot from %s did not advance past position %d", shown, since)
		}
		since = resp.NextSincePosition
	}
	result, err := gs.finishResync(ctx, held, horizon)
	if err != nil {
		return fmt.Errorf("resync from %s: %w", shown, err)
	}
	slog.Warn("state gossip: resynced from peer snapshot",
		"peer", shown,
		"applied", applied,
		"refused", refused,
		"purged", result.Purged,
		"horizonWallMS", result.Horizon.WallMS,
	)
	return nil
}

func (g *GossipEngine) storeHorizon() Timestamp {
	if gs, ok := g.store.(gcStore); ok {
		return gs.GCHorizon()
	}
	return Timestamp{}
}

func (g *GossipEngine) localHorizon() *Timestamp {
	return timestampPtr(g.storeHorizon())
}

// checkRequesterHorizon refuses an incremental exchange from a requester whose
// GC horizon is lower than ours unless we can vouch that it received every
// tombstone we removed. A lower horizon alone only means the requester has not
// collected yet; see docs/state-tombstone-gc.md "Requester horizon check".
func (g *GossipEngine) checkRequesterHorizon(ctx context.Context, local Timestamp, requester *Timestamp, requesterID string) error {
	requesterHorizon := timestampValue(requester)
	if local.isZero() || requesterHorizon.Compare(local) >= 0 {
		return nil
	}
	vouched, err := g.requesterVouched(ctx, requesterHorizon, requesterID)
	if err != nil {
		return err
	}
	if vouched {
		return nil
	}
	return &ResyncRequiredError{Peer: g.nodeID, Horizon: local}
}

// requesterVouched reports whether the requester acknowledged every tombstone
// this store removed itself (our push progress towards the address that
// answered as requesterID reaches the GC floor) and its horizon covers every
// horizon this store adopted from a peer, whose removals we cannot vouch for.
// A requester with horizon zero is never vouched once we removed anything: it
// has not collected or adopted anything yet, which is also what a node whose
// directory was wiped, and that kept its node ID and address, looks like; the
// push progress it inherits was acknowledged by the state that is gone.
func (g *GossipEngine) requesterVouched(ctx context.Context, requesterHorizon Timestamp, requesterID string) (bool, error) {
	gs, ok := g.store.(gcStore)
	if !ok || requesterID == "" {
		return false, nil
	}
	if requesterHorizon.Compare(gs.adoptedHorizon()) < 0 {
		return false, nil
	}
	floor := gs.GCFloor()
	if requesterHorizon.isZero() && floor > 0 {
		return false, nil
	}
	for _, addr := range g.peersAnsweringAs(requesterID) {
		acknowledged, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			return false, err
		}
		if acknowledged >= floor {
			return true, nil
		}
	}
	return false, nil
}

// peersAnsweringAs returns the peer addresses whose acknowledged pushes were
// answered by nodeID, from the durable record when the store keeps one and from
// this process otherwise.
func (g *GossipEngine) peersAnsweringAs(nodeID string) []string {
	var peers []string
	if acks, ok := g.store.(peerAckStore); ok {
		peers = acks.peersAnsweringAs(nodeID)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for addr, state := range g.peerStates {
		if state.nodeID == nodeID {
			peers = append(peers, addr)
		}
	}
	return peers
}

// checkRequesterHoldsCollected refuses a digest pull whose digest holds a live
// value for a key we no longer track at or below our horizon. That is the
// digest-side counterpart of a beyond_gc_horizon push rejection: horizons can be
// equal while the requester still holds a value deleted and collected here (its
// push cursor already passed it), and only its resync from us removes it.
func (g *GossipEngine) checkRequesterHoldsCollected(ctx context.Context, local Timestamp, digest map[string]DigestRecord) error {
	if local.isZero() {
		return nil
	}
	for key, record := range digest {
		if record.Deleted || record.Version.HLC.Compare(local) > 0 {
			continue
		}
		_, found, err := g.store.Get(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			return &ResyncRequiredError{Peer: g.nodeID, Horizon: local}
		}
	}
	return nil
}

func hasBeyondHorizonRejection(resp *PushResponse) bool {
	if resp == nil {
		return false
	}
	for _, rejection := range resp.Rejections {
		if rejection.Reason == RejectionReasonBeyondGCHorizon {
			return true
		}
	}
	return false
}
