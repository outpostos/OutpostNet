package state

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	failures         int
	nextAttempt      time.Time
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
			slog.Debug("state gossip: peer in backoff", "peer", addr, "until", until)
			continue
		}
		since, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			errs = append(errs, err)
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
					g.recordPeerFailure(addr, "push-resync", err, now)
					errs = append(errs, err)
					continue
				}
				g.recordPeerSuccess(addr)
				continue
			}
			g.recordPeerFailure(addr, "push", err, now)
			errs = append(errs, err)
			continue
		}
		pushed := g.pushSuccessPosition(addr, since, changes, resp)
		if err := g.recordPushSuccess(ctx, addr, pushed); err != nil {
			g.recordPeerFailure(addr, "push-progress", err, now)
			errs = append(errs, err)
			continue
		}
		if hasBeyondHorizonRejection(resp) {
			// The peer refused entries for keys it collected. We hold stale state for
			// them, so we resync from the peer, which purges those entries locally.
			if err := g.resyncFrom(ctx, addr, timestampValue(resp.GCHorizon)); err != nil {
				g.recordPeerFailure(addr, "push-resync", err, now)
				errs = append(errs, err)
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
			slog.Debug("state gossip: peer in backoff", "peer", addr, "until", until)
			continue
		}
		digest, err := g.LocalDigest(ctx)
		if err != nil {
			return err
		}
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
					g.recordPeerFailure(addr, "pull-resync", err, now)
					errs = append(errs, err)
					continue
				}
				g.recordPeerSuccess(addr)
				continue
			}
			g.recordPeerFailure(addr, "pull", err, now)
			errs = append(errs, err)
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
				applyErrs = append(applyErrs, fmt.Errorf("apply pulled entry from %s key %q: %w", addr, entry.Key, err))
			}
		}
		if beyondHorizon > 0 {
			slog.Warn("state gossip: refused pulled entries below the local GC horizon; peer must resync",
				"peer", addr,
				"refused", beyondHorizon,
			)
		}
		if err := errors.Join(applyErrs...); err != nil {
			g.recordPeerFailure(addr, "pull-apply", err, now)
			errs = append(errs, err)
			continue
		}
		g.recordPeerSuccess(addr)
	}
	return errors.Join(errs...)
}

// HandlePush applies entries from a peer.
func (g *GossipEngine) HandlePush(ctx context.Context, req PushRequest) (*PushResponse, error) {
	if err := validatePolicyID(g.policy, req.PolicyID); err != nil {
		return nil, err
	}
	horizon := g.storeHorizon()
	if err := g.checkRequesterHorizon(horizon, req.GCHorizon); err != nil {
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
// digest pull from a requester whose GC horizon is lower than ours, or whose
// digest still holds a value we collected, is refused with ResyncRequiredError. Position pulls (no digest) are always served: they
// are how a lagging requester pages through our snapshot during resync.
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
		if err := g.checkRequesterHorizon(horizon, req.GCHorizon); err != nil {
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
	if req.Limit > 0 && len(entries) == req.Limit {
		resp.More = true
		resp.NextSincePosition = entries[len(entries)-1].Position
	}
	return resp, nil
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

func (g *GossipEngine) recordPushSuccess(ctx context.Context, addr string, pushed uint64) error {
	if progress, ok := g.store.(PeerProgressStore); ok {
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
	state.failures = 0
	state.nextAttempt = time.Time{}
	return nil
}

func (g *GossipEngine) recordPeerSuccess(addr string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures = 0
	state.nextAttempt = time.Time{}
}

func (g *GossipEngine) recordPeerFailure(addr, operation string, err error, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures++
	backoff := saturatedBackoff(g.backoffBase, g.backoffMax, state.failures)
	state.nextAttempt = now.Add(backoff)
	slog.Warn("state gossip: peer sync failed", "peer", addr, "operation", operation, "err", err, "retryAt", state.nextAttempt)
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
			"peer", peer,
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

// resyncPageSize bounds one snapshot page so it fits the default response size.
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

// resyncFrom pages through the peer's full state and lets the store merge it,
// purge what the peer refuses and adopt the peer horizon.
func (g *GossipEngine) resyncFrom(ctx context.Context, addr string, knownHorizon Timestamp) error {
	gs, ok := g.store.(gcStore)
	if !ok {
		return fmt.Errorf("peer %s requires snapshot resync: %w", addr, ErrTombstoneGCUnsupported)
	}
	snapshotter, ok := g.syncer.(SnapshotSyncer)
	if !ok {
		return fmt.Errorf("peer %s requires snapshot resync, syncer cannot pull snapshots: %w", addr, ErrTombstoneGCUnsupported)
	}
	var (
		snapshot []Entry
		since    uint64
		horizon  Timestamp
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
			return fmt.Errorf("pull resync snapshot page %d from %s: %w", page, addr, err)
		}
		if page == 0 {
			// The first page's horizon was read before any snapshot entry, so it is
			// covered by everything the peer returns afterwards.
			horizon = timestampValue(resp.GCHorizon)
		}
		snapshot = append(snapshot, resp.Entries...)
		if !resp.More {
			break
		}
		if resp.NextSincePosition <= since {
			return fmt.Errorf("resync snapshot from %s did not advance past position %d", addr, since)
		}
		since = resp.NextSincePosition
	}
	if horizon.Compare(knownHorizon) < 0 {
		return fmt.Errorf("resync snapshot from %s reports horizon %+v below the refused horizon %+v", addr, horizon, knownHorizon)
	}
	result, err := gs.resyncFromSnapshot(ctx, snapshot, horizon)
	if err != nil {
		return fmt.Errorf("resync from %s: %w", addr, err)
	}
	slog.Warn("state gossip: resynced from peer snapshot",
		"peer", addr,
		"applied", result.Applied,
		"refused", result.Refused,
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
// GC horizon is lower than ours: it may hold or miss state we collected.
func (g *GossipEngine) checkRequesterHorizon(local Timestamp, requester *Timestamp) error {
	if local.isZero() || timestampValue(requester).Compare(local) >= 0 {
		return nil
	}
	return &ResyncRequiredError{Peer: g.nodeID, Horizon: local}
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
