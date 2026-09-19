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
	Store               *MemoryStore
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
	store               *MemoryStore
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
		since := g.peerLastPushPosition(addr)
		changes, err := g.store.ChangesSince(ctx, since, 0)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			continue
		}
		resp, err := g.syncer.Push(ctx, addr, PushRequest{
			ProtocolVersion: StateProtocolVersion,
			SourceNodeID:    g.nodeID,
			Entries:         changes,
		})
		if err != nil {
			g.recordPeerFailure(addr, "push", err, now)
			errs = append(errs, err)
			continue
		}
		g.recordPushSuccess(addr, g.pushSuccessPosition(addr, since, changes, resp))
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
			SourceNodeID:    g.nodeID,
			Digest:          digest,
		})
		if err != nil {
			g.recordPeerFailure(addr, "pull", err, now)
			errs = append(errs, err)
			continue
		}
		g.recordPeerSuccess(addr)
		for _, entry := range entries {
			if _, err := g.store.Apply(ctx, entry); err != nil {
				slog.Error("state gossip: apply pulled entry", "peer", addr, "key", entry.Key, "err", err)
			}
		}
	}
	return errors.Join(errs...)
}

// HandlePush applies entries from a peer.
func (g *GossipEngine) HandlePush(ctx context.Context, req PushRequest) (*PushResponse, error) {
	var accepted, rejected int
	var rejections []PushRejection
	for _, entry := range req.Entries {
		applied, err := g.store.Apply(ctx, entry)
		if err != nil {
			rejected++
			if rejection, ok := rejectionFromApplyError(entry, err); ok {
				rejections = append(rejections, rejection)
			}
			continue
		}
		if applied {
			accepted++
		} else {
			rejected++
		}
	}
	return &PushResponse{
		ProtocolVersion: StateProtocolVersion,
		Accepted:        accepted,
		Rejected:        rejected,
		Rejections:      rejections,
	}, nil
}

// HandlePull returns entries newer than the caller's cursor or digest.
func (g *GossipEngine) HandlePull(ctx context.Context, req PullRequest) ([]Entry, error) {
	if len(req.Digest) > 0 {
		return g.diffAgainstDigest(ctx, req.Digest, req.Limit)
	}
	return g.store.ChangesSince(ctx, req.SincePosition, req.Limit)
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

func (g *GossipEngine) peerLastPushPosition(addr string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peerStateLocked(addr).lastPushPosition
}

func (g *GossipEngine) recordPushSuccess(addr string, pushed uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	if pushed > state.lastPushPosition {
		state.lastPushPosition = pushed
	}
	state.failures = 0
	state.nextAttempt = time.Time{}
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
	backoff := g.backoffBase << (state.failures - 1)
	if backoff > g.backoffMax {
		backoff = g.backoffMax
	}
	state.nextAttempt = now.Add(backoff)
	slog.Warn("state gossip: peer sync failed", "peer", addr, "operation", operation, "err", err, "retryAt", state.nextAttempt)
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
