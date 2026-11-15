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
	// Clock supplies emitted replication status timestamps (default time.Now).
	// It must support concurrent calls; hosts can use TimeReference.Now.
	// Scheduling, backoff, RTT and event ordering always use local monotonic time.
	Clock func() time.Time
	// MaxPushBytes bounds the encoded push request (default 768 KiB).
	MaxPushBytes int64
	// MaxDigestBytes bounds each encoded digest request (default 768 KiB).
	MaxDigestBytes int64
	// MaxPushPagesPerRound bounds work per peer (default 16).
	MaxPushPagesPerRound int
}

// GossipEngine exchanges state with peers through a Syncer.
//
// SyncOnce and AntiEntropyOnce may run concurrently with each other, the
// handlers (HandlePush, HandlePull and HandlePullResponse), and ReplicationStatus.
// Neither SyncOnce nor AntiEntropyOnce is reentrant: callers must serialize
// calls to the same method. Run owns both round loops; do not call either
// method or another Run while it is running. Injected stores, syncers, peer
// providers and policy/clock callbacks must support concurrent use.
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
	clock               func() time.Time

	maxDigestBytes       int64
	maxPushBytes         int64
	maxPushPagesPerRound int

	progressMu  sync.Mutex // serializes acknowledgement/refusal read-modify-write
	mu          sync.Mutex
	peerStates  map[string]*peerState
	clockPeers  map[string]NodeClockStatus
	timePeerIDs map[string]string // exchange address -> responding node identity
}

type peerState struct {
	refused              []refusedPosition
	lastPushPosition     uint64
	oversizedPushEntries uint64
	// nodeID is the node that answered the latest acknowledged push here.
	nodeID          string
	failures        int
	nextAttempt     time.Time
	nextAttemptTime time.Time // emitted deadline, in the status clock's time base

	// Exchange results reported by ReplicationStatus. Event ordering uses
	// monotonic time independently of emitted wall times; zero means no event.
	lastSuccess   time.Time  // latest acknowledged push or fully applied pull
	lastPull      gossipTime // start of the latest fully applied pull
	lastError     string     // redacted text of the latest failed exchange
	lastErrorTime gossipTime
	lastErrorOp   string
	// Push rounds: the start of the latest round not skipped by backoff,
	// whether it ended caught up, and the start of the latest round that did.
	lastRound         gossipTime
	lastRoundCaughtUp bool
	lastCaughtUpRound gossipTime
}

// gossipTime keeps the event's local ordering separate from its reported time.
// A reference may step backwards or stay frozen without changing catch-up facts.
type gossipTime struct {
	monotonic time.Time
	wall      time.Time
}

func (g *GossipEngine) timestamp() gossipTime {
	return gossipTime{monotonic: time.Now(), wall: g.clock()}
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
	if cfg.Clock == nil {
		cfg.Clock = time.Now
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
	maxPushBytes := cfg.MaxPushBytes
	if maxPushBytes == 0 {
		maxPushBytes = 768 << 10
	}
	maxPages := cfg.MaxPushPagesPerRound
	if maxPages == 0 {
		maxPages = 16
	}
	if maxPushBytes < 0 || maxPages < 0 {
		return nil, fmt.Errorf("gossip push limits must be positive")
	}
	// Reserve the largest horizon encoding, even when GC is currently off.
	envelope, err := json.Marshal(PushRequest{TimeSample: TimeSample{SentPhysicalMS: math.MinInt64, TimeQuality: TimeUnsynced}, ProtocolVersion: StateProtocolVersion, PolicyID: storePolicy.ID(), SourceNodeID: cfg.NodeID, GCHorizon: &Timestamp{WallMS: math.MinInt64, Logical: math.MaxUint16}, Entries: []Entry{}})
	if err != nil {
		return nil, fmt.Errorf("encode push envelope: %w", err)
	}
	if maxPushBytes < entryLimit(cfg.Store) || maxPushBytes-entryLimit(cfg.Store) < int64(len(envelope)) {
		return nil, fmt.Errorf("gossip MaxPushBytes must cover MaxEntryBytes (%d) plus push envelope (%d)", entryLimit(cfg.Store), len(envelope))
	}
	requestLimit := defaultMaxRequestBytes
	if limits, ok := cfg.Syncer.(interface{ MaxRequestBytes() int64 }); ok {
		requestLimit = limits.MaxRequestBytes()
	}
	if err := validatePushRequestLimit(maxPushBytes, requestLimit); err != nil {
		return nil, err
	}
	maxDigestBytes := cfg.MaxDigestBytes
	if maxDigestBytes == 0 {
		maxDigestBytes = 768 << 10
	}
	if maxDigestBytes <= 0 || maxDigestBytes >= requestLimit {
		return nil, fmt.Errorf("gossip MaxDigestBytes must be positive and below the HTTP request limit (%d)", requestLimit)
	}
	if gs, ok := cfg.Store.(gcStore); ok {
		// From now on tombstones may only be collected through the engine, which
		// knows the peers that must acknowledge them first.
		gs.markReplicated()
	}
	return &GossipEngine{
		nodeID:               cfg.NodeID,
		store:                cfg.Store,
		syncer:               cfg.Syncer,
		peerProvider:         cfg.PeerProvider,
		policy:               storePolicy,
		interval:             interval,
		antiEntropyInterval:  antiEntropyInterval,
		backoffBase:          backoffBase,
		backoffMax:           backoffMax,
		clock:                cfg.Clock,
		peerStates:           make(map[string]*peerState),
		maxPushBytes:         maxPushBytes,
		maxDigestBytes:       maxDigestBytes,
		maxPushPagesPerRound: maxPages,
	}, nil
}

// Run runs independent periodic push and pull loops until ctx is canceled.
// A slow digest round does not block pushes. Maintenance follows pull rounds.
// Run waits for both loops to finish before returning; Syncer calls must honor ctx.
func (g *GossipEngine) Run(ctx context.Context) error {
	pushTicker := time.NewTicker(g.interval)
	defer pushTicker.Stop()
	antiEntropyTicker := time.NewTicker(g.antiEntropyInterval)
	defer antiEntropyTicker.Stop()
	pushDone := make(chan struct{})
	go func() {
		defer close(pushDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-pushTicker.C:
				_ = g.SyncOnce(ctx)
			}
		}
	}()
	defer func() { <-pushDone }()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-antiEntropyTicker.C:
			_ = g.AntiEntropyOnce(ctx)
			g.maintain(ctx)
		}
	}
}

// SyncOnce performs one incremental push round.
// It may overlap AntiEntropyOnce and handlers, but must not overlap another
// SyncOnce call (including Run's push loop).
func (g *GossipEngine) SyncOnce(ctx context.Context) error {
	var errs []error
	now := time.Now()
	for _, addr := range g.peerProvider.Peers() {
		if ok, until := g.allowAttempt(addr, now); !ok {
			slog.Debug("state gossip: peer in backoff", "peer", redactedPeerURL(addr), "until", until)
			continue
		}
		// The round can only claim the local position read before it collects
		// its changes; anything written later belongs to the next round.
		started := g.timestamp()
		startPosition := g.store.CurrentPosition()
		caughtUp, err := g.pushTo(ctx, addr, startPosition, now)
		if err == nil {
			refused, readErr := g.peerRefusals(ctx, addr)
			if readErr != nil {
				err = &localStoreError{err: readErr}
			}
			caughtUp = caughtUp && len(refused) == 0
		}
		g.recordPushRound(addr, started, caughtUp)
		if err != nil {
			var local *localStoreError
			if errors.As(err, &local) {
				return local.err
			}
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// localStoreError marks a failure of the local store, which aborts the whole
// push round instead of being charged to one peer.
type localStoreError struct{ err error }

func (e *localStoreError) Error() string { return e.err.Error() }

// pushTo runs one push round to addr. caughtUp reports whether the round ended
// with the peer acknowledging every local change up to startPosition; a round
// with nothing to push covers everything the store can replicate and counts.
func (g *GossipEngine) pushTo(ctx context.Context, addr string, startPosition uint64, now time.Time) (caughtUp bool, err error) {
	since, err := g.peerLastPushPosition(ctx, addr)
	if err != nil {
		return false, redactPeerErr(addr, err)
	}
	for page := 0; page < g.maxPushPagesPerRound; page++ {
		req := PushRequest{TimeSample: g.timeSample(), ProtocolVersion: StateProtocolVersion, PolicyID: g.policy.ID(), SourceNodeID: g.nodeID, GCHorizon: g.localHorizon(), Entries: []Entry{}}
		budget := req
		budget.TimeSample = TimeSample{SentPhysicalMS: math.MinInt64, TimeQuality: TimeUnsynced}
		envelope, err := json.Marshal(budget)
		if err != nil {
			return false, &localStoreError{err: err}
		}
		changes, err := pushPage(ctx, g.store, since, g.maxPushBytes-int64(len(envelope)))
		if err != nil {
			return false, &localStoreError{err: err}
		}
		if len(changes) == 0 {
			// Nothing past the acknowledged position. The local position can still be
			// ahead of it (a resync purge allocates a floor position with no entry),
			// but the peer already holds everything this store can push.
			return true, nil
		}
		for _, entry := range changes {
			encoded, err := json.Marshal(entry)
			if err != nil {
				return false, &localStoreError{err: err}
			}
			if int64(len(encoded)) > entryLimit(g.store) {
				g.mu.Lock()
				g.peerStateLocked(addr).oversizedPushEntries++
				g.mu.Unlock()
				slog.Warn("state gossip: pushing oversized legacy entry alone", "peer", redactedPeerURL(addr), "bytes", len(encoded), "maxEntryBytes", entryLimit(g.store))
			}
		}
		req.Entries = changes
		resp, err := g.pushExchange(ctx, addr, req)
		if err != nil {
			var resync *ResyncRequiredError
			if errors.As(err, &resync) {
				// The peer refused our increments because we lag behind its GC
				// horizon. Resync from it; the cursor stays put and the next round
				// pushes whatever survived the resync.
				if err := g.resyncFrom(ctx, addr, resync.Horizon); err != nil {
					return false, g.recordPeerFailure(addr, "push-resync", err, now)
				}
				g.recordPeerSuccess(addr)
				return false, nil
			}
			return false, g.recordPeerFailure(addr, "push", err, now)
		}
		if resp != nil {
			if err := g.checkPeerHorizon(addr, timestampValue(resp.GCHorizon)); err != nil {
				return false, g.recordPeerFailure(addr, "push", err, now)
			}
		}
		pushed := g.pushSuccessPosition(addr, since, changes, resp)
		if err := g.recordPushSuccess(ctx, addr, pushed, changes, resp); err != nil {
			return false, g.recordPeerFailure(addr, "push-progress", err, now)
		}
		if hasBeyondHorizonRejection(resp) {
			// The peer refused entries for keys it collected. We hold stale state for
			// them, so we resync from the peer, which purges those entries locally.
			if err := g.resyncFrom(ctx, addr, timestampValue(resp.GCHorizon)); err != nil {
				return false, g.recordPeerFailure(addr, "push-resync", err, now)
			}
		}
		if hasBeyondHorizonRejection(resp) || pushed < maxPosition(changes) {
			return false, nil
		}
		if pushed >= startPosition {
			return true, nil
		}
		since = pushed
	}
	return false, nil
}

// AntiEntropyOnce performs one digest pull round.
// It may overlap SyncOnce and handlers, but must not overlap another
// AntiEntropyOnce call (including Run's pull loop).
func (g *GossipEngine) AntiEntropyOnce(ctx context.Context) error {
	var errs []error
	now := time.Now()
	for _, addr := range g.peerProvider.Peers() {
		if ok, until := g.allowAttempt(addr, now); !ok {
			slog.Debug("state gossip: peer in backoff", "peer", redactedPeerURL(addr), "until", until)
			continue
		}
		// Each peer gets a fresh, range-complete digest; no cursor survives a round.
		digest, err := g.localDigestEntries(ctx)
		if err != nil {
			return err
		}
		started := g.timestamp()
		err = g.pullDigestRound(ctx, addr, digest)
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
		g.recordPullSuccess(addr, started)
	}
	return errors.Join(errs...)
}

// HandlePush applies entries from a peer.
func (g *GossipEngine) HandlePush(ctx context.Context, req PushRequest) (*PushResponse, error) {
	if err := validatePolicyID(g.policy, req.PolicyID); err != nil {
		return nil, err
	}
	if err := g.checkPeerHorizon(req.SourceNodeID, timestampValue(req.GCHorizon)); err != nil {
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
		TimeSample:      g.timeSample(),
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
	if err := g.checkPeerHorizon(req.SourceNodeID, timestampValue(req.GCHorizon)); err != nil {
		return nil, err
	}
	if err := validatePolicyID(g.policy, req.PolicyID); err != nil {
		return nil, err
	}
	// The horizon is read before the entries, so it never claims more than the
	// returned state reflects.
	horizon := g.storeHorizon()
	resp := &PullResponse{
		NodeID:          g.nodeID,
		TimeSample:      g.timeSample(),
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        g.policy.ID(),
		GCHorizon:       timestampPtr(horizon),
	}
	if req.StartKey != nil || len(req.Digest) > 0 {
		return g.handleDigestResponse(ctx, req, resp, horizon)
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

// LocalDigest returns the local summary using the engine policy. It lists every
// local entry. Tombstone GC safety depends on that: see
// docs/state-tombstone-gc.md "Invariant: each round covers all keys at every peer" before
// sampling, sharding or truncating it.
func (g *GossipEngine) LocalDigest(ctx context.Context) (map[string]DigestRecord, error) {
	entries, err := g.localDigestEntries(ctx)
	if err != nil {
		return nil, err
	}
	digest := make(map[string]DigestRecord, len(entries))
	for _, entry := range entries {
		digest[entry.Key] = entry.Record
	}
	return digest, nil
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
		g.mu.Lock()
		position = maxUint64(position, g.peerStateLocked(addr).lastPushPosition)
		g.mu.Unlock()
		return position, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peerStateLocked(addr).lastPushPosition, nil
}

func (g *GossipEngine) recordPushSuccess(ctx context.Context, addr string, pushed uint64, changes []Entry, resp *PushResponse) error {
	g.progressMu.Lock()
	defer g.progressMu.Unlock()
	refused, err := g.peerRefusals(ctx, addr)
	if err != nil {
		return err
	}
	refused = updateRefusals(refused, changes, resp)
	var nodeID string
	if resp != nil {
		nodeID = resp.NodeID
	}
	if err := g.savePushAck(ctx, addr, nodeID, pushed, refused); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures = 0
	state.nextAttempt = time.Time{}
	state.nextAttemptTime = time.Time{}
	state.lastSuccess = g.clock()
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
	state.nextAttemptTime = time.Time{}
	state.lastSuccess = g.clock()
}

func (g *GossipEngine) recordPullSuccess(addr string, started gossipTime) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.failures = 0
	state.nextAttempt = time.Time{}
	state.nextAttemptTime = time.Time{}
	state.lastSuccess = g.clock()
	state.lastPull = started
}

// recordPushRound records the outcome of a push round that started at started.
func (g *GossipEngine) recordPushRound(addr string, started gossipTime, caughtUp bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	state.lastRound = started
	state.lastRoundCaughtUp = caughtUp
	if caughtUp {
		state.lastCaughtUpRound = started
	}
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
	state.lastErrorTime = g.timestamp()
	// Preserve the round-start-anchored monotonic deadline. Only its emitted
	// representation is translated into the status clock's time base.
	state.nextAttemptTime = state.lastErrorTime.wall.Add(state.nextAttempt.Sub(state.lastErrorTime.monotonic))
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
		if !rejection.Transient || rejection.Reason == rejectionReasonClockOffset {
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
		Transient:   false,
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

// SnapshotSyncer returns complete pull response metadata for both digest diff
// pages and position snapshots. Anti-entropy and snapshot resync require it.
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
		position, err := g.peerSafePosition(ctx, addr)
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
	if err := g.checkPeerHorizon(addr, knownHorizon); err != nil {
		return err
	}
	gs, ok := g.store.(gcStore)
	if !ok {
		return fmt.Errorf("peer %s requires snapshot resync: %w", shown, ErrTombstoneGCUnsupported)
	}
	_, ok = g.syncer.(SnapshotSyncer)
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
		resp, err := g.pullExchange(ctx, addr, PullRequest{
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
		if err := g.checkPeerHorizon(addr, timestampValue(resp.GCHorizon)); err != nil {
			return err
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
		acknowledged, err := g.peerSafePosition(ctx, addr)
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
// Vouched requesters are checked too: this check, not vouching, is what cleans
// up a requester holding a stale value, and it is only as complete as the digest
// (docs/state-tombstone-gc.md "Invariant: each round covers all keys at every peer").
func (g *GossipEngine) checkRequesterHoldsCollected(ctx context.Context, local Timestamp, digest map[string]DigestRecord) error {
	if local.isZero() {
		return nil
	}
	for key, record := range digest {
		if record.Deleted || record.Version.HLC.Compare(local) > 0 {
			continue
		}
		var found bool
		var err error
		if reader, ok := g.store.(ReplicationReadStore); ok {
			found, err = reader.HasKey(ctx, key)
		} else {
			_, found, err = g.store.Get(ctx, key)
		}
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

func (g *GossipEngine) applyDigestPage(ctx context.Context, addr string, entries []Entry) error {
	var applyErrs []error
	var beyondHorizon int
	for _, entry := range entries {
		if _, err := g.store.Apply(ctx, entry); err != nil {
			var offset *ClockOffsetError
			if errors.As(err, &offset) {
				// A bad writer must not stop replication from its forwarders.
				continue
			}
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
		return err
	}
	return nil
}
