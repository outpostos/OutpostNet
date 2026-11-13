package state

import (
	"context"
	"fmt"
	"time"
)

// PeerReplicationStatus is this engine's read-only view of its exchanges with
// one peer. It is a local fact: it says what this node pushed, what the peer
// acknowledged and what this node pulled, never what the peer itself holds.
// Zero times mean "not in this process" (the engine does not persist them).
type PeerReplicationStatus struct {
	// Peer is the address from the PeerProvider with any password redacted.
	Peer string
	// AcknowledgedPosition is the highest local position the peer
	// acknowledged: the PeerProgressStore value when the store persists
	// progress (so it survives restarts), otherwise the engine's cursor.
	AcknowledgedPosition uint64
	// OversizedPushEntries counts attempted sends of entries exceeding the
	// store MaxEntryBytes. Includes retries; resets when this engine restarts.
	OversizedPushEntries uint64
	// OutboundCaughtUp holds when AcknowledgedPosition covers the local
	// position read at the start of ReplicationStatus.
	OutboundCaughtUp bool
	// OutboundRoundCaughtUp is the push-round view of outbound progress, which
	// tolerates writes that keep arriving. It holds when OutboundCaughtUp holds,
	// or when the latest push round to the peer ended with the peer
	// acknowledging every change up to the local position read when that round
	// started (a round with nothing to push counts), and no exchange with the
	// peer failed after that round started. The local position also grows with
	// every applied peer entry, so under sustained writes OutboundCaughtUp is
	// structurally false while this stays true; a peer that cannot be reached
	// fails its rounds and clears it. GC gating never uses it.
	OutboundRoundCaughtUp bool
	// OutboundRoundTime is when the latest push round that ended caught up
	// started, kept after later rounds fail; zero when no round has been caught
	// up in this process. Its age tells consumers how fresh the claim is (rounds
	// stop when the engine is not running) and, while OutboundRoundCaughtUp is
	// false, since when the peer has been behind.
	OutboundRoundTime time.Time
	// LastPullTime is when the latest fully applied pull from the peer
	// started. Rounds skipped because the peer is in backoff are not pulls.
	LastPullTime time.Time
	// InboundCaughtUp holds when a pull has been fully applied and no
	// exchange with the peer failed after that pull started.
	InboundCaughtUp bool
	// CaughtUp is OutboundCaughtUp && InboundCaughtUp.
	CaughtUp bool
	// LastSuccessTime is when the latest acknowledged push, fully applied
	// pull or snapshot resync with the peer completed.
	LastSuccessTime time.Time
	// LastError is the latest failed exchange, kept after later successes;
	// compare LastErrorTime with LastPullTime/LastSuccessTime. Credentials
	// are redacted.
	LastError string
	// LastErrorTime is when LastError was recorded.
	LastErrorTime time.Time
	// LastErrorOperation names the failed step: push, push-progress,
	// push-resync, pull, pull-apply or pull-resync.
	LastErrorOperation string
	// ConsecutiveFailures counts failures since the last success.
	ConsecutiveFailures int
	// NextAttemptTime is the end of the current backoff; zero when the peer
	// is not in backoff.
	NextAttemptTime time.Time
}

// ReplicationStatus is the engine's local replica status plus per-peer
// exchange results. It never claims network-wide consistency.
type ReplicationStatus struct {
	Local LocalStatus
	Peers []PeerReplicationStatus
}

// ReplicationStatus returns a read-only snapshot for every peer the
// PeerProvider currently returns, in that order. It is safe to call
// concurrently with SyncOnce, AntiEntropyOnce and Run. The local position is
// read before peer progress, so a change committed in between can only make
// OutboundCaughtUp more conservative.
func (g *GossipEngine) ReplicationStatus(ctx context.Context) (ReplicationStatus, error) {
	status := ReplicationStatus{Local: g.store.LocalStatus()}
	peers := g.peerProvider.Peers()
	status.Peers = make([]PeerReplicationStatus, 0, len(peers))
	for _, addr := range peers {
		acknowledged, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			return ReplicationStatus{}, fmt.Errorf("read replication progress for %s: %w", redactedPeerURL(addr), err)
		}
		g.mu.Lock()
		state := *g.peerStateLocked(addr)
		g.mu.Unlock()

		peer := PeerReplicationStatus{
			Peer:                 redactedPeerURL(addr),
			AcknowledgedPosition: acknowledged,
			OversizedPushEntries: state.oversizedPushEntries,
			OutboundCaughtUp:     acknowledged >= status.Local.CurrentPosition,
			LastPullTime:         state.lastPull,
			InboundCaughtUp:      !state.lastPull.IsZero() && (state.lastErrorTime.IsZero() || state.lastErrorTime.Before(state.lastPull)),
			LastSuccessTime:      state.lastSuccess,
			LastError:            state.lastError,
			LastErrorTime:        state.lastErrorTime,
			LastErrorOperation:   state.lastErrorOp,
			ConsecutiveFailures:  state.failures,
			NextAttemptTime:      state.nextAttempt,
			OutboundRoundTime:    state.lastCaughtUpRound,
		}
		roundCaughtUp := state.lastRoundCaughtUp && (state.lastErrorTime.IsZero() || state.lastErrorTime.Before(state.lastRound))
		peer.OutboundRoundCaughtUp = peer.OutboundCaughtUp || roundCaughtUp
		peer.CaughtUp = peer.OutboundCaughtUp && peer.InboundCaughtUp
		status.Peers = append(status.Peers, peer)
	}
	return status, nil
}
