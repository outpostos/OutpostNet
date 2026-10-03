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
	// OutboundCaughtUp holds when AcknowledgedPosition covers the local
	// position read at the start of ReplicationStatus.
	OutboundCaughtUp bool
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
			OutboundCaughtUp:     acknowledged >= status.Local.CurrentPosition,
			LastPullTime:         state.lastPull,
			InboundCaughtUp:      !state.lastPull.IsZero() && (state.lastErrorTime.IsZero() || state.lastErrorTime.Before(state.lastPull)),
			LastSuccessTime:      state.lastSuccess,
			LastError:            state.lastError,
			LastErrorTime:        state.lastErrorTime,
			LastErrorOperation:   state.lastErrorOp,
			ConsecutiveFailures:  state.failures,
			NextAttemptTime:      state.nextAttempt,
		}
		peer.CaughtUp = peer.OutboundCaughtUp && peer.InboundCaughtUp
		status.Peers = append(status.Peers, peer)
	}
	return status, nil
}
