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
// Timestamps use GossipConfig.Clock; catch-up ordering and backoff use local
// monotonic time even if that clock steps backwards or stays frozen.
type PeerReplicationStatus struct {
	// UnreachableFor is monotonic elapsed outage time measured at the latest
	// failure or warning transition; cleared on success. Reads do not advance it.
	UnreachableFor             time.Duration
	ResyncLossLimitApproaching bool
	// ResyncLossWarnings counts outage warning transitions in this engine process.
	ResyncLossWarnings uint64
	// Peer is the address from the PeerProvider with any password redacted.
	Peer string
	// AcknowledgedPosition is the push cursor, which may pass refused entries.
	// RefusedPositions and GCConfirmedPosition distinguish acceptance from cursor
	// progress. Built-in durable stores persist the cursor with the refusal set;
	// custom progress stores may checkpoint only the safe prefix.
	AcknowledgedPosition uint64
	// RefusedPositions counts outstanding per-entry refusals.
	RefusedPositions int
	// LowestRefusedPosition is zero when none are outstanding.
	LowestRefusedPosition uint64
	// GCConfirmedPosition is the inclusive non-refused prefix used by GC/vouching.
	GCConfirmedPosition uint64
	// OversizedPushEntries counts attempted sends of entries exceeding the
	// store MaxEntryBytes. Includes retries; resets when this engine restarts.
	OversizedPushEntries uint64
	// OutboundCaughtUp holds when there are no unresolved refusals and
	// AcknowledgedPosition covers the local position read at the start of
	// ReplicationStatus.
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
	// LastError is the latest failed exchange, kept after later successes.
	// Wall timestamps can step; catch-up flags use monotonic event ordering.
	// Credentials are redacted.
	LastError string
	// LastErrorTime is when LastError was recorded.
	LastErrorTime time.Time
	// LastErrorOperation names the failed step: push, push-progress,
	// push-resync, pull, pull-apply or pull-resync.
	LastErrorOperation string
	// ConsecutiveFailures counts failures since the last success.
	ConsecutiveFailures int
	// NextAttemptTime is the end of the current backoff; zero when the peer
	// is not in backoff. This is the monotonic deadline translated to the status
	// clock's time base when the failure was recorded; clock changes do not
	// reschedule retries.
	NextAttemptTime time.Time
}

// ReplicationStatus is the engine's local replica status plus per-peer
// exchange results. It never claims network-wide consistency.
type ReplicationStatus struct {
	Local LocalStatus
	// Clocks contains local writer/peer clock_suspect observations.
	Clocks []NodeClockStatus
	Peers  []PeerReplicationStatus
}

// ReplicationStatus returns a read-only snapshot for every peer the
// PeerProvider currently returns, in that order. It is safe to call
// concurrently with SyncOnce, AntiEntropyOnce and Run. The local position is
// read before peer progress, so a change committed in between can only make
// OutboundCaughtUp more conservative.
func (g *GossipEngine) ReplicationStatus(ctx context.Context) (ReplicationStatus, error) {
	status := ReplicationStatus{Local: g.store.LocalStatus(), Clocks: g.ClockStatus()}
	peers := g.peerProvider.Peers()
	status.Peers = make([]PeerReplicationStatus, 0, len(peers))
	for _, addr := range peers {
		g.progressMu.Lock()
		acknowledged, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			g.progressMu.Unlock()
			return ReplicationStatus{}, fmt.Errorf("read replication progress for %s: %w", redactedPeerURL(addr), err)
		}
		refused, err := g.peerRefusals(ctx, addr)
		g.progressMu.Unlock()
		if err != nil {
			return ReplicationStatus{}, err
		}
		var lowest uint64
		for _, r := range refused {
			if lowest == 0 || r.Position < lowest {
				lowest = r.Position
			}
		}
		g.mu.Lock()
		state := *g.peerStateLocked(addr)
		g.mu.Unlock()

		peer := PeerReplicationStatus{
			Peer:                       redactedPeerURL(addr),
			ResyncLossLimitApproaching: state.resyncLossLimitApproaching,
			ResyncLossWarnings:         state.resyncLossWarnings,
			UnreachableFor:             state.unreachableFor,
			AcknowledgedPosition:       acknowledged,
			RefusedPositions:           len(refused),
			LowestRefusedPosition:      lowest,
			GCConfirmedPosition:        nonRefusedPosition(acknowledged, refused),
			OversizedPushEntries:       state.oversizedPushEntries,
			OutboundCaughtUp:           len(refused) == 0 && acknowledged >= status.Local.CurrentPosition,
			LastPullTime:               state.lastPull.wall,
			InboundCaughtUp:            !state.lastPull.monotonic.IsZero() && (state.lastErrorTime.monotonic.IsZero() || state.lastErrorTime.monotonic.Before(state.lastPull.monotonic)),
			LastSuccessTime:            state.lastSuccess,
			LastError:                  state.lastError,
			LastErrorTime:              state.lastErrorTime.wall,
			LastErrorOperation:         state.lastErrorOp,
			ConsecutiveFailures:        state.failures,
			NextAttemptTime:            state.nextAttemptTime,
			OutboundRoundTime:          state.lastCaughtUpRound.wall,
		}

		roundCaughtUp := state.lastRoundCaughtUp && (state.lastErrorTime.monotonic.IsZero() || state.lastErrorTime.monotonic.Before(state.lastRound.monotonic))
		peer.OutboundRoundCaughtUp = len(refused) == 0 && (peer.OutboundCaughtUp || roundCaughtUp)
		peer.CaughtUp = peer.OutboundCaughtUp && peer.InboundCaughtUp
		status.Peers = append(status.Peers, peer)
	}
	return status, nil
}
