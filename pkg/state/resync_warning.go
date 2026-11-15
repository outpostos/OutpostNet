package state

import (
	"log/slog"
	"time"
)

// TombstoneRetention returns the configured retention (zero disables GC).
func (s *storeCore) TombstoneRetention() time.Duration { return s.retention }

func (g *GossipEngine) resyncWarningAfter() time.Duration {
	store, ok := g.store.(interface{ TombstoneRetention() time.Duration })
	if !ok || store.TombstoneRetention() <= 0 {
		return 0
	}
	return max(time.Nanosecond, time.Duration(float64(store.TombstoneRetention())*g.resyncLossWarningFraction))
}

// checkResyncLossWarnings also runs while exchanges are in backoff. Only local
// monotonic elapsed time measures an outage; emitted status clocks may freeze.
func (g *GossipEngine) checkResyncLossWarnings() {
	after := g.resyncWarningAfter()
	if after <= 0 {
		return
	}
	peers := g.peerProvider.Peers()
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, addr := range peers {
		s := g.peerStates[addr]
		if s == nil {
			continue
		}
		if s.unreachableSince.IsZero() || s.resyncLossLimitApproaching || now.Sub(s.unreachableSince) < after {
			continue
		}
		s.unreachableFor = now.Sub(s.unreachableSince)
		s.resyncLossLimitApproaching = true
		s.resyncLossWarnings++
		slog.Warn("state gossip: resync loss limit approaching", "peer", redactedPeerURL(addr), "unreachableFor", now.Sub(s.unreachableSince), "warningAfter", after)
	}
}

func (g *GossipEngine) runResyncWarnings(ctxDone <-chan struct{}) {
	after := g.resyncWarningAfter()
	if after <= 0 {
		return
	}
	interval := minDuration(time.Second, after/10)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctxDone:
			return
		case <-ticker.C:
			g.checkResyncLossWarnings()
		}
	}
}

func (s *peerState) reachable() {
	s.unreachableSince = time.Time{}
	s.unreachableFor = 0
	s.resyncLossLimitApproaching = false
}
