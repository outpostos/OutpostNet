package state

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
)

// NodeClockStatus reports process-local observations, never inferred time quality.
// A writer is suspect after any ahead entry (including one within the valve).
// NodeID is the writer ID or the peer identity/address for a refused horizon.
// Counters and sticky suspect flags reset when the store/engine is recreated.
type NodeClockStatus struct {
	NodeID          string
	ClockSuspect    bool
	AheadEntries    uint64
	RefusedEntries  uint64
	RefusedHorizons uint64
}

// TimeReferenceMS supplies the same reference used by HLC offset checks. A
// custom Store can implement this method; otherwise the engine uses system time.
// Peer-derived references and time quality are introduced by V2-NET-1b.
func (s *storeCore) TimeReferenceMS() int64 { return s.hlc.physical.NowMS() }

func checkFutureTimestamp(ts Timestamp, now int64) error {
	if now > math.MaxInt64-MaxClockOffsetMS || ts.WallMS <= now+MaxClockOffsetMS {
		return nil
	}
	offset := ts.WallMS - now
	if offset < 0 {
		offset = math.MaxInt64
	}
	return &ClockOffsetError{RemoteWallMS: ts.WallMS, PhysicalNowMS: now, OffsetMS: offset, MaxClockOffset: MaxClockOffsetMS}
}

func (s *storeCore) observeClockEntry(e Entry, refused bool) {
	if !refused && e.Version.HLC.WallMS <= s.TimeReferenceMS() {
		return
	}
	s.mu.Lock()
	if s.clockStatus == nil {
		s.clockStatus = make(map[string]NodeClockStatus)
	}
	status := s.clockStatus[e.Version.WriterNodeID]
	wasSuspect := status.ClockSuspect
	status.NodeID = e.Version.WriterNodeID
	status.ClockSuspect = true
	if refused {
		status.RefusedEntries++
	} else {
		status.AheadEntries++
	}
	s.clockStatus[status.NodeID] = status
	s.mu.Unlock()
	if !wasSuspect {
		slog.Warn("state: clock_suspect writer", "writer", status.NodeID, "refused", refused)
	}
}

// ClockStatus returns owned, sorted local writer observations.
func (s *storeCore) ClockStatus() []NodeClockStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]NodeClockStatus, 0, len(s.clockStatus))
	for _, status := range s.clockStatus {
		result = append(result, status)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result
}

func (g *GossipEngine) checkPeerHorizon(peer string, horizon Timestamp) error {
	if horizon.isZero() {
		return nil
	}
	now := SystemPhysicalClock{}.NowMS()
	if clock, ok := g.store.(interface{ TimeReferenceMS() int64 }); ok {
		now = clock.TimeReferenceMS()
	}
	if err := checkFutureTimestamp(horizon, now); err != nil {
		peer = redactedPeerURL(peer)
		g.mu.Lock()
		if g.clockPeers == nil {
			g.clockPeers = make(map[string]NodeClockStatus)
		}
		status := g.clockPeers[peer]
		status.NodeID = peer
		status.ClockSuspect = true
		status.RefusedHorizons++
		g.clockPeers[peer] = status
		g.mu.Unlock()
		slog.Warn("state: clock_suspect peer horizon", "peer", peer, "err", err)
		return fmt.Errorf("refuse GC horizon from %s: %w", peer, err)
	}
	return nil
}

// ClockStatus returns writer and peer observations for host-owned reporting.
func (g *GossipEngine) ClockStatus() []NodeClockStatus {
	combined := make(map[string]NodeClockStatus)
	if store, ok := g.store.(interface{ ClockStatus() []NodeClockStatus }); ok {
		for _, status := range store.ClockStatus() {
			combined[status.NodeID] = status
		}
	}
	g.mu.Lock()
	for id, peer := range g.clockPeers {
		status := combined[id]
		status.NodeID = id
		status.ClockSuspect = status.ClockSuspect || peer.ClockSuspect
		status.RefusedHorizons += peer.RefusedHorizons
		combined[id] = status
	}
	g.mu.Unlock()
	result := make([]NodeClockStatus, 0, len(combined))
	for _, status := range combined {
		result = append(result, status)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result
}
