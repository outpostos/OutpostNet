package state

import "context"

// refusedPosition is an unresolved refusal in this store's position space.
// Keeping all positions (rather than only the minimum) lets acknowledgements
// release one key without forgetting a later refusal of another key.
type refusedPosition struct {
	Key      string       `json:"key"`
	Position uint64       `json:"position"`
	Version  StateVersion `json:"version"`
	ChangeID ChangeID     `json:"change_id"`
}

type peerRefusalStore interface {
	peerRefusals(context.Context, string) ([]refusedPosition, error)
}

func (g *GossipEngine) peerRefusals(ctx context.Context, addr string) ([]refusedPosition, error) {
	if s, ok := g.store.(peerRefusalStore); ok {
		return s.peerRefusals(ctx, addr)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]refusedPosition(nil), g.peerStateLocked(addr).refused...), nil
}

// peerSafePosition is the inclusive, non-refused acknowledgement prefix.
// ADR-0006's minimum refused position is an exclusive boundary: subtract one
// because the existing GC/floor comparisons are inclusive (<= / >=).
func (g *GossipEngine) peerSafePosition(ctx context.Context, addr string) (uint64, error) {
	g.progressMu.Lock()
	defer g.progressMu.Unlock()
	position, err := g.peerLastPushPosition(ctx, addr)
	if err != nil {
		return 0, err
	}
	refused, err := g.peerRefusals(ctx, addr)
	if err != nil {
		return 0, err
	}
	return nonRefusedPosition(position, refused), nil
}

func updateRefusals(previous []refusedPosition, changes []Entry, resp *PushResponse) []refusedPosition {
	rejected := make(map[string]bool)
	if resp != nil {
		for _, r := range resp.Rejections {
			if r.Reason != RejectionReasonBeyondGCHorizon {
				rejected[r.Key] = true
			}
		}
	}
	out := append([]refusedPosition(nil), previous...)
	for _, entry := range changes {
		if !rejected[entry.Key] {
			kept := out[:0]
			for _, r := range out {
				if r.Key != entry.Key || r.Position > entry.Position {
					kept = append(kept, r)
				}
			}
			out = kept
			continue
		}
		present := false
		for _, r := range out {
			if r.Position == entry.Position {
				present = true
				break
			}
		}
		if !present {
			out = append(out, refusedPosition{Key: entry.Key, Position: entry.Position, Version: entry.Version, ChangeID: entry.ChangeID})
		}
	}
	return out
}

// A peer's next digest confirms entries accepted on a previous pull, including
// accepted replacements. Missing keys never confirm a refused position.
func (g *GossipEngine) confirmDigestRefusals(ctx context.Context, nodeID string, digest map[string]DigestRecord) error {
	g.progressMu.Lock()
	defer g.progressMu.Unlock()
	for _, addr := range g.peersAnsweringAs(nodeID) {
		refused, err := g.peerRefusals(ctx, addr)
		if err != nil {
			return err
		}
		kept := make([]refusedPosition, 0, len(refused))
		for _, r := range refused {
			record, ok := digest[r.Key]
			if ok {
				if record.Version == r.Version && record.ChangeID == r.ChangeID {
					continue
				}
				entry, found, err := g.store.Get(ctx, r.Key)
				if err != nil {
					return err
				}
				if found && entry.Position >= r.Position && g.policy.CompareEntryToDigest(entry, record) <= 0 {
					continue
				}
			}
			kept = append(kept, r)
		}
		if len(kept) == len(refused) {
			continue
		}
		position, err := g.peerLastPushPosition(ctx, addr)
		if err != nil {
			return err
		}
		if err := g.savePushAck(ctx, addr, nodeID, position, kept); err != nil {
			return err
		}
	}
	return nil
}

func (g *GossipEngine) savePushAck(ctx context.Context, addr, nodeID string, position uint64, refused []refusedPosition) error {
	if acks, ok := g.store.(peerAckStore); ok {
		if err := acks.savePeerAck(ctx, addr, nodeID, position, refused); err != nil {
			return err
		}
	} else if progress, ok := g.store.(PeerProgressStore); ok {
		// A custom progress store cannot atomically persist refusals. Checkpoint
		// only the safe prefix; this process still skips through its memory cursor.
		if err := progress.SavePeerProgress(ctx, addr, nonRefusedPosition(position, refused)); err != nil {
			return err
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.peerStateLocked(addr)
	if position > state.lastPushPosition {
		state.lastPushPosition = position
	}
	state.nodeID = nodeID
	state.refused = append([]refusedPosition(nil), refused...)
	return nil
}

func nonRefusedPosition(position uint64, refused []refusedPosition) uint64 {
	for _, r := range refused {
		if r.Position <= position {
			position = r.Position - 1
		}
	}
	return position
}
