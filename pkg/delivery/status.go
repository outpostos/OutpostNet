package delivery

import (
	"sort"
	"time"
)

// Counters are process-local facts, reset on reopen. Send counts are batch
// attempts (not skipped backoff polls); other counts are envelopes/receipts.
// Duplicate, Rejected and InboxFull count receiver admission results; expiry
// also counts locally committed expiry transitions. Sender full counts Send
// refusals. No counter is a claim about consumer execution.
type Counters struct {
	SendSuccess uint64
	SendFailure uint64
	UnknownPeer uint64
	Duplicate   uint64
	Expired     uint64
	Rejected    uint64
	InboxFull   uint64
	OutboxFull  uint64
}
type PeerDeliveryStatus struct {
	PeerNodeID          string
	PendingEnvelopes    uint64
	PendingBytes        int64
	PendingReceipts     uint64
	PendingReceiptBytes int64
	// Zero for entries written before enqueue timestamps were introduced.
	OldestPendingTime time.Time
	OldestPendingAge  time.Duration
	Counters          Counters
}
type DeliveryStatus struct {
	NodeID string
	Kind   string
	Peers  []PeerDeliveryStatus
}

func (b *box) counters(peer string) *Counters {
	if b.observed == nil {
		b.observed = make(map[string]*Counters)
	}
	c := b.observed[peer]
	if c == nil {
		c = &Counters{}
		b.observed[peer] = c
	}
	return c
}

// DeliveryStatus returns copied local facts, safe alongside all box methods.
// Gauges derive from recovered durable entries. This never runs maintenance or
// resolves peers, and contains no addresses, credentials, payloads or errors.
func (b *box) DeliveryStatus() DeliveryStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := DeliveryStatus{NodeID: b.cfg.NodeID, Kind: b.identity.Kind}
	peers := make(map[string]*PeerDeliveryStatus)
	unknownAge := make(map[string]bool)
	get := func(peer string) *PeerDeliveryStatus {
		p := peers[peer]
		if p == nil {
			p = &PeerDeliveryStatus{PeerNodeID: peer}
			peers[peer] = p
		}
		return p
	}
	for peer, c := range b.observed {
		get(peer).Counters = *c
	}
	now := b.cfg.Clock()
	for _, e := range b.entries {
		peer := e.Envelope.DestinationNodeID
		if b.identity.Kind == "inbox" {
			peer = e.Envelope.MessageID.SourceNodeID
		}
		p := get(peer)
		pending := (b.identity.Kind == "outbox" && e.Status == Pending) || (b.identity.Kind == "inbox" && !e.Acked)
		if pending {
			p.PendingEnvelopes++
			p.PendingBytes += int64(encodedSize(e.Envelope))
		}
		if e.ReceiptPending {
			p.PendingReceipts++
			p.PendingReceiptBytes += int64(encodedSize(Receipt{e.Envelope.MessageID, b.cfg.NodeID}))
		}
		if (pending || e.ReceiptPending) && e.QueuedAt.IsZero() {
			unknownAge[peer] = true
		}
		if (pending || e.ReceiptPending) && !e.QueuedAt.IsZero() && (p.OldestPendingTime.IsZero() || e.QueuedAt.Before(p.OldestPendingTime)) {
			p.OldestPendingTime = e.QueuedAt
		}
	}
	for _, p := range peers {
		if unknownAge[p.PeerNodeID] {
			p.OldestPendingTime = time.Time{}
		}
		if !p.OldestPendingTime.IsZero() && now.After(p.OldestPendingTime) {
			p.OldestPendingAge = now.Sub(p.OldestPendingTime)
		}
		result.Peers = append(result.Peers, *p)
	}
	sort.Slice(result.Peers, func(i, j int) bool { return result.Peers[i].PeerNodeID < result.Peers[j].PeerNodeID })
	return result
}
func (b *box) countResults(envelopes []Envelope, results []Result) {
	for i, r := range results {
		c := b.counters(envelopes[i].MessageID.SourceNodeID)
		switch r.Code {
		case Duplicate:
			c.Duplicate++
		case RejectedExpired:
			c.Expired++
		case RejectedInvalid:
			c.Rejected++
		case Full:
			c.InboxFull++
		}
	}
}

// countExpiry counts terminal transitions only after fsync, before apply, never during recovery.
func (b *box) countExpiry(tx transaction) {
	for _, e := range tx.Upserts {
		old, ok := b.entries[e.Envelope.MessageID]
		if e.Status == Rejected && (!ok || old.Status != Rejected) {
			b.counters(e.Envelope.DestinationNodeID).Rejected++
		}
		if e.Status == Expired && (!ok || old.Status != Expired) {
			peer := e.Envelope.DestinationNodeID
			if b.identity.Kind == "inbox" {
				peer = e.Envelope.MessageID.SourceNodeID
			}
			b.counters(peer).Expired++
		}
	}
	for _, id := range tx.Deletes {
		e := b.entries[id]
		if b.identity.Kind == "inbox" && !e.Acked {
			b.counters(id.SourceNodeID).Expired++
		}
	}
}
