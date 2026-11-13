package delivery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type retry struct {
	next  time.Time
	delay time.Duration
}

func destinations(b map[string]Batch) []string {
	var all []string
	for d := range b {
		all = append(all, d)
	}
	sort.Strings(all)
	return all
}
func (b *box) send(ctx context.Context, dest string, batch Batch) (BatchResult, error) {
	if b.cfg.Transport == nil {
		return BatchResult{}, errors.New("delivery transport required")
	}
	r, err := b.cfg.Transport.Send(ctx, dest, batch)
	if err != nil {
		return BatchResult{}, err
	}
	validate := func(request []MessageID, result []Result) bool {
		if len(request) != len(result) {
			return false
		}
		for i, id := range request {
			if result[i].MessageID != id {
				return false
			}
			switch result[i].Code {
			case Accepted, Duplicate, RejectedExpired, RejectedInvalid, Full:
			default:
				return false
			}
		}
		return true
	}
	var ids, receipts []MessageID
	for _, e := range batch.Envelopes {
		ids = append(ids, e.MessageID)
	}
	for _, r := range batch.Receipts {
		receipts = append(receipts, r.MessageID)
	}
	if !validate(ids, r.Envelopes) || !validate(receipts, r.Receipts) {
		return BatchResult{}, errors.New("invalid delivery transport response")
	}
	return r, nil
}

// MemoryTransport routes through the real durable Inbox/Outbox APIs. Register
// can replace a peer after restart; no network, HTTP or process wiring is added.
type MemoryTransport struct {
	mu    sync.RWMutex
	peers map[string]memoryPeer
}
type memoryPeer struct {
	in  *Inbox
	out *Outbox
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{peers: make(map[string]memoryPeer)}
}
func (m *MemoryTransport) Register(nodeID string, in *Inbox, out *Outbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers[nodeID] = memoryPeer{in, out}
}
func (m *MemoryTransport) Send(ctx context.Context, dest string, b Batch) (BatchResult, error) {
	m.mu.RLock()
	p, ok := m.peers[dest]
	m.mu.RUnlock()
	if !ok {
		return BatchResult{}, fmt.Errorf("delivery peer unavailable")
	}
	var r BatchResult
	var err error
	if len(b.Envelopes) > 0 {
		if p.in == nil {
			return r, errors.New("delivery peer has no inbox")
		}
		r.Envelopes, err = p.in.Accept(ctx, b.Envelopes)
		if err != nil {
			return BatchResult{}, err
		}
	}
	if len(b.Receipts) > 0 {
		if p.out == nil {
			return r, errors.New("delivery peer has no outbox")
		}
		r.Receipts, err = p.out.AcceptReceipts(ctx, b.Receipts)
		if err != nil {
			return BatchResult{}, err
		}
	}
	return r, nil
}
