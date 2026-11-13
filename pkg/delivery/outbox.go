package delivery

import (
	"context"
	"errors"
	"sort"
)

type Outbox struct{ *box }

func NewOutbox(cfg Config) (*Outbox, error) {
	b, e := openBox(cfg, "outbox")
	if e != nil {
		return nil, e
	}
	return &Outbox{b}, nil
}

// Send assigns the source identity and checksum. Success means the complete
// envelope is synced. Quota refusals never admit an envelope. A disk error whose
// rollback also fails is ambiguous and makes the box fail closed until reopen.
func (o *Outbox) Send(ctx context.Context, e Envelope) (MessageID, error) {
	if err := ctx.Err(); err != nil {
		return MessageID{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.writable(); err != nil {
		return MessageID{}, err
	}
	if o.messageSeq == ^uint64(0) {
		return MessageID{}, errors.New("delivery source sequence exhausted")
	}
	e = clone(e)
	e.MessageID = MessageID{o.cfg.NodeID, o.identity.Incarnation, o.messageSeq + 1}
	e.Checksum = Checksum(e.Payload)
	now := o.cfg.Clock()
	if !e.ExpiryTime.After(now) || e.ExpiryTime.After(now.Add(o.cfg.MaxExpiry)) {
		return MessageID{}, ErrInvalidEnvelope
	}
	if err := e.valid(o.cfg, now); err != nil {
		return MessageID{}, err
	}
	v := entry{Envelope: e, Status: Pending}
	if o.usage()+entryCharge(v) > o.cfg.QuotaBytes {
		return MessageID{}, ErrOutboxFull
	}
	if err := o.commit(transaction{Upserts: []entry{v}, MessageSeq: e.MessageID.Seq}); err != nil {
		return MessageID{}, err
	}
	return e.MessageID, nil
}
func (b *box) usage() int64 {
	return b.usedBytes
}
func (o *Outbox) Status(id MessageID) (Status, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return "", ErrClosed
	}
	e, ok := o.entries[id]
	if !ok {
		return "", ErrNotFound
	}
	return e.Status, nil
}

// AcceptReceipts is called by the authenticated transport adapter. Receipt
// authentication is the adapter's responsibility (see Transport).
func (o *Outbox) AcceptReceipts(ctx context.Context, receipts []Receipt) ([]Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.writable(); err != nil {
		return nil, err
	}
	results := make([]Result, len(receipts))
	updates := make(map[MessageID]entry)
	for i, r := range receipts {
		results[i] = Result{r.MessageID, RejectedInvalid}
		e, ok := o.entries[r.MessageID]
		if u, exists := updates[r.MessageID]; exists {
			e = u
		}
		if !ok || !e.Envelope.WantHandoffReceipt || e.Envelope.DestinationNodeID != r.ReceiverNodeID {
			continue
		}
		if e.Status == HandedOff {
			results[i].Code = Duplicate
			continue
		}
		if !o.cfg.Clock().Before(e.Envelope.ExpiryTime.Add(o.cfg.ClockSkew)) {
			results[i].Code = RejectedExpired
			continue
		}
		// Receipt can race ahead of the original durable-acceptance response.
		if e.Status != Pending && e.Status != Delivered {
			continue
		}
		e.Status = HandedOff
		e.Envelope.Payload = nil
		updates[r.MessageID] = e
		results[i].Code = Accepted
	}
	var up []entry
	for _, e := range updates {
		up = append(up, e)
	}
	if len(up) > 0 {
		if err := o.commit(transaction{Upserts: up}); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func ordered(entries map[MessageID]entry, keep func(entry) bool) []entry {
	var all []entry
	for _, e := range entries {
		if keep(e) {
			all = append(all, e)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i].Envelope, all[j].Envelope
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if a.MessageID.Seq != b.MessageID.Seq {
			return a.MessageID.Seq < b.MessageID.Seq
		}
		if a.MessageID.SourceNodeID != b.MessageID.SourceNodeID {
			return a.MessageID.SourceNodeID < b.MessageID.SourceNodeID
		}
		return a.MessageID.SourceIncarnation < b.MessageID.SourceIncarnation
	})
	return all
}

// Flush makes one bounded attempt per destination. Run calls it repeatedly.
// Errors are returned to the host; no background error is silently discarded.
func (o *Outbox) Flush(ctx context.Context) error {
	o.flushMu.Lock()
	defer o.flushMu.Unlock()
	if err := o.Collect(ctx); err != nil {
		return err
	}
	o.mu.Lock()
	if err := o.writable(); err != nil {
		o.mu.Unlock()
		return err
	}
	all := ordered(o.entries, func(e entry) bool { return e.Status == Pending })
	o.mu.Unlock()
	batches := make(map[string]Batch)
	filled := make(map[string]bool)
	for _, e := range all {
		dest := e.Envelope.DestinationNodeID
		if filled[dest] {
			continue
		}
		b := batches[dest]
		candidate := Batch{Envelopes: append(append([]Envelope(nil), b.Envelopes...), clone(e.Envelope))}
		if encodedSize(candidate) <= o.cfg.MaxBatchBytes {
			batches[dest] = candidate
		} else {
			filled[dest] = true
		}
	}
	var errs []error
	for _, dest := range destinations(batches) {
		batch := batches[dest]
		// Earlier peer calls may have taken long enough for this page to expire.
		if err := o.Collect(ctx); err != nil {
			return errors.Join(append(errs, err)...)
		}
		live := batch.Envelopes[:0]
		now := o.cfg.Clock()
		for _, e := range batch.Envelopes {
			if now.Before(e.ExpiryTime) {
				live = append(live, e)
			}
		}
		batch.Envelopes = live
		if len(live) == 0 {
			continue
		}
		response, err := o.send(ctx, dest, batch)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		o.mu.Lock()
		var up []entry
		for i, r := range response.Envelopes {
			e := o.entries[batch.Envelopes[i].MessageID]
			if e.Status != Pending {
				continue
			}
			switch r.Code {
			case Accepted, Duplicate:
				e.Status = Delivered
			case RejectedExpired:
				e.Status = Expired
			case RejectedInvalid:
				e.Status = Rejected
			default:
				continue
			}
			e.Envelope.Payload = nil
			up = append(up, e)
		}
		if len(up) > 0 {
			err = o.commit(transaction{Upserts: up})
		}
		o.mu.Unlock()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
