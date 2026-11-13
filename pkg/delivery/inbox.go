package delivery

import (
	"context"
	"errors"
)

type Inbox struct{ *box }

func NewInbox(cfg Config) (*Inbox, error) {
	b, e := openBox(cfg, "inbox")
	if e != nil {
		return nil, e
	}
	return &Inbox{b}, nil
}

// Accept commits every new accepted envelope in a single log record and fsync.
// An error invalidates the entire response; none of its results acknowledge data.
func (in *Inbox) Accept(ctx context.Context, envelopes []Envelope) ([]Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if err := in.writable(); err != nil {
		return nil, err
	}
	results := make([]Result, len(envelopes))
	updates := make(map[MessageID]entry)
	usage := in.usage()
	now := in.cfg.Clock()
	for i, e := range envelopes {
		results[i] = Result{e.MessageID, RejectedInvalid}
		if err := e.valid(in.cfg, now); err != nil || e.DestinationNodeID != in.cfg.NodeID {
			continue
		}
		if !now.Before(e.ExpiryTime.Add(in.cfg.ClockSkew)) {
			results[i].Code = RejectedExpired
			continue
		}
		if _, ok := in.entries[e.MessageID]; ok {
			results[i].Code = Duplicate
			continue
		}
		if _, ok := updates[e.MessageID]; ok {
			results[i].Code = Duplicate
			continue
		}
		v := entry{Envelope: clone(e), Status: Pending, QueuedAt: now}
		n := entryCharge(v)
		if usage+n > in.cfg.QuotaBytes {
			results[i].Code = Full
			continue
		}
		updates[e.MessageID] = v
		usage += n
		results[i].Code = Accepted
	}
	var up []entry
	for _, e := range updates {
		up = append(up, e)
	}
	if len(up) > 0 {
		if err := in.commit(transaction{Upserts: up}); err != nil {
			return nil, err
		}
	}
	in.countResults(envelopes, results)
	return results, nil
}

// Receive leases one envelope to the caller for this process lifetime. Ack
// removes it durably; Nack releases the lease. Restart releases all leases.
func (in *Inbox) Receive(ctx context.Context, mailbox string) (Envelope, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Envelope{}, err
		}
		in.mu.Lock()
		if err := in.writable(); err != nil {
			in.mu.Unlock()
			return Envelope{}, err
		}
		all := ordered(in.entries, func(e entry) bool {
			return !e.Acked && in.cfg.Clock().Before(e.Envelope.ExpiryTime) && e.Envelope.Mailbox == mailbox && !in.leased[e.Envelope.MessageID]
		})
		if len(all) > 0 {
			e := clone(all[0].Envelope)
			in.leased[e.MessageID] = true
			in.mu.Unlock()
			return e, nil
		}
		changed := in.changed
		in.mu.Unlock()
		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case <-changed:
		}
	}
}
func (in *Inbox) Nack(id MessageID) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.closed {
		return ErrClosed
	}
	if _, ok := in.entries[id]; !ok {
		return ErrNotFound
	}
	delete(in.leased, id)
	close(in.changed)
	in.changed = make(chan struct{})
	return nil
}
func (in *Inbox) Ack(ctx context.Context, id MessageID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if err := in.writable(); err != nil {
		return err
	}
	e, ok := in.entries[id]
	if !ok {
		return ErrNotFound
	}
	if e.Acked {
		return nil
	}
	e.Acked = true
	e.Envelope.Payload = nil
	e.ReceiptPending = e.Envelope.WantHandoffReceipt
	if err := in.commit(transaction{Upserts: []entry{e}}); err != nil {
		return err
	}
	delete(in.leased, id)
	return nil
}
func (in *Inbox) Flush(ctx context.Context) error {
	in.flushMu.Lock()
	defer in.flushMu.Unlock()
	if err := in.Collect(ctx); err != nil {
		return err
	}
	in.mu.Lock()
	if err := in.writable(); err != nil {
		in.mu.Unlock()
		return err
	}
	all := ordered(in.entries, func(e entry) bool { return e.ReceiptPending })
	in.mu.Unlock()
	batches := make(map[string]Batch)
	for _, e := range all {
		dest := e.Envelope.MessageID.SourceNodeID
		b := batches[dest]
		candidate := Batch{Receipts: append(append([]Receipt(nil), b.Receipts...), Receipt{e.Envelope.MessageID, in.cfg.NodeID})}
		if encodedSize(candidate) <= in.cfg.MaxBatchBytes {
			batches[dest] = candidate
		}
	}
	var errs []error
	for _, dest := range destinations(batches) {
		batch := batches[dest]
		response, err := in.send(ctx, dest, batch)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		in.mu.Lock()
		var up []entry
		for i, r := range response.Receipts {
			if r.Code == Full {
				continue
			}
			e, ok := in.entries[batch.Receipts[i].MessageID]
			if ok && e.ReceiptPending {
				e.ReceiptPending = false
				up = append(up, e)
			}
		}
		if len(up) > 0 {
			err = in.commit(transaction{Upserts: up})
		}
		in.mu.Unlock()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
