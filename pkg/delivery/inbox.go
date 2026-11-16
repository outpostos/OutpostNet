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
		if node := firstLive(in.mailboxes[mailbox], in.cfg.Clock()); node != nil {
			e := clone(node.value.Envelope)
			in.mailboxes[mailbox] = removeReady(in.mailboxes[mailbox], node.value)
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
	if in.leased[id] {
		delete(in.leased, id)
		e := in.entries[id]
		if !e.Acked && in.cfg.Clock().Before(e.Envelope.ExpiryTime) {
			key := e.Envelope.Mailbox
			in.mailboxes[key] = insertReady(in.mailboxes[key], e)
		}
	}
	close(in.changed)
	in.changed = make(chan struct{})
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
	batches := make(map[string]Batch)
	for dest, q := range in.receipts {
		batch := in.page(q, true)
		if len(batch.Receipts) > 0 {
			batches[dest] = batch
		}
	}
	in.mu.Unlock()
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
