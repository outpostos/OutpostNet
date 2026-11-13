package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Collect expires payloads and removes terminal/dedup records at expiry + skew.
// A newly expired outbox status survives this pass so callers can observe it.
func (b *box) Collect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.writable(); err != nil {
		return err
	}
	now := b.cfg.Clock()
	tx := transaction{}
	for id, e := range b.entries {
		if now.Before(e.Envelope.ExpiryTime) {
			continue
		}
		if b.identity.Kind == "outbox" && e.Status == Pending {
			e.Status = Expired
			e.Envelope.Payload = nil
			tx.Upserts = append(tx.Upserts, e)
			continue
		}
		if !now.Before(e.Envelope.ExpiryTime.Add(b.cfg.ClockSkew)) {
			tx.Deletes = append(tx.Deletes, id)
			continue
		}
		if b.identity.Kind == "inbox" && !e.Acked {
			e.Acked = true
			e.Status = Expired
			e.Envelope.Payload = nil
			e.ReceiptPending = false
			tx.Upserts = append(tx.Upserts, e)
		}
	}
	if len(tx.Upserts)+len(tx.Deletes) == 0 {
		return nil
	}
	return b.commit(tx)
}

// Run retries in the background until cancellation or close. Transient errors
// are reported and retried. A nil reporter logs them; fatal disk errors return.
// Hosts own the goroutine and must cancel and join it before discarding a box.
func (o *Outbox) Run(ctx context.Context, report func(error)) error {
	return o.run(ctx, o.Flush, report)
}
func (in *Inbox) Run(ctx context.Context, report func(error)) error {
	return in.run(ctx, in.Flush, report)
}
func (b *box) run(ctx context.Context, flush func(context.Context) error, report func(error)) error {
	tick := time.NewTicker(b.cfg.RetryMin)
	defer tick.Stop()
	for {
		if err := flush(ctx); err != nil {
			if errors.Is(err, ErrClosed) || errors.Is(err, ErrUnwritable) || ctx.Err() != nil {
				return err
			}
			if report != nil {
				report(err)
			} else {
				slog.Error("delivery: retryable flush failure", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
