package delivery

import "context"

const maxAckGroup = 256

type ackRequest struct {
	ctx  context.Context
	id   MessageID
	done chan error
}

// Ack returns only after the record (including its receipt) is durable. The
// first caller starts a worker; concurrent callers arriving during a commit
// share subsequent bounded transactions. No timer delays a solitary Ack.
func (in *Inbox) Ack(ctx context.Context, id MessageID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r := ackRequest{ctx, id, make(chan error, 1)}
	in.ackMu.Lock()
	in.acks = append(in.acks, r)
	if !in.ackRunning {
		in.ackRunning = true
		go in.commitAcks()
	}
	in.ackMu.Unlock()
	// Once admitted, wait for the definitive result even if ctx is cancelled.
	return <-r.done
}
func (in *Inbox) commitAcks() {
	for {
		in.ackMu.Lock()
		n := min(len(in.acks), maxAckGroup)
		if n == 0 {
			in.ackRunning = false
			in.ackMu.Unlock()
			return
		}
		group := append([]ackRequest(nil), in.acks[:n]...)
		clear(in.acks[:n])
		in.acks = in.acks[n:]
		in.ackMu.Unlock()
		in.mu.Lock()
		errs := make([]error, n)
		updates := make(map[MessageID]bool, n)
		tx := transaction{}
		for i, r := range group {
			if errs[i] = in.writable(); errs[i] != nil {
				continue
			}
			if errs[i] = r.ctx.Err(); errs[i] != nil {
				continue
			}
			e, ok := in.entries[r.id]
			if !ok {
				errs[i] = ErrNotFound
				continue
			}
			if e.Acked || updates[r.id] {
				continue
			}
			e.Acked = true
			e.Envelope.Payload = nil
			e.ReceiptPending = e.Envelope.WantHandoffReceipt
			updates[r.id] = true
			tx.Upserts = append(tx.Upserts, e)
		}
		var err error
		if len(tx.Upserts) > 0 {
			err = in.commit(tx)
		}
		if err == nil {
			for _, e := range tx.Upserts {
				delete(in.leased, e.Envelope.MessageID)
			}
		}
		in.mu.Unlock()
		for i, r := range group {
			if errs[i] != nil {
				r.done <- errs[i]
			} else {
				r.done <- err
			}
		}
	}
}
