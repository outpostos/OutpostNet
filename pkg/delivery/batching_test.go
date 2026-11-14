package delivery

import (
	"context"
	"runtime"
	"testing"
	"time"
)

type batchRecorder struct{ batches []Batch }

func (r *batchRecorder) Send(_ context.Context, _ string, b Batch) (BatchResult, error) {
	r.batches = append(r.batches, b)
	result := BatchResult{}
	for _, e := range b.Envelopes {
		result.Envelopes = append(result.Envelopes, Result{e.MessageID, Accepted})
	}
	for _, e := range b.Receipts {
		result.Receipts = append(result.Receipts, Result{e.MessageID, Accepted})
	}
	return result, nil
}

// Include retained metadata and a full receipt page. A quadratic builder
// allocates gigabytes for this single bounded Flush.
func TestFlushAllocationBudget(t *testing.T) {
	for _, kind := range []string{"inbox", "outbox"} {
		t.Run(kind, func(t *testing.T) {
			wire := &batchRecorder{}
			cfg := Config{Dir: t.TempDir(), NodeID: "b", QuotaBytes: 256 << 20, Transport: wire}
			var b *box
			var flush func(context.Context) error
			if kind == "inbox" {
				in := mustIn(t, cfg)
				b = in.box
				flush = in.Flush
			} else {
				o := mustOut(t, cfg)
				b = o.box
				flush = o.Flush
			}
			const count = 50000
			up := make([]entry, 2*count)
			now := time.Now()
			for i := range up {
				e := external(now, uint64(i+1))
				v := entry{Envelope: e, Status: Pending}
				if kind == "inbox" {
					v.Acked = true
					v.ReceiptPending = i < count
					v.Envelope.Payload = nil
				} else {
					if i >= count {
						v.Status = Delivered
						v.Envelope.Payload = nil
					}
				}
				up[i] = v
			}
			if err := b.commit(transaction{Upserts: up}); err != nil {
				t.Fatal(err)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if err := flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			if used := after.TotalAlloc - before.TotalAlloc; used > 128<<20 {
				t.Fatalf("one byte-bounded Flush allocated %d bytes", used)
			}
			if len(wire.batches) != 1 {
				t.Fatalf("batches=%d", len(wire.batches))
			}
			page := wire.batches[0]
			if size := encodedSize(page); size > b.cfg.MaxBatchBytes {
				t.Fatalf("page exceeds cap: %d", size)
			}
			if len(page.Receipts)+len(page.Envelopes) < 1000 {
				t.Fatal("page did not fill")
			}
		})
	}
}
