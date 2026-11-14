package delivery

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Count clock reads at the public Receive boundary: scanning retained entries
// spends the budget on the very first receive, independent of machine speed.
func TestInboxDrainWorkBudget(t *testing.T) {
	for _, n := range []int{5000, 50000} {
		t.Run(time.Duration(n).String(), func(t *testing.T) {
			now := time.Now()
			var reads atomic.Int64
			in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", QuotaBytes: 256 << 20, Clock: func() time.Time { reads.Add(1); return now }})
			// A compatible persisted snapshot models already retained dedup records.
			up := make([]entry, 0, 2*n)
			for i := 0; i < 2*n; i++ {
				e := external(now, uint64(i+1))
				v := entry{Envelope: e, Status: Pending}
				if i < n {
					v.Acked = true
					v.Envelope.Payload = nil
				}
				up = append(up, v)
			}
			if err := in.commit(transaction{Upserts: up}); err != nil {
				t.Fatal(err)
			}
			reads.Store(0)
			started := time.Now()
			for i := 0; i < n; i++ {
				e, err := in.Receive(context.Background(), "m")
				if err != nil {
					t.Fatal(err)
				}
				if e.MessageID.Seq != uint64(n+i+1) {
					t.Fatalf("order: %v", e.MessageID)
				}
				if got := reads.Load(); got > int64(8*(i+1)) {
					t.Fatalf("Receive visited retained/backlog entries: clock reads=%d after %d envelopes", got, i+1)
				}
				if err = in.Ack(context.Background(), e.MessageID); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("%d envelopes: %.0f envelopes/s", n, float64(n)/time.Since(started).Seconds())
		})
	}
}

type syncBarrierFile struct {
	logFile
	entered chan struct{}
	release chan struct{}
	syncs   atomic.Int64
	armed   atomic.Bool
}

func (f *syncBarrierFile) Sync() error {
	f.syncs.Add(1)
	if f.armed.CompareAndSwap(true, false) {
		close(f.entered)
		<-f.release
	}
	return f.logFile.Sync()
}

func TestConcurrentAcksShareDurableCommit(t *testing.T) {
	var disk *syncBarrierFile
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", wrapLog: func(f logFile) logFile {
		disk = &syncBarrierFile{logFile: f, entered: make(chan struct{}), release: make(chan struct{})}
		return disk
	}})
	const count = 64
	envelopes := make([]Envelope, count)
	for i := range envelopes {
		envelopes[i] = external(time.Now(), uint64(i+1))
	}
	if _, err := in.Accept(context.Background(), envelopes); err != nil {
		t.Fatal(err)
	}
	before := disk.syncs.Load()
	disk.armed.Store(true)
	done := make(chan error, count)
	go func() { done <- in.Ack(context.Background(), envelopes[0].MessageID) }()
	<-disk.entered
	var started sync.WaitGroup
	started.Add(count - 1)
	for _, e := range envelopes[1:] {
		go func(e Envelope) { started.Done(); done <- in.Ack(context.Background(), e.MessageID) }(e)
	}
	started.Wait()
	// While Sync is blocked no caller may report success.
	select {
	case err := <-done:
		t.Fatalf("Ack returned before durable sync: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(disk.release)
	for range count {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if syncs := disk.syncs.Load() - before; syncs > 4 {
		t.Fatalf("concurrent Acks used %d syncs for %d records", syncs, count)
	}
	recovered := mustIn(t, Config{Dir: crashImage(t, in.cfg.Dir), NodeID: "b"})
	noDelivery(t, recovered)
}

func BenchmarkReceiveBacklog(b *testing.B) {
	now := time.Now()
	in, err := NewInbox(Config{Dir: b.TempDir(), NodeID: "b", QuotaBytes: 256 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer in.Close()
	up := make([]entry, 100000)
	for i := range up {
		up[i] = entry{Envelope: external(now, uint64(i+1)), Status: Pending, Acked: i < 50000}
		if up[i].Acked {
			up[i].Envelope.Payload = nil
		}
	}
	if err = in.commit(transaction{Upserts: up}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		e, err := in.Receive(context.Background(), "m")
		if err != nil {
			b.Fatal(err)
		}
		if err = in.Nack(e.MessageID); err != nil {
			b.Fatal(err)
		}
	}
}
