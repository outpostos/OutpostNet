package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestByteBatchDoesNotSkipEarlierSequence(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	var seqs []uint64
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", MaxEnvelopeBytes: 1000, MaxBatchBytes: 1350, Transport: transportFunc(func(_ context.Context, _ string, b Batch) (BatchResult, error) {
		r := BatchResult{}
		for _, e := range b.Envelopes {
			seqs = append(seqs, e.MessageID.Seq)
			r.Envelopes = append(r.Envelopes, Result{e.MessageID, Accepted})
		}
		return r, nil
	})})
	for _, size := range []int{100, 500, 0} {
		e := message(now)
		e.Payload = []byte(strings.Repeat("x", size))
		if _, err := o.Send(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if e := o.Flush(ctx); e != nil {
		t.Fatal(e)
	}
	if len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("batch jumped over sequence 2: %v", seqs)
	}
	for n := 0; n < 2; n++ {
		if e := o.Flush(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if len(seqs) != 3 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatal(seqs)
	}
}

func TestSlowDestinationDoesNotCauseExpiredSendToNext(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	var sent []string
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Clock: func() time.Time { return now }, Transport: transportFunc(func(_ context.Context, d string, b Batch) (BatchResult, error) {
		sent = append(sent, d)
		now = now.Add(2 * time.Hour)
		r := BatchResult{}
		for _, e := range b.Envelopes {
			r.Envelopes = append(r.Envelopes, Result{e.MessageID, Accepted})
		}
		return r, nil
	})})
	for _, dest := range []string{"b", "c"} {
		e := message(now)
		e.DestinationNodeID = dest
		if _, err := o.Send(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if e := o.Flush(ctx); e != nil {
		t.Fatal(e)
	}
	if len(sent) != 1 {
		t.Fatalf("sent after expiry: %v", sent)
	}
}

func TestFlushKeepsNewExpiredStatusWhileAnotherMessageIsLive(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Clock: func() time.Time { return now }, Transport: transportFunc(func(_ context.Context, _ string, b Batch) (BatchResult, error) {
		r := BatchResult{}
		for _, e := range b.Envelopes {
			r.Envelopes = append(r.Envelopes, Result{e.MessageID, Accepted})
		}
		return r, nil
	})})
	e := message(now)
	e.ExpiryTime = now.Add(time.Second)
	id, err := o.Send(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.Send(ctx, message(now)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s, err := o.Status(id); err != nil || s != Expired {
		t.Fatalf("newly expired status = %s, %v", s, err)
	}
}

func TestMalformedAcknowledgementsKeepPayloadPending(t *testing.T) {
	for _, mode := range []string{"empty", "wrong-id", "unknown-code"} {
		t.Run(mode, func(t *testing.T) {
			o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Transport: transportFunc(func(_ context.Context, _ string, b Batch) (BatchResult, error) {
				if mode == "empty" {
					return BatchResult{}, nil
				}
				r := Result{b.Envelopes[0].MessageID, Accepted}
				if mode == "wrong-id" {
					r.MessageID.Seq++
				} else {
					r.Code = "other"
				}
				return BatchResult{Envelopes: []Result{r}}, nil
			})})
			id, e := o.Send(context.Background(), message(time.Now()))
			if e != nil {
				t.Fatal(e)
			}
			if e = o.Flush(context.Background()); e == nil {
				t.Fatal("malformed response accepted")
			}
			if s, _ := o.Status(id); s != Pending {
				t.Fatal(s)
			}
		})
	}
}

func TestReceiptRaceAndLossSurviveRestart(t *testing.T) {
	ctx := context.Background()
	wire := NewMemoryTransport()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", Transport: wire})
	wire.Register("a", nil, o)
	e := message(time.Now())
	e.WantHandoffReceipt = true
	id, err := o.Send(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	o.cfg.Transport = transportFunc(func(c context.Context, d string, b Batch) (BatchResult, error) {
		r, e := in.Accept(c, b.Envelopes)
		if e != nil {
			return BatchResult{}, e
		}
		delivery, e := in.Receive(c, "m")
		if e != nil {
			return BatchResult{}, e
		}
		if e = in.Ack(c, delivery.MessageID); e != nil {
			return BatchResult{}, e
		}
		if e = in.Flush(c); e != nil {
			return BatchResult{}, e
		}
		return BatchResult{Envelopes: r}, nil
	})
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := o.Status(id); s != HandedOff {
		t.Fatalf("receipt race regressed status: %s", s)
	}
	// Recover a pending durable receipt after Ack; lose its response, then retry.
	e2 := external(time.Now(), 500)
	e2.WantHandoffReceipt = true
	if _, err = in.Accept(ctx, []Envelope{e2}); err != nil {
		t.Fatal(err)
	}
	if err = in.Ack(ctx, e2.MessageID); err != nil {
		t.Fatal(err)
	}
	if err = in.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	ri := mustIn(t, Config{Dir: crashImage(t, in.cfg.Dir), NodeID: "b", Transport: transportFunc(func(_ context.Context, _ string, b Batch) (BatchResult, error) {
		if len(b.Receipts) != 1 || b.Receipts[0].MessageID != e2.MessageID {
			t.Fatal(b)
		}
		return BatchResult{}, errors.New("response lost")
	})})
	if err = ri.Flush(ctx); err == nil {
		t.Fatal("receipt failure not reported")
	}
	noDelivery(t, ri)
}

func TestConcurrentRunSendReceiveAckAndClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wire := NewMemoryTransport()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Transport: wire, RetryMin: time.Millisecond})
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", Transport: wire, RetryMin: time.Millisecond})
	wire.Register("a", nil, o)
	wire.Register("b", in, nil)
	var runners sync.WaitGroup
	for _, run := range []func(context.Context, func(error)) error{o.Run, in.Run} {
		runners.Add(1)
		go func(f func(context.Context, func(error)) error) {
			defer runners.Done()
			_ = f(ctx, func(e error) { t.Error(e) })
		}(run)
	}
	var senders sync.WaitGroup
	for n := 0; n < 4; n++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for i := 0; i < 10; i++ {
				e := message(time.Now())
				e.WantHandoffReceipt = true
				id, err := o.Send(ctx, e)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err = o.Status(id); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	seen := make(map[MessageID]bool)
	for n := 0; n < 40; n++ {
		e, err := in.Receive(ctx, "m")
		if err != nil {
			t.Fatal(err)
		}
		if seen[e.MessageID] {
			t.Fatal("concurrent duplicate")
		}
		seen[e.MessageID] = true
		if err = in.Ack(ctx, e.MessageID); err != nil {
			t.Fatal(err)
		}
	}
	senders.Wait()
	cancel()
	runners.Wait()
	waiter := make(chan error, 1)
	go func() { _, e := in.Receive(context.Background(), "empty"); waiter <- e }()
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-waiter:
		if !errors.Is(e, ErrClosed) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake Receive")
	}
}

func TestCompactionFailuresPreserveRecoverableState(t *testing.T) {
	for _, step := range []string{"write-temp", "sync-temp", "rename", "sync-dir", "reopen"} {
		t.Run(step, func(t *testing.T) {
			o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
			id, e := o.Send(context.Background(), message(time.Now()))
			if e != nil {
				t.Fatal(e)
			}
			o.cfg.compactHook = func(s string) error {
				if s == step {
					return errors.New("injected compaction failure")
				}
				return nil
			}
			if e = o.Compact(context.Background()); e == nil {
				t.Fatal("compaction succeeded")
			}
			_, e = o.Send(context.Background(), message(time.Now()))
			if step == "sync-dir" || step == "reopen" {
				if !errors.Is(e, ErrUnwritable) {
					t.Fatal(e)
				}
			} else if e != nil {
				t.Fatal(e)
			}
			recovered := mustOut(t, Config{Dir: crashImage(t, o.cfg.Dir), NodeID: "a"})
			if s, _ := recovered.Status(id); s != Pending {
				t.Fatal(s)
			}
		})
	}
}

func TestRecoveryRefusesSemanticallyCorruptConfirmedRecord(t *testing.T) {
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
	if _, err := o.Send(context.Background(), message(time.Now())); err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range []func(*transaction){func(tx *transaction) { tx.Upserts[0].Status = "UNKNOWN" }, func(tx *transaction) { tx.Upserts[0].Envelope.Payload = []byte("tampered") }, func(tx *transaction) { tx.MessageSeq = 0 }} {
		dir := crashImage(t, o.cfg.Dir)
		path := filepath.Join(dir, "delivery.log")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var tx transaction
		if err = json.Unmarshal(data, &tx); err != nil {
			t.Fatal(err)
		}
		corrupt(&tx)
		data, err = json.Marshal(tx)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		if b, err := NewOutbox(Config{Dir: dir, NodeID: "a"}); err == nil {
			_ = b.Close()
			t.Fatal("accepted corrupt confirmed record")
		}
	}
}
