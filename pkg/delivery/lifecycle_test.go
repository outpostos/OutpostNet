package delivery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func mustOut(t *testing.T, c Config) *Outbox {
	t.Helper()
	o, e := NewOutbox(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = o.Close() })
	return o
}
func mustIn(t *testing.T, c Config) *Inbox {
	t.Helper()
	in, e := NewInbox(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = in.Close() })
	return in
}
func message(now time.Time) Envelope {
	return Envelope{DestinationNodeID: "b", Mailbox: "m", Category: "event", ExpiryTime: now.Add(time.Hour), Payload: []byte("data")}
}
func external(now time.Time, seq uint64) Envelope {
	e := message(now)
	e.MessageID = MessageID{"a", "incarnation", seq}
	e.Checksum = Checksum(e.Payload)
	return e
}
func noDelivery(t *testing.T, in *Inbox) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, e := in.Receive(ctx, "m")
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("unexpected delivery: %v", e)
	}
}

func TestExpiryDedupAndCollection(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	wire := NewMemoryTransport()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Clock: clock, Transport: wire})
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", Clock: clock, ClockSkew: time.Minute})
	wire.Register("b", in, nil)
	e := external(now, 1)
	for n := 0; n < 50; n++ {
		r, err := in.Accept(ctx, []Envelope{e, e})
		if err != nil {
			t.Fatal(err)
		}
		want := Duplicate
		if n == 0 {
			want = Accepted
		}
		if r[0].Code != want || r[1].Code != Duplicate {
			t.Fatal(r)
		}
	}
	d, err := in.Receive(ctx, "m")
	if err != nil || !d.ExpiryTime.Equal(e.ExpiryTime) {
		t.Fatalf("%+v %v", d, err)
	}
	if err = in.Ack(ctx, d.MessageID); err != nil {
		t.Fatal(err)
	}
	noDelivery(t, in)
	// Dedup survives consumer Ack, until expiry + tolerance.
	r, err := in.Accept(ctx, []Envelope{e})
	if err != nil || r[0].Code != Duplicate {
		t.Fatalf("%v %v", r, err)
	}
	id, err := o.Send(ctx, message(now))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if err = o.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := o.Status(id); s != Expired {
		t.Fatalf("status %s", s)
	}
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	noDelivery(t, in)
	// A new, already expired arrival is rejected at expiry + skew.
	now = now.Add(time.Minute)
	r, err = in.Accept(ctx, []Envelope{e})
	if err != nil || r[0].Code != RejectedExpired {
		t.Fatalf("%v %v", r, err)
	}
	if err = in.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	// Clock reversal is only a test probe: after collection the ID is absent.
	now = now.Add(-time.Minute)
	r, err = in.Accept(ctx, []Envelope{e})
	if err != nil || r[0].Code != Accepted {
		t.Fatalf("dedup not collected: %v %v", r, err)
	}
}

func TestInboxNeverDeliversExpiredQueuedEnvelope(t *testing.T) {
	now := time.Now()
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", Clock: func() time.Time { return now }, ClockSkew: time.Minute})
	if _, e := in.Accept(context.Background(), []Envelope{external(now, 1)}); e != nil {
		t.Fatal(e)
	}
	now = now.Add(time.Hour)
	noDelivery(t, in)
}

type transportFunc func(context.Context, string, Batch) (BatchResult, error)

func (f transportFunc) Send(c context.Context, d string, b Batch) (BatchResult, error) {
	return f(c, d, b)
}

func TestQuotaBackoffAndPayloadRelease(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	clock := func() time.Time { return now }
	wire := NewMemoryTransport()
	calls := 0
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Clock: clock, QuotaBytes: 3000, Transport: transportFunc(func(c context.Context, d string, b Batch) (BatchResult, error) { calls++; return wire.Send(c, d, b) })})
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", Clock: clock, QuotaBytes: 3000})
	wire.Register("b", in, nil)
	e := message(now)
	e.Payload = []byte(strings.Repeat("x", 1200))
	id, err := o.Send(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.Send(ctx, e); !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("quota error %v", err)
	}
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	id2, err := o.Send(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := o.Status(id2); s != Pending {
		t.Fatal(s)
	}
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("full was not backed off: %d calls", calls)
	}
	d, err := in.Receive(ctx, "m")
	if err != nil || d.MessageID != id {
		t.Fatalf("%v %v", d, err)
	}
	if err = in.Ack(ctx, id); err != nil {
		t.Fatal(err)
	}
	now = now.Add(100 * time.Millisecond)
	if err = o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	d, err = in.Receive(ctx, "m")
	if err != nil || d.MessageID != id2 {
		t.Fatalf("%v %v", d, err)
	}
	if err = in.Ack(ctx, id2); err != nil {
		t.Fatal(err)
	}
	noDelivery(t, in)
}

func TestPriorityByteBatchesAndTransportErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	var seen []Envelope
	var sizes []int
	fail := true
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", MaxEnvelopeBytes: 1000, MaxBatchBytes: 1032, Clock: func() time.Time { return now }, Transport: transportFunc(func(_ context.Context, _ string, b Batch) (BatchResult, error) {
		if fail {
			return BatchResult{}, errors.New("offline")
		}
		sizes = append(sizes, encodedSize(b))
		seen = append(seen, b.Envelopes...)
		r := BatchResult{}
		for _, e := range b.Envelopes {
			r.Envelopes = append(r.Envelopes, Result{e.MessageID, Accepted})
		}
		return r, nil
	})})
	for _, p := range []Priority{P3, P0, P1, P0} {
		e := message(now)
		e.Priority = p
		e.Payload = []byte(strings.Repeat("x", 200))
		if _, err := o.Send(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Flush(ctx); err == nil {
		t.Fatal("lost transport error")
	}
	fail = false
	if err := o.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 0 {
		t.Fatal("retry ignored backoff")
	}
	for n := 0; n < 4; n++ {
		now = now.Add(time.Second)
		if err := o.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 4 {
		t.Fatal(len(seen))
	}
	for i, want := range []uint64{2, 4, 3, 1} {
		if seen[i].MessageID.Seq != want {
			t.Fatal(seen)
		}
	}
	for _, n := range sizes {
		if n > 1032 {
			t.Fatal(n)
		}
	}
}

func TestEnvelopeValidation(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a", Clock: func() time.Time { return now }, MaxEnvelopeBytes: 1000, MaxBatchBytes: 1032})
	for _, mutate := range []func(*Envelope){func(e *Envelope) { e.ExpiryTime = time.Time{} }, func(e *Envelope) { e.ExpiryTime = now.Add(73 * time.Hour) }, func(e *Envelope) { e.Mailbox = "" }, func(e *Envelope) { e.Priority = 4 }, func(e *Envelope) { e.Payload = make([]byte, 1000) }} {
		e := message(now)
		mutate(&e)
		if _, err := o.Send(ctx, e); err == nil {
			t.Fatal("accepted invalid envelope")
		}
	}
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", Clock: func() time.Time { return now }})
	e := external(now, 1)
	e.Checksum = "bad"
	r, err := in.Accept(ctx, []Envelope{e})
	if err != nil || r[0].Code != RejectedInvalid {
		t.Fatalf("%v %v", r, err)
	}
}
