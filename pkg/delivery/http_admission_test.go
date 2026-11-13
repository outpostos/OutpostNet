package delivery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
)

func TestHTTPAdmissionResultsAndCounters(t *testing.T) {
	peers := map[string]string{}
	b := newHTTPNode(t, "b", peers, func(c *delivery.Config, _ *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
		c.QuotaBytes = 2200
	})
	a := newHTTPNode(t, "a", peers, nil)
	env := func(seq uint64) delivery.Envelope {
		p := make([]byte, 800)
		return delivery.Envelope{MessageID: delivery.MessageID{SourceNodeID: "a", SourceIncarnation: "inc", Seq: seq}, DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: p, Checksum: delivery.Checksum(p), ExpiryTime: time.Now().Add(time.Hour)}
	}
	e := env(1)
	bad := env(2)
	bad.Checksum = "invalid"
	expired := env(3)
	expired.ExpiryTime = time.Now().Add(-time.Hour)
	batch := delivery.Batch{Envelopes: []delivery.Envelope{e, e, bad, expired, env(4)}}
	r, err := a.wire.Send(context.Background(), "b", batch)
	if err != nil {
		t.Fatal(err)
	}
	want := []delivery.ResultCode{delivery.Accepted, delivery.Duplicate, delivery.RejectedInvalid, delivery.RejectedExpired, delivery.Full}
	for i, code := range want {
		if r.Envelopes[i].Code != code || r.Envelopes[i].MessageID != batch.Envelopes[i].MessageID {
			t.Fatalf("result %d: %+v", i, r.Envelopes[i])
		}
	}
	s := b.in.DeliveryStatus()
	c := s.Peers[0].Counters
	if c.Duplicate != 1 || c.Rejected != 1 || c.Expired != 1 || c.InboxFull != 1 {
		t.Fatalf("counts: %+v", c)
	}
	if err := b.in.Ack(context.Background(), e.MessageID); err != nil {
		t.Fatal(err)
	}
	r, err = a.wire.Send(context.Background(), "b", delivery.Batch{Envelopes: []delivery.Envelope{env(4)}})
	if err != nil || r.Envelopes[0].Code != delivery.Accepted {
		t.Fatalf("space release: %+v %v", r, err)
	}
}

func TestDeliveryStatusRecoveryExpiryAndOutboxFull(t *testing.T) {
	now := time.Now()
	cfg := delivery.Config{Dir: t.TempDir(), NodeID: "a", QuotaBytes: 2200, Clock: func() time.Time { return now }}
	out, err := delivery.NewOutbox(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := delivery.Envelope{DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: make([]byte, 800), ExpiryTime: now.Add(time.Hour)}
	id, err := out.Send(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.Send(context.Background(), e); !errors.Is(err, delivery.ErrOutboxFull) {
		t.Fatalf("full: %v", err)
	}
	if out.DeliveryStatus().Peers[0].Counters.OutboxFull != 1 {
		t.Fatal("full uncounted")
	}
	if err := out.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Second)
	out, err = delivery.NewOutbox(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	s := out.DeliveryStatus()
	if s.Peers[0].PendingEnvelopes != 1 || s.Peers[0].OldestPendingAge != 5*time.Second || s.Peers[0].Counters.OutboxFull != 0 {
		t.Fatalf("recovery: %+v", s)
	}
	now = now.Add(time.Hour)
	if err := out.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status, err := out.Status(id); err != nil || status != delivery.Expired {
		t.Fatalf("expired: %s %v", status, err)
	}
	s = out.DeliveryStatus()
	if s.Peers[0].PendingEnvelopes != 0 || s.Peers[0].Counters.Expired != 1 {
		t.Fatalf("expired stats: %+v", s)
	}
}

func TestHTTPResponseBudgetRefusesBeforePersistence(t *testing.T) {
	peers := map[string]string{}
	b := newHTTPNode(t, "b", peers, func(_ *delivery.Config, _ *delivery.HTTPTransportConfig, h *delivery.HTTPHandlerConfig) {
		h.MaxResponseBytes = 100
	})
	a := newHTTPNode(t, "a", peers, nil)
	id := sendHTTP(t, a, "b", []byte("hello"))
	if err := a.out.Flush(context.Background()); err == nil {
		t.Fatal("expected response-budget refusal")
	}
	statusHTTP(t, a, id, delivery.Pending)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := b.in.Receive(ctx, "commands"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("committed before response reservation: %v", err)
	}
}
