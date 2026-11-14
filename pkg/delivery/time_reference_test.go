package delivery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestTimeReferenceDeliveryAdmissionExpiryAndRestart(t *testing.T) {
	ctx := context.Background()
	mono := time.Unix(0, 0)
	ref, err := state.NewTimeReference(state.TimeReferenceConfig{SystemClock: state.PhysicalClockFunc(func() int64 { return 1_000_000 }), MonotonicNow: func() time.Time { return mono }})
	if err != nil {
		t.Fatal(err)
	}
	// Envelopes written before a reference is known retain their original expiry.
	wire := delivery.NewMemoryTransport()
	outDir, inDir := t.TempDir(), t.TempDir()
	out, err := delivery.NewOutbox(delivery.Config{Dir: outDir, NodeID: "sender", TimeReference: ref, Transport: wire})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { out.Close() }()
	oldID, err := out.Send(ctx, delivery.Envelope{DestinationNodeID: "receiver", Mailbox: "commands", Category: "control", ExpiryTime: ref.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	ref.ObservePeer("healthy", state.TimeSample{SentPhysicalMS: 31_537_000_000, TimeQuality: state.TimeSynced}, 0)
	in, err := delivery.NewInbox(delivery.Config{Dir: inDir, NodeID: "receiver", TimeReference: ref})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	wire.Register("receiver", in, nil)
	id, err := out.Send(ctx, delivery.Envelope{DestinationNodeID: "receiver", Mailbox: "commands", Category: "control", ExpiryTime: ref.Now().Add(time.Second), Payload: []byte("current")})
	if err != nil {
		t.Fatalf("reference expiry rejected: %v", err)
	}
	if err := out.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if status, err := out.Status(oldID); err != nil || status != delivery.Expired {
		t.Fatalf("old expiry was rewritten or retained: %s %v", status, err)
	}
	envelope, err := in.Receive(ctx, "commands")
	if err != nil || envelope.MessageID != id {
		t.Fatalf("reference envelope not delivered: %+v %v", envelope, err)
	}
	if err := in.Nack(id); err != nil {
		t.Fatal(err)
	}
	out.Close()
	out, err = delivery.NewOutbox(delivery.Config{Dir: outDir, NodeID: "sender", TimeReference: ref, Transport: wire})
	if err != nil {
		t.Fatal(err)
	}
	mono = mono.Add(2 * time.Second)
	results, err := in.Accept(ctx, []delivery.Envelope{envelope})
	if err != nil || results[0].Code != delivery.RejectedExpired {
		t.Fatalf("reference expiry not used: %+v %v", results, err)
	}
	expiredCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := in.Receive(expiredCtx, "commands"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired envelope delivered: %v", err)
	}
}
