package delivery

import (
	"context"
	"testing"
	"time"
)

func TestSendReceiveAckHandoff(t *testing.T) {
	ctx := context.Background()
	wire := NewMemoryTransport()
	out, err := NewOutbox(Config{Dir: t.TempDir(), NodeID: "sender", Transport: wire})
	if err != nil { t.Fatal(err) }
	defer out.Close()
	in, err := NewInbox(Config{Dir: t.TempDir(), NodeID: "receiver", Transport: wire})
	if err != nil { t.Fatal(err) }
	defer in.Close()
	wire.Register("receiver", in, nil)
	wire.Register("sender", nil, out)
	id, err := out.Send(ctx, Envelope{DestinationNodeID: "receiver", Mailbox: "commands", Category: "control", Priority: P1, IdempotencyKey: "business-1", CorrelationID: "request-1", ExpiryTime: time.Now().Add(time.Hour), Payload: []byte("hello"), WantHandoffReceipt: true})
	if err != nil { t.Fatal(err) }
	if err := out.Flush(ctx); err != nil { t.Fatal(err) }
	if s, err := out.Status(id); err != nil || s != Delivered { t.Fatalf("status = %s, %v", s, err) }
	d, err := in.Receive(ctx, "commands")
	if err != nil { t.Fatal(err) }
	if d.MessageID != id || string(d.Payload) != "hello" || d.IdempotencyKey != "business-1" || d.CorrelationID != "request-1" || d.ExpiryTime.IsZero() { t.Fatalf("delivery = %+v", d) }
	if err := in.Ack(ctx, id); err != nil { t.Fatal(err) }
	if err := in.Flush(ctx); err != nil { t.Fatal(err) }
	if s, err := out.Status(id); err != nil || s != HandedOff { t.Fatalf("status = %s, %v", s, err) }
}
