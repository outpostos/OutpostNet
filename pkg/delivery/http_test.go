package delivery_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestHTTPDurableHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var aURL, bURL string
	transport := func(node string) *delivery.HTTPTransport {
		wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{
			NodeID: node, AuthToken: "test-token",
			PeerProvider: state.PeerProviderFunc(func() []string { return []string{aURL, bURL} }),
			PeerIdentity: func(addr string) string {
				if addr == aURL {
					return "a"
				}
				if addr == bURL {
					return "b"
				}
				return ""
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	aOut, err := delivery.NewOutbox(delivery.Config{Dir: t.TempDir(), NodeID: "a", Transport: transport("a")})
	if err != nil {
		t.Fatal(err)
	}
	defer aOut.Close()
	bIn, err := delivery.NewInbox(delivery.Config{Dir: t.TempDir(), NodeID: "b", Transport: transport("b")})
	if err != nil {
		t.Fatal(err)
	}
	defer bIn.Close()
	serve := func(node string, in *delivery.Inbox, out *delivery.Outbox) *httptest.Server {
		handler, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: node, AuthToken: "test-token", Inbox: in, Outbox: out})
		if err != nil {
			t.Fatal(err)
		}
		return httptest.NewServer(handler)
	}
	a := serve("a", nil, aOut)
	defer a.Close()
	aURL = a.URL
	b := serve("b", bIn, nil)
	defer b.Close()
	bURL = b.URL
	id, err := aOut.Send(ctx, delivery.Envelope{DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: []byte("hello"), ExpiryTime: time.Now().Add(time.Hour), WantHandoffReceipt: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := aOut.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s, err := aOut.Status(id); err != nil || s != delivery.Delivered {
		t.Fatalf("status %s: %v", s, err)
	}
	e, err := bIn.Receive(ctx, "commands")
	if err != nil {
		t.Fatal(err)
	}
	if e.MessageID != id || string(e.Payload) != "hello" {
		t.Fatalf("delivery: %+v", e)
	}
	if err := bIn.Ack(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := bIn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s, err := aOut.Status(id); err != nil || s != delivery.HandedOff {
		t.Fatalf("status %s: %v", s, err)
	}
	// The same handler mounts next to state replication on a host-owned mux.
	mux := http.NewServeMux()
	h, _ := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: "a", AuthToken: "test-token", Outbox: aOut})
	mux.Handle(delivery.BatchPath, h)
}
