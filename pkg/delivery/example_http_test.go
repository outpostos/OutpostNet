package delivery_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
	"github.com/outpostos/OutpostNet/pkg/state"
)

// Two independent durable nodes share only an HTTP channel and a host-owned
// membership/identity mapping. In a gateway, use its existing replication mux
// and membership instead of httptest servers and this in-process directory.
func ExampleHTTPTransport() {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	dir, err := os.MkdirTemp("", "delivery-example-")
	check(err)
	defer os.RemoveAll(dir)
	peers := map[string]string{}
	type node struct {
		in     *delivery.Inbox
		out    *delivery.Outbox
		server *httptest.Server
	}
	open := func(id string) node {
		wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{
			NodeID: id, AuthToken: "example-only-token",
			PeerProvider: state.PeerProviderFunc(func() []string {
				all := make([]string, 0, len(peers))
				for _, addr := range peers {
					all = append(all, addr)
				}
				return all
			}),
			PeerIdentity: func(addr string) string {
				for nodeID, a := range peers {
					if a == addr {
						return nodeID
					}
				}
				return ""
			},
		})
		check(err)
		cfg := delivery.Config{NodeID: id, Dir: filepath.Join(dir, id, "inbox"), Transport: wire}
		in, err := delivery.NewInbox(cfg)
		check(err)
		cfg.Dir = filepath.Join(dir, id, "outbox")
		out, err := delivery.NewOutbox(cfg)
		check(err)
		h, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: id, AuthToken: "example-only-token", Inbox: in, Outbox: out})
		check(err)
		server := httptest.NewServer(h)
		peers[id] = server.URL
		return node{in, out, server}
	}
	a, b := open("a"), open("b")
	defer a.in.Close()
	defer a.out.Close()
	defer a.server.Close()
	defer b.in.Close()
	defer b.out.Close()
	defer b.server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exchange := func(from, to node, dest, payload string) delivery.MessageID {
		id, err := from.out.Send(ctx, delivery.Envelope{DestinationNodeID: dest, Mailbox: "events", Category: "example", ExpiryTime: time.Now().Add(time.Hour), Payload: []byte(payload), WantHandoffReceipt: true})
		check(err)
		check(from.out.Flush(ctx))
		e, err := to.in.Receive(ctx, "events")
		check(err)
		fmt.Printf("%s -> %s: %s\n", e.MessageID.SourceNodeID, e.DestinationNodeID, e.Payload)
		// Commit business changes and processed ID in the consumer's own transaction
		// before Ack. This example only prints the payload.
		check(to.in.Ack(ctx, e.MessageID))
		check(to.in.Flush(ctx))
		return id
	}
	first := exchange(a, b, "b", "hello")
	second := exchange(b, a, "a", "reply")
	sa, err := a.out.Status(first)
	check(err)
	sb, err := b.out.Status(second)
	check(err)
	fmt.Println(sa, sb)
	// Output:
	// a -> b: hello
	// b -> a: reply
	// HANDED_OFF HANDED_OFF
}
