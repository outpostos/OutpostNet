package delivery_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type httpNode struct {
	inboxConfig delivery.Config
	in          *delivery.Inbox
	out         *delivery.Outbox
	wire        *delivery.HTTPTransport
	handler     http.Handler
	server      *httptest.Server
}

func newHTTPNode(t *testing.T, node string, peers map[string]string, change func(*delivery.Config, *delivery.HTTPTransportConfig, *delivery.HTTPHandlerConfig)) *httpNode {
	t.Helper()
	cfg := delivery.Config{NodeID: node}
	wc := delivery.HTTPTransportConfig{NodeID: node, AuthToken: "test-token", PeerProvider: state.PeerProviderFunc(func() []string {
		var all []string
		for _, addr := range peers {
			all = append(all, addr)
		}
		return all
	}), PeerIdentity: func(addr string) string {
		for id, a := range peers {
			if a == addr {
				return id
			}
		}
		return ""
	}}
	hc := delivery.HTTPHandlerConfig{NodeID: node, AuthToken: "test-token"}
	if change != nil {
		change(&cfg, &wc, &hc)
	}
	wire, err := delivery.NewHTTPTransport(wc)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Transport = wire
	cfg.Dir = t.TempDir()
	inboxConfig := cfg
	in, err := delivery.NewInbox(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { in.Close() })
	cfg.Dir = t.TempDir()
	out, err := delivery.NewOutbox(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { out.Close() })
	hc.Inbox = in
	hc.Outbox = out
	h, err := delivery.NewHTTPHandler(hc)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	peers[node] = s.URL
	return &httpNode{inboxConfig: inboxConfig, in: in, out: out, wire: wire, handler: h, server: s}
}
func sendHTTP(t *testing.T, n *httpNode, dest string, payload []byte) delivery.MessageID {
	t.Helper()
	id, err := n.out.Send(context.Background(), delivery.Envelope{DestinationNodeID: dest, Mailbox: "commands", Category: "test", Payload: payload, ExpiryTime: time.Now().Add(time.Hour), WantHandoffReceipt: true})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func statusHTTP(t *testing.T, n *httpNode, id delivery.MessageID, want delivery.Status) {
	t.Helper()
	s, err := n.out.Status(id)
	if err != nil || s != want {
		t.Fatalf("status %s, %v; want %s", s, err, want)
	}
}

func TestHTTPAuthenticationKeepsPending(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
		code   int
	}{
		{"token", func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") }, 401},
		{"source", func(r *http.Request) { r.Header.Set("X-Outpost-Node-ID", "impostor") }, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peers := map[string]string{}
			b := newHTTPNode(t, "b", peers, nil)
			var code int
			a := newHTTPNode(t, "a", peers, func(_ *delivery.Config, w *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
				w.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					tc.mutate(r)
					resp, err := http.DefaultTransport.RoundTrip(r)
					if err == nil {
						code = resp.StatusCode
					}
					return resp, err
				})}
			})
			id := sendHTTP(t, a, "b", []byte("hello"))
			if err := a.out.Flush(context.Background()); err == nil {
				t.Fatal("expected failure")
			}
			if code != tc.code {
				t.Fatalf("status %d", code)
			}
			statusHTTP(t, a, id, delivery.Pending)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if _, err := b.in.Receive(ctx, "commands"); err != context.DeadlineExceeded {
				t.Fatalf("unauthenticated delivery: %v", err)
			}
		})
	}
}

func TestHTTPWireBytePaging(t *testing.T) {
	peers := map[string]string{}
	b := newHTTPNode(t, "b", peers, nil)
	var sizes []int64
	var responseSizes []int
	a := newHTTPNode(t, "a", peers, func(_ *delivery.Config, w *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
		w.MaxRequestBytes = 1200
		w.MaxResponseBytes = 600
		w.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			sizes = append(sizes, r.ContentLength)
			resp, err := http.DefaultTransport.RoundTrip(r)
			if err != nil {
				return nil, err
			}
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}
			responseSizes = append(responseSizes, len(data))
			resp.Body = io.NopCloser(bytes.NewReader(data))
			return resp, nil
		})}
	})
	var ids []delivery.MessageID
	for range 8 {
		ids = append(ids, sendHTTP(t, a, "b", make([]byte, 180)))
	}
	if err := a.out.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sizes) < 2 {
		t.Fatalf("not paged: %v", sizes)
	}
	for _, n := range sizes {
		if n > 1200 {
			t.Fatalf("request %d", n)
		}
	}
	for _, n := range responseSizes {
		if n > 600 {
			t.Fatalf("response %d", n)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, id := range ids {
		statusHTTP(t, a, id, delivery.Delivered)
		e, err := b.in.Receive(ctx, "commands")
		if err != nil || e.MessageID != id {
			t.Fatalf("receive %v: %v", e.MessageID, err)
		}
		if err := b.in.Ack(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.in.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		statusHTTP(t, a, id, delivery.HandedOff)
	}
}

func TestHTTPReceiverByteLimitKeepsPending(t *testing.T) {
	peers := map[string]string{}
	newHTTPNode(t, "b", peers, func(_ *delivery.Config, _ *delivery.HTTPTransportConfig, h *delivery.HTTPHandlerConfig) {
		h.MaxRequestBytes = 500
	})
	var code int
	a := newHTTPNode(t, "a", peers, func(_ *delivery.Config, w *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
		w.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := http.DefaultTransport.RoundTrip(r)
			if err == nil {
				code = resp.StatusCode
			}
			return resp, err
		})}
	})
	id := sendHTTP(t, a, "b", make([]byte, 300))
	if err := a.out.Flush(context.Background()); err == nil {
		t.Fatal("expected 413")
	}
	if code != 413 {
		t.Fatalf("status %d", code)
	}
	statusHTTP(t, a, id, delivery.Pending)
	_, err := a.out.Send(context.Background(), delivery.Envelope{DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: make([]byte, 600<<10), ExpiryTime: time.Now().Add(time.Hour)})
	if !errors.Is(err, delivery.ErrEnvelopeTooLarge) {
		t.Fatalf("oversize Send: %v", err)
	}
}

func TestHTTPConnectionAndResponseLoss(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(fmt.Sprintf("persisted_%v", persist), func(t *testing.T) {
			peers := map[string]string{}
			b := newHTTPNode(t, "b", peers, nil)
			a := newHTTPNode(t, "a", peers, func(c *delivery.Config, _ *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
				c.RetryMin = time.Millisecond
				c.RetryMax = time.Millisecond
			})
			lost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if persist {
					b.handler.ServeHTTP(httptest.NewRecorder(), r)
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					panic(err)
				}
				conn.Close()
			}))
			defer lost.Close()
			peers["b"] = lost.URL
			id := sendHTTP(t, a, "b", []byte("once"))
			if err := a.out.Flush(context.Background()); err == nil {
				t.Fatal("lost reply accepted")
			}
			statusHTTP(t, a, id, delivery.Pending)
			peers["b"] = b.server.URL
			if persist {
				if err := b.in.Close(); err != nil {
					t.Fatal(err)
				}
				restored, err := delivery.NewInbox(b.inboxConfig)
				if err != nil {
					t.Fatal(err)
				}
				defer restored.Close()
				b.in = restored
				h, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: "b", AuthToken: "test-token", Inbox: restored, Outbox: b.out})
				if err != nil {
					t.Fatal(err)
				}
				recovered := httptest.NewServer(h)
				defer recovered.Close()
				peers["b"] = recovered.URL
			}
			time.Sleep(2 * time.Millisecond)
			if err := a.out.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			statusHTTP(t, a, id, delivery.Delivered)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			e, err := b.in.Receive(ctx, "commands")
			if err != nil || e.MessageID != id {
				t.Fatalf("receive: %v %v", e.MessageID, err)
			}
			if err := b.in.Ack(ctx, id); err != nil {
				t.Fatal(err)
			}
			empty, stop := context.WithTimeout(ctx, 10*time.Millisecond)
			defer stop()
			if _, err := b.in.Receive(empty, "commands"); err != context.DeadlineExceeded {
				t.Fatalf("duplicate delivery: %v", err)
			}
			snapshot := b.in.DeliveryStatus()
			if persist && snapshot.Peers[0].Counters.Duplicate != 1 {
				t.Fatalf("dedup: %+v", snapshot)
			}
		})
	}
}

func TestHTTPUnknownPeerBackoffRecoveryAndObservation(t *testing.T) {
	peers := map[string]string{}
	now := time.Now()
	clock := func() time.Time { return now }
	a := newHTTPNode(t, "a", peers, func(c *delivery.Config, _ *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
		c.Clock = clock
		c.RetryMin = time.Second
		c.RetryMax = 2 * time.Second
	})
	id := sendHTTP(t, a, "b", []byte("queued"))
	now = now.Add(3 * time.Second)
	if err := a.out.Flush(context.Background()); !errors.Is(err, delivery.ErrUnknownPeer) {
		t.Fatalf("unknown: %v", err)
	}
	s := a.out.DeliveryStatus()
	if len(s.Peers) != 1 || s.Peers[0].PendingEnvelopes != 1 || s.Peers[0].PendingBytes < 6 || s.Peers[0].OldestPendingAge != 3*time.Second || s.Peers[0].Counters.UnknownPeer != 1 || s.Peers[0].Counters.SendFailure != 1 {
		t.Fatalf("status: %+v", s)
	}
	if err := a.out.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.out.DeliveryStatus().Peers[0].Counters.UnknownPeer != 1 {
		t.Fatal("backoff counted as attempt")
	}
	b := newHTTPNode(t, "b", peers, nil)
	peers["b"] = "http://127.0.0.1:0"
	now = now.Add(time.Second)
	if err := a.out.Flush(context.Background()); err == nil {
		t.Fatal("unreachable succeeded")
	}
	statusHTTP(t, a, id, delivery.Pending)
	peers["b"] = b.server.URL
	now = now.Add(2 * time.Second)
	if err := a.out.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	statusHTTP(t, a, id, delivery.Delivered)
	s = a.out.DeliveryStatus()
	if s.Peers[0].PendingEnvelopes != 0 || s.Peers[0].Counters.SendSuccess != 1 || s.Peers[0].Counters.SendFailure != 2 {
		t.Fatalf("recovered: %+v", s)
	}
}

func TestHTTPHandoffReceiptResponseLoss(t *testing.T) {
	peers := map[string]string{}
	a := newHTTPNode(t, "a", peers, nil)
	b := newHTTPNode(t, "b", peers, func(c *delivery.Config, _ *delivery.HTTPTransportConfig, _ *delivery.HTTPHandlerConfig) {
		c.RetryMin = time.Millisecond
		c.RetryMax = time.Millisecond
	})
	id := sendHTTP(t, a, "b", []byte("once"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.out.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	e, err := b.in.Receive(ctx, "commands")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.in.Ack(ctx, e.MessageID); err != nil {
		t.Fatal(err)
	}
	lost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.handler.ServeHTTP(httptest.NewRecorder(), r)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			panic(err)
		}
		conn.Close()
	}))
	defer lost.Close()
	peers["a"] = lost.URL
	if err := b.in.Flush(ctx); err == nil {
		t.Fatal("lost receipt response accepted")
	}
	statusHTTP(t, a, id, delivery.HandedOff)
	if b.in.DeliveryStatus().Peers[0].PendingReceipts != 1 {
		t.Fatal("unconfirmed receipt discarded")
	}
	peers["a"] = a.server.URL
	time.Sleep(2 * time.Millisecond)
	if err := b.in.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if b.in.DeliveryStatus().Peers[0].PendingReceipts != 0 || a.out.DeliveryStatus().Peers[0].Counters.Duplicate != 1 {
		t.Fatal("receipt retry did not deduplicate")
	}
	statusHTTP(t, a, id, delivery.HandedOff)
}
