package delivery_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
	"github.com/outpostos/OutpostNet/pkg/state"
	"github.com/outpostos/OutpostNet/pkg/state/httpauth"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func httpGroup(t *testing.T) func(string) *tlsutil.Identity {
	t.Helper()
	c, k, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := tls.X509KeyPair(c, k)
	if err != nil {
		t.Fatal(err)
	}
	return func(node string) *tlsutil.Identity {
		t.Helper()
		c, k, err := tlsutil.IssueNode(ca, node, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(c, k)
		if err != nil {
			t.Fatal(err)
		}
		id, err := tlsutil.NewIdentity(cert, mustCAPEM(ca), node, nil)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
}

func certificateHTTPServer(t *testing.T, id *tlsutil.Identity, h http.Handler) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.TLS = httpauth.TLSConfig(id, true)
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

func TestCertificateHTTPReplication(t *testing.T) {
	group := httpGroup(t)
	a, b := group("a"), group("b")
	store, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	syncer, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := state.NewGossipEngine(state.GossipConfig{NodeID: "b", Store: store, Syncer: syncer, PeerProvider: state.PeerProviderFunc(func() []string { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "b", Engine: engine, AuthMode: httpauth.Cert, Identity: b})
	if err != nil {
		t.Fatal(err)
	}
	srv := certificateHTTPServer(t, b, handler)
	source, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := source.Put(context.Background(), "hello", []byte("secure"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := syncer.Push(context.Background(), srv.URL, state.PushRequest{Entries: []state.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	entries, err := syncer.Pull(context.Background(), strings.TrimPrefix(srv.URL, "https://"), state.PullRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || string(entries[0].Value) != "secure" {
		t.Fatalf("pull: %+v", entries)
	}
	if state.StateProtocolVersion != (state.ProtocolVersion{Major: 3, Minor: 0}) {
		t.Fatal("protocol changed")
	}
}

func mustCAPEM(ca tls.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Certificate[0]})
}

func TestCertificateHTTPDeliveryHandoff(t *testing.T) {
	for _, mode := range []httpauth.Mode{httpauth.Cert, httpauth.TokenCert} {
		t.Run(string(mode), func(t *testing.T) {
			group := httpGroup(t)
			a, b := group("a"), group("b")
			var aURL, bURL string
			wire := func(node string, id *tlsutil.Identity) *delivery.HTTPTransport {
				w, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: node, AuthMode: mode, Identity: id, AuthToken: "transition-token", PeerProvider: state.PeerProviderFunc(func() []string { return []string{aURL, bURL} }), PeerIdentity: func(addr string) string {
					if addr == aURL {
						return "a"
					}
					if addr == bURL {
						return "b"
					}
					return ""
				}})
				if err != nil {
					t.Fatal(err)
				}
				return w
			}
			aOut, err := delivery.NewOutbox(delivery.Config{Dir: t.TempDir(), NodeID: "a", Transport: wire("a", a)})
			if err != nil {
				t.Fatal(err)
			}
			defer aOut.Close()
			bDir := t.TempDir()
			bIn, err := delivery.NewInbox(delivery.Config{Dir: bDir, NodeID: "b", Transport: wire("b", b)})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { bIn.Close() }()
			serve := func(node string, id *tlsutil.Identity, in *delivery.Inbox, out *delivery.Outbox) *httptest.Server {
				h, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: node, AuthMode: mode, Identity: id, AuthToken: "transition-token", Inbox: in, Outbox: out})
				if err != nil {
					t.Fatal(err)
				}
				return certificateHTTPServer(t, id, h)
			}
			aURL = serve("a", a, nil, aOut).URL
			bURL = serve("b", b, bIn, nil).URL
			ctx := context.Background()
			id, err := aOut.Send(ctx, delivery.Envelope{DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: []byte("secure"), ExpiryTime: time.Now().Add(time.Hour), WantHandoffReceipt: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := aOut.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := bIn.Close(); err != nil {
				t.Fatal(err)
			}
			bIn, err = delivery.NewInbox(delivery.Config{Dir: bDir, NodeID: "b", Transport: wire("b", b)})
			if err != nil {
				t.Fatal(err)
			}
			bURL = serve("b", b, bIn, nil).URL
			e, err := bIn.Receive(ctx, "commands")
			if err != nil {
				t.Fatal(err)
			}
			if e.MessageID != id || string(e.Payload) != "secure" {
				t.Fatal("wrong delivery")
			}
			if err := bIn.Ack(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err := bIn.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if status, err := aOut.Status(id); err != nil || status != delivery.HandedOff {
				t.Fatalf("handoff %s: %v", status, err)
			}
		})
	}
}

func TestCertificateHTTPRejectsDeliveryBatchBeforeAdmission(t *testing.T) {
	group := httpGroup(t)
	a, b := group("a"), group("b")
	in, err := delivery.NewInbox(delivery.Config{Dir: t.TempDir(), NodeID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	h, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: "b", AuthMode: httpauth.Cert, Identity: b, Inbox: in})
	if err != nil {
		t.Fatal(err)
	}
	srv := certificateHTTPServer(t, b, h)
	valid := delivery.Envelope{MessageID: delivery.MessageID{SourceNodeID: "a", SourceIncarnation: "inc", Seq: 1}, DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: []byte("valid"), Checksum: delivery.Checksum([]byte("valid")), ExpiryTime: time.Now().Add(time.Hour)}
	wrong := valid
	wrong.MessageID.Seq = 2
	wrong.DestinationNodeID = "c"
	status := certificatePost(t, certificateClient(a, a), srv.URL+delivery.BatchPath, "a", "", map[string]any{"protocol_version": delivery.DeliveryProtocolVersion, "source": "a", "envelopes": []delivery.Envelope{valid, wrong}})
	if status != 403 {
		t.Fatalf("wrong destination batch status %d, want 403", status)
	}
	if len(in.DeliveryStatus().Peers) != 0 {
		t.Fatal("partially admitted unauthorized batch")
	}
	if b.Stats().ClaimRejected != 1 {
		t.Fatalf("counters: %+v", b.Stats())
	}
}

func certificateClient(trust, credential *tlsutil.Identity) *http.Client {
	cfg := httpauth.TLSConfig(trust, false)
	if credential == nil {
		cfg.Certificates = nil
	} else {
		cfg.Certificates = httpauth.TLSConfig(credential, false).Certificates
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 3 * time.Second}
}
func certificatePost(t *testing.T, client *http.Client, url, source, token string, body any) int {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Outpost-Node-ID", source)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func replicationCertificateHandler(t *testing.T, id *tlsutil.Identity, mode httpauth.Mode, token string) (http.Handler, *state.MemoryStore) {
	t.Helper()
	store, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: id.NodeID()})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: id.NodeID(), AuthMode: mode, Identity: id, AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := state.NewGossipEngine(state.GossipConfig{NodeID: id.NodeID(), Store: store, Syncer: wire})
	if err != nil {
		t.Fatal(err)
	}
	h, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: id.NodeID(), Engine: engine, AuthMode: mode, Identity: id, AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}
	return h, store
}

func TestCertificateHTTPRejectsUntrustedAndForgedSources(t *testing.T) {
	for _, mode := range []httpauth.Mode{httpauth.Cert, httpauth.TokenCert} {
		t.Run(string(mode), func(t *testing.T) {
			group := httpGroup(t)
			a, b := group("a"), group("b")
			foreign := httpGroup(t)("a")
			stateHandler, store := replicationCertificateHandler(t, b, mode, "transition-token")
			source, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "a"})
			if err != nil {
				t.Fatal(err)
			}
			entry, err := source.Put(context.Background(), "must-not-appear", []byte("forbidden"))
			if err != nil {
				t.Fatal(err)
			}
			inbox, err := delivery.NewInbox(delivery.Config{Dir: t.TempDir(), NodeID: "b"})
			if err != nil {
				t.Fatal(err)
			}
			defer inbox.Close()
			dh, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: "b", Inbox: inbox, AuthMode: mode, Identity: b, AuthToken: "transition-token"})
			if err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			mux.Handle(state.PushPath, stateHandler)
			mux.Handle(state.PullPath, stateHandler)
			mux.Handle(delivery.BatchPath, dh)
			srv := certificateHTTPServer(t, b, mux)
			for _, path := range []string{state.PushPath, state.PullPath, delivery.BatchPath} {
				t.Run(path, func(t *testing.T) {
					version := state.StateProtocolVersion
					if path == delivery.BatchPath {
						version = delivery.DeliveryProtocolVersion
					}
					for _, tc := range []struct {
						name, source, header, token string
						credential                  *tlsutil.Identity
						status                      int
						certificate                 bool
					}{
						{"missing_certificate", "a", "a", "transition-token", nil, 0, true},
						{"foreign_group", "a", "a", "transition-token", foreign, 0, true},
						{"forged_source", "c", "c", "transition-token", a, 403, false},
						{"forged_header", "a", "c", "transition-token", a, 403, false},
						{"missing_source", "", "a", "transition-token", a, 403, false},
					} {
						t.Run(tc.name, func(t *testing.T) {
							before := b.Stats()
							code := certificatePost(t, certificateClient(a, tc.credential), srv.URL+path, tc.header, tc.token, map[string]any{"protocol_version": version, "source": tc.source, "policy_id": state.NewHLCLWWPolicy().ID(), "entries": []state.Entry{entry}})
							if code != tc.status {
								t.Fatalf("status %d, want %d", code, tc.status)
							}
							after := b.Stats()
							if tc.certificate {
								if after.CertificateRejected != before.CertificateRejected+1 {
									t.Fatalf("certificate counters: %+v -> %+v", before, after)
								}
							} else if after.ClaimRejected != before.ClaimRejected+1 {
								t.Fatalf("claim counters: %+v -> %+v", before, after)
							}
						})
					}
					if mode == httpauth.TokenCert {
						code := certificatePost(t, certificateClient(a, a), srv.URL+path, "a", "wrong", map[string]any{"protocol_version": version, "source": "a", "policy_id": state.NewHLCLWWPolicy().ID(), "entries": []state.Entry{entry}})
						if code != 401 {
							t.Fatalf("token+cert accepted wrong token: %d", code)
						}
					}
				})
			}
			if store.LocalStatus().CurrentPosition != 0 || len(inbox.DeliveryStatus().Peers) != 0 {
				t.Fatal("rejected request changed state")
			}
			// A mistakenly exposed plain listener cannot authenticate from proxy headers.
			plain := httptest.NewServer(mux)
			defer plain.Close()
			if code := certificatePost(t, plain.Client(), plain.URL+state.PushPath, "a", "transition-token", map[string]any{}); code != 403 {
				t.Fatalf("plain listener accepted: %d", code)
			}
			// Even a TLS listener that accepts arbitrary client certs is checked again.
			weak := httptest.NewUnstartedServer(mux)
			weak.TLS = httpauth.TLSConfig(b, true)
			weak.TLS.VerifyPeerCertificate = nil
			weak.TLS.ClientAuth = tls.RequestClientCert
			weak.StartTLS()
			defer weak.Close()
			if code := certificatePost(t, certificateClient(a, foreign), weak.URL+delivery.BatchPath, "a", "transition-token", map[string]any{}); code != 403 {
				t.Fatalf("unverified listener accepted foreign group: %d", code)
			}
		})
	}
}

func TestCertificateHTTPBindsResponseIdentity(t *testing.T) {
	group := httpGroup(t)
	a, b, c := group("a"), group("b"), group("c")
	for _, status := range []int{200, 410} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				json.NewEncoder(w).Encode(map[string]any{"protocol_version": state.StateProtocolVersion, "policy_id": state.NewHLCLWWPolicy().ID(), "node_id": "c", "gc_horizon": state.Timestamp{WallMS: 1}})
			})
			srv := certificateHTTPServer(t, b, h)
			syncer, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a})
			if err != nil {
				t.Fatal(err)
			}
			before := a.Stats().ClaimRejected
			if _, err := syncer.Push(context.Background(), srv.URL, state.PushRequest{}); err == nil {
				t.Fatal("forged responder accepted")
			}
			if _, err := syncer.Pull(context.Background(), srv.URL, state.PullRequest{}); err == nil {
				t.Fatal("forged pull responder accepted")
			}
			if a.Stats().ClaimRejected != before+2 {
				t.Fatal("response refusals not counted")
			}
		})
	}
	// A valid group node C cannot impersonate the mapped destination B in receipts.
	srv := certificateHTTPServer(t, c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"protocol_version": delivery.DeliveryProtocolVersion, "node_id": "b"})
	}))
	wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a, PeerProvider: state.NewStaticPeerProvider([]string{srv.URL}), PeerIdentity: func(string) string { return "b" }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wire.Send(context.Background(), "b", delivery.Batch{Receipts: []delivery.Receipt{{MessageID: delivery.MessageID{SourceNodeID: "b", SourceIncarnation: "inc", Seq: 1}, ReceiverNodeID: "a"}}}); err == nil {
		t.Fatal("wrong certificate destination accepted")
	}
}

func TestCertificateHTTPClientsRefuseDowngradeAndRedirect(t *testing.T) {
	group := httpGroup(t)
	a, b := group("a"), group("b")
	var calls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer target.Close()
	redirect := certificateHTTPServer(t, b, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	for _, address := range []string{target.URL, redirect.URL} {
		syncer, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.TokenCert, AuthToken: "transition-token", Identity: a})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := syncer.Push(context.Background(), address, state.PushRequest{}); err == nil {
			t.Fatal("unsafe push succeeded")
		}
		if _, err := syncer.Pull(context.Background(), address, state.PullRequest{}); err == nil {
			t.Fatal("unsafe pull succeeded")
		}
		wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: "a", AuthMode: httpauth.TokenCert, AuthToken: "transition-token", Identity: a, PeerProvider: state.NewStaticPeerProvider([]string{address}), PeerIdentity: func(string) string { return "b" }})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wire.Send(context.Background(), "b", delivery.Batch{Receipts: []delivery.Receipt{{MessageID: delivery.MessageID{SourceNodeID: "b", SourceIncarnation: "inc", Seq: 1}, ReceiverNodeID: "a"}}}); err == nil {
			t.Fatal("unsafe delivery succeeded")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("sent %d requests to plaintext/redirect target", calls.Load())
	}
}

func TestCertificateHTTPClientsRejectForeignServers(t *testing.T) {
	a := httpGroup(t)("a")
	foreign := httpGroup(t)("b")
	srv := certificateHTTPServer(t, foreign, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted server received application request") }))
	syncer, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a})
	if err != nil {
		t.Fatal(err)
	}
	before := a.Stats().CertificateRejected
	if _, err := syncer.Pull(context.Background(), srv.URL, state.PullRequest{}); err == nil {
		t.Fatal("untrusted server accepted")
	}
	wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a, PeerProvider: state.NewStaticPeerProvider([]string{srv.URL}), PeerIdentity: func(string) string { return "b" }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wire.Send(context.Background(), "b", delivery.Batch{Receipts: []delivery.Receipt{{MessageID: delivery.MessageID{SourceNodeID: "b", SourceIncarnation: "inc", Seq: 1}, ReceiverNodeID: "a"}}}); err == nil {
		t.Fatal("untrusted delivery server accepted")
	}
	if a.Stats().CertificateRejected != before+2 {
		t.Fatalf("client counter: %+v", a.Stats())
	}
}

func TestCertificateHTTPRejectsForgedEnvelopeAndReceipt(t *testing.T) {
	group := httpGroup(t)
	a, b := group("a"), group("b")
	in, err := delivery.NewInbox(delivery.Config{Dir: t.TempDir(), NodeID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := delivery.NewOutbox(delivery.Config{Dir: t.TempDir(), NodeID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	handler, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: "b", AuthMode: httpauth.Cert, Identity: b, Inbox: in, Outbox: out})
	if err != nil {
		t.Fatal(err)
	}
	srv := certificateHTTPServer(t, b, handler)
	for _, items := range []map[string]any{
		{"envelopes": []delivery.Envelope{{MessageID: delivery.MessageID{SourceNodeID: "c"}, DestinationNodeID: "b"}}},
		{"receipts": []delivery.Receipt{{MessageID: delivery.MessageID{SourceNodeID: "b"}, ReceiverNodeID: "c"}}},
		{"receipts": []delivery.Receipt{{MessageID: delivery.MessageID{SourceNodeID: "c"}, ReceiverNodeID: "a"}}},
	} {
		items["protocol_version"] = delivery.DeliveryProtocolVersion
		items["source"] = "a"
		before := b.Stats().ClaimRejected
		if code := certificatePost(t, certificateClient(a, a), srv.URL+delivery.BatchPath, "a", "", items); code != 403 {
			t.Fatalf("forgery status %d", code)
		}
		if b.Stats().ClaimRejected != before+1 {
			t.Fatal("forgery not counted")
		}
	}
	if len(in.DeliveryStatus().Peers) != 0 || len(out.DeliveryStatus().Peers) != 0 {
		t.Fatal("forgery persisted")
	}
}

func TestCertificateHTTPConcurrentReplication(t *testing.T) {
	group := httpGroup(t)
	a, b := group("a"), group("b")
	h, store := replicationCertificateHandler(t, b, httpauth.Cert, "")
	srv := certificateHTTPServer(t, b, h)
	wire, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a})
	if err != nil {
		t.Fatal(err)
	}
	source, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		entry, err := source.Put(context.Background(), fmt.Sprint(n), []byte("secure"))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := wire.Push(context.Background(), srv.URL, state.PushRequest{Entries: []state.Entry{entry}}); err != nil {
				t.Error(err)
			}
			_ = b.Stats()
		}()
	}
	wg.Wait()
	entries, err := store.ListEntries(context.Background())
	if err != nil || len(entries) != 16 {
		t.Fatalf("concurrent entries=%d: %v", len(entries), err)
	}
}

func TestCertificateHTTPConfigurationFailsClosed(t *testing.T) {
	group := httpGroup(t)
	a, b := group("a"), group("b")
	store, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	syncer, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := state.NewGossipEngine(state.GossipConfig{NodeID: "a", Store: store, Syncer: syncer})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := delivery.NewInbox(delivery.Config{Dir: t.TempDir(), NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	defer inbox.Close()
	for _, tc := range []struct {
		name  string
		mode  httpauth.Mode
		token string
		id    *tlsutil.Identity
	}{
		{"unknown_mode", "typo", "token", a},
		{"missing_certificate", httpauth.Cert, "", nil},
		{"wrong_local_identity", httpauth.Cert, "", b},
		{"transition_without_token", httpauth.TokenCert, "", a},
		{"certificate_without_explicit_mode", "", "token", a},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: tc.mode, AuthToken: tc.token, Identity: tc.id}); err == nil {
				t.Fatal("state client accepted invalid auth configuration")
			}
			if _, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "a", AuthMode: tc.mode, AuthToken: tc.token, Identity: tc.id, Engine: engine}); err == nil {
				t.Fatal("state handler accepted invalid auth configuration")
			}
			if _, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: "a", AuthMode: tc.mode, AuthToken: tc.token, Identity: tc.id, PeerProvider: state.NewStaticPeerProvider(nil), PeerIdentity: func(string) string { return "b" }}); err == nil {
				t.Fatal("delivery client accepted invalid auth configuration")
			}
			if _, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{NodeID: "a", AuthMode: tc.mode, AuthToken: tc.token, Identity: tc.id, Inbox: inbox}); err == nil {
				t.Fatal("delivery handler accepted invalid auth configuration")
			}
		})
	}
	if _, err := state.NewHTTPHandler(state.HTTPHandlerConfig{NodeID: "b", AuthMode: httpauth.Cert, Identity: b, Engine: engine}); err == nil {
		t.Fatal("handler accepted engine with different node identity")
	}
	for _, client := range []*http.Client{
		{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { t.Error("custom transport invoked"); return nil, nil })},
		{Transport: &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
			t.Error("custom TLS dialer invoked")
			return nil, nil
		}}},
	} {
		if _, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a, Client: client}); err == nil {
			t.Fatal("state custom TLS bypass accepted")
		}
		if _, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: "a", AuthMode: httpauth.Cert, Identity: a, Client: client, PeerProvider: state.NewStaticPeerProvider(nil), PeerIdentity: func(string) string { return "b" }}); err == nil {
			t.Fatal("delivery custom TLS bypass accepted")
		}
	}
}
