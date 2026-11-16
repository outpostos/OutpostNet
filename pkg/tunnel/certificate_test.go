package tunnel

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func groupCA(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	c, k, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := tls.X509KeyPair(c, k)
	if err != nil {
		t.Fatal(err)
	}
	return ca, c
}
func nodeIdentity(t *testing.T, ca tls.Certificate, roots []byte, node string) *tlsutil.Identity {
	t.Helper()
	c, k, err := tlsutil.IssueNode(ca, node, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(c, k)
	if err != nil {
		t.Fatal(err)
	}
	id, err := tlsutil.NewIdentity(cert, roots, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func certificateServer(t *testing.T, id *tlsutil.Identity) *Server {
	t.Helper()
	s := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Identity: id}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe(ctx) }()
	t.Cleanup(func() {
		cancel()
		s.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("certificate server did not stop")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for s.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server not listening")
		}
		time.Sleep(time.Millisecond)
	}
	return s
}
func TestCertificateTunnelAuthorizesBothDirectionsWithoutToken(t *testing.T) {
	ca, roots := groupCA(t)
	serverID := nodeIdentity(t, ca, roots, "node-b")
	clientID := nodeIdentity(t, ca, roots, "node-a")
	srv := certificateServer(t, serverID)
	echo := startEchoServer(t)
	srv.AddRoute("echo", echo.Addr().String())
	rt := routeTable(t, map[string]string{"echo": echo.Addr().String()})
	mc := NewManagedClient(ManagedClientConfig{ClientConfig: ClientConfig{ServerAddr: srv.Addr().String(), Identity: clientID, NodeID: "node-a", ExpectedPeerID: "node-b"}, StreamHandler: rt.ServeStream}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer mc.Close()
	if err := mc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	waitAccepted(t, srv, []string{"node-a"})
	forward, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer forward.Close()
	defer mc.StreamDone()
	echoRoundTrip(t, forward, "forward")
	reverse, err := srv.OpenPeerStream(ctx, "node-a", "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer reverse.Close()
	echoRoundTrip(t, reverse, "reverse")
}

func TestCertificateTunnelRejectsUnauthenticatedAndForgedPeers(t *testing.T) {
	ca, roots := groupCA(t)
	otherCA, otherRoots := groupCA(t)
	for _, tc := range []struct {
		name              string
		identity          *tlsutil.Identity
		claim, expected   string
		certificateReject bool
	}{
		{name: "missing certificate", claim: "node-a", certificateReject: true},
		{name: "foreign group", identity: nodeIdentity(t, otherCA, append(otherRoots, roots...), "node-a"), claim: "node-a", certificateReject: true},
		{name: "forged handshake claim", identity: nodeIdentity(t, ca, roots, "node-a"), claim: "node-c"},
		{name: "wrong directory destination", identity: nodeIdentity(t, ca, roots, "node-a"), claim: "node-a", expected: "node-c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := nodeIdentity(t, ca, roots, "node-b")
			srv := certificateServer(t, id)
			cfg := ClientConfig{ServerAddr: srv.Addr().String(), Identity: tc.identity, NodeID: tc.claim, ExpectedPeerID: tc.expected, TLS: tlsutil.ClientTLSConfig(true)}
			if tc.identity == nil {
				cfg.Token = "test-handshake"
			}
			c := NewClient(cfg, nil)
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := c.Connect(ctx); err == nil {
				t.Fatal("unauthorized connection accepted")
			}
			if len(srv.AcceptedPeers()) != 0 {
				t.Fatal("unauthorized reverse channel registered")
			}
			deadline := time.Now().Add(time.Second)
			for {
				stats := id.Stats()
				if tc.expected != "" {
					stats = tc.identity.Stats()
				}
				if (tc.certificateReject && stats.CertificateRejected > 0) || (!tc.certificateReject && stats.ClaimRejected > 0) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("rejection not counted: %+v", stats)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

// Exercise crypto/tls's actual clock with real QUIC handshakes, without
// changing the host/VM clock or disturbing other jobs.
func TestCertificateTLSWithSkewedClocks(t *testing.T) {
	ca, roots := groupCA(t)
	for _, now := range []time.Time{time.Unix(0, 0), time.Now().AddDate(10, 0, 1)} {
		t.Run(now.Format("2006"), func(t *testing.T) {
			serverID := nodeIdentity(t, ca, roots, "node-b")
			clientID := nodeIdentity(t, ca, roots, "node-a")
			serverTLS := serverID.TLSConfig(true)
			serverTLS.Time = func() time.Time { return now }
			clientTLS := clientID.TLSConfig(false)
			clientTLS.Time = func() time.Time { return now }
			srv := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", TLS: serverTLS, Token: peerTestToken}, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			defer srv.Close()
			go srv.ListenAndServe(ctx)
			for srv.Addr() == nil {
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			srv.AddRoute("echo", startEchoServer(t).Addr().String())
			c := NewClient(ClientConfig{ServerAddr: srv.Addr().String(), TLS: clientTLS, Token: peerTestToken, NodeID: "node-a"}, nil)
			defer c.Close()
			if err := c.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			s, err := c.OpenServiceStream(ctx, "echo")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			echoRoundTrip(t, s, "skew")
			if serverID.Stats().ValidityWarnings != 1 || clientID.Stats().ValidityWarnings != 1 {
				t.Fatalf("missing validity counters: server=%+v client=%+v", serverID.Stats(), clientID.Stats())
			}
		})
	}
}

// waitConnected blocks until mc holds a live client connection. waitAccepted
// only observes the server's view, and the server registers the peer during
// the TLS auth handshake — before reconnect() installs the new connection on
// the client. Opening streams in that window sees conn == nil and returns
// net.ErrClosed, so the test must also gate on client-side readiness.
func waitConnected(t *testing.T, mc *ManagedClient) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !mc.Connected() {
		if time.Now().After(deadline) {
			t.Fatalf("managed client did not become connected, state=%v", mc.State())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCertificateReconnectAndConcurrentStreams(t *testing.T) {
	ca, roots := groupCA(t)
	sid := nodeIdentity(t, ca, roots, "node-b")
	cid := nodeIdentity(t, ca, roots, "node-a")
	first := certificateServer(t, sid)
	second := certificateServer(t, sid)
	echo := startEchoServer(t)
	first.AddRoute("echo", echo.Addr().String())
	second.AddRoute("echo", echo.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc := NewManagedClient(ManagedClientConfig{ClientConfig: ClientConfig{ServerAddr: first.Addr().String(), Identity: cid, ExpectedPeerID: "node-b"}, InitialReconnectDelay: time.Millisecond, MaxReconnectDelay: 10 * time.Millisecond}, nil)
	defer mc.Close()
	if err := mc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	waitAccepted(t, first, []string{"node-a"})
	if err := mc.UpdateServerAddr(ctx, second.Addr().String()); err != nil {
		t.Fatal(err)
	}
	waitAccepted(t, first, nil)
	waitAccepted(t, second, []string{"node-a"})
	waitConnected(t, mc)
	done := make(chan error, 8)
	for n := 0; n < 8; n++ {
		go func() {
			s, err := mc.OpenServiceStream(ctx, "echo")
			if err != nil {
				done <- err
				return
			}
			defer mc.StreamDone()
			defer s.Close()
			s.SetDeadline(time.Now().Add(time.Second))
			_, err = s.Write([]byte("x"))
			if err == nil {
				var b [1]byte
				_, err = io.ReadFull(s, b[:])
				if err == nil && b[0] != 'x' {
					err = fmt.Errorf("wrong echo")
				}
			}
			done <- err
		}()
	}
	for n := 0; n < 8; n++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCertificateTunnelRejectsMissingDeclarationBeforeServiceTraffic(t *testing.T) {
	ca, roots := groupCA(t)
	sid := nodeIdentity(t, ca, roots, "node-b")
	cid := nodeIdentity(t, ca, roots, "node-a")
	srv := certificateServer(t, sid)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := Dial(ctx, ClientConfig{ServerAddr: srv.Addr().String(), Identity: cid})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "test complete")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.CancelRead(0)
	// A valid TLS peer sends only the empty token, then tries to skip the
	// identity declaration. No reverse registration or service access is allowed.
	if _, err := stream.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	stream.Close()
	stream.SetReadDeadline(time.Now().Add(time.Second))
	var response [1]byte
	if _, err := io.ReadFull(stream, response[:]); err == nil && response[0] == 0 {
		t.Fatal("missing claim acknowledged")
	}
	select {
	case <-conn.Context().Done():
	case <-ctx.Done():
		t.Fatal("unauthorized connection was not closed")
	}
	if sid.Stats().ClaimRejected != 1 || len(srv.AcceptedPeers()) != 0 {
		t.Fatalf("stats=%+v peers=%v", sid.Stats(), srv.AcceptedPeers())
	}
}
