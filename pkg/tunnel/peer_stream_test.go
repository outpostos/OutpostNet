package tunnel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

const peerTestToken = "net5-token"

// routeTable is a dialer-side route table: a Server that is never started,
// used only for ServeStream, as TunnelManager does with its own server.
func routeTable(t *testing.T, routes map[string]string) *Server {
	t.Helper()
	rt := NewServer(ServerConfig{}, nil)
	for name, addr := range routes {
		rt.AddRoute(name, addr)
	}
	return rt
}

func newPeerClient(t *testing.T, addr, nodeID string, rt *Server) *ManagedClient {
	t.Helper()
	cfg := ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr:      addr,
			TLS:             tlsutil.ClientTLSConfig(true),
			Token:           peerTestToken,
			NodeID:          nodeID,
			KeepAlivePeriod: time.Second,
			MaxIdleTimeout:  5 * time.Second,
		},
		InitialReconnectDelay: 50 * time.Millisecond,
		MaxReconnectDelay:     200 * time.Millisecond,
	}
	if rt != nil {
		cfg.StreamHandler = rt.ServeStream
	}
	mc := NewManagedClient(cfg, nil)
	// Connect's ctx also bounds the reconnect monitor, so it must outlive
	// this helper.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("connect %s: %v", nodeID, err)
	}
	t.Cleanup(func() { mc.Close() })
	return mc
}

func waitAccepted(t *testing.T, srv *Server, want []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := srv.AcceptedPeers()
		if equalStrings(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("AcceptedPeers = %v, want %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func echoRoundTrip(t *testing.T, s io.ReadWriter, msg string) {
	t.Helper()
	if _, err := io.WriteString(s, msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(s, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != msg {
		t.Fatalf("echo = %q, want %q", buf, msg)
	}
}

func TestPeerHelloWireFormat(t *testing.T) {
	var buf bytes.Buffer
	if err := writePeerHello(&buf, "node-a"); err != nil {
		t.Fatal(err)
	}
	if want := append([]byte{peerHelloV1, 6}, "node-a"...); !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("wire = %v, want %v", buf.Bytes(), want)
	}
	got, err := readPeerHello(&buf)
	if err != nil || got != "node-a" {
		t.Fatalf("readPeerHello = %q, %v", got, err)
	}
}

func TestPeerHelloAbsentIsNotAnError(t *testing.T) {
	got, err := readPeerHello(bytes.NewReader(nil))
	if err != nil || got != "" {
		t.Fatalf("readPeerHello(empty) = %q, %v; want \"\", nil", got, err)
	}
}

func TestPeerHelloRejectsMalformed(t *testing.T) {
	long := make([]byte, maxPeerNodeIDLen+1)
	for i := range long {
		long[i] = 'a'
	}
	if err := writePeerHello(io.Discard, string(long)); err == nil {
		t.Error("writePeerHello accepted a 256-byte node_id")
	}
	if err := writePeerHello(io.Discard, ""); err == nil {
		t.Error("writePeerHello accepted an empty node_id")
	}
	for name, wire := range map[string][]byte{
		"bad version":  {9, 1, 'a'},
		"zero length":  {peerHelloV1, 0},
		"truncated id": {peerHelloV1, 5, 'a', 'b'},
		"no length":    {peerHelloV1},
	} {
		if _, err := readPeerHello(bytes.NewReader(wire)); err == nil {
			t.Errorf("%s: readPeerHello accepted %v", name, wire)
		}
	}
}

// Acceptance 1 (transport level): the accepting side opens a stream toward
// the dialer, which routes it with its own route table.
func TestServerOpensReverseStreamToDialer(t *testing.T) {
	echo := startEchoServer(t)
	srv, addr := newTestServerWithToken(t, peerTestToken)
	newPeerClient(t, addr, "node-a", routeTable(t, map[string]string{"echo": echo.Addr().String()}))
	waitAccepted(t, srv, []string{"node-a"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := srv.OpenPeerStream(ctx, "node-a", "echo")
	if err != nil {
		t.Fatalf("OpenPeerStream: %v", err)
	}
	defer s.Close()
	echoRoundTrip(t, s, "reverse-hello")

	if _, err := srv.OpenPeerStream(ctx, "node-x", "echo"); !errors.Is(err, ErrNoPeerConn) {
		t.Fatalf("unknown peer: err = %v, want ErrNoPeerConn", err)
	}
}

// Forward streams keep working on a connection that also carries reverse
// streams.
func TestForwardAndReverseStreamsShareConnection(t *testing.T) {
	fwdEcho := startEchoServer(t)
	revEcho := startEchoServer(t)
	srv, addr := newTestServerWithToken(t, peerTestToken)
	srv.AddRoute("fwd", fwdEcho.Addr().String())
	mc := newPeerClient(t, addr, "node-a", routeTable(t, map[string]string{"rev": revEcho.Addr().String()}))
	waitAccepted(t, srv, []string{"node-a"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fwd, err := mc.OpenServiceStream(ctx, "fwd")
	if err != nil {
		t.Fatal(err)
	}
	defer fwd.Close()
	rev, err := srv.OpenPeerStream(ctx, "node-a", "rev")
	if err != nil {
		t.Fatal(err)
	}
	defer rev.Close()
	echoRoundTrip(t, fwd, "forward")
	echoRoundTrip(t, rev, "reverse")
}

// Acceptance 2: an older dialer (no declaration) against a new acceptor.
func TestOldDialerAgainstNewAcceptor(t *testing.T) {
	echo := startEchoServer(t)
	srv, addr := newTestServerWithToken(t, peerTestToken)
	srv.AddRoute("echo", echo.Addr().String())
	// NodeID "" produces exactly the previous wire: token only.
	mc := newPeerClient(t, addr, "", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	echoRoundTrip(t, s, "old-dialer")
	if got := srv.AcceptedPeers(); len(got) != 0 {
		t.Fatalf("AcceptedPeers = %v, want none for an undeclared dialer", got)
	}
}

// oldAcceptor is the previous server behaviour, kept verbatim in the test:
// read the token, answer, close the auth stream without reading further,
// then route streams by header to one echo address.
func oldAcceptor(t *testing.T, echoAddr string) string {
	t.Helper()
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", tlsutil.ServerTLSConfig(cert), &quic.Config{MaxIdleTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				ctx := conn.Context()
				auth, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}
				tok, err := readAuthToken(auth)
				if err != nil || tok != peerTestToken {
					auth.Write([]byte{authFail})
					conn.CloseWithError(1, "auth")
					return
				}
				auth.Write([]byte{authOK})
				auth.Close()
				for {
					s, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func() {
						defer s.Close()
						if _, err := ReadServiceHeader(s); err != nil {
							return
						}
						c, err := net.Dial("tcp", echoAddr)
						if err != nil {
							return
						}
						defer c.Close()
						relay(s, c)
					}()
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// Acceptance 2: a new (declaring) dialer against an older acceptor.
func TestNewDialerAgainstOldAcceptor(t *testing.T) {
	echo := startEchoServer(t)
	addr := oldAcceptor(t, echo.Addr().String())
	mc := newPeerClient(t, addr, "node-a", routeTable(t, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		s, err := mc.OpenServiceStream(ctx, "echo")
		if err != nil {
			t.Fatal(err)
		}
		echoRoundTrip(t, s, "new-dialer")
		s.Close()
	}
}

// Acceptance 3: a newer connection from the same node_id replaces the
// registration, and closing the older one does not remove it.
func TestReconnectReplacesRegistration(t *testing.T) {
	echoOld := startPrefixEchoServer(t, "old:")
	echoNew := startPrefixEchoServer(t, "new:")
	srv, addr := newTestServerWithToken(t, peerTestToken)

	first := newPeerClient(t, addr, "node-a", routeTable(t, map[string]string{"echo": echoOld.Addr().String()}))
	waitAccepted(t, srv, []string{"node-a"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	inflight, err := srv.OpenPeerStream(ctx, "node-a", "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer inflight.Close()
	expectPrefixed(t, inflight, "old:", "x")

	newPeerClient(t, addr, "node-a", routeTable(t, map[string]string{"echo": echoNew.Addr().String()}))
	waitForReply(t, srv, "node-a", "new:")

	first.Close()
	// The in-flight stream on the replaced connection ends with it.
	inflight.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := inflight.Write([]byte("y")); err == nil {
		buf := make([]byte, 8)
		if _, err := inflight.Read(buf); err == nil {
			t.Fatal("in-flight stream on the closed connection still works")
		}
	}
	waitAccepted(t, srv, []string{"node-a"})
	waitForReply(t, srv, "node-a", "new:")
}

// Acceptance 3: when the dialer re-points to another acceptor
// (PUT /v1/peers address change), the old acceptor drops the registration
// and the new one gains it.
func TestAddressChangeMovesRegistration(t *testing.T) {
	echo := startEchoServer(t)
	srv1, addr1 := newTestServerWithToken(t, peerTestToken)
	srv2, addr2 := newTestServerWithToken(t, peerTestToken)
	mc := newPeerClient(t, addr1, "node-a", routeTable(t, map[string]string{"echo": echo.Addr().String()}))
	waitAccepted(t, srv1, []string{"node-a"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	inflight, err := srv1.OpenPeerStream(ctx, "node-a", "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer inflight.Close()
	echoRoundTrip(t, inflight, "before")

	if err := mc.UpdateServerAddr(ctx, addr2); err != nil {
		t.Fatal(err)
	}
	waitAccepted(t, srv1, nil)
	waitAccepted(t, srv2, []string{"node-a"})
	s, err := srv2.OpenPeerStream(ctx, "node-a", "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	echoRoundTrip(t, s, "after")

	inflight.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(inflight, "z"); err == nil {
		if _, err := io.ReadFull(inflight, make([]byte, 1)); err == nil {
			t.Fatal("in-flight reverse stream survived the address change")
		}
	}
}

// Acceptance 4: concurrent reverse opens while the dialer reconnects and
// closes. Run with -race -count=5.
func TestReverseStreamsConcurrentWithReconnect(t *testing.T) {
	echo := startEchoServer(t)
	srv, addr := newTestServerWithToken(t, peerTestToken)
	rt := routeTable(t, map[string]string{"echo": echo.Addr().String()})
	mc := newPeerClient(t, addr, "node-a", rt)
	waitAccepted(t, srv, []string{"node-a"})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var okMu sync.Mutex
	ok := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s, err := srv.OpenPeerStream(ctx, "node-a", "echo")
				if err != nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				s.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err := io.WriteString(s, "p"); err == nil {
					if _, err := io.ReadFull(s, make([]byte, 1)); err == nil {
						okMu.Lock()
						ok++
						okMu.Unlock()
					}
				}
				s.CancelRead(0)
				s.Close()
			}
		}()
	}
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		// Drop the connection; the managed client reconnects and
		// re-declares.
		mc.mu.RLock()
		conn := mc.conn
		mc.mu.RUnlock()
		if conn != nil {
			conn.CloseWithError(0, "test drop")
		}
		waitAccepted(t, srv, []string{"node-a"})
	}
	close(stop)
	wg.Wait()
	if ok == 0 {
		t.Fatal("no reverse stream succeeded")
	}
	mc.Close()
	waitAccepted(t, srv, nil)
	if _, err := srv.OpenPeerStream(ctx, "node-a", "echo"); !errors.Is(err, ErrNoPeerConn) {
		t.Fatalf("after close: err = %v, want ErrNoPeerConn", err)
	}
}

// Without an auth token there is no auth stream and so no declaration: the
// previous behaviour is kept and reverse streams are unavailable.
func TestNoTokenNoDeclaration(t *testing.T) {
	srv, addr := newTestServerWithToken(t, "")
	mc := NewManagedClient(ManagedClientConfig{ClientConfig: ClientConfig{
		ServerAddr: addr, TLS: tlsutil.ClientTLSConfig(true), NodeID: "node-a",
	}}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer mc.Close()
	time.Sleep(100 * time.Millisecond)
	if got := srv.AcceptedPeers(); len(got) != 0 {
		t.Fatalf("AcceptedPeers = %v without a token", got)
	}
}

func expectPrefixed(t *testing.T, s io.ReadWriter, prefix, msg string) {
	t.Helper()
	if _, err := io.WriteString(s, msg); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(prefix)+len(msg))
	if _, err := io.ReadFull(s, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != prefix+msg {
		t.Fatalf("reply = %q, want %q", buf, prefix+msg)
	}
}

func waitForReply(t *testing.T, srv *Server, nodeID, prefix string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s, err := srv.OpenPeerStream(ctx, nodeID, "echo")
		if err == nil {
			s.SetDeadline(time.Now().Add(time.Second))
			io.WriteString(s, "q")
			buf := make([]byte, len(prefix)+1)
			_, err = io.ReadFull(s, buf)
			s.Close()
			if err == nil && string(buf) == prefix+"q" {
				cancel()
				return
			}
		}
		cancel()
		if time.Now().After(deadline) {
			t.Fatalf("no %q reply from %s (last err %v)", prefix, nodeID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
