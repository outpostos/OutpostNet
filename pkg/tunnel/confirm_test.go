package tunnel

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestConfirmedHeaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteConfirmedServiceHeader(&buf, "echo"); err != nil {
		t.Fatal(err)
	}
	hdr, err := ReadStreamHeader(&buf)
	if err != nil || hdr.ServiceName != "echo" || !hdr.WantsConfirm || hdr.IsRelay {
		t.Fatalf("hdr = %+v, %v", hdr, err)
	}
	buf.Reset()
	WriteServiceHeader(&buf, "echo")
	if hdr, _ := ReadStreamHeader(&buf); hdr.WantsConfirm {
		t.Fatal("plain header asks for confirmation")
	}
	// Relay + confirm is not a valid combination.
	if _, err := ReadStreamHeader(bytes.NewReader([]byte{0xC0, 4, 'e', 'c', 'h', 'o', 1})); err == nil {
		t.Fatal("relay+confirm header accepted")
	}
}

func TestReadStreamAck(t *testing.T) {
	for b, want := range map[byte]error{StreamAckOK: nil, StreamAckNoRoute: ErrStreamNoRoute, StreamAckDialFailed: ErrStreamBackendUnavailable, 9: ErrStreamNotConfirmed} {
		if err := ReadStreamAck(bytes.NewReader([]byte{b})); !errors.Is(err, want) && !(want == nil && err == nil) {
			t.Errorf("status %d: err = %v, want %v", b, err, want)
		}
	}
	if err := ReadStreamAck(bytes.NewReader(nil)); !errors.Is(err, ErrStreamNotConfirmed) {
		t.Errorf("EOF: err = %v", err)
	}
}

func confirmedOpen(t *testing.T, mc *ManagedClient, svc string) (*bytes.Buffer, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := mc.OpenConfirmedServiceStream(ctx, svc)
	if err != nil {
		return nil, err
	}
	defer func() { s.Close(); mc.StreamDone() }()
	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ReadStreamAck(s); err != nil {
		return nil, err
	}
	echoRoundTrip(t, s, "after-ack")
	return nil, nil
}

// The provider confirms only after its backend is connected, and reports
// why it cannot serve otherwise.
func TestConfirmedStreamOutcomes(t *testing.T) {
	echo := startEchoServer(t)
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close()
	fallbackCalled := false

	srv, addr := newTestServerWithToken(t, peerTestToken)
	srv.AddRoute("echo", echo.Addr().String())
	srv.AddRoute("dead", deadAddr)
	srv.SetStreamFallback(func(context.Context, *quic.Stream, StreamHeader) { fallbackCalled = true })
	mc := newPeerClient(t, addr, "node-a", nil)

	if _, err := confirmedOpen(t, mc, "echo"); err != nil {
		t.Fatalf("echo: %v", err)
	}
	if _, err := confirmedOpen(t, mc, "missing"); !errors.Is(err, ErrStreamNoRoute) {
		t.Fatalf("missing: err = %v", err)
	}
	if fallbackCalled {
		t.Fatal("confirmed stream went to the relay fallback")
	}
	if _, err := confirmedOpen(t, mc, "dead"); !errors.Is(err, ErrStreamBackendUnavailable) {
		t.Fatalf("dead: err = %v", err)
	}
	// Plain streams are unchanged: no status byte precedes the data.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatal(err)
	}
	echoRoundTrip(t, s, "plain")
	s.Close()
	mc.StreamDone()
}

// An older provider does not understand the flag and closes the stream:
// the opener sees ErrStreamNotConfirmed, never data.
func TestConfirmedStreamAgainstOldAcceptor(t *testing.T) {
	echo := startEchoServer(t)
	addr := oldAcceptor(t, echo.Addr().String())
	mc := newPeerClient(t, addr, "node-a", nil)
	if _, err := confirmedOpen(t, mc, "echo"); !errors.Is(err, ErrStreamNotConfirmed) {
		t.Fatalf("err = %v, want ErrStreamNotConfirmed", err)
	}
}

// Reverse direction: the accepting side opens a confirmed stream toward the
// dialer, which answers through its own route table.
func TestConfirmedReverseStream(t *testing.T) {
	echo := startEchoServer(t)
	srv, addr := newTestServerWithToken(t, peerTestToken)
	newPeerClient(t, addr, "node-a", routeTable(t, map[string]string{"echo": echo.Addr().String()}))
	waitAccepted(t, srv, []string{"node-a"})
	if !srv.HasAcceptedPeer("node-a") || srv.HasAcceptedPeer("node-x") {
		t.Fatal("HasAcceptedPeer wrong")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := srv.OpenConfirmedPeerStream(ctx, "node-a", "echo")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ReadStreamAck(s); err != nil {
		t.Fatal(err)
	}
	echoRoundTrip(t, s, "reverse-confirmed")
	s2, err := srv.OpenConfirmedPeerStream(ctx, "node-a", "missing")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ReadStreamAck(s2); !errors.Is(err, ErrStreamNoRoute) {
		t.Fatalf("missing reverse: %v", err)
	}
	if _, err := srv.OpenConfirmedPeerStream(ctx, "node-x", "echo"); !errors.Is(err, ErrNoPeerConn) {
		t.Fatalf("unknown peer: %v", err)
	}
}
