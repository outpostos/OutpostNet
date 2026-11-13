package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
	"github.com/quic-go/quic-go"
)

// Reproduces RANGE-1's failure on a single accepted peer connection. The
// opener closes rejected streams exactly as TunnelManager does, without
// sending application data or a FIN before the provider rejects them.
func TestReverseStreamRecoversAfterRejections(t *testing.T) {
	testConfirmedStreamRecoversAfterRejections(t, true)
}

func TestForwardStreamRecoversAfterRejections(t *testing.T) {
	testConfirmedStreamRecoversAfterRejections(t, false)
}

func testConfirmedStreamRecoversAfterRejections(t *testing.T, reverse bool) {
	t.Helper()
	for _, failure := range []struct {
		name string
		want error
	}{
		{"no_route", ErrStreamNoRoute},
		{"dial_failed", ErrStreamBackendUnavailable},
	} {
		t.Run(failure.name, func(t *testing.T) {
			srv, addr := newTestServerWithToken(t, peerTestToken)
			provider := srv
			if reverse {
				provider = routeTable(t, nil)
			}
			if failure.want == ErrStreamBackendUnavailable {
				dead, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				deadAddr := dead.Addr().String()
				if err := dead.Close(); err != nil {
					t.Fatal(err)
				}
				provider.AddRoute("echo", deadAddr)
			}
			mc := newPeerClient(t, addr, "node-a", provider)
			waitAccepted(t, srv, []string{"node-a"})
			open := func(ctx context.Context) (*quic.Stream, error) {
				return srv.OpenConfirmedPeerStream(ctx, "node-a", "echo")
			}
			done := func() {}
			if !reverse {
				open = func(ctx context.Context) (*quic.Stream, error) {
					return mc.OpenConfirmedServiceStream(ctx, "echo")
				}
				done = mc.StreamDone
			}

			const rejections = 150 // Exceeds quic-go's default incoming stream limit.
			for i := 0; i < rejections; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				s, err := open(ctx)
				if err != nil {
					cancel()
					t.Fatalf("rejection %d: %v", i+1, err)
				}
				s.SetDeadline(time.Now().Add(3 * time.Second))
				err = ReadStreamAck(s)
				s.CancelRead(0)
				s.Close()
				done()
				cancel()
				if !errors.Is(err, failure.want) {
					t.Fatalf("rejection %d: got %v, want %v", i+1, err, failure.want)
				}
			}

			echo := startEchoServer(t)
			provider.AddRoute("echo", echo.Addr().String())
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			s, err := open(ctx)
			if err != nil {
				t.Fatalf("healthy backend after %d rejections: %v", rejections, err)
			}
			defer func() { s.CancelRead(0); s.Close(); done() }()
			s.SetDeadline(time.Now().Add(3 * time.Second))
			if err := ReadStreamAck(s); err != nil {
				t.Fatal(err)
			}
			echoRoundTrip(t, s, "recovered")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if tail, err := io.ReadAll(s); err != nil || len(tail) != 0 {
				t.Fatalf("forwarding ended with %q, %v; want clean EOF", tail, err)
			}
		})
	}
}

// Leave the opener's write side open until the provider rejects the stream:
// sending FIN first would hide an unread receive side in the provider.
func TestStreamRecoversAfterEarlyReturns(t *testing.T) {
	for _, path := range []string{"no_route", "invalid_header", "fallback_return"} {
		t.Run(path, func(t *testing.T) {
			srv, addr := newTestServer(t)
			if path == "fallback_return" {
				srv.SetStreamFallback(func(context.Context, *quic.Stream, StreamHeader) {})
			}
			conn, err := Dial(t.Context(), ClientConfig{
				ServerAddr: addr, TLS: tlsutil.ClientTLSConfig(true),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseWithError(0, "test done")

			const rejections = 150
			for i := 0; i < rejections; i++ {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				s, err := conn.OpenStreamSync(ctx)
				cancel()
				if err != nil {
					t.Fatalf("rejection %d: %v", i+1, err)
				}
				s.SetDeadline(time.Now().Add(3 * time.Second))
				if path == "invalid_header" {
					// NameLen 256 is invalid, so parsing fails without reading FIN.
					_, err = s.Write([]byte{1, 0})
				} else {
					err = WriteServiceHeader(s, "echo")
				}
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(s)
				s.CancelRead(0)
				s.Close()
				if err != nil || len(body) != 0 {
					t.Fatalf("rejection %d: got %q, %v; want empty EOF", i+1, body, err)
				}
			}

			echo := startEchoServer(t)
			srv.AddRoute("echo", echo.Addr().String())
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			s, err := conn.OpenStreamSync(ctx)
			if err != nil {
				t.Fatalf("healthy backend after %d rejections: %v", rejections, err)
			}
			defer s.Close()
			defer s.CancelRead(0)
			s.SetDeadline(time.Now().Add(3 * time.Second))
			if err := WriteServiceHeader(s, "echo"); err != nil {
				t.Fatal(err)
			}
			echoRoundTrip(t, s, "recovered-without-confirmation")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if tail, err := io.ReadAll(s); err != nil || len(tail) != 0 {
				t.Fatalf("forwarding ended with %q, %v; want clean EOF", tail, err)
			}
		})
	}
}
