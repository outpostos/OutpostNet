package tunnel

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func TestManagedClientConnectAndState(t *testing.T) {
	_, srvAddr := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var states []ConnState
	var mu sync.Mutex

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr: srvAddr,
			TLS:        tlsutil.ClientTLSConfig(true),
		},
		OnStateChange: func(s ConnState) {
			mu.Lock()
			states = append(states, s)
			mu.Unlock()
		},
	}, nil)
	defer mc.Close()

	if mc.State() != StateDisconnected {
		t.Fatalf("initial state should be disconnected, got %v", mc.State())
	}

	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if mc.State() != StateConnected {
		t.Fatalf("expected connected state, got %v", mc.State())
	}

	mu.Lock()
	// Should have seen: Connecting → Connected
	if len(states) < 2 {
		t.Fatalf("expected at least 2 state transitions, got %d: %v", len(states), states)
	}
	if states[0] != StateConnecting {
		t.Fatalf("first state should be connecting, got %v", states[0])
	}
	if states[1] != StateConnected {
		t.Fatalf("second state should be connected, got %v", states[1])
	}
	mu.Unlock()
}

func TestManagedClientReconnect(t *testing.T) {
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("cert: %v", err)
	}

	// Start first server.
	serverCfg := ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		TLS:             tlsutil.ServerTLSConfig(cert),
		KeepAlivePeriod: 2 * time.Second,
		MaxIdleTimeout:  3 * time.Second,
	}
	srv := NewServer(serverCfg, nil)
	echoLn := startEchoServer(t)
	srv.AddRoute("echo", echoLn.Addr().String())

	serverCtx, serverCancel := context.WithCancel(context.Background())
	go srv.ListenAndServe(serverCtx)

	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	srvAddr := srv.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reconnected := make(chan struct{}, 5)
	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr:      srvAddr,
			TLS:             tlsutil.ClientTLSConfig(true),
			KeepAlivePeriod: 2 * time.Second,
			MaxIdleTimeout:  3 * time.Second,
		},
		InitialReconnectDelay: 500 * time.Millisecond,
		MaxReconnectDelay:     2 * time.Second,
		OnStateChange: func(s ConnState) {
			if s == StateConnected {
				select {
				case reconnected <- struct{}{}:
				default:
				}
			}
		},
	}, nil)
	defer mc.Close()

	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Verify initial connection works.
	stream, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("initial stream: %v", err)
	}
	stream.Write([]byte("test"))
	buf := make([]byte, 4)
	io.ReadFull(stream, buf)
	stream.Close()
	mc.StreamDone()

	// Wait for initial Connected state.
	<-reconnected

	// Kill the server to trigger reconnect.
	serverCancel()
	srv.Close()
	t.Log("server stopped, waiting for reconnect cycle...")

	// Wait for reconnecting state.
	time.Sleep(1 * time.Second)

	// Restart server on same address.
	srv2 := NewServer(ServerConfig{
		ListenAddr:      srvAddr,
		TLS:             tlsutil.ServerTLSConfig(cert),
		KeepAlivePeriod: 2 * time.Second,
		MaxIdleTimeout:  5 * time.Second,
	}, nil)
	srv2.AddRoute("echo", echoLn.Addr().String())

	serverCtx2, serverCancel2 := context.WithCancel(ctx)
	defer serverCancel2()
	go srv2.ListenAndServe(serverCtx2)
	defer srv2.Close()

	// Wait for reconnection.
	select {
	case <-reconnected:
		t.Log("reconnected successfully")
	case <-time.After(15 * time.Second):
		t.Fatal("did not reconnect within 15 seconds")
	}

	if mc.State() != StateConnected {
		t.Fatalf("expected connected after reconnect, got %v", mc.State())
	}
}

func TestManagedClientGracefulShutdown(t *testing.T) {
	srv, srvAddr := newTestServer(t)
	echoLn := startEchoServer(t)
	srv.AddRoute("echo", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr: srvAddr,
			TLS:        tlsutil.ClientTLSConfig(true),
		},
	}, nil)

	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Open a stream that takes some time to complete.
	stream, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		defer mc.StreamDone()
		defer stream.Close()

		msg := []byte("during-shutdown")
		stream.Write(msg)
		buf := make([]byte, len(msg))
		io.ReadFull(stream, buf)

		if !bytes.Equal(buf, msg) {
			t.Errorf("expected %q, got %q", msg, buf)
		}

		// Simulate some work.
		time.Sleep(200 * time.Millisecond)
	}()

	// Start graceful shutdown — should wait for stream to finish.
	start := time.Now()
	if err := mc.GracefulShutdown(5 * time.Second); err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}
	elapsed := time.Since(start)

	<-streamDone

	if elapsed < 100*time.Millisecond {
		t.Fatalf("shutdown was too fast (%v), should have waited for stream", elapsed)
	}
	t.Logf("graceful shutdown completed in %v", elapsed)

	if mc.State() != StateDisconnected {
		t.Fatalf("expected disconnected after shutdown, got %v", mc.State())
	}
}

func TestManagedClientShutdownTimeout(t *testing.T) {
	srv, srvAddr := newTestServer(t)
	echoLn := startEchoServer(t)
	srv.AddRoute("echo", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr: srvAddr,
			TLS:        tlsutil.ClientTLSConfig(true),
		},
	}, nil)

	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Open a stream that will take a long time (simulating a hung operation).
	stream, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}

	// Intentionally don't call StreamDone — simulate a "stuck" stream.
	go func() {
		defer stream.Close()
		time.Sleep(10 * time.Second) // Way longer than our timeout.
		mc.StreamDone()
	}()

	// Graceful shutdown with short timeout — should force close.
	start := time.Now()
	mc.GracefulShutdown(300 * time.Millisecond)
	elapsed := time.Since(start)

	// Should have timed out within ~300ms.
	if elapsed > 1*time.Second {
		t.Fatalf("shutdown took too long (%v), should timeout at ~300ms", elapsed)
	}
	t.Logf("forced shutdown completed in %v", elapsed)
}

func TestConnStateString(t *testing.T) {
	tests := []struct {
		state ConnState
		want  string
	}{
		{StateDisconnected, "disconnected"},
		{StateConnecting, "connecting"},
		{StateConnected, "connected"},
		{StateReconnecting, "reconnecting"},
		{ConnState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("ConnState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestManagedClientDefaults(t *testing.T) {
	mc := NewManagedClient(ManagedClientConfig{}, nil)
	if mc.cfg.InitialReconnectDelay != 1*time.Second {
		t.Errorf("expected 1s initial delay, got %v", mc.cfg.InitialReconnectDelay)
	}
	if mc.cfg.MaxReconnectDelay != 30*time.Second {
		t.Errorf("expected 30s max delay, got %v", mc.cfg.MaxReconnectDelay)
	}
}

func TestManagedClientOpenRelayStream(t *testing.T) {
	srv, srvAddr := newTestServer(t)

	// Set up a fallback that records the header and echoes data back.
	fallbackCalled := make(chan StreamHeader, 1)
	srv.SetStreamFallback(func(ctx context.Context, stream *quic.Stream, hdr StreamHeader) {
		fallbackCalled <- hdr
		io.Copy(stream, stream)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr: srvAddr,
			TLS:        tlsutil.ClientTLSConfig(true),
		},
	}, nil)
	defer mc.Close()

	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	stream, err := mc.OpenRelayStream(ctx, "relay-target", 5)
	if err != nil {
		t.Fatalf("OpenRelayStream: %v", err)
	}
	defer func() {
		stream.Close()
		mc.StreamDone()
	}()

	// Verify the server fallback received the relay header.
	select {
	case hdr := <-fallbackCalled:
		if hdr.ServiceName != "relay-target" {
			t.Fatalf("expected service name %q, got %q", "relay-target", hdr.ServiceName)
		}
		if !hdr.IsRelay {
			t.Fatal("expected IsRelay=true")
		}
		if hdr.HopCount != 5 {
			t.Fatalf("expected HopCount=5, got %d", hdr.HopCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fallback was not called within 5 seconds")
	}

	// Verify the stream is usable for data transfer.
	msg := []byte("relay-data")
	if _, err := stream.Write(msg); err != nil {
		t.Fatalf("write data: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}
}

func TestManagedClientOpenRelayStreamNilConn(t *testing.T) {
	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr: "127.0.0.1:19999",
			TLS:        tlsutil.ClientTLSConfig(true),
		},
	}, nil)
	// Do NOT call mc.Connect — conn stays nil.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := mc.OpenRelayStream(ctx, "some-service", 1)
	if err == nil {
		t.Fatal("expected error when connection is nil")
	}
	if err != net.ErrClosed {
		t.Fatalf("expected net.ErrClosed, got %v", err)
	}
}
