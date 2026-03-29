package tunnel

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// newTestServerWithToken starts a tunnel server requiring the given token.
// An empty token disables authentication.
func newTestServerWithToken(t *testing.T, token string) (*Server, string) {
	t.Helper()
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}
	cfg := ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		TLS:             tlsutil.ServerTLSConfig(cert),
		KeepAlivePeriod: 5 * time.Second,
		MaxIdleTimeout:  10 * time.Second,
		Token:           token,
	}
	srv := NewServer(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { srv.ListenAndServe(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not start in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	return srv, srv.Addr().String()
}

// TestAuthCorrectToken verifies that a matching token allows the connection and
// service streams work normally.
func TestAuthCorrectToken(t *testing.T) {
	echoLn := startEchoServer(t)

	srv, srvAddr := newTestServerWithToken(t, "correct-secret")
	srv.AddRoute("echo", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
		Token:      "correct-secret",
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	stream, err := client.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}
	defer stream.Close()

	msg := []byte("auth-ok-test")
	if _, err := stream.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := stream.Read(buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}
}

// TestAuthWrongToken verifies that a mismatched token causes Connect to return
// an error and the connection is rejected by the server.
func TestAuthWrongToken(t *testing.T) {
	_, srvAddr := newTestServerWithToken(t, "correct-secret")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
		Token:      "wrong-token",
	}, nil)
	defer client.Close()

	err := client.Connect(ctx)
	if err == nil {
		t.Fatal("expected Connect to fail with wrong token, got nil")
	}
}

// TestAuthEmptyToken verifies backward compatibility: when no token is
// configured on either side, the connection proceeds without auth.
func TestAuthEmptyToken(t *testing.T) {
	echoLn := startEchoServer(t)

	srv, srvAddr := newTestServerWithToken(t, "") // no token
	srv.AddRoute("echo", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
		Token:      "", // no token
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect without auth: %v", err)
	}

	stream, err := client.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}
	defer stream.Close()

	msg := []byte("no-auth-test")
	if _, err := stream.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := stream.Read(buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}
}

// TestAuthTokenWireFormat verifies the auth token wire encoding round-trip.
func TestAuthTokenWireFormat(t *testing.T) {
	tests := []struct {
		token string
	}{
		{""},
		{"secret"},
		{"very-long-token-with-special-chars-!@#$%"},
	}

	for _, tt := range tests {
		t.Run("token="+tt.token, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeAuthToken(&buf, tt.token); err != nil {
				t.Fatalf("writeAuthToken: %v", err)
			}
			got, err := readAuthToken(&buf)
			if err != nil {
				t.Fatalf("readAuthToken: %v", err)
			}
			if got != tt.token {
				t.Fatalf("expected %q, got %q", tt.token, got)
			}
		})
	}
}

// TestAuthTokenTooLong verifies that an oversized token is rejected.
func TestAuthTokenTooLong(t *testing.T) {
	token := make([]byte, maxTokenLen+1)
	for i := range token {
		token[i] = 'x'
	}
	var buf bytes.Buffer
	if err := writeAuthToken(&buf, string(token)); err == nil {
		t.Fatal("expected error for token exceeding maxTokenLen")
	}
}
