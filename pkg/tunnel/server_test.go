package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// startEchoServer starts a TCP echo server that echoes back whatever it receives.
func startEchoServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo server: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, conn)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

// startPrefixEchoServer starts a TCP server that prepends a prefix to responses.
func startPrefixEchoServer(t *testing.T, prefix string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen prefix echo server: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 1024)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					resp := fmt.Sprintf("%s%s", prefix, string(buf[:n]))
					conn.Write([]byte(resp))
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

func newTestServer(t *testing.T) (*Server, string) {
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
	}
	logger := slog.Default()
	srv := NewServer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Start server in background.
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	// Wait for listener to be ready.
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

func TestServerRouteManagement(t *testing.T) {
	cfg := ServerConfig{ListenAddr: "127.0.0.1:0"}
	srv := NewServer(cfg, nil)

	srv.AddRoute("svc-a", "127.0.0.1:8080")
	addr, ok := srv.getRoute("svc-a")
	if !ok || addr != "127.0.0.1:8080" {
		t.Fatalf("expected route for svc-a, got %q %v", addr, ok)
	}

	_, ok = srv.getRoute("nonexistent")
	if ok {
		t.Fatal("expected no route for nonexistent")
	}

	srv.RemoveRoute("svc-a")
	_, ok = srv.getRoute("svc-a")
	if ok {
		t.Fatal("expected route to be removed")
	}
}

func TestServerStreamRouting(t *testing.T) {
	// Start echo service.
	echoLn := startEchoServer(t)

	// Start tunnel server.
	srv, srvAddr := newTestServer(t)
	srv.AddRoute("echo-service", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect client.
	conn, err := Dial(ctx, ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "done")

	// Open stream and send service header.
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := WriteServiceHeader(stream, "echo-service"); err != nil {
		t.Fatalf("write service header: %v", err)
	}

	// Send data and read echo.
	msg := []byte("hello tunnel")
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

func TestServerNoRoute(t *testing.T) {
	// Start tunnel server with no routes.
	_, srvAddr := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := Dial(ctx, ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "done")

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := WriteServiceHeader(stream, "unknown-service"); err != nil {
		t.Fatalf("write header: %v", err)
	}

	// Write some data — the server should close the stream since no route exists.
	stream.Write([]byte("test"))

	// Read should return EOF or error since server closes stream.
	buf := make([]byte, 64)
	_, err = stream.Read(buf)
	if err == nil {
		t.Fatal("expected error reading from stream with no route")
	}
}

func TestServiceHeaderRoundtrip(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"echo-service", false},
		{"my-svc-123", false},
		{"a", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteServiceHeader(&buf, tt.name); err != nil {
				if !tt.wantErr {
					t.Fatalf("write: %v", err)
				}
				return
			}
			got, err := ReadServiceHeader(&buf)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if got != tt.name {
				t.Fatalf("expected %q, got %q", tt.name, got)
			}
		})
	}
}

func TestServiceHeaderEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteServiceHeader(&buf, ""); err == nil {
		t.Fatal("expected error for empty service name")
	}
}

func TestServiceHeaderTooLong(t *testing.T) {
	longName := make([]byte, maxServiceNameLen+1)
	for i := range longName {
		longName[i] = 'a'
	}
	var buf bytes.Buffer
	if err := WriteServiceHeader(&buf, string(longName)); err == nil {
		t.Fatal("expected error for too-long service name")
	}
}
