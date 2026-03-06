package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func TestClientConnectAndOpenStream(t *testing.T) {
	// Start echo TCP service.
	echoLn := startEchoServer(t)

	// Start tunnel server.
	srv, srvAddr := newTestServer(t)
	srv.AddRoute("echo-service", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create and connect client.
	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Open stream and exchange data.
	stream, err := client.OpenServiceStream(ctx, "echo-service")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}
	defer stream.Close()

	msg := []byte("hello from client")
	if _, err := stream.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read: %v", err)
	}

	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}
}

func TestClientServeImport(t *testing.T) {
	// Start a prefix-echo TCP service (simulates a remote service).
	echoLn := startPrefixEchoServer(t, "REMOTE:")

	// Start tunnel server with route.
	srv, srvAddr := newTestServer(t)
	srv.AddRoute("remote-svc", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create and connect client.
	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Start serving import on a random port.
	importLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	importAddr := importLn.Addr().String()
	importLn.Close() // Free the port for ServeImport.

	importCtx, importCancel := context.WithCancel(ctx)
	defer importCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.ServeImport(importCtx, "remote-svc", importAddr)
	}()

	// Wait for import listener to start.
	time.Sleep(100 * time.Millisecond)

	// Connect to the import listener as a local app would.
	conn, err := net.DialTimeout("tcp", importAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial import: %v", err)
	}
	defer conn.Close()

	// Send data through the tunnel.
	msg := "hello"
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read response (should be prefixed by remote service).
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	expected := fmt.Sprintf("REMOTE:%s", msg)
	if string(buf[:n]) != expected {
		t.Fatalf("expected %q, got %q", expected, string(buf[:n]))
	}
}

func TestClientOpenStreamBeforeConnect(t *testing.T) {
	client := NewClient(ClientConfig{ServerAddr: "127.0.0.1:1"}, nil)
	ctx := context.Background()

	_, err := client.OpenServiceStream(ctx, "svc")
	if err == nil {
		t.Fatal("expected error opening stream without connection")
	}
}

func TestClientEndToEndMultipleMessages(t *testing.T) {
	echoLn := startEchoServer(t)
	srv, srvAddr := newTestServer(t)
	srv.AddRoute("echo", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
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

	// Send multiple messages.
	for i := range 5 {
		msg := []byte(fmt.Sprintf("message-%d", i))
		if _, err := stream.Write(msg); err != nil {
			t.Fatalf("write[%d]: %v", i, err)
		}
		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(stream, buf); err != nil {
			t.Fatalf("read[%d]: %v", i, err)
		}
		if !bytes.Equal(buf, msg) {
			t.Fatalf("msg[%d]: expected %q, got %q", i, msg, buf)
		}
	}
}
