package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
