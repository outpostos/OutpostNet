package tunnel

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// TestIntegrationEndToEnd verifies the full tunnel path:
// Client → QUIC Tunnel → Server → TCP echo service → verify data integrity.
func TestIntegrationEndToEnd(t *testing.T) {
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

	// Send varied-size messages.
	sizes := []int{1, 10, 100, 1000, 4096, 16384}
	for _, size := range sizes {
		msg := make([]byte, size)
		rand.Read(msg)

		if _, err := stream.Write(msg); err != nil {
			t.Fatalf("write %d bytes: %v", size, err)
		}

		buf := make([]byte, size)
		if _, err := io.ReadFull(stream, buf); err != nil {
			t.Fatalf("read %d bytes: %v", size, err)
		}

		if !bytes.Equal(buf, msg) {
			t.Fatalf("data mismatch at size %d", size)
		}
	}
}

// TestIntegrationMultiServiceConcurrent tests 10 services communicating
// simultaneously through a single tunnel connection.
func TestIntegrationMultiServiceConcurrent(t *testing.T) {
	const numServices = 10
	const messagesPerService = 10

	srv, srvAddr := newTestServer(t)

	for i := range numServices {
		name := fmt.Sprintf("svc-%d", i)
		ln := startPrefixEchoServer(t, fmt.Sprintf("[%d]", i))
		srv.AddRoute(name, ln.Addr().String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numServices)

	for i := range numServices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("svc-%d", i)
			prefix := fmt.Sprintf("[%d]", i)

			stream, err := client.OpenServiceStream(ctx, name)
			if err != nil {
				errCh <- fmt.Errorf("open %s: %w", name, err)
				return
			}
			defer stream.Close()

			for j := range messagesPerService {
				msg := fmt.Sprintf("data-%d-%d", i, j)
				if _, err := stream.Write([]byte(msg)); err != nil {
					errCh <- fmt.Errorf("%s write %d: %w", name, j, err)
					return
				}

				expected := prefix + msg
				buf := make([]byte, len(expected))
				if _, err := io.ReadFull(stream, buf); err != nil {
					errCh <- fmt.Errorf("%s read %d: %w", name, j, err)
					return
				}

				if string(buf) != expected {
					errCh <- fmt.Errorf("%s msg %d: expected %q, got %q", name, j, expected, string(buf))
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestIntegrationLargeDataTransfer transfers 10MB through the tunnel and
// verifies data integrity using SHA-256 checksum.
func TestIntegrationLargeDataTransfer(t *testing.T) {
	const dataSize = 10 * 1024 * 1024 // 10MB

	echoLn := startEchoServer(t)
	srv, srvAddr := newTestServer(t)
	srv.AddRoute("bulk", echoLn.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	stream, err := client.OpenServiceStream(ctx, "bulk")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}

	// Generate random data and compute hash.
	data := make([]byte, dataSize)
	rand.Read(data)
	expectedHash := sha256.Sum256(data)

	// Write data concurrently (to avoid deadlock with echo server).
	// Write and read concurrently to avoid buffer deadlock.
	writeErr := make(chan error, 1)
	go func() {
		// Write in 64KB chunks.
		_, err := io.Copy(stream, bytes.NewReader(data))
		writeErr <- err
	}()

	// Read exactly dataSize bytes (echo returns same amount).
	received := make([]byte, dataSize)
	_, err = io.ReadFull(stream, received)
	if err != nil {
		t.Fatalf("read full: %v", err)
	}
	stream.Close()

	if werr := <-writeErr; werr != nil {
		t.Logf("write error (may be expected): %v", werr)
	}

	actualHash := sha256.Sum256(received)
	if actualHash != expectedHash {
		t.Fatalf("data integrity check failed: hash mismatch")
	}
	t.Logf("transferred %d bytes with matching SHA-256", dataSize)
}

// TestIntegrationReconnectAndRecover tests that after a tunnel reconnection,
// new streams can be opened and used.
func TestIntegrationReconnectAndRecover(t *testing.T) {
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}

	echoLn := startEchoServer(t)
	srvAddr := "127.0.0.1:0"

	// Start server.
	srv1 := NewServer(ServerConfig{
		ListenAddr:      srvAddr,
		TLS:             tlsutil.ServerTLSConfig(cert),
		KeepAlivePeriod: 2 * time.Second,
		MaxIdleTimeout:  3 * time.Second,
	}, nil)
	srv1.AddRoute("echo", echoLn.Addr().String())

	ctx1, cancel1 := context.WithCancel(context.Background())
	go srv1.ListenAndServe(ctx1)
	for srv1.Addr() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	actualAddr := srv1.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			ServerAddr:      actualAddr,
			TLS:             tlsutil.ClientTLSConfig(true),
			KeepAlivePeriod: 2 * time.Second,
			MaxIdleTimeout:  3 * time.Second,
		},
		InitialReconnectDelay: 500 * time.Millisecond,
		MaxReconnectDelay:     2 * time.Second,
	}, nil)
	defer mc.Close()

	if err := mc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Use initial connection.
	stream1, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("stream1: %v", err)
	}
	stream1.Write([]byte("hello"))
	buf := make([]byte, 5)
	io.ReadFull(stream1, buf)
	if string(buf) != "hello" {
		t.Fatalf("expected hello, got %q", buf)
	}
	stream1.Close()
	mc.StreamDone()

	// Kill server.
	cancel1()
	srv1.Close()

	// Wait for connection to detect failure.
	time.Sleep(1 * time.Second)

	// Restart server on same address.
	srv2 := NewServer(ServerConfig{
		ListenAddr:      actualAddr,
		TLS:             tlsutil.ServerTLSConfig(cert),
		KeepAlivePeriod: 2 * time.Second,
		MaxIdleTimeout:  5 * time.Second,
	}, nil)
	srv2.AddRoute("echo", echoLn.Addr().String())

	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go srv2.ListenAndServe(ctx2)
	defer srv2.Close()

	// Wait for reconnect.
	deadline := time.Now().Add(15 * time.Second)
	for mc.State() != StateConnected {
		if time.Now().After(deadline) {
			t.Fatal("did not reconnect")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Use new connection.
	stream2, err := mc.OpenServiceStream(ctx, "echo")
	if err != nil {
		t.Fatalf("stream2 after reconnect: %v", err)
	}
	defer stream2.Close()
	mc.StreamDone()

	stream2.Write([]byte("world"))
	buf2 := make([]byte, 5)
	io.ReadFull(stream2, buf2)
	if string(buf2) != "world" {
		t.Fatalf("expected world, got %q", buf2)
	}
	t.Log("reconnect and recover successful")
}

// TestIntegrationGracefulShutdownNoDataLoss verifies that during graceful
// shutdown, in-flight data is not lost.
func TestIntegrationGracefulShutdownNoDataLoss(t *testing.T) {
	echoLn := startEchoServer(t)
	srv, srvAddr := newTestServer(t)
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

	// Open multiple streams and send data.
	const numStreams = 5
	results := make(chan bool, numStreams)

	for i := range numStreams {
		stream, err := mc.OpenServiceStream(ctx, "echo")
		if err != nil {
			t.Fatalf("open stream %d: %v", i, err)
		}

		go func() {
			defer mc.StreamDone()
			defer stream.Close()

			msg := []byte(fmt.Sprintf("stream-%d-data", i))
			stream.Write(msg)

			buf := make([]byte, len(msg))
			_, err := io.ReadFull(stream, buf)
			results <- err == nil && bytes.Equal(buf, msg)

			// Simulate some processing time.
			time.Sleep(100 * time.Millisecond)
		}()
	}

	// Initiate graceful shutdown while streams are active.
	time.Sleep(50 * time.Millisecond) // Let streams start.
	if err := mc.GracefulShutdown(5 * time.Second); err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}

	close(results)
	success := 0
	for ok := range results {
		if ok {
			success++
		}
	}
	t.Logf("streams completed successfully: %d/%d", success, numStreams)
	if success < numStreams {
		t.Fatalf("expected all %d streams to complete, got %d", numStreams, success)
	}
}
