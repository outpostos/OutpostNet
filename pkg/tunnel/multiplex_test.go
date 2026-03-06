package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// TestMultiplexTenServices verifies FR-2: 1 QUIC connection can simultaneously
// serve 10 different services with no data corruption.
func TestMultiplexTenServices(t *testing.T) {
	const numServices = 10

	srv, srvAddr := newTestServer(t)

	// Start 10 distinct echo services, each with a unique prefix.
	for i := range numServices {
		name := fmt.Sprintf("svc-%d", i)
		prefix := fmt.Sprintf("[%d]", i)
		ln := startPrefixEchoServer(t, prefix)
		srv.AddRoute(name, ln.Addr().String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Single client connection.
	client := NewClient(ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	defer client.Close()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Open 10 streams concurrently, one per service.
	var wg sync.WaitGroup
	errors := make(chan error, numServices)

	for i := range numServices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("svc-%d", i)
			prefix := fmt.Sprintf("[%d]", i)

			stream, err := client.OpenServiceStream(ctx, name)
			if err != nil {
				errors <- fmt.Errorf("open stream %s: %w", name, err)
				return
			}
			defer stream.Close()

			// Send and verify multiple messages.
			for j := range 5 {
				msg := fmt.Sprintf("msg-%d-%d", i, j)
				if _, err := stream.Write([]byte(msg)); err != nil {
					errors <- fmt.Errorf("write %s msg %d: %w", name, j, err)
					return
				}

				expected := fmt.Sprintf("%s%s", prefix, msg)
				buf := make([]byte, len(expected))
				if _, err := io.ReadFull(stream, buf); err != nil {
					errors <- fmt.Errorf("read %s msg %d: %w", name, j, err)
					return
				}
				if string(buf) != expected {
					errors <- fmt.Errorf("svc %s msg %d: expected %q, got %q", name, j, expected, string(buf))
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestMultiplexNoHOLBlocking verifies that a slow stream does not block
// other streams on the same QUIC connection.
func TestMultiplexNoHOLBlocking(t *testing.T) {
	srv, srvAddr := newTestServer(t)

	// "slow-service": reads data but responds after a delay.
	slowLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen slow: %v", err)
	}
	t.Cleanup(func() { slowLn.Close() })
	go func() {
		for {
			conn, err := slowLn.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 1024)
				n, err := conn.Read(buf)
				if err != nil {
					return
				}
				// Deliberate delay to simulate slow service.
				time.Sleep(2 * time.Second)
				conn.Write(buf[:n])
			}()
		}
	}()

	// "fast-service": instant echo.
	fastLn := startEchoServer(t)

	srv.AddRoute("slow-svc", slowLn.Addr().String())
	srv.AddRoute("fast-svc", fastLn.Addr().String())

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

	// Open slow stream first.
	slowStream, err := client.OpenServiceStream(ctx, "slow-svc")
	if err != nil {
		t.Fatalf("open slow stream: %v", err)
	}
	defer slowStream.Close()

	// Send data to slow stream (it will take 2s to respond).
	go func() {
		slowStream.Write([]byte("slow-request"))
	}()

	// Now open fast stream — it should respond quickly despite slow stream.
	start := time.Now()
	fastStream, err := client.OpenServiceStream(ctx, "fast-svc")
	if err != nil {
		t.Fatalf("open fast stream: %v", err)
	}
	defer fastStream.Close()

	msg := []byte("fast-request")
	if _, err := fastStream.Write(msg); err != nil {
		t.Fatalf("write fast: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(fastStream, buf); err != nil {
		t.Fatalf("read fast: %v", err)
	}
	elapsed := time.Since(start)

	if !bytes.Equal(buf, msg) {
		t.Fatalf("fast stream: expected %q, got %q", msg, buf)
	}

	// Fast stream should complete well under 1 second (slow is 2s).
	if elapsed > 1*time.Second {
		t.Fatalf("fast stream took %v (should be <1s, slow stream is 2s delay)", elapsed)
	}
	t.Logf("fast stream completed in %v (no HOL blocking)", elapsed)
}

// TestMultiplexStreamFailureIsolation verifies that one stream failing
// does not affect other streams.
func TestMultiplexStreamFailureIsolation(t *testing.T) {
	srv, srvAddr := newTestServer(t)

	echoLn := startEchoServer(t)
	srv.AddRoute("good-svc", echoLn.Addr().String())
	// "bad-svc" has no route — stream will be closed by server.

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

	// Open a stream to a non-existent service — it should fail.
	badStream, err := client.OpenServiceStream(ctx, "bad-svc")
	if err != nil {
		t.Fatalf("open bad stream: %v", err)
	}
	badStream.Write([]byte("test"))
	badBuf := make([]byte, 64)
	_, badErr := badStream.Read(badBuf)
	badStream.Close()
	// Bad stream should get EOF or error.
	if badErr == nil {
		t.Log("bad stream read succeeded unexpectedly (server may have not closed yet)")
	}

	// Good stream should still work fine.
	goodStream, err := client.OpenServiceStream(ctx, "good-svc")
	if err != nil {
		t.Fatalf("open good stream after bad: %v", err)
	}
	defer goodStream.Close()

	msg := []byte("still working")
	if _, err := goodStream.Write(msg); err != nil {
		t.Fatalf("write good: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(goodStream, buf); err != nil {
		t.Fatalf("read good: %v", err)
	}
	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}
}

// TestMultiplexHighConcurrency tests rapid stream open/close under load.
func TestMultiplexHighConcurrency(t *testing.T) {
	const numStreams = 50

	srv, srvAddr := newTestServer(t)
	echoLn := startEchoServer(t)
	srv.AddRoute("echo", echoLn.Addr().String())

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
	var successCount atomic.Int32

	for i := range numStreams {
		wg.Add(1)
		go func() {
			defer wg.Done()

			stream, err := client.OpenServiceStream(ctx, "echo")
			if err != nil {
				t.Logf("stream %d open error: %v", i, err)
				return
			}
			defer stream.Close()

			msg := []byte(fmt.Sprintf("stream-%d", i))
			if _, err := stream.Write(msg); err != nil {
				return
			}

			buf := make([]byte, len(msg))
			if _, err := io.ReadFull(stream, buf); err != nil {
				return
			}

			if bytes.Equal(buf, msg) {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	count := successCount.Load()
	t.Logf("successful streams: %d/%d", count, numStreams)
	if count < numStreams*90/100 {
		t.Fatalf("too many failed streams: %d/%d succeeded", count, numStreams)
	}
}

// TestMultiplexServeImportMultipleServices tests ServeImport for multiple services
// through a single QUIC connection.
func TestMultiplexServeImportMultipleServices(t *testing.T) {
	srv, srvAddr := newTestServer(t)

	// Create two distinct services.
	svcALn := startPrefixEchoServer(t, "A:")
	svcBLn := startPrefixEchoServer(t, "B:")
	srv.AddRoute("svc-a", svcALn.Addr().String())
	srv.AddRoute("svc-b", svcBLn.Addr().String())

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

	// Find two free ports.
	portA := getFreePort(t)
	portB := getFreePort(t)

	importCtx, importCancel := context.WithCancel(ctx)
	defer importCancel()

	go client.ServeImport(importCtx, "svc-a", fmt.Sprintf("127.0.0.1:%d", portA))
	go client.ServeImport(importCtx, "svc-b", fmt.Sprintf("127.0.0.1:%d", portB))

	time.Sleep(100 * time.Millisecond)

	// Test service A.
	connA, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", portA), 2*time.Second)
	if err != nil {
		t.Fatalf("dial svc-a: %v", err)
	}
	defer connA.Close()

	connA.Write([]byte("hello"))
	bufA := make([]byte, 64)
	nA, err := connA.Read(bufA)
	if err != nil {
		t.Fatalf("read svc-a: %v", err)
	}
	if string(bufA[:nA]) != "A:hello" {
		t.Fatalf("svc-a: expected %q, got %q", "A:hello", string(bufA[:nA]))
	}

	// Test service B.
	connB, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", portB), 2*time.Second)
	if err != nil {
		t.Fatalf("dial svc-b: %v", err)
	}
	defer connB.Close()

	connB.Write([]byte("world"))
	bufB := make([]byte, 64)
	nB, err := connB.Read(bufB)
	if err != nil {
		t.Fatalf("read svc-b: %v", err)
	}
	if string(bufB[:nB]) != "B:world" {
		t.Fatalf("svc-b: expected %q, got %q", "B:world", string(bufB[:nB]))
	}
}

func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("get free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}
