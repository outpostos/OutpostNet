package tunnel

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// BenchmarkSingleStreamThroughput measures single-stream throughput through
// a QUIC tunnel. Target: ≥100 Mbps (NFR-3).
func BenchmarkSingleStreamThroughput(b *testing.B) {
	// Start a discard server (reads everything, writes nothing back).
	discardLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer discardLn.Close()
	go func() {
		for {
			conn, err := discardLn.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(io.Discard, conn)
			}()
		}
	}()

	cert, _ := tlsutil.GenerateSelfSignedCert()
	srv := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		TLS:        tlsutil.ServerTLSConfig(cert),
	}, nil)
	srv.AddRoute("discard", discardLn.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	for srv.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	defer srv.Close()

	client := NewClient(ClientConfig{
		ServerAddr: srv.Addr().String(),
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	if err := client.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	stream, err := client.OpenServiceStream(ctx, "discard")
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	// Write 32KB chunks.
	chunk := make([]byte, 32*1024)
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()

	for range b.N {
		if _, err := stream.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamOpenClose measures stream creation/teardown latency.
func BenchmarkStreamOpenClose(b *testing.B) {
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer echoLn.Close()
	go func() {
		for {
			conn, err := echoLn.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, conn)
			}()
		}
	}()

	cert, _ := tlsutil.GenerateSelfSignedCert()
	srv := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		TLS:        tlsutil.ServerTLSConfig(cert),
	}, nil)
	srv.AddRoute("echo", echoLn.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.ListenAndServe(ctx)
	for srv.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	defer srv.Close()

	client := NewClient(ClientConfig{
		ServerAddr: srv.Addr().String(),
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	if err := client.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	b.ResetTimer()

	for range b.N {
		stream, err := client.OpenServiceStream(ctx, "echo")
		if err != nil {
			b.Fatal(err)
		}
		stream.Write([]byte("x"))
		buf := make([]byte, 1)
		io.ReadFull(stream, buf)
		stream.Close()
	}
}
