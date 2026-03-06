package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// startTCPEchoServer creates a TCP echo server for testing.
func startTCPEchoServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

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

	return ln
}

func TestTunnelManagerServerMode(t *testing.T) {
	echoLn := startTCPEchoServer(t)

	cfg := &agentconfig.Config{
		Region: "cloud",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "edge-a",
				Role:         "server",
				ListenAddr:   "127.0.0.1:0",
			},
		},
		Exports: []agentconfig.ExportConfig{
			{
				Name:      "echo-svc",
				LocalAddr: echoLn.Addr().String(),
				Protocol:  "tcp",
			},
		},
		TLS: agentconfig.TLSConfig{Insecure: true},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tm := NewTunnelManager(cfg, nil, nil)
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tm.Stop()

	servers := tm.Servers()
	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}

	srvAddr := servers[0].Addr().String()
	t.Logf("tunnel server listening on %s", srvAddr)

	// Connect a tunnel client to verify the server works.
	client := tunnel.NewClient(tunnel.ClientConfig{
		ServerAddr: srvAddr,
		TLS:        tlsutil.ClientTLSConfig(true),
	}, nil)
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("client Connect: %v", err)
	}

	stream, err := client.OpenServiceStream(ctx, "echo-svc")
	if err != nil {
		t.Fatalf("OpenServiceStream: %v", err)
	}
	defer stream.Close()

	msg := []byte("hello from tunnel")
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

func TestTunnelManagerClientMode(t *testing.T) {
	// Set up a TCP echo server as the exported service.
	echoLn := startTCPEchoServer(t)

	// Set up a tunnel server (simulating the remote region).
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}

	srv := tunnel.NewServer(tunnel.ServerConfig{
		ListenAddr: "127.0.0.1:0",
		TLS:        tlsutil.ServerTLSConfig(cert),
	}, nil)
	srv.AddRoute("remote-echo", echoLn.Addr().String())

	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	go srv.ListenAndServe(srvCtx)
	defer srv.Close()

	// Wait for server to start.
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("tunnel server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	srvAddr := srv.Addr().String()

	// Find a free port for import listener.
	tmpLn, _ := net.Listen("tcp", "127.0.0.1:0")
	importPort := tmpLn.Addr().(*net.TCPAddr).Port
	tmpLn.Close()

	cfg := &agentconfig.Config{
		Region: "edge-a",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "cloud",
				Role:         "client",
				RemoteAddr:   srvAddr,
			},
		},
		Imports: []agentconfig.ImportConfig{
			{
				Name:         "remote-echo",
				ListenAddr:   fmt.Sprintf("127.0.0.1:%d", importPort),
				RemoteRegion: "cloud",
				Protocol:     "tcp",
			},
		},
		TLS: agentconfig.TLSConfig{Insecure: true},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tm := NewTunnelManager(cfg, nil, nil)
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tm.Stop()

	clients := tm.Clients()
	if len(clients) != 1 {
		t.Fatalf("expected 1 client, got %d", len(clients))
	}

	// Give import proxy time to start listening.
	time.Sleep(200 * time.Millisecond)

	// Connect to the import proxy and verify data flows through the tunnel.
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", importPort))
	if err != nil {
		t.Fatalf("dial import proxy: %v", err)
	}
	defer conn.Close()

	msg := []byte("cross-region data")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}

	if !bytes.Equal(buf, msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}
	t.Log("cross-region echo through tunnel successful")
}

func TestTunnelManagerStop(t *testing.T) {
	echoLn := startTCPEchoServer(t)

	cfg := &agentconfig.Config{
		Region: "test",
		Tunnels: []agentconfig.TunnelConfig{
			{
				RemoteRegion: "other",
				Role:         "server",
				ListenAddr:   "127.0.0.1:0",
			},
		},
		Exports: []agentconfig.ExportConfig{
			{
				Name:      "svc",
				LocalAddr: echoLn.Addr().String(),
				Protocol:  "tcp",
			},
		},
		TLS: agentconfig.TLSConfig{Insecure: true},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tm := NewTunnelManager(cfg, nil, nil)
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if len(tm.Servers()) != 1 {
		t.Fatal("expected 1 server before stop")
	}

	tm.Stop()

	if len(tm.Servers()) != 0 {
		t.Fatal("expected 0 servers after stop")
	}
}
