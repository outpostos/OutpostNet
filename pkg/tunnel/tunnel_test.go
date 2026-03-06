package tunnel

import (
	"context"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

func TestListenAndDial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a listener with self-signed cert.
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}

	serverCfg := ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		TLS:             tlsutil.ServerTLSConfig(cert),
		KeepAlivePeriod: 5 * time.Second,
		MaxIdleTimeout:  10 * time.Second,
	}

	listener, err := Listen(ctx, serverCfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	t.Logf("Listener address: %s", addr)

	// Accept connections in background.
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.CloseWithError(0, "done")
		serverDone <- nil
	}()

	// Dial from client.
	clientCfg := ClientConfig{
		ServerAddr:      addr,
		TLS:             tlsutil.ClientTLSConfig(true),
		KeepAlivePeriod: 5 * time.Second,
		MaxIdleTimeout:  10 * time.Second,
	}

	conn, err := Dial(ctx, clientCfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseWithError(0, "done")

	// Verify connection is established.
	t.Logf("Client connected to %s", conn.RemoteAddr())

	// Wait for server to accept.
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server accept: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for server accept")
	}
}

func TestListenDefaultTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ServerConfig with nil TLS should auto-generate self-signed cert.
	serverCfg := ServerConfig{
		ListenAddr: "127.0.0.1:0",
	}

	listener, err := Listen(ctx, serverCfg)
	if err != nil {
		t.Fatalf("Listen with default TLS: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Accept in background.
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			return
		}
		conn.CloseWithError(0, "done")
	}()

	// Dial with insecure client.
	clientCfg := ClientConfig{
		ServerAddr: addr,
	}
	conn, err := Dial(ctx, clientCfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	conn.CloseWithError(0, "done")
}

func TestDialInvalidAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := Dial(ctx, ClientConfig{
		ServerAddr: "127.0.0.1:1", // unlikely to have a QUIC server here
	})
	if err == nil {
		t.Fatal("expected error dialing invalid address")
	}
}

func TestServerConfigDefaults(t *testing.T) {
	cfg := &ServerConfig{}
	qcfg := cfg.quicConfig()
	if qcfg.KeepAlivePeriod != DefaultKeepAlive {
		t.Errorf("expected default keepalive %v, got %v", DefaultKeepAlive, qcfg.KeepAlivePeriod)
	}
	if qcfg.MaxIdleTimeout != DefaultMaxIdleTimeout {
		t.Errorf("expected default max idle %v, got %v", DefaultMaxIdleTimeout, qcfg.MaxIdleTimeout)
	}
}

func TestClientConfigDefaults(t *testing.T) {
	cfg := &ClientConfig{}
	qcfg := cfg.quicConfig()
	if qcfg.KeepAlivePeriod != DefaultKeepAlive {
		t.Errorf("expected default keepalive %v, got %v", DefaultKeepAlive, qcfg.KeepAlivePeriod)
	}
	if qcfg.MaxIdleTimeout != DefaultMaxIdleTimeout {
		t.Errorf("expected default max idle %v, got %v", DefaultMaxIdleTimeout, qcfg.MaxIdleTimeout)
	}
}
