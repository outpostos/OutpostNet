// Package tunnel provides a QUIC-based tunnel for cross-region service communication.
//
// Key concepts:
//   - A single QUIC connection multiplexes traffic for multiple Services
//   - Each Service maps to independent QUIC Streams (no head-of-line blocking)
//   - TLS 1.3 is built into QUIC (no extra encryption layer)
//   - Connection migration: seamless IP changes on edge devices
//   - 0-RTT reconnect: fast recovery after disconnection
package tunnel

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// ServerConfig configures a tunnel server (the receiving side).
type ServerConfig struct {
	// ListenAddr is the UDP address to listen on (e.g. ":4242").
	ListenAddr string

	// TLS is the TLS configuration. If nil, a self-signed cert is generated.
	TLS *tls.Config

	// KeepAlivePeriod is the QUIC keepalive interval. Default: 10s.
	KeepAlivePeriod time.Duration

	// MaxIdleTimeout is the max idle timeout before closing a connection. Default: 30s.
	MaxIdleTimeout time.Duration

	// Token is the shared secret for authenticating incoming connections.
	// An empty token disables authentication (backward compatible).
	Token string
}

// ClientConfig configures a tunnel client (the initiating side).
type ClientConfig struct {
	// ServerAddr is the remote tunnel server address (e.g. "10.0.0.1:4242").
	ServerAddr string

	// TLS is the TLS configuration. If nil, insecure (skip verify) is used.
	TLS *tls.Config

	// KeepAlivePeriod is the QUIC keepalive interval. Default: 10s.
	KeepAlivePeriod time.Duration

	// MaxIdleTimeout is the max idle timeout before closing a connection. Default: 30s.
	MaxIdleTimeout time.Duration

	// Token is the shared secret sent to the server for authentication.
	// An empty token disables authentication (backward compatible).
	Token string
	// NodeID, when set together with Token, is declared to the server after
	// authentication so it can open reverse streams to this client
	// (ADR-0003 D5). It is self-reported, not authenticated.
	NodeID string
}

// ServiceRoute maps a service name to a local TCP address.
type ServiceRoute struct {
	// Name is the service identifier (e.g. "echo-service").
	Name string

	// LocalAddr is the local TCP address to forward to (e.g. "127.0.0.1:8080").
	LocalAddr string

	// Protocol is "tcp" or "udp". Currently only TCP is supported via QUIC streams.
	Protocol string
}

// DefaultKeepAlive is the default QUIC keepalive period.
const DefaultKeepAlive = 10 * time.Second

// DefaultMaxIdleTimeout is the default max idle timeout.
const DefaultMaxIdleTimeout = 30 * time.Second

func (c *ServerConfig) quicConfig() *quic.Config {
	keepAlive := c.KeepAlivePeriod
	if keepAlive == 0 {
		keepAlive = DefaultKeepAlive
	}
	maxIdle := c.MaxIdleTimeout
	if maxIdle == 0 {
		maxIdle = DefaultMaxIdleTimeout
	}
	return &quic.Config{
		KeepAlivePeriod: keepAlive,
		MaxIdleTimeout:  maxIdle,
	}
}

func (c *ServerConfig) tlsConfig() (*tls.Config, error) {
	if c.TLS != nil {
		return c.TLS, nil
	}
	cert, err := tlsutil.GenerateSelfSignedCert()
	if err != nil {
		return nil, fmt.Errorf("generate self-signed cert: %w", err)
	}
	return tlsutil.ServerTLSConfig(cert), nil
}

func (c *ClientConfig) quicConfig() *quic.Config {
	keepAlive := c.KeepAlivePeriod
	if keepAlive == 0 {
		keepAlive = DefaultKeepAlive
	}
	maxIdle := c.MaxIdleTimeout
	if maxIdle == 0 {
		maxIdle = DefaultMaxIdleTimeout
	}
	return &quic.Config{
		KeepAlivePeriod: keepAlive,
		MaxIdleTimeout:  maxIdle,
	}
}

func (c *ClientConfig) tlsConfig() *tls.Config {
	if c.TLS != nil {
		return c.TLS
	}
	return tlsutil.ClientTLSConfig(true)
}

// Listen creates a QUIC listener on the configured address.
func Listen(ctx context.Context, cfg ServerConfig) (*quic.Listener, error) {
	tlsCfg, err := cfg.tlsConfig()
	if err != nil {
		return nil, err
	}
	listener, err := quic.ListenAddr(cfg.ListenAddr, tlsCfg, cfg.quicConfig())
	if err != nil {
		return nil, fmt.Errorf("quic listen %s: %w", cfg.ListenAddr, err)
	}
	return listener, nil
}

// Dial establishes a QUIC connection to the tunnel server.
func Dial(ctx context.Context, cfg ClientConfig) (*quic.Conn, error) {
	conn, err := quic.DialAddr(ctx, cfg.ServerAddr, cfg.tlsConfig(), cfg.quicConfig())
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", cfg.ServerAddr, err)
	}
	return conn, nil
}
