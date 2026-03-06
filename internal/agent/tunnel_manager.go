package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// TunnelManager creates and manages QUIC tunnels based on Agent configuration.
// For each configured tunnel:
//   - role=server: starts a tunnel.Server that accepts incoming connections
//     and routes streams to local services (exports).
//   - role=client: starts a tunnel.ManagedClient that connects to a remote
//     server and serves imports as local TCP listeners.
type TunnelManager struct {
	cfg    *agentconfig.Config
	logger *slog.Logger

	mu      sync.Mutex
	servers []*tunnel.Server
	clients []*tunnel.ManagedClient
}

// NewTunnelManager creates a new TunnelManager.
func NewTunnelManager(cfg *agentconfig.Config, logger *slog.Logger) *TunnelManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &TunnelManager{
		cfg:    cfg,
		logger: logger,
	}
}

// Start creates and starts all configured tunnels.
// It registers export routes on servers and starts import proxies on clients.
func (tm *TunnelManager) Start(ctx context.Context) error {
	tlsCfg, err := tm.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("build TLS config: %w", err)
	}

	for _, tc := range tm.cfg.Tunnels {
		switch tc.Role {
		case "server":
			if err := tm.startServer(ctx, tc, tlsCfg); err != nil {
				return fmt.Errorf("start tunnel server for %s: %w", tc.RemoteRegion, err)
			}
		case "client":
			if err := tm.startClient(ctx, tc, tlsCfg); err != nil {
				return fmt.Errorf("start tunnel client for %s: %w", tc.RemoteRegion, err)
			}
		}
	}

	return nil
}

func (tm *TunnelManager) buildTLSConfig() (*tls.Config, error) {
	var cert tls.Certificate
	var err error

	if tm.cfg.TLS.CertFile != "" && tm.cfg.TLS.KeyFile != "" {
		cert, err = tlsutil.LoadTLSConfig(tm.cfg.TLS.CertFile, tm.cfg.TLS.KeyFile)
	} else {
		cert, err = tlsutil.GenerateSelfSignedCert()
	}
	if err != nil {
		return nil, err
	}
	return tlsutil.ServerTLSConfig(cert), nil
}

func (tm *TunnelManager) startServer(ctx context.Context, tc agentconfig.TunnelConfig, tlsCfg *tls.Config) error {
	srv := tunnel.NewServer(tunnel.ServerConfig{
		ListenAddr: tc.ListenAddr,
		TLS:        tlsCfg,
	}, tm.logger.With("tunnel", "server", "remote_region", tc.RemoteRegion))

	// Register all exports as routes on this server.
	for _, exp := range tm.cfg.Exports {
		srv.AddRoute(exp.Name, exp.LocalAddr)
	}

	tm.mu.Lock()
	tm.servers = append(tm.servers, srv)
	tm.mu.Unlock()

	go func() {
		if err := srv.ListenAndServe(ctx); err != nil {
			tm.logger.Error("tunnel server stopped", "remote_region", tc.RemoteRegion, "error", err)
		}
	}()

	// Wait for server to start listening.
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			return fmt.Errorf("tunnel server did not start within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	tm.logger.Info("tunnel server started", "remote_region", tc.RemoteRegion, "addr", srv.Addr())
	return nil
}

func (tm *TunnelManager) startClient(ctx context.Context, tc agentconfig.TunnelConfig, _ *tls.Config) error {
	mc := tunnel.NewManagedClient(tunnel.ManagedClientConfig{
		ClientConfig: tunnel.ClientConfig{
			ServerAddr: tc.RemoteAddr,
			TLS:        tlsutil.ClientTLSConfig(tm.cfg.TLS.Insecure),
		},
		InitialReconnectDelay: 1 * time.Second,
		MaxReconnectDelay:     30 * time.Second,
		OnStateChange: func(s tunnel.ConnState) {
			tm.logger.Info("tunnel state changed", "remote_region", tc.RemoteRegion, "state", s)
		},
	}, tm.logger.With("tunnel", "client", "remote_region", tc.RemoteRegion))

	if err := mc.Connect(ctx); err != nil {
		return fmt.Errorf("connect to %s: %w", tc.RemoteAddr, err)
	}

	tm.mu.Lock()
	tm.clients = append(tm.clients, mc)
	tm.mu.Unlock()

	// Start import proxies for services from this remote region.
	for _, imp := range tm.cfg.Imports {
		if imp.RemoteRegion != tc.RemoteRegion {
			continue
		}
		go tm.serveImport(ctx, mc, imp)
	}

	tm.logger.Info("tunnel client connected", "remote_region", tc.RemoteRegion, "addr", tc.RemoteAddr)
	return nil
}

func (tm *TunnelManager) serveImport(ctx context.Context, mc *tunnel.ManagedClient, imp agentconfig.ImportConfig) {
	tm.logger.Info("serving import", "service", imp.Name, "listen", imp.ListenAddr, "remote_region", imp.RemoteRegion)

	for {
		err := tm.listenAndProxy(ctx, mc, imp.Name, imp.ListenAddr)
		if ctx.Err() != nil {
			return
		}
		tm.logger.Error("import proxy stopped, restarting...", "service", imp.Name, "error", err)
		time.Sleep(1 * time.Second)
	}
}

// listenAndProxy listens on listenAddr and for each incoming TCP connection,
// opens a QUIC stream via the ManagedClient for the given service.
func (tm *TunnelManager) listenAndProxy(ctx context.Context, mc *tunnel.ManagedClient, serviceName, listenAddr string) error {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()

			stream, err := mc.OpenServiceStream(ctx, serviceName)
			if err != nil {
				tm.logger.Error("open service stream", "service", serviceName, "error", err)
				return
			}
			defer stream.Close()
			defer mc.StreamDone()

			biRelay(stream, conn)
		}()
	}
}

// Stop gracefully shuts down all tunnels.
func (tm *TunnelManager) Stop() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, mc := range tm.clients {
		mc.GracefulShutdown(5 * time.Second)
	}
	tm.clients = nil

	for _, srv := range tm.servers {
		srv.Close()
	}
	tm.servers = nil

	tm.logger.Info("all tunnels stopped")
}

// Servers returns the current tunnel servers (for testing).
func (tm *TunnelManager) Servers() []*tunnel.Server {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.servers
}

// Clients returns the current managed clients (for testing).
func (tm *TunnelManager) Clients() []*tunnel.ManagedClient {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.clients
}

// biRelay copies data bidirectionally between two ReadWriteClosers.
func biRelay(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(b, a)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(a, b)
		done <- struct{}{}
	}()
	<-done
}
