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
	"github.com/quic-go/quic-go"
)

// TunnelManager creates and manages QUIC tunnels based on Agent configuration.
// For each configured tunnel:
//   - role=server: starts a tunnel.Server that accepts incoming connections
//     and routes streams to local services (exports).
//   - role=client: starts a tunnel.ManagedClient that connects to a remote
//     server and serves imports as local TCP listeners.
type TunnelManager struct {
	cfg     *agentconfig.Config
	logger  *slog.Logger
	metrics *Metrics

	mu              sync.Mutex
	servers         []*tunnel.Server
	clients         []*tunnel.ManagedClient
	clientsByRegion map[string]*tunnel.ManagedClient
	clientsByNode   map[string]*tunnel.ManagedClient
	relay           *RelayManager

	connectPeerClient      func(context.Context, *tunnel.ManagedClient) error
	connectPeerClientUntil func(context.Context, *tunnel.ManagedClient)
}

// NewTunnelManager creates a new TunnelManager.
func NewTunnelManager(cfg *agentconfig.Config, logger *slog.Logger, metrics *Metrics) *TunnelManager {
	if logger == nil {
		logger = slog.Default()
	}
	tm := &TunnelManager{
		cfg:             cfg,
		logger:          logger,
		metrics:         metrics,
		clientsByRegion: make(map[string]*tunnel.ManagedClient),
		clientsByNode:   make(map[string]*tunnel.ManagedClient),
	}
	tm.connectPeerClient = func(ctx context.Context, mc *tunnel.ManagedClient) error {
		return mc.Connect(ctx)
	}
	tm.connectPeerClientUntil = func(ctx context.Context, mc *tunnel.ManagedClient) {
		mc.ConnectUntilReady(ctx)
	}
	return tm
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

	// Set up relay forwarding rules if configured.
	if len(tm.cfg.Relays) > 0 {
		tm.setupRelay()
	}

	return nil
}

func (tm *TunnelManager) setupRelay() {
	tm.relay = NewRelayManager(tm.logger, tm.metrics)

	for _, rc := range tm.cfg.Relays {
		client, ok := tm.clientsByRegion[rc.ToRegion]
		if !ok {
			tm.logger.Warn("relay rule skipped: no client for to_region",
				"service", rc.Service, "to_region", rc.ToRegion)
			continue
		}
		tm.relay.AddRule(rc, client)
	}

	// Set the relay handler as the stream fallback on all servers.
	tm.mu.Lock()
	for _, srv := range tm.servers {
		srv.SetStreamFallback(tm.relay.HandleStream)
	}
	tm.mu.Unlock()
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
	srvLogger := tm.logger.With("tunnel", "server", "listen_addr", tc.ListenAddr)
	if tc.RemoteRegion != "" {
		srvLogger = srvLogger.With("remote_region", tc.RemoteRegion)
	}
	srv := tunnel.NewServer(tunnel.ServerConfig{
		ListenAddr: tc.ListenAddr,
		TLS:        tlsCfg,
		Token:      tm.cfg.Auth.Token,
	}, srvLogger)

	// Register all exports as routes on this server.
	for _, exp := range tm.cfg.Exports {
		srv.AddRoute(exp.Name, exp.LocalAddr)
	}

	tm.mu.Lock()
	tm.servers = append(tm.servers, srv)
	tm.mu.Unlock()

	go func() {
		if err := srv.ListenAndServe(ctx); err != nil {
			srvLogger.Error("tunnel server stopped", "error", err)
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

	srvLogger.Info("tunnel server started", "addr", srv.Addr())
	return nil
}

func (tm *TunnelManager) startClient(ctx context.Context, tc agentconfig.TunnelConfig, _ *tls.Config) error {
	mc := tm.newManagedClient(tc.RemoteAddr, tc.RemoteRegion)

	if err := mc.Connect(ctx); err != nil {
		return fmt.Errorf("connect to %s: %w", tc.RemoteAddr, err)
	}

	tm.mu.Lock()
	tm.clients = append(tm.clients, mc)
	tm.clientsByRegion[tc.RemoteRegion] = mc
	tm.mu.Unlock()

	// Start legacy import proxies for services from this remote region.
	for _, imp := range tm.cfg.Imports {
		if isServiceImport(imp) || imp.RemoteRegion != tc.RemoteRegion {
			continue
		}
		go tm.serveImport(ctx, mc, imp)
	}

	tm.logger.Info("tunnel client connected", "remote_region", tc.RemoteRegion, "addr", tc.RemoteAddr)
	return nil
}

func (tm *TunnelManager) newManagedClient(remoteAddr, remoteName string) *tunnel.ManagedClient {
	return tunnel.NewManagedClient(tunnel.ManagedClientConfig{
		ClientConfig: tunnel.ClientConfig{
			ServerAddr: remoteAddr,
			TLS:        tlsutil.ClientTLSConfig(tm.cfg.TLS.Insecure),
			Token:      tm.cfg.Auth.Token,
		},
		InitialReconnectDelay: 1 * time.Second,
		MaxReconnectDelay:     30 * time.Second,
		OnStateChange: func(s tunnel.ConnState) {
			tm.logger.Info("tunnel state changed", "remote", remoteName, "state", s)
		},
	}, tm.logger.With("tunnel", "client", "remote", remoteName))
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

func (tm *TunnelManager) StartDynamicImports(ctx context.Context) {
	for _, imp := range tm.cfg.Imports {
		if !isServiceImport(imp) {
			continue
		}
		go tm.serveDynamicImport(ctx, imp)
	}
}

func (tm *TunnelManager) serveDynamicImport(ctx context.Context, imp agentconfig.ImportConfig) {
	serviceName, nodeID, err := agentconfig.ParseServiceRef(imp.Service)
	if err != nil {
		tm.logger.Error("invalid service import", "service", imp.Service, "error", err)
		return
	}
	tm.logger.Info("serving dynamic import", "service", serviceName, "node_id", nodeID, "listen", imp.ListenAddr)

	for {
		err := tm.listenAndProxyDynamic(ctx, nodeID, serviceName, imp.ListenAddr)
		if ctx.Err() != nil {
			return
		}
		tm.logger.Error("dynamic import proxy stopped, restarting...", "service", serviceName, "node_id", nodeID, "error", err)
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

func (tm *TunnelManager) listenAndProxyDynamic(ctx context.Context, nodeID, serviceName, listenAddr string) error {
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

			stream, mc, err := tm.openDynamicServiceStream(ctx, nodeID, serviceName)
			if err != nil {
				tm.logger.Warn("dynamic import pending or unavailable", "service", serviceName, "node_id", nodeID, "error", err)
				return
			}
			defer stream.Close()
			defer mc.StreamDone()

			biRelay(stream, conn)
		}()
	}
}

func (tm *TunnelManager) openDynamicServiceStream(ctx context.Context, nodeID, serviceName string) (*quic.Stream, *tunnel.ManagedClient, error) {
	mc, ok := tm.DynamicClient(nodeID)
	if !ok {
		return nil, nil, fmt.Errorf("peer %q not in directory", nodeID)
	}
	stream, err := mc.OpenServiceStream(ctx, serviceName)
	if err != nil {
		return nil, nil, err
	}
	return stream, mc, nil
}

func (tm *TunnelManager) AddOrUpdatePeerClient(ctx context.Context, nodeID, addr string) error {
	tm.mu.Lock()
	if mc, ok := tm.clientsByNode[nodeID]; ok {
		tm.mu.Unlock()
		if mc.ServerAddr() == addr {
			return nil
		}
		return mc.UpdateServerAddr(ctx, addr)
	}

	mc := tm.newManagedClient(addr, nodeID)
	tm.clients = append(tm.clients, mc)
	tm.clientsByNode[nodeID] = mc
	tm.mu.Unlock()

	if err := tm.connectPeerClient(ctx, mc); err != nil {
		tm.logger.Warn("dynamic peer tunnel pending", "node_id", nodeID, "addr", addr, "error", err)
		go tm.connectPeerClientUntil(ctx, mc)
	}
	return nil
}

func (tm *TunnelManager) RemovePeerClient(nodeID string) {
	tm.mu.Lock()
	mc, ok := tm.clientsByNode[nodeID]
	if ok {
		delete(tm.clientsByNode, nodeID)
		for i, client := range tm.clients {
			if client == mc {
				tm.clients = append(tm.clients[:i], tm.clients[i+1:]...)
				break
			}
		}
	}
	tm.mu.Unlock()
	if ok {
		_ = mc.GracefulShutdown(5 * time.Second)
	}
}

func (tm *TunnelManager) DynamicClient(nodeID string) (*tunnel.ManagedClient, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	mc, ok := tm.clientsByNode[nodeID]
	return mc, ok
}

func (tm *TunnelManager) DynamicClientState(nodeID string) (tunnel.ConnState, bool) {
	mc, ok := tm.DynamicClient(nodeID)
	if !ok {
		return tunnel.StateDisconnected, false
	}
	return mc.State(), true
}

func isServiceImport(imp agentconfig.ImportConfig) bool {
	return imp.Service != ""
}

// Stop gracefully shuts down all tunnels.
func (tm *TunnelManager) Stop() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, mc := range tm.clients {
		mc.GracefulShutdown(5 * time.Second)
	}
	tm.clients = nil
	tm.clientsByNode = make(map[string]*tunnel.ManagedClient)
	tm.clientsByRegion = make(map[string]*tunnel.ManagedClient)

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
