package mesh

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// ClusterConfig describes the cluster topology and local node identity.
type ClusterConfig struct {
	// LocalNodeID is the unique identifier of this node.
	LocalNodeID string

	// LocalAddr is the address to bind for peer-to-peer communication
	// (forwarding + sync). For example "0.0.0.0:7630".
	LocalAddr string

	// Peers lists the other nodes in this Site's cluster.
	Peers []PeerConfig

	// SyncInterval is how often a full service sync is performed with
	// peers. Defaults to 30s.
	SyncInterval time.Duration

	// DialTimeout for connecting to peer nodes.
	DialTimeout time.Duration

	// MaxForwardRetries is the maximum number of failover attempts.
	MaxForwardRetries int
}

// PeerConfig describes a single peer node.
type PeerConfig struct {
	NodeID  string
	Address string // host:port
}

// DefaultClusterConfig returns a config with sensible defaults.
func DefaultClusterConfig(localNodeID, localAddr string) ClusterConfig {
	return ClusterConfig{
		LocalNodeID:       localNodeID,
		LocalAddr:         localAddr,
		SyncInterval:      30 * time.Second,
		DialTimeout:       5 * time.Second,
		MaxForwardRetries: 2,
	}
}

// Manager coordinates cluster membership, service data synchronization,
// and cross-node request forwarding for a Site's OutpostNet servers.
type Manager struct {
	config    ClusterConfig
	logger    *slog.Logger
	state     *ClusterState
	forwarder *Forwarder

	// Local service handler for incoming forwarded requests.
	handler LocalServiceHandler

	// syncListener accepts peer sync connections.
	syncListener net.Listener

	mu             sync.Mutex
	localServices  map[string]ServiceEndpoint // services registered on this node

	done chan struct{}
	wg   sync.WaitGroup
}

// NewManager creates a new cluster Manager.
func NewManager(config ClusterConfig, handler LocalServiceHandler, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}

	state := NewClusterState(config.LocalNodeID)

	// Register peers from config.
	for _, p := range config.Peers {
		state.AddNode(NodeInfo{
			NodeID:   p.NodeID,
			Address:  p.Address,
			Status:   NodeStatusHealthy,
			LastSeen: time.Now(),
		})
	}

	fwdConfig := ForwarderConfig{
		ListenAddr:  config.LocalAddr,
		DialTimeout: config.DialTimeout,
		MaxRetries:  config.MaxForwardRetries,
	}

	forwarder := NewForwarder(fwdConfig, state, handler, logger.With("component", "forwarder"))

	mgr := &Manager{
		config:        config,
		logger:        logger,
		state:         state,
		forwarder:     forwarder,
		handler:       handler,
		localServices: make(map[string]ServiceEndpoint),
		done:          make(chan struct{}),
	}

	// Set sync handler on the shared listener.
	forwarder.SyncHandler = mgr.handleSyncConn

	return mgr
}

// handleSyncConn handles an incoming sync connection from a peer.
// The sync magic byte has already been consumed by the forwarder.
func (m *Manager) handleSyncConn(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Read peer's services.
	remote, err := readSyncPayload(conn)
	if err != nil {
		m.logger.Error("read sync payload", "error", err)
		return
	}

	// Send our local services.
	m.mu.Lock()
	locals := make([]ServiceEndpoint, 0, len(m.localServices))
	for _, ep := range m.localServices {
		locals = append(locals, ep)
	}
	m.mu.Unlock()

	if err := writeSyncPayload(conn, locals); err != nil {
		m.logger.Error("write sync payload", "error", err)
		return
	}

	// Update cluster state with peer's services.
	// We don't know the peer's NodeID from the sync protocol alone,
	// so we rely on the ServiceEndpoint.NodeID being set by the sender
	// or match by connection address.
	for _, ep := range remote {
		if ep.NodeID == "" {
			continue
		}
		m.state.AddService(ep)
	}

	m.logger.Debug("handled incoming sync", "remote_services", len(remote))
}

// Start initializes the cluster manager: starts the forwarding listener,
// performs initial peer sync, and begins the periodic sync loop.
func (m *Manager) Start(ctx context.Context) error {
	// Start forwarding listener.
	if m.config.LocalAddr != "" {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if err := m.forwarder.ListenAndServe(); err != nil {
				select {
				case <-m.done:
				default:
					m.logger.Error("forwarding listener stopped", "error", err)
				}
			}
		}()

		// Wait for listener to be ready.
		deadline := time.Now().Add(5 * time.Second)
		for m.forwarder.Addr() == nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("forwarding listener did not start within 5s")
			}
			time.Sleep(10 * time.Millisecond)
		}
		m.logger.Info("cluster manager started",
			"node", m.config.LocalNodeID,
			"addr", m.forwarder.Addr(),
			"peers", len(m.config.Peers),
		)
	}

	// Perform initial sync with peers.
	m.syncWithPeers()

	// Start periodic sync loop.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.syncLoop(ctx)
	}()

	return nil
}

// RegisterLocalService registers a service that is available on this node.
// This also broadcasts the service to peers during the next sync cycle.
func (m *Manager) RegisterLocalService(serviceName string, protocol uint8, localPort uint16) {
	ep := ServiceEndpoint{
		NodeID:      m.config.LocalNodeID,
		ServiceName: serviceName,
		Protocol:    protocol,
		LocalPort:   localPort,
		Weight:      1,
		Healthy:     true,
	}

	m.mu.Lock()
	m.localServices[serviceName] = ep
	m.mu.Unlock()

	m.state.AddService(ep)

	m.logger.Info("local service registered", "service", serviceName, "port", localPort)
}

// UnregisterLocalService removes a locally registered service.
func (m *Manager) UnregisterLocalService(serviceName string) {
	m.mu.Lock()
	delete(m.localServices, serviceName)
	m.mu.Unlock()

	m.state.RemoveService(serviceName, m.config.LocalNodeID)

	m.logger.Info("local service unregistered", "service", serviceName)
}

// Forward attempts to forward a service request to a peer node.
// Returns nil, ErrServiceNotFound if no remote endpoint exists.
func (m *Manager) Forward(ctx context.Context, serviceName string) (io.ReadWriteCloser, error) {
	return m.forwarder.Forward(ctx, serviceName)
}

// IsLocal checks if the service has an endpoint on this node.
func (m *Manager) IsLocal(serviceName string) bool {
	return m.state.IsLocal(serviceName)
}

// State returns the underlying ClusterState for direct queries.
func (m *Manager) State() *ClusterState {
	return m.state
}

// ForwardingMetrics returns the forwarder's metrics.
func (m *Manager) ForwardingMetrics() *ForwardingMetrics {
	return &m.forwarder.Metrics
}

// Stop gracefully shuts down the cluster manager.
func (m *Manager) Stop() error {
	close(m.done)
	if m.syncListener != nil {
		m.syncListener.Close()
	}
	err := m.forwarder.Close()
	m.wg.Wait()
	m.logger.Info("cluster manager stopped")
	return err
}

// syncLoop periodically syncs service data with peers.
func (m *Manager) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(m.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case <-ticker.C:
			m.syncWithPeers()
		}
	}
}

// syncWithPeers performs a full service sync with all healthy peers.
// It connects to each peer, exchanges service lists, and updates local state.
func (m *Manager) syncWithPeers() {
	peers := m.state.HealthyNodes()
	if len(peers) == 0 {
		return
	}

	m.mu.Lock()
	locals := make([]ServiceEndpoint, 0, len(m.localServices))
	for _, ep := range m.localServices {
		locals = append(locals, ep)
	}
	m.mu.Unlock()

	for _, peer := range peers {
		remote, err := m.syncWithPeer(peer, locals)
		if err != nil {
			m.logger.Warn("sync with peer failed",
				"peer", peer.NodeID,
				"addr", peer.Address,
				"error", err,
			)
			continue
		}

		// Update cluster state with peer's services.
		for _, ep := range remote {
			ep.NodeID = peer.NodeID
			m.state.AddService(ep)
		}

		m.logger.Debug("synced with peer",
			"peer", peer.NodeID,
			"remote_services", len(remote),
		)
	}
}

// syncWithPeer exchanges service lists with a single peer via TCP.
// Returns the peer's service list.
func (m *Manager) syncWithPeer(peer NodeInfo, localServices []ServiceEndpoint) ([]ServiceEndpoint, error) {
	dialer := net.Dialer{Timeout: m.config.DialTimeout}
	conn, err := dialer.Dial("tcp", peer.Address)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Send sync magic byte to distinguish from forwarding requests.
	if _, err := conn.Write([]byte{syncMagic}); err != nil {
		return nil, fmt.Errorf("write sync magic: %w", err)
	}

	// Send our services.
	if err := writeSyncPayload(conn, localServices); err != nil {
		return nil, fmt.Errorf("write services: %w", err)
	}

	// Read peer's services.
	remote, err := readSyncPayload(conn)
	if err != nil {
		return nil, fmt.Errorf("read services: %w", err)
	}

	return remote, nil
}

// --- Sync wire protocol ---
// The sync protocol uses a magic byte (0xAA) to distinguish sync
// connections from forwarding connections on the same listener.
//
// After the magic byte:
//   +──────────+──────────────────────────+
//   │ Count(2) │ ServiceEntry * Count     │
//   +──────────+──────────────────────────+
//
// ServiceEntry:
//   +──────────────+──────────────────+──────────+──────────+
//   │ NameLen (2B) │ ServiceName (var)│ Proto(1) │ Port(2)  │
//   +──────────────+──────────────────+──────────+──────────+

const syncMagic byte = 0xAA

func writeSyncPayload(w io.Writer, services []ServiceEndpoint) error {
	var countBuf [2]byte
	binary.BigEndian.PutUint16(countBuf[:], uint16(len(services)))
	if _, err := w.Write(countBuf[:]); err != nil {
		return err
	}

	for _, ep := range services {
		name := []byte(ep.ServiceName)
		buf := make([]byte, 2+len(name)+1+2)
		binary.BigEndian.PutUint16(buf[:2], uint16(len(name)))
		copy(buf[2:2+len(name)], name)
		buf[2+len(name)] = ep.Protocol
		binary.BigEndian.PutUint16(buf[2+len(name)+1:], ep.LocalPort)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func readSyncPayload(r io.Reader) ([]ServiceEndpoint, error) {
	var countBuf [2]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, err
	}
	count := binary.BigEndian.Uint16(countBuf[:])

	services := make([]ServiceEndpoint, 0, count)
	for range count {
		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}
		nameLen := binary.BigEndian.Uint16(lenBuf[:])
		if nameLen == 0 || nameLen > 4096 {
			return nil, fmt.Errorf("invalid service name length: %d", nameLen)
		}

		buf := make([]byte, nameLen+3) // name + proto(1) + port(2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		ep := ServiceEndpoint{
			ServiceName: string(buf[:nameLen]),
			Protocol:    buf[nameLen],
			LocalPort:   binary.BigEndian.Uint16(buf[nameLen+1:]),
			Healthy:     true,
			Weight:      1,
		}
		services = append(services, ep)
	}
	return services, nil
}
