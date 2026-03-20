package mesh

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Federation wire protocol constants.
const (
	// federationMagic distinguishes federation connections from intra-cluster
	// forwarding and sync on the same listener.
	federationMagic byte = 0xBB

	// Federation message types (sent after magic byte).
	fedMsgSync    byte = 0x01 // service catalog sync
	fedMsgForward byte = 0x02 // cross-cluster service forward
	fedMsgPing    byte = 0x03 // health check ping
	fedMsgPong    byte = 0x04 // health check pong
)

// ClusterInfo describes a remote cluster in the federation.
type ClusterInfo struct {
	ClusterID string
	Address   string // host:port of the cluster's federation endpoint
	Status    NodeStatus
	LastSeen  time.Time
}

// FederationConfig configures the FederationManager.
type FederationConfig struct {
	// LocalClusterID uniquely identifies this cluster in the federation.
	LocalClusterID string

	// ListenAddr is the address to bind for federation connections.
	// If empty, the federation manager only initiates outbound connections.
	ListenAddr string

	// RemoteClusters lists the known clusters to connect to.
	RemoteClusters []RemoteClusterConfig

	// SyncInterval is how often service catalogs are synced between clusters.
	// Defaults to 60s (longer than intra-cluster 30s).
	SyncInterval time.Duration

	// HealthCheckInterval is how often ping/pong health checks run.
	// Defaults to 15s.
	HealthCheckInterval time.Duration

	// DialTimeout for connecting to remote clusters.
	DialTimeout time.Duration

	// HealthTimeout is how long before a cluster is marked unhealthy
	// after missing health checks. Defaults to 45s.
	HealthTimeout time.Duration
}

// RemoteClusterConfig describes a single remote cluster.
type RemoteClusterConfig struct {
	ClusterID string
	Address   string // host:port
}

// DefaultFederationConfig returns a config with sensible defaults.
func DefaultFederationConfig(localClusterID, listenAddr string) FederationConfig {
	return FederationConfig{
		LocalClusterID:      localClusterID,
		ListenAddr:          listenAddr,
		SyncInterval:        60 * time.Second,
		HealthCheckInterval: 15 * time.Second,
		DialTimeout:         5 * time.Second,
		HealthTimeout:       45 * time.Second,
	}
}

// FederationMetrics tracks federation performance counters.
type FederationMetrics struct {
	SyncTotal        atomic.Int64
	SyncErrors       atomic.Int64
	ForwardTotal     atomic.Int64
	ForwardErrors    atomic.Int64
	ForwardActive    atomic.Int64
	HealthCheckTotal atomic.Int64
	HealthCheckFails atomic.Int64
}

// FederationState tracks the federation topology and remote service catalogs.
// It is safe for concurrent use.
type FederationState struct {
	mu             sync.RWMutex
	localClusterID string
	clusters       map[string]*ClusterInfo                   // clusterID -> info
	services       map[string]map[string][]ServiceEndpoint   // clusterID -> serviceName -> endpoints
}

// NewFederationState creates a new FederationState.
func NewFederationState(localClusterID string) *FederationState {
	return &FederationState{
		localClusterID: localClusterID,
		clusters:       make(map[string]*ClusterInfo),
		services:       make(map[string]map[string][]ServiceEndpoint),
	}
}

// AddCluster registers or updates a remote cluster.
func (fs *FederationState) AddCluster(info ClusterInfo) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.clusters[info.ClusterID] = &info
}

// RemoveCluster removes a cluster and all its associated services.
func (fs *FederationState) RemoveCluster(clusterID string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.clusters, clusterID)
	delete(fs.services, clusterID)
}

// UpdateClusterStatus updates a cluster's health status and last seen time.
func (fs *FederationState) UpdateClusterStatus(clusterID string, status NodeStatus) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if c, ok := fs.clusters[clusterID]; ok {
		c.Status = status
		if status == NodeStatusHealthy {
			c.LastSeen = time.Now()
		}
	}
}

// GetCluster returns info about a remote cluster.
func (fs *FederationState) GetCluster(clusterID string) (ClusterInfo, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if c, ok := fs.clusters[clusterID]; ok {
		return *c, true
	}
	return ClusterInfo{}, false
}

// HealthyClusters returns all healthy remote clusters.
func (fs *FederationState) HealthyClusters() []ClusterInfo {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var result []ClusterInfo
	for _, c := range fs.clusters {
		if c.Status == NodeStatusHealthy {
			result = append(result, *c)
		}
	}
	return result
}

// SetRemoteServices replaces the entire service catalog for a remote cluster.
func (fs *FederationState) SetRemoteServices(clusterID string, services []ServiceEndpoint) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	svcMap := make(map[string][]ServiceEndpoint)
	for _, ep := range services {
		svcMap[ep.ServiceName] = append(svcMap[ep.ServiceName], ep)
	}
	fs.services[clusterID] = svcMap
}

// LookupRemoteService finds endpoints for a service across all healthy remote clusters.
// Returns endpoints along with their cluster IDs.
func (fs *FederationState) LookupRemoteService(serviceName string) []FederatedEndpoint {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var result []FederatedEndpoint
	for clusterID, svcMap := range fs.services {
		cluster, ok := fs.clusters[clusterID]
		if !ok || cluster.Status != NodeStatusHealthy {
			continue
		}
		if eps, ok := svcMap[serviceName]; ok {
			for _, ep := range eps {
				result = append(result, FederatedEndpoint{
					ClusterID: clusterID,
					Address:   cluster.Address,
					Endpoint:  ep,
				})
			}
		}
	}
	return result
}

// AllRemoteServices returns a snapshot of all remote services grouped by cluster.
func (fs *FederationState) AllRemoteServices() map[string][]ServiceEndpoint {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	result := make(map[string][]ServiceEndpoint)
	for clusterID, svcMap := range fs.services {
		for _, eps := range svcMap {
			result[clusterID] = append(result[clusterID], eps...)
		}
	}
	return result
}

// ClusterCount returns the number of known remote clusters.
func (fs *FederationState) ClusterCount() int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.clusters)
}

// FederatedEndpoint combines a service endpoint with its cluster location.
type FederatedEndpoint struct {
	ClusterID string
	Address   string // cluster's federation endpoint address
	Endpoint  ServiceEndpoint
}

// FederationManager manages cross-cluster service discovery, health checking,
// and request forwarding. It sits above the intra-cluster Manager and provides
// federation-level operations.
type FederationManager struct {
	config  FederationConfig
	logger  *slog.Logger
	state   *FederationState
	Metrics FederationMetrics

	// localManager provides access to local cluster's services.
	localManager *Manager

	// QueryHandler is called when a service query message is received.
	// Set by ServiceDiscovery after creation.
	QueryHandler ConnHandler

	// listener for incoming federation connections.
	listener net.Listener

	done chan struct{}
	wg   sync.WaitGroup
}

// NewFederationManager creates a new FederationManager.
func NewFederationManager(config FederationConfig, localManager *Manager, logger *slog.Logger) *FederationManager {
	if logger == nil {
		logger = slog.Default()
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 60 * time.Second
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 15 * time.Second
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.HealthTimeout == 0 {
		config.HealthTimeout = 45 * time.Second
	}

	state := NewFederationState(config.LocalClusterID)

	// Register remote clusters from config.
	for _, rc := range config.RemoteClusters {
		state.AddCluster(ClusterInfo{
			ClusterID: rc.ClusterID,
			Address:   rc.Address,
			Status:    NodeStatusUnknown,
			LastSeen:  time.Time{},
		})
	}

	return &FederationManager{
		config:       config,
		logger:       logger.With("component", "federation"),
		state:        state,
		localManager: localManager,
		done:         make(chan struct{}),
	}
}

// State returns the federation state for direct queries.
func (fm *FederationManager) State() *FederationState {
	return fm.state
}

// Start initializes the federation manager: starts the listener,
// performs initial health checks and sync, and begins periodic loops.
func (fm *FederationManager) Start(ctx context.Context) error {
	// Start federation listener if configured.
	if fm.config.ListenAddr != "" {
		ln, err := net.Listen("tcp", fm.config.ListenAddr)
		if err != nil {
			return fmt.Errorf("federation listen: %w", err)
		}
		fm.listener = ln

		fm.wg.Add(1)
		go func() {
			defer fm.wg.Done()
			fm.acceptLoop()
		}()

		fm.logger.Info("federation listener started",
			"cluster", fm.config.LocalClusterID,
			"addr", ln.Addr(),
			"remote_clusters", len(fm.config.RemoteClusters),
		)
	}

	// Initial health check to discover cluster status.
	fm.healthCheckAll()

	// Initial sync with healthy clusters.
	fm.syncWithAllClusters()

	// Start periodic health check loop.
	fm.wg.Add(1)
	go func() {
		defer fm.wg.Done()
		fm.healthCheckLoop(ctx)
	}()

	// Start periodic sync loop.
	fm.wg.Add(1)
	go func() {
		defer fm.wg.Done()
		fm.syncLoop(ctx)
	}()

	return nil
}

// Stop gracefully shuts down the federation manager.
func (fm *FederationManager) Stop() error {
	close(fm.done)
	if fm.listener != nil {
		fm.listener.Close()
	}
	fm.wg.Wait()
	fm.logger.Info("federation manager stopped")
	return nil
}

// ForwardToCluster forwards a service request to a specific remote cluster.
// Returns a ReadWriteCloser for bidirectional data relay.
func (fm *FederationManager) ForwardToCluster(ctx context.Context, clusterID, serviceName string) (io.ReadWriteCloser, error) {
	select {
	case <-fm.done:
		return nil, ErrForwarderClosed
	default:
	}

	fm.Metrics.ForwardTotal.Add(1)

	cluster, ok := fm.state.GetCluster(clusterID)
	if !ok {
		fm.Metrics.ForwardErrors.Add(1)
		return nil, fmt.Errorf("unknown cluster: %s", clusterID)
	}
	if cluster.Status != NodeStatusHealthy {
		fm.Metrics.ForwardErrors.Add(1)
		return nil, fmt.Errorf("cluster %s is %s", clusterID, cluster.Status)
	}

	conn, err := fm.dialFederation(ctx, cluster.Address, fedMsgForward)
	if err != nil {
		fm.Metrics.ForwardErrors.Add(1)
		return nil, fmt.Errorf("dial cluster %s: %w", clusterID, err)
	}

	// Send the service name using the same forward header format.
	if err := writeForwardHeader(conn, serviceName); err != nil {
		conn.Close()
		fm.Metrics.ForwardErrors.Add(1)
		return nil, fmt.Errorf("write forward header: %w", err)
	}

	// Read response.
	status, msg, err := readForwardResponse(conn)
	if err != nil {
		conn.Close()
		fm.Metrics.ForwardErrors.Add(1)
		return nil, fmt.Errorf("read forward response: %w", err)
	}
	if status != forwardStatusOK {
		conn.Close()
		fm.Metrics.ForwardErrors.Add(1)
		return nil, fmt.Errorf("cluster %s rejected: status=%d msg=%s", clusterID, status, msg)
	}

	fm.Metrics.ForwardActive.Add(1)
	return &trackedConn{
		ReadWriteCloser: conn,
		onClose: func() {
			fm.Metrics.ForwardActive.Add(-1)
		},
	}, nil
}

// ForwardFederated attempts to forward a service request to any remote cluster
// that has the service. Returns the first successful connection.
func (fm *FederationManager) ForwardFederated(ctx context.Context, serviceName string) (io.ReadWriteCloser, error) {
	endpoints := fm.state.LookupRemoteService(serviceName)
	if len(endpoints) == 0 {
		return nil, ErrServiceNotFound
	}

	// Try clusters in order, deduplicating by cluster ID.
	tried := make(map[string]bool)
	var lastErr error
	for _, ep := range endpoints {
		if tried[ep.ClusterID] {
			continue
		}
		tried[ep.ClusterID] = true

		conn, err := fm.ForwardToCluster(ctx, ep.ClusterID, serviceName)
		if err != nil {
			lastErr = err
			fm.logger.Warn("federation forward attempt failed",
				"service", serviceName,
				"cluster", ep.ClusterID,
				"error", err,
			)
			continue
		}
		return conn, nil
	}

	return nil, fmt.Errorf("%w: %v", ErrForwardingFailed, lastErr)
}

// --- Accept loop ---

func (fm *FederationManager) acceptLoop() {
	for {
		conn, err := fm.listener.Accept()
		if err != nil {
			select {
			case <-fm.done:
				return
			default:
				fm.logger.Error("federation accept error", "error", err)
				continue
			}
		}
		fm.wg.Add(1)
		go func() {
			defer fm.wg.Done()
			fm.handleDirectConn(conn)
		}()
	}
}

// handleDirectConn handles a connection on the dedicated federation listener.
// It reads and validates the federation magic byte, then delegates to handleFederationConn.
func (fm *FederationManager) handleDirectConn(conn net.Conn) {
	defer conn.Close()

	// Read and verify federation magic byte.
	var magic [1]byte
	if _, err := io.ReadFull(conn, magic[:]); err != nil {
		fm.logger.Error("read federation magic", "error", err)
		return
	}
	if magic[0] != federationMagic {
		fm.logger.Warn("invalid federation magic", "got", magic[0])
		return
	}

	fm.handleFederationConn(conn)
}

// handleFederationConn handles a federation connection after the magic byte
// has been consumed (either by the direct listener or by the Forwarder).
func (fm *FederationManager) handleFederationConn(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Read message type byte (magic already consumed).
	var msgType [1]byte
	if _, err := io.ReadFull(conn, msgType[:]); err != nil {
		fm.logger.Error("read federation msg type", "error", err)
		return
	}

	switch msgType[0] {
	case fedMsgSync:
		fm.handleIncomingSync(conn)
	case fedMsgForward:
		fm.handleIncomingForward(conn)
	case fedMsgPing:
		fm.handleIncomingPing(conn)
	case fedMsgQuery:
		if fm.QueryHandler != nil {
			fm.QueryHandler(conn)
		}
	default:
		fm.logger.Warn("unknown federation message type", "type", msgType[0])
	}
}

// --- Health check ---

func (fm *FederationManager) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(fm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-fm.done:
			return
		case <-ticker.C:
			fm.healthCheckAll()
		}
	}
}

func (fm *FederationManager) healthCheckAll() {
	fm.state.mu.RLock()
	clusters := make([]ClusterInfo, 0, len(fm.state.clusters))
	for _, c := range fm.state.clusters {
		clusters = append(clusters, *c)
	}
	fm.state.mu.RUnlock()

	for _, cluster := range clusters {
		fm.Metrics.HealthCheckTotal.Add(1)

		if err := fm.pingCluster(cluster); err != nil {
			fm.Metrics.HealthCheckFails.Add(1)
			fm.logger.Warn("health check failed",
				"cluster", cluster.ClusterID,
				"error", err,
			)

			// Check if cluster should be marked unhealthy based on timeout.
			if !cluster.LastSeen.IsZero() && time.Since(cluster.LastSeen) > fm.config.HealthTimeout {
				fm.state.UpdateClusterStatus(cluster.ClusterID, NodeStatusUnhealthy)
				fm.logger.Warn("cluster marked unhealthy",
					"cluster", cluster.ClusterID,
					"last_seen", cluster.LastSeen,
				)
			}
		} else {
			fm.state.UpdateClusterStatus(cluster.ClusterID, NodeStatusHealthy)
		}
	}
}

func (fm *FederationManager) pingCluster(cluster ClusterInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), fm.config.DialTimeout)
	defer cancel()

	conn, err := fm.dialFederation(ctx, cluster.Address, fedMsgPing)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(fm.config.DialTimeout))

	// Send our cluster ID.
	clusterIDBytes := []byte(fm.config.LocalClusterID)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(clusterIDBytes)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write cluster ID length: %w", err)
	}
	if _, err := conn.Write(clusterIDBytes); err != nil {
		return fmt.Errorf("write cluster ID: %w", err)
	}

	// Read pong response (1 byte).
	var pong [1]byte
	if _, err := io.ReadFull(conn, pong[:]); err != nil {
		return fmt.Errorf("read pong: %w", err)
	}
	if pong[0] != fedMsgPong {
		return fmt.Errorf("unexpected pong response: 0x%02x", pong[0])
	}

	return nil
}

func (fm *FederationManager) handleIncomingPing(conn net.Conn) {
	// Read sender's cluster ID.
	var lenBuf [2]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		fm.logger.Error("read ping cluster ID length", "error", err)
		return
	}
	nameLen := binary.BigEndian.Uint16(lenBuf[:])
	if nameLen == 0 || nameLen > 256 {
		fm.logger.Error("invalid ping cluster ID length", "len", nameLen)
		return
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		fm.logger.Error("read ping cluster ID", "error", err)
		return
	}

	remoteClusterID := string(nameBuf)

	// Update cluster status if known.
	if _, ok := fm.state.GetCluster(remoteClusterID); ok {
		fm.state.UpdateClusterStatus(remoteClusterID, NodeStatusHealthy)
	}

	// Send pong.
	conn.Write([]byte{fedMsgPong})

	fm.logger.Debug("handled ping", "from_cluster", remoteClusterID)
}

// --- Sync ---

func (fm *FederationManager) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(fm.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-fm.done:
			return
		case <-ticker.C:
			fm.syncWithAllClusters()
		}
	}
}

func (fm *FederationManager) syncWithAllClusters() {
	clusters := fm.state.HealthyClusters()
	if len(clusters) == 0 {
		return
	}

	// Collect local services from the intra-cluster manager.
	localServices := fm.collectLocalServices()

	for _, cluster := range clusters {
		fm.Metrics.SyncTotal.Add(1)

		remote, err := fm.syncWithCluster(cluster, localServices)
		if err != nil {
			fm.Metrics.SyncErrors.Add(1)
			fm.logger.Warn("federation sync failed",
				"cluster", cluster.ClusterID,
				"error", err,
			)
			continue
		}

		// Update federation state with remote cluster's services.
		fm.state.SetRemoteServices(cluster.ClusterID, remote)

		fm.logger.Debug("synced with remote cluster",
			"cluster", cluster.ClusterID,
			"remote_services", len(remote),
		)
	}
}

func (fm *FederationManager) collectLocalServices() []ServiceEndpoint {
	if fm.localManager == nil {
		return nil
	}
	allSvcs := fm.localManager.State().AllServices()
	var result []ServiceEndpoint
	for _, eps := range allSvcs {
		result = append(result, eps...)
	}
	return result
}

func (fm *FederationManager) syncWithCluster(cluster ClusterInfo, localServices []ServiceEndpoint) ([]ServiceEndpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := fm.dialFederation(ctx, cluster.Address, fedMsgSync)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Send our cluster ID first.
	clusterIDBytes := []byte(fm.config.LocalClusterID)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(clusterIDBytes)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return nil, fmt.Errorf("write cluster ID: %w", err)
	}
	if _, err := conn.Write(clusterIDBytes); err != nil {
		return nil, fmt.Errorf("write cluster ID data: %w", err)
	}

	// Send our services using the existing sync payload format.
	if err := writeSyncPayload(conn, localServices); err != nil {
		return nil, fmt.Errorf("write services: %w", err)
	}

	// Read remote services.
	remote, err := readSyncPayload(conn)
	if err != nil {
		return nil, fmt.Errorf("read services: %w", err)
	}

	return remote, nil
}

func (fm *FederationManager) handleIncomingSync(conn net.Conn) {
	// Read sender's cluster ID.
	var lenBuf [2]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		fm.logger.Error("read sync cluster ID length", "error", err)
		return
	}
	nameLen := binary.BigEndian.Uint16(lenBuf[:])
	if nameLen == 0 || nameLen > 256 {
		fm.logger.Error("invalid sync cluster ID length", "len", nameLen)
		return
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		fm.logger.Error("read sync cluster ID", "error", err)
		return
	}
	remoteClusterID := string(nameBuf)

	// Read remote services.
	remote, err := readSyncPayload(conn)
	if err != nil {
		fm.logger.Error("read sync payload", "error", err, "cluster", remoteClusterID)
		return
	}

	// Send our local services.
	localServices := fm.collectLocalServices()
	if err := writeSyncPayload(conn, localServices); err != nil {
		fm.logger.Error("write sync payload", "error", err, "cluster", remoteClusterID)
		return
	}

	// Update state.
	fm.state.SetRemoteServices(remoteClusterID, remote)
	if _, ok := fm.state.GetCluster(remoteClusterID); ok {
		fm.state.UpdateClusterStatus(remoteClusterID, NodeStatusHealthy)
	}

	fm.logger.Debug("handled incoming sync",
		"from_cluster", remoteClusterID,
		"remote_services", len(remote),
	)
}

// --- Forward handling ---

func (fm *FederationManager) handleIncomingForward(conn net.Conn) {
	fm.Metrics.ForwardTotal.Add(1)
	fm.Metrics.ForwardActive.Add(1)
	defer fm.Metrics.ForwardActive.Add(-1)

	// Read service name.
	serviceName, err := readForwardHeader(conn)
	if err != nil {
		fm.logger.Error("read federation forward header", "error", err)
		fm.Metrics.ForwardErrors.Add(1)
		return
	}

	fm.logger.Debug("incoming federation forward", "service", serviceName)

	// Try to connect to the service locally (via intra-cluster manager).
	if fm.localManager != nil {
		// First check local services.
		if fm.localManager.IsLocal(serviceName) {
			if fm.localManager.handler != nil {
				local, err := fm.localManager.handler.ConnectLocalService(serviceName)
				if err == nil {
					writeForwardResponse(conn, forwardStatusOK, "")
					biCopy(conn, local)
					local.Close()
					return
				}
			}
		}

		// Try forwarding within the local cluster.
		rwc, err := fm.localManager.Forward(context.Background(), serviceName)
		if err == nil {
			writeForwardResponse(conn, forwardStatusOK, "")
			biCopy(conn, rwc)
			rwc.Close()
			return
		}
	}

	writeForwardResponse(conn, forwardStatusNotFound, "service not found in cluster")
	fm.Metrics.ForwardErrors.Add(1)
}

// --- Connection helpers ---

// dialFederation establishes a federation connection to a remote cluster.
// It sends the federation magic byte followed by the message type.
func (fm *FederationManager) dialFederation(ctx context.Context, addr string, msgType byte) (net.Conn, error) {
	dialer := net.Dialer{Timeout: fm.config.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	// Send federation magic + message type.
	if _, err := conn.Write([]byte{federationMagic, msgType}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write federation header: %w", err)
	}

	return conn, nil
}

// Addr returns the federation listener's address, or nil if not listening.
func (fm *FederationManager) Addr() net.Addr {
	if fm.listener == nil {
		return nil
	}
	return fm.listener.Addr()
}
