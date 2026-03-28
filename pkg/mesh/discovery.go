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

// Discovery wire protocol message types (sent after federation magic + fedMsgQuery).
const (
	fedMsgQuery    byte = 0x05 // on-demand service query
	fedMsgResponse byte = 0x06 // query response
)

// ServiceCacheEntry holds a cached remote service with TTL tracking.
type ServiceCacheEntry struct {
	ClusterID string
	Endpoints []ServiceEndpoint
	CachedAt  time.Time
	TTL       time.Duration
}

// IsExpired returns true if the cache entry has expired.
func (e *ServiceCacheEntry) IsExpired() bool {
	return time.Since(e.CachedAt) > e.TTL
}

// DiscoveryConfig configures the cross-cluster service discovery.
type DiscoveryConfig struct {
	// CacheTTL is how long cached service lookups are valid.
	// Defaults to 30s.
	CacheTTL time.Duration

	// QueryTimeout is the maximum time to wait for a query response.
	// Defaults to 5s.
	QueryTimeout time.Duration

	// DialTimeout for connecting to remote clusters.
	DialTimeout time.Duration
}

// DefaultDiscoveryConfig returns a config with sensible defaults.
func DefaultDiscoveryConfig() DiscoveryConfig {
	return DiscoveryConfig{
		CacheTTL:     30 * time.Second,
		QueryTimeout: 5 * time.Second,
		DialTimeout:  5 * time.Second,
	}
}

// DiscoveryMetrics tracks service discovery counters.
type DiscoveryMetrics struct {
	QueriesSent     atomic.Int64
	QueriesReceived atomic.Int64
	QueryErrors     atomic.Int64
	CacheHits       atomic.Int64
	CacheMisses     atomic.Int64
}

// ServiceDiscovery provides on-demand cross-cluster service discovery with caching.
// While FederationManager performs periodic bulk sync, ServiceDiscovery handles
// real-time, on-demand lookups for specific services.
type ServiceDiscovery struct {
	config     DiscoveryConfig
	logger     *slog.Logger
	federation *FederationManager
	Metrics    DiscoveryMetrics

	// cache stores recent lookup results keyed by "clusterID:serviceName".
	mu    sync.RWMutex
	cache map[string]*ServiceCacheEntry

	// watchers are notified when service state changes.
	watchMu  sync.RWMutex
	watchers map[string][]ServiceWatcher // serviceName -> watchers

	done chan struct{}
	wg   sync.WaitGroup
}

// ServiceWatcher is called when a service's endpoints change.
type ServiceWatcher func(serviceName string, endpoints []FederatedEndpoint)

// NewServiceDiscovery creates a new ServiceDiscovery.
func NewServiceDiscovery(config DiscoveryConfig, federation *FederationManager, logger *slog.Logger) *ServiceDiscovery {
	if logger == nil {
		logger = slog.Default()
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 30 * time.Second
	}
	if config.QueryTimeout == 0 {
		config.QueryTimeout = 5 * time.Second
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}

	return &ServiceDiscovery{
		config:     config,
		logger:     logger.With("component", "discovery"),
		federation: federation,
		cache:      make(map[string]*ServiceCacheEntry),
		watchers:   make(map[string][]ServiceWatcher),
		done:       make(chan struct{}),
	}
}

// Start begins the cache maintenance loop.
func (sd *ServiceDiscovery) Start(ctx context.Context) {
	sd.wg.Add(1)
	go func() {
		defer sd.wg.Done()
		sd.cacheMaintenanceLoop(ctx)
	}()
}

// Stop shuts down the service discovery.
func (sd *ServiceDiscovery) Stop() {
	close(sd.done)
	sd.wg.Wait()
}

// Discover finds endpoints for a service across all federated clusters.
// It first checks the cache, then queries remote clusters on cache miss.
func (sd *ServiceDiscovery) Discover(ctx context.Context, serviceName string) ([]FederatedEndpoint, error) {
	// Check federation state first (populated by periodic sync).
	eps := sd.federation.State().LookupRemoteService(serviceName)
	if len(eps) > 0 {
		sd.Metrics.CacheHits.Add(1)
		return eps, nil
	}

	// Check local cache.
	if cached := sd.getCached(serviceName); cached != nil {
		sd.Metrics.CacheHits.Add(1)
		var result []FederatedEndpoint
		for _, ep := range cached.Endpoints {
			result = append(result, FederatedEndpoint{
				ClusterID: cached.ClusterID,
				Endpoint:  ep,
			})
		}
		return result, nil
	}

	sd.Metrics.CacheMisses.Add(1)

	// Query all healthy clusters.
	clusters := sd.federation.State().HealthyClusters()
	if len(clusters) == 0 {
		return nil, ErrServiceNotFound
	}

	var allEndpoints []FederatedEndpoint
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, cluster := range clusters {
		wg.Add(1)
		go func(c ClusterInfo) {
			defer wg.Done()
			eps, err := sd.queryCluster(ctx, c, serviceName)
			if err != nil {
				sd.logger.Debug("query cluster failed",
					"cluster", c.ClusterID,
					"service", serviceName,
					"error", err,
				)
				return
			}

			if len(eps) > 0 {
				// Cache the result.
				sd.setCache(c.ClusterID, serviceName, eps)

				mu.Lock()
				for _, ep := range eps {
					allEndpoints = append(allEndpoints, FederatedEndpoint{
						ClusterID: c.ClusterID,
						Address:   c.Address,
						Endpoint:  ep,
					})
				}
				mu.Unlock()
			}
		}(cluster)
	}

	wg.Wait()

	if len(allEndpoints) == 0 {
		return nil, ErrServiceNotFound
	}

	// Notify watchers.
	sd.notifyWatchers(serviceName, allEndpoints)

	return allEndpoints, nil
}

// Watch registers a watcher for service endpoint changes.
func (sd *ServiceDiscovery) Watch(serviceName string, watcher ServiceWatcher) {
	sd.watchMu.Lock()
	defer sd.watchMu.Unlock()
	sd.watchers[serviceName] = append(sd.watchers[serviceName], watcher)
}

// HandleServiceQuery processes an incoming service query from a remote cluster.
// This is called by the FederationManager when it receives a fedMsgQuery.
func (sd *ServiceDiscovery) HandleServiceQuery(conn net.Conn) {
	sd.Metrics.QueriesReceived.Add(1)
	conn.SetDeadline(time.Now().Add(sd.config.QueryTimeout))

	// Read service name.
	serviceName, err := readDiscoveryQuery(conn)
	if err != nil {
		sd.logger.Error("read discovery query", "error", err)
		sd.Metrics.QueryErrors.Add(1)
		return
	}

	sd.logger.Debug("received service query", "service", serviceName)

	// Look up service locally.
	var endpoints []ServiceEndpoint
	if sd.federation.localManager != nil {
		allSvcs := sd.federation.localManager.State().AllServices()
		if eps, ok := allSvcs[serviceName]; ok {
			endpoints = eps
		}
	}

	// Send response.
	if err := writeDiscoveryResponse(conn, endpoints); err != nil {
		sd.logger.Error("write discovery response", "error", err)
		sd.Metrics.QueryErrors.Add(1)
		return
	}
}

// --- Query logic ---

func (sd *ServiceDiscovery) queryCluster(ctx context.Context, cluster ClusterInfo, serviceName string) ([]ServiceEndpoint, error) {
	sd.Metrics.QueriesSent.Add(1)

	conn, err := sd.federation.dialFederation(ctx, cluster.Address, fedMsgQuery)
	if err != nil {
		sd.Metrics.QueryErrors.Add(1)
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(sd.config.QueryTimeout))

	// Send service name query.
	if err := writeDiscoveryQuery(conn, serviceName); err != nil {
		sd.Metrics.QueryErrors.Add(1)
		return nil, fmt.Errorf("write query: %w", err)
	}

	// Read response.
	eps, err := readDiscoveryResponse(conn)
	if err != nil {
		sd.Metrics.QueryErrors.Add(1)
		return nil, fmt.Errorf("read response: %w", err)
	}

	return eps, nil
}

// --- Cache management ---

func (sd *ServiceDiscovery) getCached(serviceName string) *ServiceCacheEntry {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// Check all clusters for cached entries.
	for _, entry := range sd.cache {
		if entry.IsExpired() {
			continue
		}
		for _, ep := range entry.Endpoints {
			if ep.ServiceName == serviceName {
				return entry
			}
		}
	}
	return nil
}

func (sd *ServiceDiscovery) setCache(clusterID, serviceName string, endpoints []ServiceEndpoint) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	key := clusterID + ":" + serviceName
	sd.cache[key] = &ServiceCacheEntry{
		ClusterID: clusterID,
		Endpoints: endpoints,
		CachedAt:  time.Now(),
		TTL:       sd.config.CacheTTL,
	}
}

func (sd *ServiceDiscovery) cacheMaintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(sd.config.CacheTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sd.done:
			return
		case <-ticker.C:
			sd.evictExpired()
		}
	}
}

func (sd *ServiceDiscovery) evictExpired() {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	for key, entry := range sd.cache {
		if entry.IsExpired() {
			delete(sd.cache, key)
		}
	}
}

// CacheSize returns the number of cached entries (for testing).
func (sd *ServiceDiscovery) CacheSize() int {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return len(sd.cache)
}

// --- Watcher notification ---

func (sd *ServiceDiscovery) notifyWatchers(serviceName string, endpoints []FederatedEndpoint) {
	sd.watchMu.RLock()
	watchers := sd.watchers[serviceName]
	sd.watchMu.RUnlock()

	for _, w := range watchers {
		w(serviceName, endpoints)
	}
}

// --- Wire protocol for discovery queries ---

// Query format: serviceNameLen(2) + serviceName(var)
func writeDiscoveryQuery(w io.Writer, serviceName string) error {
	name := []byte(serviceName)
	buf := make([]byte, 2+len(name))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(name)))
	copy(buf[2:], name)
	_, err := w.Write(buf)
	return err
}

func readDiscoveryQuery(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	nameLen := binary.BigEndian.Uint16(lenBuf[:])
	if nameLen == 0 || nameLen > 4096 {
		return "", fmt.Errorf("invalid query name length: %d", nameLen)
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBuf); err != nil {
		return "", err
	}
	return string(nameBuf), nil
}

// Response format: uses the existing writeSyncPayload/readSyncPayload format.
func writeDiscoveryResponse(w io.Writer, endpoints []ServiceEndpoint) error {
	return writeSyncPayload(w, endpoints)
}

func readDiscoveryResponse(r io.Reader) ([]ServiceEndpoint, error) {
	return readSyncPayload(r)
}
