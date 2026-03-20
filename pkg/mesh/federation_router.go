package mesh

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
)

// FederationRouter provides unified service routing that checks local cluster
// first, then falls back to federated clusters. It integrates the intra-cluster
// Manager, FederationManager, and ServiceDiscovery into a single lookup path.
type FederationRouter struct {
	logger    *slog.Logger
	local     *Manager
	federation *FederationManager
	discovery  *ServiceDiscovery
	election   *LeaderElection
	Metrics    RouterMetrics
}

// RouterMetrics tracks routing performance counters.
type RouterMetrics struct {
	LocalHits       atomic.Int64
	ClusterHits     atomic.Int64
	FederationHits  atomic.Int64
	RoutingErrors   atomic.Int64
	TotalRequests   atomic.Int64
}

// NewFederationRouter creates a router that integrates all mesh components.
func NewFederationRouter(
	local *Manager,
	federation *FederationManager,
	discovery *ServiceDiscovery,
	election *LeaderElection,
	logger *slog.Logger,
) *FederationRouter {
	if logger == nil {
		logger = slog.Default()
	}
	return &FederationRouter{
		logger:     logger.With("component", "router"),
		local:      local,
		federation: federation,
		discovery:  discovery,
		election:   election,
	}
}

// Route attempts to connect to a service using a tiered lookup strategy:
//  1. Check if the service is available on the local node
//  2. Check if the service is available on other nodes in the local cluster
//  3. Check if the service is available on federated remote clusters
//
// Returns a ReadWriteCloser for bidirectional data relay.
func (r *FederationRouter) Route(ctx context.Context, serviceName string) (io.ReadWriteCloser, error) {
	r.Metrics.TotalRequests.Add(1)

	// Tier 1: Local node.
	if r.local != nil && r.local.IsLocal(serviceName) {
		if r.local.handler != nil {
			rwc, err := r.local.handler.ConnectLocalService(serviceName)
			if err == nil {
				r.Metrics.LocalHits.Add(1)
				r.logger.Debug("routed to local node", "service", serviceName)
				return rwc, nil
			}
		}
	}

	// Tier 2: Other nodes in local cluster.
	if r.local != nil {
		rwc, err := r.local.Forward(ctx, serviceName)
		if err == nil {
			r.Metrics.ClusterHits.Add(1)
			r.logger.Debug("routed to cluster peer", "service", serviceName)
			return rwc, nil
		}
		if err != ErrServiceNotFound {
			r.logger.Debug("cluster forward failed", "service", serviceName, "error", err)
		}
	}

	// Tier 3: Federated remote clusters.
	if r.federation != nil {
		rwc, err := r.routeFederated(ctx, serviceName)
		if err == nil {
			r.Metrics.FederationHits.Add(1)
			r.logger.Debug("routed to federated cluster", "service", serviceName)
			return rwc, nil
		}
		if err != ErrServiceNotFound {
			r.logger.Debug("federation forward failed", "service", serviceName, "error", err)
		}
	}

	r.Metrics.RoutingErrors.Add(1)
	return nil, fmt.Errorf("%w: %s not found in local cluster or federation", ErrServiceNotFound, serviceName)
}

// routeFederated handles the federation tier of routing.
// If leader election is configured, only the leader performs federation forwarding.
func (r *FederationRouter) routeFederated(ctx context.Context, serviceName string) (io.ReadWriteCloser, error) {
	// If election is configured, check if we're the leader.
	// Only leaders handle cross-cluster traffic to avoid duplicate forwarding.
	if r.election != nil && !r.election.IsLeader() {
		return nil, ErrServiceNotFound
	}

	// First check if we already know about the service from periodic sync.
	eps := r.federation.State().LookupRemoteService(serviceName)
	if len(eps) > 0 {
		// Try each cluster that has the service.
		return r.federation.ForwardFederated(ctx, serviceName)
	}

	// If not found in sync state, try on-demand discovery.
	if r.discovery != nil {
		discovered, err := r.discovery.Discover(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		if len(discovered) > 0 {
			// Now try forwarding to the discovered cluster.
			return r.federation.ForwardToCluster(ctx, discovered[0].ClusterID, serviceName)
		}
	}

	return nil, ErrServiceNotFound
}

// RouteInfo describes where a service can be found.
type RouteInfo struct {
	// Tier indicates where the service was found.
	Tier string // "local", "cluster", "federation"

	// ClusterID is set for federation-tier services.
	ClusterID string

	// NodeID is set for cluster-tier services.
	NodeID string

	// Endpoints lists all known endpoints for the service.
	Endpoints []ServiceEndpoint
}

// Lookup returns information about where a service can be found without
// establishing a connection. Useful for diagnostics and monitoring.
func (r *FederationRouter) Lookup(serviceName string) []RouteInfo {
	var routes []RouteInfo

	// Check local.
	if r.local != nil && r.local.IsLocal(serviceName) {
		routes = append(routes, RouteInfo{
			Tier:   "local",
			NodeID: r.local.config.LocalNodeID,
		})
	}

	// Check cluster peers.
	if r.local != nil {
		remoteEps := r.local.State().LookupRemote(serviceName)
		for _, ep := range remoteEps {
			routes = append(routes, RouteInfo{
				Tier:      "cluster",
				NodeID:    ep.NodeID,
				Endpoints: []ServiceEndpoint{ep},
			})
		}
	}

	// Check federation.
	if r.federation != nil {
		fedEps := r.federation.State().LookupRemoteService(serviceName)
		for _, fep := range fedEps {
			routes = append(routes, RouteInfo{
				Tier:      "federation",
				ClusterID: fep.ClusterID,
				Endpoints: []ServiceEndpoint{fep.Endpoint},
			})
		}
	}

	return routes
}
