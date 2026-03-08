// Package mesh implements cluster-level service mesh functionality for OutpostNet,
// including cross-node service request forwarding and cluster state management.
package mesh

import (
	"sync"
	"time"
)

// NodeStatus represents the health status of a cluster node.
type NodeStatus int

const (
	NodeStatusUnknown   NodeStatus = iota
	NodeStatusHealthy
	NodeStatusUnhealthy
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusHealthy:
		return "healthy"
	case NodeStatusUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// NodeInfo holds metadata about a peer node in the cluster.
type NodeInfo struct {
	NodeID   string
	Address  string // host:port for forwarding connections
	Status   NodeStatus
	LastSeen time.Time
}

// ServiceEndpoint describes a service available on a specific node.
type ServiceEndpoint struct {
	NodeID      string
	ServiceName string
	Protocol    uint8 // 0=TCP, 1=UDP
	LocalPort   uint16
	Weight      int // for weighted load balancing; default 1
	Healthy     bool
}

// ClusterState tracks the cluster topology and service distribution.
// It is safe for concurrent use.
type ClusterState struct {
	mu          sync.RWMutex
	localNodeID string
	nodes       map[string]*NodeInfo
	services    map[string][]ServiceEndpoint // serviceName -> endpoints
}

// NewClusterState creates a new ClusterState for the given local node.
func NewClusterState(localNodeID string) *ClusterState {
	return &ClusterState{
		localNodeID: localNodeID,
		nodes:       make(map[string]*NodeInfo),
		services:    make(map[string][]ServiceEndpoint),
	}
}

// LocalNodeID returns the local node's identifier.
func (cs *ClusterState) LocalNodeID() string {
	return cs.localNodeID
}

// AddNode registers or updates a peer node.
func (cs *ClusterState) AddNode(info NodeInfo) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.nodes[info.NodeID] = &info
}

// RemoveNode removes a peer node and all its associated services.
func (cs *ClusterState) RemoveNode(nodeID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	delete(cs.nodes, nodeID)

	for svcName, eps := range cs.services {
		filtered := eps[:0]
		for _, ep := range eps {
			if ep.NodeID != nodeID {
				filtered = append(filtered, ep)
			}
		}
		if len(filtered) == 0 {
			delete(cs.services, svcName)
		} else {
			cs.services[svcName] = filtered
		}
	}
}

// AddService registers a service endpoint. If an endpoint for the same
// node and service already exists, it is updated.
func (cs *ClusterState) AddService(ep ServiceEndpoint) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if ep.Weight <= 0 {
		ep.Weight = 1
	}

	eps := cs.services[ep.ServiceName]
	for i, existing := range eps {
		if existing.NodeID == ep.NodeID {
			eps[i] = ep
			return
		}
	}
	cs.services[ep.ServiceName] = append(eps, ep)
}

// RemoveService removes a service endpoint from a specific node.
func (cs *ClusterState) RemoveService(serviceName, nodeID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	eps := cs.services[serviceName]
	for i, ep := range eps {
		if ep.NodeID == nodeID {
			cs.services[serviceName] = append(eps[:i], eps[i+1:]...)
			if len(cs.services[serviceName]) == 0 {
				delete(cs.services, serviceName)
			}
			return
		}
	}
}

// LookupRemote returns service endpoints on remote nodes (excluding local).
// Returns only endpoints on healthy nodes.
func (cs *ClusterState) LookupRemote(serviceName string) []ServiceEndpoint {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var remote []ServiceEndpoint
	for _, ep := range cs.services[serviceName] {
		if ep.NodeID == cs.localNodeID {
			continue
		}
		if !ep.Healthy {
			continue
		}
		node, ok := cs.nodes[ep.NodeID]
		if !ok || node.Status != NodeStatusHealthy {
			continue
		}
		remote = append(remote, ep)
	}
	return remote
}

// IsLocal checks whether the service has an endpoint on the local node.
func (cs *ClusterState) IsLocal(serviceName string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for _, ep := range cs.services[serviceName] {
		if ep.NodeID == cs.localNodeID {
			return true
		}
	}
	return false
}

// GetNode returns information about a specific node.
func (cs *ClusterState) GetNode(nodeID string) (NodeInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if n, ok := cs.nodes[nodeID]; ok {
		return *n, true
	}
	return NodeInfo{}, false
}

// HealthyNodes returns all healthy peer nodes.
func (cs *ClusterState) HealthyNodes() []NodeInfo {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var result []NodeInfo
	for _, n := range cs.nodes {
		if n.Status == NodeStatusHealthy {
			result = append(result, *n)
		}
	}
	return result
}

// AllServices returns a snapshot of all known services across the cluster.
func (cs *ClusterState) AllServices() map[string][]ServiceEndpoint {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string][]ServiceEndpoint, len(cs.services))
	for name, eps := range cs.services {
		cpy := make([]ServiceEndpoint, len(eps))
		copy(cpy, eps)
		result[name] = cpy
	}
	return result
}

// NodeCount returns the number of known peer nodes.
func (cs *ClusterState) NodeCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.nodes)
}
