# KEP-002: Site-Based Service Mesh for Edge Computing

## Summary

This KEP proposes implementing a site-based service mesh architecture in OutpostNet to address the unique challenges of edge computing scenarios. Unlike traditional Kubernetes clusters with flat network topology, edge computing requires geographical awareness where services are inherently tied to physical locations (sites) such as bank branches, toll stations, or retail stores.

## Motivation

### Current State
OutpostNet Phase 1 provides node-level service mesh capabilities, where a single OutpostNet-server manages services within one edge node. This works well for isolated edge deployments but lacks the ability to discover and communicate with services across multiple nodes within the same geographical location.

### Problem Statement
In edge computing scenarios:
1. **Geographical Constraints**: Services are distributed across multiple edge nodes but within the same physical site (e.g., different systems within a bank branch)
2. **Network Topology**: Unlike K8s flat networks, edge sites have hierarchical network structures
3. **Service Locality**: Service discovery and routing should be site-aware rather than cluster-wide
4. **Fault Isolation**: Network partitions or node failures should not affect the entire site

### Goals
- Enable service discovery and communication within a site (multiple nodes)
- Maintain full compatibility with Phase 1 deployment architecture
- Provide simple service synchronization mechanism without complex routing logic
- Support K8s-compatible service metadata with site extensions

### Non-Goals
- Implement cross-site service routing (reserved for Phase 3)
- Replace existing Phase 1 functionality
- Implement complex service mesh policies (delegated to ServiceKeel)

## Proposal

### Core Concepts

#### Site
A **site** represents a geographical unit containing multiple edge nodes with network connectivity. Examples:
- Bank branch with multiple edge devices
- Toll station with payment and monitoring systems
- Retail store with POS and inventory systems

#### Node
A **node** remains a single OutpostNet-server instance, now with site membership.

#### Service with Site Metadata
Services carry rich metadata including geographical and application context:

```yaml
service:
  metadata:
    name: "payment-service"
    namespace: "charging" 
    labels:
      app: "payment-app"
      version: "v1.2.0"
      # Geographical metadata
      site: "toll-station-001"
      zone: "highway-section-a"
      region: "east-china"
      # Application metadata
      cluster: "edge-cluster-1"
      tier: "backend"
```

### Architecture Overview

```
Site: toll-station-001
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  Node-1              Node-2              Node-3         │
│ ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│ │OutpostNet-     │    │OutpostNet-     │    │OutpostNet-     │   │
│ │server       │◄──►│server       │◄──►│server       │   │
│ │             │    │             │    │             │   │
│ │Local:       │    │Local:       │    │Local:       │   │
│ │- payment    │    │- inventory  │    │- monitoring │   │
│ │             │    │             │    │             │   │
│ │Site Cache:  │    │Site Cache:  │    │Site Cache:  │   │
│ │- inventory  │    │- payment    │    │- payment    │   │
│ │- monitoring │    │- monitoring │    │- inventory  │   │
│ └─────────────┘    └─────────────┘    └─────────────┘   │
│        │                  │                  │          │
│   ┌────┴────┐         ┌───┴────┐        ┌───┴────┐      │
│   │clients  │         │clients │        │clients │      │
│   └─────────┘         └────────┘        └────────┘      │
└─────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Phase 1 Compatibility**: No changes to existing deployment architecture
2. **Simple Synchronization**: Lightweight HTTP-based service sharing between nodes
3. **Metadata Rich**: Services carry complete context for upper-layer decision making
4. **Fault Isolation**: Single node failure doesn't affect other nodes
5. **K8s Compatible**: Standard metadata format with site extensions

### Configuration Design

#### Server Configuration Extension

```yaml
# Existing Phase 1 configuration (unchanged)
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "OutpostNet-token"

services:
  - metadata:
      name: "payment-service"
      namespace: "charging"
      labels:
        app: "payment-service"
        site: "toll-station-001"  # Site information as label
    spec:
      ports:
        - port: 8080
          targetPort: 8080

# Phase 2 addition: Site configuration (optional)
site:
  enabled: true
  site_id: "toll-station-001"
  node_id: "node-1"
  neighbors:
    - node_id: "node-2"
      address: "192.168.1.11:7629"
    - node_id: "node-3"
      address: "192.168.1.12:7629"
  sync:
    interval: "30s"
    health_check: true
    retry_count: 3
```

#### Client Configuration (Unchanged)

```yaml
# Phase 1 client configuration remains 100% compatible
client:
  server_addr: "127.0.0.1:7629"
  auth_token: "OutpostNet-token"
  mode: "register"

workloads:
  - metadata:
      name: "payment-service"
      namespace: "charging"
      labels:
        app: "payment-service"
        site: "toll-station-001"  # Site info in labels
    spec:
      local_addr: "127.0.0.1"
      local_port: 8080
```

### Service Discovery Scope

```yaml
# Phase 1: Node-local services only
query_scope: "local"
results:
  - "payment-service.charging" (local node)

# Phase 2: Site-wide services
query_scope: "site"  
results:
  - "payment-service.charging" (local node)
  - "inventory-service.retail" (node-2)
  - "monitoring-service.ops" (node-3)
```

### API Extensions

#### Service Registry Interface

```go
type SiteRegistry interface {
    // Phase 1 compatible methods (unchanged)
    RegisterService(service *Service) error
    UnregisterService(namespace, name string) error
    QueryServices(namespace, name string, selector map[string]string) ([]*Endpoint, error)
    
    // Phase 2 additions
    QuerySiteServices(namespace, name string, selector map[string]string) ([]*Endpoint, error)
    ListSiteNodes() ([]*Node, error)
    GetSiteInfo() (*SiteInfo, error)
}
```

#### Site Synchronization

```go
type SiteSync interface {
    // Sync local services to site neighbors
    SyncToSite(service *Service) error
    
    // Receive service updates from neighbors
    ReceiveFromSite(nodeID string, services []*Service) error
    
    // Health check neighbors
    CheckNeighborHealth(nodeID string) error
}
```

### Express Mode Enhancements

```bash
# Express Mode with site configuration
./OutpostNet-server --express \
  --site toll-station-001 \
  --node node-1 \
  --service payment-service:8080:8080 \
  --neighbor node-2:192.168.1.11:7629

# Client remains unchanged (site-agnostic)
./OutpostNet-client --express \
  --mode register \
  --service payment-service \
  --backend 127.0.0.1:8080
```

### Implementation Phases

#### Phase 2.1: Service Metadata Enhancement (Week 1-2)
- Extend service metadata to support site labels
- Update service registration to handle rich metadata
- Maintain 100% Phase 1 compatibility

#### Phase 2.2: Site Configuration (Week 2-3)
- Add site configuration to server config
- Implement neighbor discovery and management
- Add site information to Express Mode

#### Phase 2.3: Service Synchronization (Week 3-4)
- Implement HTTP-based service sync protocol
- Add remote service caching
- Handle neighbor health checking

#### Phase 2.4: Site-wide Service Discovery (Week 4-5)
- Extend query APIs for site-wide discovery
- Implement local + remote service aggregation
- Add site-aware load balancing hints

#### Phase 2.5: Testing and Documentation (Week 5-6)
- Comprehensive testing of site scenarios
- Update documentation and examples
- Create site deployment guides

### Testing Strategy

#### Compatibility Testing
- All Phase 1 configurations must work unchanged
- Client APIs remain fully compatible
- Performance regression tests

#### Functional Testing
- Multi-node site deployment
- Service synchronization across nodes
- Network partition recovery
- Neighbor failure handling

#### Integration Testing
- Express Mode site configuration
- Site-wide service discovery
- Cross-node service routing

### Rollout Plan

#### Stage 1: Internal Testing
- Deploy in controlled test environment
- Validate Phase 1 compatibility
- Test site synchronization

#### Stage 2: Limited Edge Deployment
- Deploy to 2-3 edge sites
- Monitor service discovery performance
- Collect operational feedback

#### Stage 3: Production Rollout
- Gradual rollout to production sites
- Monitor and optimize performance
- Document best practices

### Risks and Mitigations

#### Risk: Configuration Complexity
- **Mitigation**: Make site configuration optional and provide Express Mode
- **Fallback**: Continue operating in Phase 1 mode if site config fails

#### Risk: Network Partition Handling
- **Mitigation**: Implement service cache timeout and health checking
- **Fallback**: Gracefully degrade to local services only

#### Risk: Performance Impact
- **Mitigation**: Asynchronous service synchronization
- **Monitoring**: Track sync latency and adjust intervals

### Alternatives Considered

#### Alternative 1: Centralized Site Registry
**Rejected**: Introduces single point of failure and goes against edge autonomy principles.

#### Alternative 2: Complex Service Mesh Policies
**Rejected**: Too complex for Phase 2, delegated to ServiceKeel layer.

#### Alternative 3: Full K8s Cluster Simulation
**Rejected**: Over-engineering for edge scenarios, doesn't address geographical constraints.

## Design Details

### Service Metadata Schema

```yaml
apiVersion: OutpostNet.io/v1
kind: Service
metadata:
  name: "payment-service"
  namespace: "charging"
  labels:
    # Application context
    app: "payment-service"
    version: "v1.2.0" 
    tier: "backend"
    
    # Geographical context  
    site: "toll-station-001"
    zone: "highway-section-a"
    region: "east-china"
    
    # Infrastructure context
    cluster: "edge-cluster-1"
    node: "node-1"
    
  annotations:
    OutpostNet.io/site-role: "primary"
    OutpostNet.io/sync-policy: "eager"
spec:
  selector:
    app: "payment-service"
  ports:
    - name: "http"
      port: 8080
      targetPort: 8080
      protocol: "tcp"
  proxy_mode: "tcp"
  load_balancer: "round_robin"
```

### Site Configuration Schema

```yaml
apiVersion: OutpostNet.io/v1
kind: SiteConfig
metadata:
  name: "toll-station-001"
spec:
  site_id: "toll-station-001"
  node_id: "node-1"
  
  # Neighbor configuration
  neighbors:
    - node_id: "node-2"
      address: "192.168.1.11:7629"
      weight: 1
      zone: "same"
    - node_id: "node-3"  
      address: "192.168.1.12:7629"
      weight: 1
      zone: "same"
      
  # Synchronization settings
  sync:
    interval: "30s"
    timeout: "10s"
    retry_count: 3
    health_check: true
    
  # Security settings
  security:
    tls_enabled: false
    auth_token: "shared-site-token"
    ip_whitelist:
      - "192.168.1.0/24"
```

### Backward Compatibility

#### Configuration Migration

```yaml
# Phase 1 configuration (before)
server:
  listen_addr: "0.0.0.0:7629"
  auth_token: "token"

services:
  - metadata:
      name: "web-service"
      namespace: "default"
    spec:
      ports:
        - port: 80
          targetPort: 8080

# Phase 2 configuration (after) - fully compatible
server:
  listen_addr: "0.0.0.0:7629"  # unchanged
  auth_token: "token"          # unchanged

services:
  - metadata:
      name: "web-service"       # unchanged
      namespace: "default"     # unchanged
      labels:                  # enhanced
        site: "my-site"        # new
    spec:
      ports:                   # unchanged
        - port: 80
          targetPort: 8080

# Optional site configuration
site:                          # new, optional
  enabled: true
  site_id: "my-site"
  node_id: "node-1"
```

## Production Readiness

### Monitoring and Observability

#### Metrics
- Site synchronization latency
- Neighbor health status
- Service discovery cache hit rates
- Cross-node connection counts

#### Logging
- Service sync events
- Neighbor discovery activities
- Network partition detection
- Configuration validation errors

#### Health Checks
- Site neighbor connectivity
- Service sync status
- Local service health
- Remote service availability

### Security Considerations

#### Site Network Security
- IP whitelist for neighbor communication
- Optional TLS for inter-node communication
- Shared authentication tokens
- Service access control inheritance

#### Data Protection
- Service metadata encryption in transit
- Secure neighbor authentication
- Audit logging for site operations

### Performance Characteristics

#### Latency Targets
- Local service discovery: < 1ms (unchanged from Phase 1)
- Site-wide service discovery: < 50ms
- Service synchronization: < 100ms
- Neighbor health check: < 30s

#### Throughput Targets
- Support 50+ services per site
- Handle 100+ queries/second per node
- Sync up to 10 neighbor nodes per site

#### Resource Usage
- Memory overhead: < 10MB for site caching
- Network overhead: < 1KB/s for synchronization
- CPU overhead: < 5% additional load

## Graduation Criteria

### Alpha
- [ ] Basic site configuration support
- [ ] Service metadata enhancement
- [ ] Simple neighbor synchronization
- [ ] Phase 1 compatibility verified

### Beta  
- [ ] Production deployment in 3+ sites
- [ ] Performance benchmarks met
- [ ] Security features implemented
- [ ] Express Mode site support

### Stable
- [ ] 6+ months production usage
- [ ] Zero backward compatibility breaks
- [ ] Complete documentation
- [ ] Migration tools available

## Implementation History

- 2024-XX-XX: KEP created and initial design
- 2024-XX-XX: Phase 2.1 implementation started
- 2024-XX-XX: Alpha release with basic site support
- 2024-XX-XX: Beta release with full feature set
- 2024-XX-XX: Stable release

## References

- [KEP-001: Express Mode Simplification](KEP-001-express-mode-simplification.md)
- [Phase 2 Cluster Design](../phase2-cluster-design.md)
- [Current Implementation Status](../current-implementation.md)
- [Edge Computing Service Mesh Blog](../blog/edge-service-mesh-evolution.md)