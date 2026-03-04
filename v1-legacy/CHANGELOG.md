# Changelog

All notable changes to OutpostNet will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2025-07-15 - Phase A Complete

### 🎯 Major Features Added

#### SUDP Mode - Encrypted UDP over TCP Tunneling
- **Client-side UDP listening** - Monitors local UDP port and captures packets
- **Encrypted TCP tunnel** - Establishes secure tunnel to server
- **Server-side UDP forwarding** - Forwards decrypted packets to target UDP services
- **Binary packet encoding** - Efficient packet format with client address mapping
- **Session management** - Handles multiple client sessions through single tunnel

#### Enhanced Load Balancing System
- **5 Load Balancing Algorithms**:
  - `round_robin` - Evenly distributes requests across endpoints
  - `weighted_round_robin` - Distributes based on endpoint weights (high/medium/low)
  - `least_connections` - Routes to endpoint with fewest active connections
  - `random` - Random endpoint selection for stateless services
  - `consistent_hash` - Session affinity using consistent hashing with virtual nodes
- **Per-service Load Balancer Management** - Isolated load balancer instances per service
- **Dynamic Algorithm Switching** - Configure different algorithms per service

#### Comprehensive Health Check Mechanism
- **TCP/UDP Health Checkers** - Protocol-specific health validation
- **Configurable Thresholds** - Customizable healthy/unhealthy confirmation counts
- **Background Health Monitoring** - Continuous endpoint health tracking
- **Health-aware Load Balancing** - Automatic exclusion of unhealthy endpoints
- **Graceful Fallback** - Falls back to all endpoints when none are healthy
- **Automatic Endpoint Registration** - Health checks auto-added for new workloads
- **Clean Disconnection Handling** - Removes health checks on client disconnect

### 🔧 Technical Improvements

#### Server Enhancements
- **Health Check Integration** - Embedded health checker in server lifecycle
- **Enhanced Endpoint Selection** - Health-aware endpoint filtering in all proxy modes
- **Improved Connection Management** - Better client connection tracking and cleanup
- **Load Balancer Manager Integration** - Unified load balancing across all services

#### Client Enhancements  
- **Type Safety Improvements** - Fixed endpoint type compatibility across client/server
- **Better Error Handling** - Improved error reporting and connection recovery
- **Load Balancer Integration** - Client-side load balancing for SUDP mode

#### Architecture Improvements
- **Unified Load Balancing Interface** - Consistent API across all algorithms
- **Plugin Architecture** - Extensible design for adding new algorithms
- **Thread-safe Operations** - Concurrent-safe load balancer and health check operations
- **Resource Management** - Proper cleanup and resource lifecycle management

### 🐛 Bug Fixes
- Fixed type mismatches between `[]Endpoint` and `[]*Endpoint` in client code
- Resolved port formatting issues in health check connections
- Fixed duplicate function definitions in load balancer package
- Corrected build compilation errors across all packages

### 📚 Documentation Updates
- Updated README.md with Phase A completion status
- Added comprehensive load balancing algorithm documentation
- Enhanced proxy mode comparison table
- Updated development status and roadmap
- Expanded technical features section

### 🧪 Testing & Quality
- All existing tests pass with new functionality
- Build system validates across all packages
- Integration tests cover new health check features
- Load balancer algorithms have unit test coverage

## [0.1.0] - 2025-07-14 - Initial Implementation

### Added
- Basic TCP/UDP proxy functionality
- STCP encrypted tunnel mode
- K8s-style service discovery (Service/Workload/Endpoint model)
- Basic load balancing (round robin)
- YAML configuration system
- Client register/proxy modes
- Service registry with label selectors
- Basic health monitoring

### Technical Foundation
- Go module setup with proper dependency management
- Makefile for build automation
- Basic test infrastructure
- Configuration validation system
- Client-server architecture with JSON messaging protocol

---

## Development Phases

### Phase A ✅ - Core Functionality Enhancement (Completed)
**Goal**: Complete the core proxy functionality with advanced features
- ✅ SUDP mode implementation
- ✅ Enhanced load balancing algorithms
- ✅ Comprehensive health check mechanism

### Phase B 🔄 - Hierarchical Service Discovery (Next)
**Goal**: Implement distributed service discovery and cross-cluster communication
- 🔲 Hierarchical routing (local → cross-node → cross-cluster → cloud)
- 🔲 Distributed service registry with gossip protocol
- 🔲 Edge node communication via Unix sockets
- 🔲 Cross-cluster service mesh capabilities

### Phase C 📋 - Product Completeness (Future)
**Goal**: Enterprise-ready features and management capabilities
- 🔲 HTTP/HTTPS ingress gateway
- 🔲 Prometheus metrics and monitoring
- 🔲 Web UI and enhanced CLI tools
- 🔲 RBAC and enterprise security features

---

## Upgrade Notes

### From 0.1.0 to 0.2.0
- **Configuration compatibility**: All existing configurations remain valid
- **API compatibility**: No breaking changes to client/server protocol
- **New features**: Health checks and enhanced load balancing are opt-in
- **Performance**: Improved endpoint selection performance with health filtering
- **Dependencies**: No new external dependencies added

### Migration Guide
1. Update binaries to 0.2.0
2. Optionally configure health check settings in server config
3. Consider upgrading load balancer algorithms for better performance
4. Review service configurations for optimal load balancing strategy

For detailed migration assistance, see [MIGRATION.md](docs/MIGRATION.md).