# OutpostNet Demo Directory

This directory contains **real-world demonstration materials** from OutpostNet development and testing. Unlike the curated examples in `/examples/`, these demos show actual usage scenarios and test results.

## 📁 Directory Structure

```
demo/
└── phase2-region/          # Phase 2 Regional Mesh Demonstrations
    ├── configs/             # Real configurations used in testing
    │   ├── node-a-config.yaml          # Regional mesh primary node
    │   ├── client-proxy.yaml           # Multi-service proxy client
    │   ├── client-register.yaml        # Multi-service registration
    │   └── standalone-config.yaml      # Single-node configuration
    ├── api-response.txt     # Example API service response
    └── web-response.txt     # Example web service response
```

## 🎯 What's Here

### Phase 2 Regional Mesh Demo

This demo showcases OutpostNet's **advanced Phase 2 capabilities**:

- **🌍 Regional Service Mesh**: Multi-node service discovery across geographic regions
- **⚖️ Geographic Load Balancing**: Intelligent routing based on node location
- **🔗 Cross-Cluster Communication**: Service synchronization between edge clusters  
- **🔒 STCP Encrypted Tunnels**: Secure point-to-point encrypted connections
- **🏷️ Rich Service Labeling**: Version-aware and location-aware service routing

### Key Features Demonstrated

1. **Multi-Protocol Support**:
   - TCP for web services (fast, transparent)
   - STCP for API services (secure, encrypted)

2. **Advanced Routing**:
   - Geographic-aware load balancing
   - Label-based service selection
   - Health-aware endpoint routing

3. **Scalable Architecture**:
   - Multiple services per client
   - Cross-region service discovery
   - Automatic failover capabilities

## 🚀 How to Use These Demos

### Option 1: Copy to Examples (Recommended)
The most valuable configurations have already been copied to `/examples/configs/` with enhanced documentation:

- `phase2-region-mesh-config.yaml` - Regional mesh server configuration
- `phase2-multi-proxy-config.yaml` - Multi-service proxy client
- `phase2-multi-register-config.yaml` - Multi-service registration client

### Option 2: Run Directly
You can run these configurations directly for testing:

```bash
# Build binaries
make build

# Start regional mesh server
./build/outpostnet-server -config demo/phase2-region/configs/node-a-config.yaml

# In another terminal: register services
./build/outpostnet-client -config demo/phase2-region/configs/client-register.yaml

# In another terminal: proxy services
./build/outpostnet-client -config demo/phase2-region/configs/client-proxy.yaml

# Test the services
curl http://localhost:9080  # Web service (TCP)
curl http://localhost:9081  # API service (STCP)
```

### Expected Results
- **Web Service**: See content similar to `web-response.txt`
- **API Service**: See JSON similar to `api-response.txt`

## 🔄 Relationship to Examples

| Demo Files | Examples Equivalent | Purpose |
|------------|-------------------|---------|
| `demo/phase2-region/` | `examples/configs/phase2-*` | Real → Polished |
| Raw test output | Clean documentation | Testing → Learning |
| Working configurations | Pedagogical examples | Usage → Teaching |

## 💡 Learning Path

1. **Start with Examples**: Use `/examples/` for learning and development
2. **Reference Demos**: Check `/demo/` for real-world usage patterns  
3. **Customize**: Adapt configurations for your specific environment

---

*These demos represent actual testing scenarios and prove that OutpostNet's advanced features work in practice.*