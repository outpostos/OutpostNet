# OutpostNet Examples

This directory contains configuration examples and test programs for different OutpostNet proxy modes.

## Directory Structure

```
examples/
├── configs/           # Configuration examples for different proxy modes
│   ├── demo-*-config.yaml    # Complete configuration examples
│   ├── client-*-config.yaml  # Legacy configuration examples
│   └── server-config.yaml    # Legacy server configuration
├── tests/             # Test programs and scripts
│   ├── test_*_server.go      # Test server programs
│   ├── test_*_client.go      # Test client programs
│   └── test_*_proxy.sh       # End-to-end test scripts
└── quick-start/       # Quick start demonstration
    └── demo.sh               # All-in-one demonstration script
```

## Available Proxy Modes

### 1. TCP Mode (Standard)
- **Config**: `configs/demo-server-config.yaml`, `demo-register-config.yaml`, `demo-proxy-config.yaml`
- **Test**: `tests/test_tcp_proxy.sh` (uses existing HTTP server)
- **Description**: Standard TCP transparent proxy with service discovery

### 2. UDP Mode 
- **Config**: `configs/demo-udp-server-config.yaml`, `demo-udp-register-config.yaml`, `demo-udp-proxy-config.yaml`
- **Test**: `tests/test_udp_proxy.sh`
- **Description**: UDP packet forwarding with session management

### 3. STCP Mode (Encrypted Tunnel)
- **Config**: `configs/demo-stcp-server-config.yaml`, `demo-stcp-register-config.yaml`, `demo-stcp-proxy-config.yaml`
- **Test**: `tests/test_stcp_proxy.sh`
- **Description**: Encrypted tunnel for secure point-to-point communication

## Quick Start

Run the comprehensive demonstration:

```bash
# Build binaries first
make build

# Run all proxy modes demonstration
./examples/quick-start/demo.sh
```

Or test individual modes:

```bash
# Test UDP proxy
./examples/tests/test_udp_proxy.sh

# Test STCP encrypted tunnel
./examples/tests/test_stcp_proxy.sh
```

## Configuration Guide

### Server Configuration
All server configurations follow the same pattern:
- Define services with selectors
- Configure authentication tokens
- Set load balancing algorithms

### Client Register Configuration  
Register mode clients expose local services:
- Define workload metadata with labels
- Specify local service address and port
- Labels are used for service discovery

### Client Proxy Configuration
Proxy mode clients provide local access to remote services:
- Specify target service name and namespace
- Define local proxy port
- Configure service selectors for endpoint filtering

## K8s 兼容 API 管理

OutpostNet-server 提供完全兼容 Kubernetes 的 REST API，可以直接使用 kubectl 进行管理：

```bash
# 启动服务器（启用 K8s API）
./bin/OutpostNet-server -config examples/configs/server-with-k8s-api.yaml

# 配置 kubectl
kubectl config set-cluster OutpostNet --server=http://localhost:8080 --insecure-skip-tls-verify=true
kubectl config set-credentials OutpostNet-user
kubectl config set-context OutpostNet --cluster=OutpostNet --user=OutpostNet-user --namespace=default
kubectl config use-context OutpostNet

# 使用 kubectl 管理服务
kubectl get services
kubectl apply -f examples/kubectl/nginx-service.yaml
kubectl describe service nginx-service
```

更多详细用法请参考 [kubectl 使用指南](kubectl/README.md)。