# Changelog

All notable changes to OutpostNet will be documented in this file.

## [1.0.0] - 2026-03-29

First stable release. OutpostNet 1.0 provides lightweight TCP/UDP tunneling for edge computing environments, comparable to FRP at the L4 proxy layer with added QUIC transport and multi-hop relay capabilities.

### Core Features

- **TCP/UDP proxy** — Full Layer 4 proxy support for TCP and UDP protocols
- **QUIC encrypted tunnels** — Secure cross-region tunnels using QUIC protocol with TLS 1.3
- **Token authentication** — Agent-to-agent token-based authentication for QUIC connections
- **Multi-hop relay** — Chain tunnels across 3+ regions for complex network topologies
- **Auto-reconnect** — Automatic tunnel reconnection with exponential backoff
- **Configuration persistence** — YAML-based config with runtime state persistence for offline recovery
- **Prometheus metrics** — Built-in observability with tunnel state, stream counts, and traffic metrics

### Components

- `outpostnet-server` — Node-level server managing local sidecar registration and routing
- `outpostnet-sidecar` — Application-level proxy for exporting and importing services
- `outpostnet-agent` — Region-level coordinator managing cross-region QUIC tunnels
- `outpostnet-controller` — Kubernetes controller with automatic sidecar injection (K8s only)

### Deployment Modes

- **Standalone** — Direct binary execution, no Kubernetes required
- **Kubernetes** — Automatic sidecar injection via mutating webhook

### Platforms

- linux/amd64, linux/arm64
- darwin/amd64, darwin/arm64

### Documentation

- Quick-start guide (5 minutes to first tunnel)
- Complete configuration reference
- Architecture overview with data flow diagrams
- Standalone and Kubernetes deployment examples
