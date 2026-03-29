# OutpostNet

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8.svg)](https://go.dev)
[![Release](https://img.shields.io/github/v/release/outpostos/OutpostNet)](https://github.com/outpostos/OutpostNet/releases)

Lightweight TCP/UDP tunneling proxy for edge computing. Expose local services across regions without public IPs — like [frp](https://github.com/fatedier/frp), built on QUIC with multi-hop relay.

边缘计算环境的轻量级 TCP/UDP 隧道代理，支持跨区域服务访问，无需公网 IP。基于 QUIC 协议，支持多跳中继。

## Features

- **TCP & UDP proxy** — Full L4 protocol support for HTTP, gRPC, DNS, gaming, and more
- **QUIC tunnels** — Encrypted cross-region tunnels with TLS 1.3, multiplexed streams
- **Token auth** — Agent-to-agent authentication for secure tunnel establishment
- **Multi-hop relay** — Chain tunnels across 3+ regions for complex topologies
- **Auto-reconnect** — Automatic reconnection with exponential backoff and config persistence
- **Prometheus metrics** — Built-in observability for tunnel state, streams, and traffic
- **Dual deployment** — Standalone binaries or Kubernetes with automatic sidecar injection

## Components

| Component | Role |
|-----------|------|
| `outpostnet-server` | Node-level server — manages local sidecar registration and routing (one per host) |
| `outpostnet-sidecar` | App-level proxy — exports local services and imports remote services |
| `outpostnet-agent` | Region-level coordinator — manages cross-region QUIC tunnels |
| `outpostnet-controller` | K8s controller — automatic sidecar injection via mutating webhook |

## Quick Install

### Download pre-built binaries

```bash
# Linux amd64
curl -sL https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-linux-amd64.tar.gz | tar xz
sudo mv outpostnet-* /usr/local/bin/

# Linux arm64
curl -sL https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-linux-arm64.tar.gz | tar xz
sudo mv outpostnet-* /usr/local/bin/

# macOS (Apple Silicon)
curl -sL https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-darwin-arm64.tar.gz | tar xz
sudo mv outpostnet-* /usr/local/bin/
```

### Build from source

```bash
git clone https://github.com/outpostos/OutpostNet.git
cd OutpostNet
make build
# Binaries in build/
```

## 5-Minute Quick Start

**Host A** (service provider — has an HTTP service on port 8080):

```bash
# Start agent in server role
cat > agent-a.yaml << EOF
region: region-a
tunnels:
  - remote_region: region-b
    role: server
    listen_addr: 0.0.0.0:4242
exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
EOF

outpostnet-agent --config agent-a.yaml
```

**Host B** (service consumer — wants to access Host A's service):

```bash
# Start agent in client role
cat > agent-b.yaml << EOF
region: region-b
tunnels:
  - remote_region: region-a
    role: client
    remote_addr: <HOST_A_IP>:4242
imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    remote_region: region-a
    protocol: tcp
EOF

outpostnet-agent --config agent-b.yaml
```

**Verify:**

```bash
# On Host B — traffic transparently forwards to Host A:8080
curl http://127.0.0.1:9080
```

See [docs/quickstart.md](docs/quickstart.md) for the full walkthrough including server + sidecar mode.

## Documentation

- [Quick Start](docs/quickstart.md) — 5 minutes to your first tunnel
- [Configuration Reference](docs/configuration.md) — All config options explained
- [Architecture Overview](docs/architecture.md) — Component design and data flow
- [Deployment Examples](deploy/examples/) — Ready-to-use YAML configs

## How It Works

```
┌─────────────────────────────────┐     QUIC Tunnel      ┌─────────────────────────────────┐
│           Region A              │◄════════════════════►│           Region B              │
│                                 │   (TLS 1.3, mux)     │                                 │
│  App ──► Sidecar ──► Server ──► Agent ◄─────────────► Agent ──► Server ──► Sidecar ──► App │
│  :8080   (export)    (route)   (tunnel)              (tunnel)  (route)   (import)   :9080│
└─────────────────────────────────┘                      └─────────────────────────────────┘
```

For multi-hop relay across 3+ regions, see [docs/architecture.md](docs/architecture.md).

## Version

```bash
outpostnet-server version
outpostnet-agent version
outpostnet-sidecar version
```

## Platforms

| OS | Architecture |
|----|-------------|
| Linux | amd64, arm64 |
| macOS | amd64, arm64 (Apple Silicon) |

## License

Apache 2.0 — see [LICENSE](LICENSE).
