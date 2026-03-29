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

OutpostNet has 4 components. Which ones you need depends on your deployment scenario:

| Component | When to use |
|-----------|-------------|
| `outpostnet-agent` | **Cross-region tunnels** — the main binary for connecting services across hosts/regions via QUIC. Most users start here. |
| `outpostnet-server` | **Single-host routing** — manages multiple sidecar registrations on one host. Use when multiple apps on the same machine share a tunnel. |
| `outpostnet-sidecar` | **App-level proxy** — exports/imports individual services through a server. Pairs with `outpostnet-server`. |
| `outpostnet-controller` | **Kubernetes only** — automatic sidecar injection via mutating webhook. Not needed for standalone deployments. |

**Simplest setup:** Two hosts, one service → just `outpostnet-agent` on each side.
**Multi-app setup:** Multiple services on one host → `outpostnet-server` + `outpostnet-sidecar` per app + `outpostnet-agent` for cross-region.

## Quick Install

Each component is released as a separate binary. Download only what you need.

### Agent only (most common)

```bash
# Linux amd64
curl -sL https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-agent-v1.0.0-linux-amd64.tar.gz | tar xz
sudo mv outpostnet-agent /usr/local/bin/

# Linux arm64
curl -sL https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-agent-v1.0.0-linux-arm64.tar.gz | tar xz
sudo mv outpostnet-agent /usr/local/bin/

# macOS (Apple Silicon)
curl -sL https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-agent-v1.0.0-darwin-arm64.tar.gz | tar xz
sudo mv outpostnet-agent /usr/local/bin/
```

### Full stack (server + sidecar + agent)

```bash
# Linux amd64 — download all three
for comp in server sidecar agent; do
  curl -sL "https://github.com/outpostos/OutpostNet/releases/latest/download/outpostnet-${comp}-v1.0.0-linux-amd64.tar.gz" | tar xz
done
sudo mv outpostnet-server outpostnet-sidecar outpostnet-agent /usr/local/bin/
```

### Build from source

```bash
git clone https://github.com/outpostos/OutpostNet.git
cd OutpostNet
make build
# Binaries in build/
```

## 5-Minute Quick Start

The simplest scenario: expose a service from Host A to Host B using only `outpostnet-agent`.

**Host A** (service provider — has an HTTP service on port 8080):

```bash
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

For multi-app setups using server + sidecar, see [docs/quickstart.md](docs/quickstart.md).

## Documentation

- [Quick Start](docs/quickstart.md) — Full walkthrough including server + sidecar mode
- [Configuration Reference](docs/configuration.md) — All config options explained
- [Architecture Overview](docs/architecture.md) — Component design and data flow
- [Deployment Examples](deploy/examples/) — Ready-to-use YAML configs

## How It Works

**Agent-only mode** (simplest — two hosts, one service):

```
Host A                          Host B
┌──────────────────┐            ┌──────────────────┐
│  App (:8080)     │            │     App (:9080)   │
│   │              │ QUIC Tunnel│              │    │
│   ▼              │  TLS 1.3   │              ▼    │
│  Agent (export) ◄════════════►  Agent (import)   │
└──────────────────┘            └──────────────────┘
```

**Full stack mode** (multiple apps per host):

```
┌──────────────────────────────┐     QUIC Tunnel     ┌──────────────────────────────┐
│          Region A            │◄══════════════════►│          Region B            │
│                              │   (TLS 1.3, mux)   │                              │
│ App ► Sidecar ► Server ► Agent ◄────────────────► Agent ► Server ► Sidecar ► App │
│ :8080 (export)  (route) (tunnel)                (tunnel) (route)  (import) :9080 │
└──────────────────────────────┘                    └──────────────────────────────┘
```

For multi-hop relay across 3+ regions, see [docs/architecture.md](docs/architecture.md).

## Version

```bash
outpostnet-agent version
outpostnet-server version
outpostnet-sidecar version
```

## Platforms

| OS | Architecture |
|----|-------------|
| Linux | amd64, arm64 |
| macOS | amd64, arm64 (Apple Silicon) |

## License

Apache 2.0 — see [LICENSE](LICENSE).
