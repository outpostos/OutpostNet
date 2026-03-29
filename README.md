# OutpostNet

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8.svg)](https://go.dev)
[![Release](https://img.shields.io/github/v/release/outpostos/OutpostNet)](https://github.com/outpostos/OutpostNet/releases)

Lightweight TCP/UDP tunneling for edge computing. Like [frp](https://github.com/fatedier/frp), but built on QUIC with multi-hop relay support.

边缘计算环境的轻量级隧道工具，类似 frp，基于 QUIC 协议，支持多跳中继。

## How It Works

One binary, two roles — just like frp's `frps` + `frpc`, but unified into `outpostnet-agent`:

```
Host A (server role)              Host B (client role)
┌──────────────────┐              ┌──────────────────┐
│  App (:8080)     │  QUIC Tunnel │     App (:9080)   │
│   │              │  (TLS 1.3)   │              │    │
│   ▼              │              │              ▼    │
│  outpostnet-agent ◄════════════► outpostnet-agent  │
│  (export)        │              │        (import)   │
└──────────────────┘              └──────────────────┘
```

## Install

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

Or build from source:

```bash
git clone https://github.com/outpostos/OutpostNet.git && cd OutpostNet
make build-agent    # Output: build/outpostnet-agent
```

## Quick Start

**Host A** — expose a local HTTP service:

```yaml
# agent-server.yaml
region: site-a
tunnels:
  - remote_region: site-b
    role: server
    listen_addr: 0.0.0.0:4242
exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
```

```bash
outpostnet-agent --config agent-server.yaml
```

**Host B** — access the remote service locally:

```yaml
# agent-client.yaml
region: site-b
tunnels:
  - remote_region: site-a
    role: client
    remote_addr: <HOST_A_IP>:4242
imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    remote_region: site-a
    protocol: tcp
```

```bash
outpostnet-agent --config agent-client.yaml
```

**Verify:**

```bash
curl http://127.0.0.1:9080   # Transparently forwards to Host A:8080
```

## Features

| Feature | Description |
|---------|-------------|
| TCP & UDP | Full L4 proxy — HTTP, gRPC, DNS, gaming, anything |
| QUIC Tunnel | Encrypted transport with TLS 1.3, multiplexed streams |
| Token Auth | Agent-to-agent authentication, no PKI required |
| Multi-Hop Relay | Chain through 3+ regions (A → B → C) |
| Auto-Reconnect | Exponential backoff, config persistence for offline recovery |
| Metrics | Prometheus endpoint for tunnel state, streams, traffic |

## vs FRP

| | OutpostNet | FRP |
|---|---|---|
| Binaries | 1 (`outpostnet-agent`) | 2 (`frps` + `frpc`) |
| Transport | QUIC (TLS 1.3) | TCP / KCP / QUIC |
| Multi-hop relay | Native | No |
| Stream mux | QUIC native | yamux |
| Edge-first design | Yes | Generic |
| HTTP vhost | Roadmap | Yes |
| Dashboard | Roadmap | Yes |

## Multi-Hop Relay

Connect services across 3+ regions through relay agents:

```yaml
# Region B — relay between A and C
region: site-b
tunnels:
  - remote_region: site-a
    role: client
    remote_addr: <HOST_A_IP>:4242
  - remote_region: site-c
    role: server
    listen_addr: 0.0.0.0:4243
relays:
  - service: my-http
    from_region: site-a
    to_region: site-c
```

```
Site A ──── QUIC ────► Site B (relay) ──── QUIC ────► Site C
```

## Documentation

- [Quick Start](docs/quickstart.md) — Full setup walkthrough
- [Configuration](docs/configuration.md) — All config options
- [Architecture](docs/architecture.md) — Design and data flow

## Advanced: Multi-App Deployments

For scenarios with multiple services on a single host, OutpostNet provides additional components:

- `outpostnet-server` — Local routing hub (one per host, manages service registration via Unix socket)
- `outpostnet-sidecar` — Per-app proxy (registers with server, one per application)

These are not needed for basic tunneling. See [docs/quickstart.md](docs/quickstart.md) for details.

## Platforms

Linux (amd64, arm64) and macOS (amd64, arm64).

## License

Apache 2.0 — see [LICENSE](LICENSE).
