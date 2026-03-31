# OutpostNet

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8.svg)](https://go.dev)
[![Release](https://img.shields.io/github/v/release/outpostos/OutpostNet)](https://github.com/outpostos/OutpostNet/releases)

OutpostNet is a lightweight TCP/UDP tunneling tool for edge computing, similar to [frp](https://github.com/fatedier/frp). It creates encrypted QUIC tunnels between hosts, allowing you to expose local services without a public IP.

One binary (`outpostnet-agent`), two roles (`server` / `client`).

## How It Works

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

Host A exports a local service. Host B imports it as a local port. Traffic flows through an encrypted QUIC tunnel. No public IP needed on either side.

## Install

```bash
# Linux amd64
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v1.0.1/outpostnet-agent-v1.0.1-linux-amd64.tar.gz | tar xz
sudo mv outpostnet-agent-v1.0.1-linux-amd64 /usr/local/bin/outpostnet-agent

# Linux arm64
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v1.0.1/outpostnet-agent-v1.0.1-linux-arm64.tar.gz | tar xz
sudo mv outpostnet-agent-v1.0.1-linux-arm64 /usr/local/bin/outpostnet-agent

# macOS (Apple Silicon)
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v1.0.1/outpostnet-agent-v1.0.1-darwin-arm64.tar.gz | tar xz
sudo mv outpostnet-agent-v1.0.1-darwin-arm64 /usr/local/bin/outpostnet-agent

# macOS (Intel)
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v1.0.1/outpostnet-agent-v1.0.1-darwin-amd64.tar.gz | tar xz
sudo mv outpostnet-agent-v1.0.1-darwin-amd64 /usr/local/bin/outpostnet-agent
```

Build from source:

```bash
git clone https://github.com/outpostos/OutpostNet.git && cd OutpostNet
make build-agent
# Output: build/outpostnet-agent
```

## Quick Start

### Step 1: Start agent on Host A (server role)

Host A has an HTTP service running on port 8080 that you want to expose.

```yaml
# agent-server.yaml
auth:
  token: changeme
tunnels:
  - role: server
    listen_addr: 0.0.0.0:4242
exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
```

```bash
outpostnet-agent --config agent-server.yaml
```

### Step 2: Start agent on Host B (client role)

Host B connects to Host A and imports the service as a local port. Replace `<HOST_A_IP>` with Host A's actual IP.

```yaml
# agent-client.yaml
auth:
  token: changeme
tunnels:
  - role: client
    remote_addr: <HOST_A_IP>:4242
imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    protocol: tcp
```

```bash
outpostnet-agent --config agent-client.yaml
```

### Step 3: Verify

```bash
# On Host B — traffic forwards transparently to Host A:8080
curl http://127.0.0.1:9080
```

## Features

| Feature | Description |
|---------|-------------|
| TCP & UDP | Full L4 proxy for HTTP, gRPC, DNS, gaming, and more |
| QUIC Tunnel | Encrypted transport with TLS 1.3 and multiplexed streams |
| Token Auth | Shared-secret authentication between agents, no PKI needed |
| Auto-Reconnect | Exponential backoff with config persistence for offline recovery |
| Metrics | Prometheus endpoint for tunnel state, streams, and traffic |

## Comparison with FRP

| | OutpostNet | FRP |
|---|---|---|
| Binaries | 1 (`outpostnet-agent`) | 2 (`frps` + `frpc`) |
| Transport | QUIC (TLS 1.3) | TCP / KCP / QUIC |
| Stream Multiplexing | QUIC native | yamux |
| HTTP Vhost | Roadmap | Supported |
| Dashboard UI | Roadmap | Supported |

## Documentation

- [Quick Start Guide](docs/quickstart.md)
- [Configuration Reference](docs/configuration.md)
- [Architecture Overview](docs/architecture.md)
- [Deployment Examples](deploy/examples/)

## Supported Platforms

| OS | Architecture |
|----|-------------|
| Linux | amd64, arm64 |
| macOS | amd64, arm64 |

## License

Apache 2.0 — see [LICENSE](LICENSE).
