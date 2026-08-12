# OutpostNet

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8.svg)](https://go.dev)
[![Release](https://img.shields.io/github/v/release/outpostos/OutpostNet)](https://github.com/outpostos/OutpostNet/releases)

OutpostNet makes unreachable services reachable — lightweight encrypted QUIC tunnels for edge computing. Use it standalone (like [frp](https://github.com/fatedier/frp)) or deploy the Kubernetes-native operator for annotation-driven service exposure across clusters and edge nodes.

**v2.0**: Kubernetes-native with controller, mutating webhook, and peer directory.
**v1.x**: Standalone agent — one binary, two roles (`server` / `client`).

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

### Standalone Agent (v2.0)

```bash
# Linux amd64
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v2.0.0/outpostnet-agent-v2.0.0-linux-amd64.tar.gz | tar xz
sudo mv outpostnet-agent-v2.0.0-linux-amd64 /usr/local/bin/outpostnet-agent

# Linux arm64
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v2.0.0/outpostnet-agent-v2.0.0-linux-arm64.tar.gz | tar xz
sudo mv outpostnet-agent-v2.0.0-linux-arm64 /usr/local/bin/outpostnet-agent

# macOS (Apple Silicon)
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v2.0.0/outpostnet-agent-v2.0.0-darwin-arm64.tar.gz | tar xz
sudo mv outpostnet-agent-v2.0.0-darwin-arm64 /usr/local/bin/outpostnet-agent

# macOS (Intel)
curl -sL https://github.com/outpostos/OutpostNet/releases/download/v2.0.0/outpostnet-agent-v2.0.0-darwin-amd64.tar.gz | tar xz
sudo mv outpostnet-agent-v2.0.0-darwin-amd64 /usr/local/bin/outpostnet-agent
```

### Kubernetes (v2.0)

```bash
kubectl apply -f https://github.com/outpostos/OutpostNet/releases/download/v2.0.0/outpostnet-k8s.yaml
```

Build from source:

```bash
git clone https://github.com/outpostos/OutpostNet.git && cd OutpostNet
make build-agent        # standalone agent only
make build              # all four components
```

## Quick Start

### Standalone Mode (Host-to-Host)

**Step 1:** Export a service on Host A:

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

**Step 2:** Import the service on Host B:

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

**Step 3:** Verify on Host B:

```bash
curl http://127.0.0.1:9080   # forwards to Host A:8080
```

### Kubernetes Mode (Annotation-Driven)

Deploy the controller, then annotate your Pods:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-service
spec:
  template:
    metadata:
      annotations:
        outpostnet.io/inject: "true"
        outpostnet.io/exports: "my-api:8080:tcp"
```

The controller injects a sidecar and creates the tunnel configuration automatically.

See [deploy/examples/](deploy/examples/) for complete examples including cross-cluster service imports.

## Features

| Feature | Description |
|---------|-------------|
| TCP & UDP | Full L4 proxy for HTTP, gRPC, DNS, gaming, and more |
| QUIC Tunnel | Encrypted transport with TLS 1.3 and multiplexed streams |
| Token Auth | Shared-secret authentication between agents, no PKI needed |
| Auto-Reconnect | Exponential backoff with config persistence for offline recovery |
| K8s Controller | Annotation-driven service exposure with sidecar auto-injection |
| Peer Directory | Named-node addressing with stable local listen addresses |
| Relay | Three-region relay for networks without direct QUIC paths |
| Metrics | Prometheus endpoint for tunnel state, streams, and traffic |

## Comparison with FRP

| | OutpostNet v2 | FRP |
|---|---|---|
| Binaries | 1 standalone or K8s operator | 2 (`frps` + `frpc`) |
| Transport | QUIC (TLS 1.3) | TCP / KCP / QUIC |
| Stream Multiplexing | QUIC native | yamux |
| Kubernetes Integration | Native controller + webhook | Manual |
| Peer Discovery | Built-in peer directory | Manual config |
| HTTP Vhost | Not in scope | Supported |
| Dashboard UI | Not in scope | Supported |

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
