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

The manifests (this one, [deploy/](deploy/), [deploy/kustomize/](deploy/kustomize/))
reference `outpostnet-controller:latest`, `outpostnet-server:latest` and
`outpostnet-sidecar:latest` (the standalone overlay also `outpostnet-agent:latest`).
These are unqualified local names (`imagePullPolicy: IfNotPresent`). Build them with the
repository [Dockerfile](Dockerfile), one target per image, then load them onto the nodes
(or push them to your registry and point the manifests there):

```bash
for c in outpostnet-controller outpostnet-server outpostnet-sidecar outpostnet-agent; do
  docker build --target "$c" -t "$c:latest" \
    --secret id=outpostrange_deploy_key,src=<OutpostRange read-only deploy key file> .
done
```

The build needs read access to the private module `github.com/outpostos/OutpostRange`,
passed only as a BuildKit secret (never a build arg or a copied file); without it the
build fails. When the key is held in a variable (for example the CI secret
`OUTPOSTRANGE_DEPLOY_KEY`), write it to a 0600 file and pass the file with `src=`,
then delete it:

```bash
keyfile=$(mktemp); trap 'rm -f "$keyfile"' EXIT
( umask 077; printf '%s\n' "$OUTPOSTRANGE_DEPLOY_KEY" > "$keyfile" )
docker build --target outpostnet-controller -t outpostnet-controller:latest \
  --secret id=outpostrange_deploy_key,src="$keyfile" .
```

Do not use the secret's `env=` form. GitHub strips the trailing newline from stored
secrets, `env=` hands the value to BuildKit unchanged, and OpenSSH rejects a private
key without its trailing newline (`error in libcrypto: unsupported`). `printf '%s\n'`
restores the newline. Base images come from Huawei SWR (`swr.cn-east-3.myhuaweicloud.com/risecloud`).
Where `proxy.golang.org` is unreachable add `--build-arg GOPROXY=<module proxy>`.

Build from source:

Building from source needs the private observation contract module
`github.com/outpostos/OutpostRange`: set `GOPRIVATE=github.com/outpostos
GONOSUMDB=github.com/outpostos` (the Makefile exports both) and give git SSH
access to the `outpostos` organization
(`git config --global url.git@github.com:.insteadOf https://github.com/`).
See [docs/observation.md](docs/observation.md).

```bash
git clone https://github.com/outpostos/OutpostNet.git && cd OutpostNet
make build-agent        # standalone agent only
make build              # all five components (agent, service, server, sidecar, controller)
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
| TCP & UDP | TCP across nodes over the QUIC tunnel; UDP only node-local via server + sidecar (cross-node UDP not implemented) |
| QUIC Tunnel | Encrypted transport with TLS 1.3 and multiplexed streams |
| Token Auth | Shared-secret authentication between agents, no PKI needed |
| Auto-Reconnect | Exponential backoff with config persistence for offline recovery |
| K8s Controller | Annotation-driven service exposure with sidecar auto-injection |
| Peer Directory | Named-node addressing with stable local listen addresses |
| Service Entry | `outpostnet-service`: stable entries for `<service>@<node_id>` that follow instance replacement ([docs](docs/service-layer.md)) |
| Relay | Three-region relay for networks without direct QUIC paths |
| Metrics | Prometheus endpoint for tunnel state, streams, and traffic |

## Role in Outpost OS

Within the Outpost OS four-layer architecture OutpostNet is the Net layer and
also ships the Service layer's entry point:

| Layer | What this repository provides |
|-------|-------------------------------|
| Net — communication and state primitives | QUIC tunnels and the peer directory (`pkg/tunnel`, `outpostnet-agent`); generic state replication primitives (`pkg/state`: durable store, gossip, tombstone GC, read-only replication status), and durable direct-peer delivery (`pkg/delivery`: authenticated HTTP, Inbox/Outbox, deduplication, handoff receipts and local observation). OutpostMesh and OutpostLet embed `pkg/state`; this repository does not interpret their state |
| Service — stable access | `outpostnet-service`: stable local entries for `<service>@<node_id>` ([docs](docs/service-layer.md)) |
| Range — observation | `outpostnet-agent` and `outpostnet-service` expose loopback observation endpoints for OutpostRange ([docs](docs/observation.md)); upgrade Range before them |

Mesh (entities, tasks, objects) and Let (autonomous scheduling) live in their
own repositories and report their own replication status.

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
- [Service Layer (outpostnet-service)](docs/service-layer.md)
- [Migrating agent imports/exports to outpostnet-service](docs/service-migration.md)
- [Observation producers (Range)](docs/observation.md)
- [State primitives (`pkg/state`)](pkg/state/README.md) and [tombstone GC / compaction](docs/state-tombstone-gc.md)
- [Durable delivery library (`pkg/delivery`)](docs/delivery.md)
- [Deployment Examples](deploy/examples/)

## Supported Platforms

| OS | Architecture |
|----|-------------|
| Linux | amd64, arm64 |
| macOS | amd64, arm64 |

## License

Apache 2.0 — see [LICENSE](LICENSE).
