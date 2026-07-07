# Architecture Overview

## Core Concept

OutpostNet's core is `outpostnet-agent` — one binary that acts as either a server or client role, creating encrypted QUIC tunnels between hosts.

This model is similar to frp: frp requires `frps` (server) + `frpc` (client); OutpostNet unifies both into a single `outpostnet-agent`, distinguished by `role: server` / `role: client`.

```
Host A (server role)              Host B (client role)
┌──────────────────┐              ┌──────────────────┐
│  App (:8080)     │  QUIC Tunnel │     App (:9080)  │
│   │              │  (TLS 1.3)   │              │   │
│   ▼              │              │              ▼   │
│  outpostnet-agent ◄════════════► outpostnet-agent  │
│  (export)        │              │        (import)  │
└──────────────────┘              └──────────────────┘
```

---

## Data Flow

### Scenario: Two Hosts, One Service (Most Common)

```
App-B (Host B, :9080) → agent-B (client) ──── QUIC ────► agent-A (server) → App-A (Host A, :8080)
```

Host A's agent exports the local service on port 8080. Host B's agent imports it and listens on local port 9080. Users on Host B access `localhost:9080`, and traffic is transparently forwarded through the QUIC tunnel to Host A's port 8080.

### Scenario: Multiple Services

A single agent can configure:
- Multiple tunnels (connecting to different hosts)
- Multiple exports (exposing multiple local services)
- Multiple imports (importing services from different hosts)

```yaml
tunnels:
  - role: client
    remote_addr: 10.0.1.1:4242
  - role: server
    listen_addr: 0.0.0.0:4243

exports:
  - name: api
    local_addr: 127.0.0.1:8080
    protocol: tcp

imports:
  - name: database
    listen_addr: 127.0.0.1:5432
    protocol: tcp
```

---

## Protocol

### QUIC Tunnel (agent ↔ agent)

- Built on QUIC (quic-go implementation), with TLS 1.3 encryption
- Native stream multiplexing — one QUIC connection carries multiple service streams
- Optional token authentication (both ends configure the same `auth.token`)
- Automatic recovery on network changes (QUIC protocol feature)

### Peer Directory Management API

When configured with `node_id`, `outpostnet-agent` also exposes a local HTTP API over a Unix socket. `PUT /v1/peers` replaces the full peer list using `node_id` and `addr`; `GET /v1/peers` reports tunnel status for assertions. For each node pair, the lexicographically smaller `node_id` dials as the QUIC client and the larger node accepts as the server, which avoids duplicate tunnels without a coordinator.

Imports can use `service: <name>@<node_id>` to bind a stable local `listen_addr` while the backing peer tunnel is resolved and reconnected from the directory.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| One binary, two roles | Simplifies deployment, reduces cognitive overhead (like frp but simpler) |
| QUIC as tunnel protocol | Native multiplexing, built-in TLS, resilient under mobile/unstable networks |
| Configuration persistence | Edge nodes have unreliable networks; agent auto-recovers tunnels on restart |
| Token auth instead of certificates | Lowers deployment barrier, no PKI infrastructure to manage |

---

## Advanced: Multi-Application Architecture

> The following applies to scenarios where **multiple applications on a single host** need to share tunnels. Basic cross-host tunneling does not require these components.

For multi-application scenarios, OutpostNet provides two additional components:

- `outpostnet-server` — Local routing hub (Unix socket), manages service registration for multiple sidecars
- `outpostnet-sidecar` — Runs alongside each application, connects to the server, handles that app's exports/imports

```
Same host:
┌────────────────────────────────────────────┐
│  App-1 → sidecar-1 ┐                      │
│                     ├─► server ──► agent ──┼──► QUIC tunnel
│  App-2 → sidecar-2 ┘   (route)   (tunnel) │
└────────────────────────────────────────────┘
```

### Component Responsibilities

**outpostnet-server**
- Accepts sidecar connections via Unix domain socket (same-host communication, low overhead)
- Maintains a service registry: which sidecar exports which services
- Routes connection requests to the appropriate sidecar
- Heartbeat monitoring and cleanup of stale connections

**outpostnet-sidecar**
- Connects to the local server, registers local services (export) or listens on local ports for remote services (import)
- Supports both TCP and UDP
- Each application has its own config and process, fully isolated

### Internal Protocol (server ↔ sidecar)

Uses a custom binary protocol over Unix socket:
- Frame format: length prefix + message type + payload
- Message types: Handshake / Register / Connect / OpenStream / Data / StreamClose / Heartbeat
- Stream model: 32-bit stream ID; control stream (ID=0) for handshake and registration
- Buffer: 32KB per stream

See the [Configuration Reference](configuration.md) for server and sidecar configuration details.
