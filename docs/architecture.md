# Architecture Overview

## Role in Outpost OS

OutpostNet is the Net layer of the Outpost OS four-layer architecture
(Net state primitives / Mesh domain services / Let autonomous runtime /
Service stable access, observed by Range). Responsibilities in this repository:

| Area | Component | Responsibility |
|------|-----------|----------------|
| Net: communication | `pkg/tunnel`, `outpostnet-agent` (`internal/agent`) | QUIC connections, group mutual TLS / legacy token auth, stream headers, reconnect and address change, relay, peer directory (`PUT/GET /v1/peers`) |
| Net: state primitives | `pkg/state` (library) | Opaque-entry replication: `Store` (memory or durable), `GossipEngine` push/pull and anti-entropy, tombstone GC with GC horizon and resync, compaction, `ReplicationStatus`. Consumers (OutpostMesh, OutpostLet) embed it and own the meaning of their keys; see [pkg/state/README.md](../pkg/state/README.md) and [state-tombstone-gc.md](state-tombstone-gc.md) |
| Net: reliable delivery | `pkg/delivery` (library) | Durable direct-peer Outbox/Inbox, bounded deduplication, expiry, quotas and optional consumer handoff receipts. Separate from state replication; in-memory and authenticated HTTP transports, host-owned peer resolution, read-only delivery status and observation metric mapping. See [delivery.md](delivery.md) |
| Service | `outpostnet-service` (`pkg/service`, `internal/servicehost`) | Stable local entries for `<service>@<node_id>` on top of the Net host; see [service-layer.md](service-layer.md) |
| Observation | `internal/observation` in `outpostnet-agent` and `outpostnet-service` | Loopback snapshot/SSE endpoints reporting Net peer links (111) and Service access (116) to OutpostRange; see [observation.md](observation.md) |
| Node-local / Kubernetes | `outpostnet-server`, `outpostnet-sidecar`, `outpostnet-controller` | Same-host multi-application routing over a Unix socket, and annotation-driven sidecar injection; not part of the cross-node four-layer path |

`outpostnet-agent` keeps the combined `imports`/`exports` configuration for
existing deployments (OutpostLet's e2e environment uses it); new stable-entry
deployments use `outpostnet-service` (moving an existing agent's entries:
[service-migration.md](service-migration.md)). Upgrade OutpostRange before the
components that report to it ([observation.md](observation.md) section 7).

## State replication limits

`pkg/state` sends ordinary push in position order as JSON pages, bounded by
`GossipConfig.MaxPushBytes` (768 KiB by default, including the envelope) and
`MaxPushPagesPerRound` (16 per peer per round). Every acknowledged page saves
its cursor before the next is sent. Failed transfers resume after confirmed
pages; a capped round yields to the next peer. The push-round caught-up fact
only covers the local position read at the start of that round.

Memory and durable stores bound local writes by `MaxEntryBytes` (512 KiB of
encoded entry, including base64 and metadata); `Put`/`CompareAndSet` return
`ErrEntryTooLarge` before committing larger entries. Remote Apply and recovery
preserve legacy entries: oversized entries travel alone and emit a warning
and a per-peer `OversizedPushEntries` count. They may exceed the page budget;
an entry above the peer's HTTP request limit still fails visibly and requires
a larger configured request limit.

Startup validates that a page can hold the store's largest local entry plus
the envelope and remains strictly below the HTTP request limit (default
1 MiB). The syncer's `MaxRequestBytes` declares the smallest peer limit;
the local handler also validates its own limit. There is no limit negotiation.
Store page reads copy page payloads instead of the whole backlog. See
[state configuration and semantics](../pkg/state/README.md) for the exact
encoding budget, optional store interfaces and counter lifetime.

Digest anti-entropy sends contiguous key intervals within each round, bounded
by `GossipConfig.MaxDigestBytes` (768 KiB including the request envelope).
Intervals cover the entire key space at every peer: first start is empty,
last end is unbounded, and each digest is complete within its half-open bounds.
Diff responses page by encoded `MaxBytes` and an exclusive key cursor; the
requester finishes each interval before advancing. Any failed page or resync
abandons the round; the next round starts over. Pull success is recorded only
after every interval completes. Collected-value checks run over the complete
interval on every continuation (see the [GC proof](state-tombstone-gc.md)).

NET-8 raises the state protocol to **2.0**; old/new majors reject each other.
Upgrade the whole cluster of hosts embedding `pkg/state` together, including
consumer dependency pins. Durable format is unchanged. Per-entry rejection
and 410 snapshot resync retain their existing semantics.

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
| Offline node certificates | Group mutual TLS and certificate-bound identities per ADR-0008; [configuration and legacy compatibility](node-certificates.md) |

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
- Supports both TCP and UDP (node-local only; the cross-node QUIC tunnel carries TCP only)
- Each application has its own config and process, fully isolated

### Internal Protocol (server ↔ sidecar)

Uses a custom binary protocol over Unix socket:
- Frame format: length prefix + message type + payload
- Message types: Handshake / Register / Connect / OpenStream / Data / StreamClose / Heartbeat
- Stream model: 32-bit stream ID; control stream (ID=0) for handshake and registration
- Buffer: 32KB per stream

See the [Configuration Reference](configuration.md) for server and sidecar configuration details.
