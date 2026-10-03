# Configuration Reference

This document covers all configuration options for OutpostNet.

> **v1.0 users**: Most scenarios only need `outpostnet-agent` — see the first section.

---

## outpostnet-agent

The agent is OutpostNet's core component. One binary, two roles (`server` / `client`) — similar to frp's `frps` + `frpc`, but unified.

### YAML Configuration

```yaml
node_id: node-a                  # Required for peer-directory service imports

# QUIC tunnel definitions
tunnels:
  - role: server               # server (listen) or client (connect)
    listen_addr: 0.0.0.0:4242  # Required for role=server: listen address
    # remote_addr: host:port   # Required for role=client: remote address

# Services exported to remote agents (local services)
exports:
  - name: my-api               # Service name
    local_addr: 127.0.0.1:8080 # Local service address
    protocol: tcp              # tcp or udp

# Services imported from remote agents (mapped to local ports)
imports:
  - name: remote-db            # Remote service name
    listen_addr: 127.0.0.1:5432 # Local listen address
    protocol: tcp               # tcp or udp
  - service: my-api@node-b      # Peer-directory form: <name>@<node_id>
    listen_addr: 127.0.0.1:9080
    protocol: tcp

# Local management API
management:
  socket_path: /run/outpostnet/outpostnet.sock

# Authentication (optional, recommended)
auth:
  token: ""                    # Shared secret, must match on both ends

# Persistence
persistence:
  dir: /var/lib/outpostnet     # Persistence directory, default /var/lib/outpostnet
  enabled: true                # Enable/disable, default true

# Prometheus metrics
metrics:
  addr: :9090                  # Metrics address, default :9090

# Health check endpoint
health_addr: :8082             # /healthz and /readyz, default :8082

# TLS configuration (optional)
tls:
  cert_file: /path/to/cert.pem
  key_file: /path/to/key.pem
  insecure: false              # Skip TLS verification (development only)
```

### Field Reference

#### `tunnels[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `role` | string | Yes | `server` (listen for connections) or `client` (connect to remote) |
| `listen_addr` | string | role=server | Listen address, format `host:port` |
| `remote_addr` | string | role=client | Remote address, format `host:port` |

> **FRP mapping**: `role: server` = frps, `role: client` = frpc.

#### `exports[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Service name |
| `local_addr` | string | Yes | Local service address, format `host:port` |
| `protocol` | string | Yes | `tcp` or `udp` (accepted for compatibility; the agent's cross-node QUIC tunnel forwards TCP only — `udp` is not implemented there and is served as TCP) |

#### `imports[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes, unless `service` is set | Remote service name to import |
| `service` | string | No | Peer-directory service reference, format `<name>@<node_id>` |
| `listen_addr` | string | Yes | Local listen address, format `host:port` |
| `protocol` | string | Yes | `tcp` or `udp` (accepted for compatibility; the agent's cross-node QUIC tunnel forwards TCP only — `udp` is not implemented there and is served as TCP) |

`service` is additive to the static `name` form. With `service: <name>@<node_id>`, the agent keeps `listen_addr` stable and resolves the peer tunnel through the local peer directory. Missing peers stay pending and retry without exiting the process. Export `local_addr` changes for an existing name are re-pointed in place by `ReloadAgent` (new streams only).

#### `management`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `socket_path` | string | `/run/outpostnet/outpostnet.sock` | Unix socket for the local HTTP management API |

### Peer Directory API

When `node_id` is set, the agent listens on `management.socket_path` using HTTP over a Unix domain socket.

| Endpoint | Description |
|----------|-------------|
| `PUT /v1/peers` | Replace the full peer list. Body: `[{"node_id":"node-b","addr":"127.0.0.1:4242"}]` |
| `GET /v1/peers` | Return peer entries with tunnel status: `connected`, `reconnecting`, or `pending` |

#### `auth`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token` | string | `""` | Shared authentication secret; must match on both ends; empty string disables auth |

#### `persistence`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dir` | string | `/var/lib/outpostnet` | Persistence file directory |
| `enabled` | bool | `true` | Enable configuration persistence |

#### `tls`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cert_file` | string | — | TLS certificate path |
| `key_file` | string | — | TLS private key path |
| `insecure` | bool | `false` | Skip certificate verification (development only) |

> **Warning**: Do not use `insecure: true` in production.

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config <path>` | — | YAML config file path |
| `--node-id <id>` | — | Node ID override for peer-directory role arbitration |
| `--metrics-addr <addr>` | `:9090` | Metrics address (overrides config) |
| `--health-addr <addr>` | `:8082` | Health check address (overrides config) |
| `--persistence-dir <path>` | `/var/lib/outpostnet` | Persistence directory (overrides config) |

### Health Check Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /healthz` | Liveness check |
| `GET /readyz` | Readiness check |
| `GET /metrics` | Prometheus metrics |

---

## outpostnet-service

`outpostnet-service` is the OutpostService entry point: applications dial a stable local entry
for a service identity `<service>@<node_id>`; the command reuses the agent's QUIC tunnels, peer
directory and management API underneath. Design and protocol support: [service-layer.md](service-layer.md).
Runnable two-node example: [`examples/service/`](../examples/service/README.md).

The config file uses the agent schema (`node_id`, `tunnels`, `exports`, `imports`, `tls`, `auth`,
`management`, `health_addr`) plus `peers`, with these differences:

| Item | outpostnet-service |
|------|--------------------|
| `node_id` | Required |
| `imports[]` | Stable entries. `service: <name>@<node_id>` = dynamic path (peer directory); `name` + `remote_region` = static path (configured client tunnel — a `tunnels` entry with `role: client` and the same `remote_region` is required, otherwise startup fails). `listen_addr` must be unique across entries (port `0` excepted); an address already in use fails startup with the bind error |
| `protocol` | `tcp` only for imports and exports; `udp` is rejected (UDP over the QUIC tunnel is not implemented) |
| `relays` | Rejected (relay forwarding stays on `outpostnet-agent`) |
| `peers[]` | `{node_id, addr}` seed for the peer directory at startup; later changes via `PUT /v1/peers`. `node_id` must be unique and differ from this node's `node_id`; `addr` must be `host:port` (port 1-65535) |
| `management.socket_path` | Default `/run/outpostnet/service.sock` |
| `health_addr` | Default `:8083` (`/healthz`, `/readyz`) |
| `persistence`, `metrics` | Accepted for schema compatibility, not used |
| Unknown fields | Rejected |

Runtime changes: export `local_addr` add/change/remove applies in place when the config file
changes (`--watch-interval`, default 5s). `node_id`, tunnels, entries, auth, TLS, management,
`health_addr` and `peers` require a restart (the watcher logs a warning; `--node-id` /
`--health-addr` overrides are re-applied before comparing, so they do not trigger it); peer
address changes at runtime go through `PUT /v1/peers`.

| Flag | Default | Description |
|------|---------|-------------|
| `--config <path>` | — | Service YAML config (required) |
| `--node-id <id>` | — | Node ID override |
| `--health-addr <addr>` | `:8083` | Health address override |
| `--watch-interval <dur>` | `5s` | Config poll interval for in-place export changes; `0` disables |

---

## Advanced: outpostnet-server + outpostnet-sidecar

> These components are for **multi-application on a single host** — when multiple apps on the same machine need to share tunnels. Basic cross-host tunneling does not require them.

### outpostnet-server

Runs on a single host, manages service registration and routing for multiple sidecars via Unix domain socket.

```yaml
# Required: Unix socket listen path
listen: /var/run/outpostnet/server.sock

# Optional: log level, default info
log_level: info

# Optional: sidecar heartbeat timeout, default 30s
heartbeat_timeout: 30s
```

| CLI Flag | Default | Description |
|----------|---------|-------------|
| `--config <path>` | — | YAML config file path |
| `--socket <path>` | `/var/run/outpostnet/server.sock` | Unix socket path |
| `--heartbeat-timeout <duration>` | `30s` | Heartbeat timeout |

### outpostnet-sidecar

Runs alongside each application, connects to the local server to export local services or import remote services as local ports.

```yaml
# Required: server's Unix socket path
server: /var/run/outpostnet/server.sock

# Optional: unique sidecar ID, defaults to hostname
sidecar_id: my-sidecar

# Optional: heartbeat interval, default 10s
heartbeat_interval: 10s

# Exported local services
exports:
  - name: my-api
    local_port: 8080
    protocol: tcp

# Imported remote services
imports:
  - name: remote-db
    listen_port: 5432
    protocol: tcp
```

| CLI Flag | Default | Description |
|----------|---------|-------------|
| `--config <path>` | — | YAML config file path |
| `--server-socket <path>` | `/var/run/outpostnet/server.sock` | Server socket path |
| `--sidecar-id <id>` | hostname | Unique sidecar ID |
| `--heartbeat-interval <duration>` | `10s` | Heartbeat interval |

---

## Defaults Summary

| Parameter | Default |
|-----------|---------|
| agent health_addr | `:8082` |
| agent metrics addr | `:9090` |
| agent persistence dir | `/var/lib/outpostnet` |
| server socket | `/var/run/outpostnet/server.sock` |
| server heartbeat_timeout | `30s` |
| sidecar heartbeat_interval | `10s` |
