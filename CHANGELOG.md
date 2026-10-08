# Changelog

All notable changes to OutpostNet will be documented in this file.

## [Unreleased]

Outpost OS four-layer refactor (Net state primitives / Service stable access / Range observation).

### Added
- `pkg/state`: generic state replication primitives — memory and durable stores, gossip push/pull and anti-entropy over authenticated `/state/v1`, tombstone GC with GC horizon and snapshot resync, durable log compaction, read-only `ReplicationStatus` including push-round catch-up (`OutboundRoundCaughtUp`, `OutboundRoundTime`). Used by OutpostMesh and OutpostLet.
- `outpostnet-service`: OutpostService stable entries for `<service>@<node_id>`; included in `make release`.
- `Dockerfile` for the images the Kubernetes entries reference (`outpostnet-controller`, `outpostnet-server`, `outpostnet-sidecar`, `outpostnet-agent`; `docker build --target <name>`). Base images come from Huawei SWR; the private OutpostRange module is fetched with a read-only deploy key passed as the required BuildKit secret `outpostrange_deploy_key`. Static guards in `deploy/docker/dockerfile_test.go`.
- Loopback OutpostRange observation endpoints on `outpostnet-agent` (`--observation-addr`, default `127.0.0.1:17960`) and `outpostnet-service` (default `127.0.0.1:17961`). Upgrade OutpostRange before these components.

### Documentation
- `docs/service-migration.md`: moving stable entries from the combined `outpostnet-agent` (`imports`/`exports`) to `outpostnet-service` — config mapping, phased order, mixed-period behavior, rollback; example configs in `examples/migration/` are loaded by tests.
- Dockerfile header and README "Kubernetes": when the deploy key is held in a variable, write it with `printf '%s\n'` to a 0600 file, pass it as `--secret id=outpostrange_deploy_key,src=<file>` and delete it afterwards. The earlier `env=` suggestion fails: GitHub strips the trailing newline from stored secrets and OpenSSH rejects the key (`error in libcrypto: unsupported`). Guarded by `TestBuildDocsPassDeployKeyAsFile`.

### Removed
- `pkg/tunnel`: `Client.ServeImport` (use `pkg/service` entries or `outpostnet-service`).
- `pkg/state`: decoding a bare-integer `protocol_version`; the wire form is `{"major":M,"minor":N}`.
- Makefile `docker-build` and `docker-build-combined` targets, whose Dockerfiles were deleted in v1.0.1 (`2158a5b`).

### Build
- Building from source needs the private module `github.com/outpostos/OutpostRange` (`GOPRIVATE`/`GONOSUMDB`, SSH access).

## [2.0.0] - 2026-08-12

Second major release. OutpostNet 2.0 adds full Kubernetes-native integration: a controller, mutating webhook, sidecar auto-injection, and cross-region peer discovery — while keeping the standalone agent from v1.x fully supported.

### What's New

#### Kubernetes-Native Architecture
- **Controller** (`outpostnet-controller`) — Kubernetes controller-runtime operator that watches Pod annotations and manages tunnel configuration automatically
- **Mutating Webhook** — Auto-injects OutpostNet sidecar containers into annotated Pods at admission time
- **Sidecar** (`outpostnet-sidecar`) — Per-Pod sidecar that registers services with the local server and manages imports/exports
- **Server** (`outpostnet-server`) — Local Unix-socket routing hub for multi-sidecar scenarios on a single node

#### Annotation-Driven Service Exposure
- Declare exports and imports with Pod annotations: `outpostnet.io/exports`, `outpostnet.io/imports`
- Controller generates ConfigMaps and reconciles tunnel state automatically
- EndpointSlice management for headless Service discovery

#### Peer Directory (G3.1/G3.2)
- `node_id`-based peer addressing — agents discover and maintain direct QUIC tunnels to named peers
- Service imports using `service: <name>@<node_id>` syntax — stable local listen address with dynamic peer resolution
- Management API over Unix socket (`PUT /v1/peers`, `GET /v1/peers`) for peer list updates
- Lexicographic role arbitration — smaller node_id dials as client, eliminating duplicate tunnels without a coordinator

#### Relay Support (Epic 5)
- Three-region relay forwarding for networks where direct QUIC connections are not possible
- Controller computes relay paths; Agent performs transparent relay forwarding
- Relay hop limit and metrics for observability

### Compatibility

OutpostNet v1.x standalone agent configurations (YAML with `tunnels`, `exports`, `imports`) are fully supported in v2.0.

### Platforms

- linux/amd64, linux/arm64
- darwin/amd64, darwin/arm64

## [1.0.1] - 2026-03-29

### Documentation
- Removed multi-hop relay references from docs
- All documentation now pure English
- Consistent with 1.0 feature scope (TCP/UDP tunneling only)

## [1.0.0] - 2026-03-29

First stable release. OutpostNet 1.0 is a lightweight TCP/UDP tunneling tool for edge computing, comparable to FRP at the L4 proxy layer with added QUIC transport.

### What's New

One binary (`outpostnet-agent`), two roles (`server` / `client`) — like frp's `frps` + `frpc`, unified.

- **TCP/UDP proxy** — Full Layer 4 support for TCP and UDP protocols
- **QUIC encrypted tunnels** — Secure cross-region tunnels using QUIC with TLS 1.3
- **Token authentication** — Agent-to-agent shared-secret auth for tunnel establishment
- **Auto-reconnect** — Automatic reconnection with exponential backoff
- **Configuration persistence** — YAML config with runtime state persistence for offline recovery
- **Prometheus metrics** — Built-in observability for tunnel state, streams, and traffic

### Platforms

- linux/amd64, linux/arm64
- darwin/amd64, darwin/arm64

### Documentation

- Quick-start guide (5 minutes to first tunnel)
- Complete configuration reference
- Architecture overview with data flow diagrams
- Standalone deployment examples
