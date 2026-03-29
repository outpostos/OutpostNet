# Changelog

All notable changes to OutpostNet will be documented in this file.

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
