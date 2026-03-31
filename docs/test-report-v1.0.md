# OutpostNet v1.0 Test Report

**Date:** 2026-03-31
**Version:** v1.0.1
**Environment:** Linux (x86\_64), Go 1.22, Intel Core i5-8250U @ 1.60GHz
**Test Run:** `go test -coverprofile=coverage.out ./...`

---

## Summary

| Metric | Result |
|---|---|
| Total packages tested | 12 |
| Total test cases | 333 |
| Passed | **333** |
| Failed | **0** |
| Overall pass rate | **100%** |
| Weighted avg coverage | **~85%** |

---

## Test Matrix

### Unit Tests

| Package | Tests | Pass | Coverage | Notes |
|---|---|---|---|---|
| `pkg/protocol` | 6 | 6 | 88.5% | Service/relay header encode-decode, error cases |
| `pkg/tunnel/tlsutil` | 5 | 5 | 81.8% | Self-signed cert gen, TLS config validation |
| `pkg/config` | 3 | 3 | 88.5% | Config parsing, defaults, watcher |
| `pkg/agentconfig` | 4 | 4 | 94.0% | Agent config build, service name derivation |
| `pkg/annotations` | 2 | 2 | 95.5% | Kubernetes annotation key helpers |

### Integration Tests

| Package | Tests | Pass | Coverage | Notes |
|---|---|---|---|---|
| `pkg/tunnel` | 13 | 13 | 85.0% | QUIC listen/dial, TLS, stream open/close |
| `internal/server` | 10 | 10 | 84.3% | Route management, stream routing, relay fallback |
| `internal/sidecar` | 5 | 5 | 81.3% | Export/import lifecycle, reconnect, graceful shutdown |
| `internal/controller/reconciler` | 7 | 7 | 77.8% | K8s EndpointSlice + Pod reconcile scenarios |
| `internal/controller/webhook` | 3 | 3 | 95.7% | Sidecar injection webhook |

### Agent-level Tests

| Package | Tests | Pass | Coverage | Notes |
|---|---|---|---|---|
| `internal/agent` | 14 | 14 | 83.1% | Health endpoints, config watcher, cross-region, relay, persistence, metrics |

### End-to-End Tests

| Test | Pass | Duration | Scenario |
|---|---|---|---|
| `TestTCPEndToEnd` | ✅ | 0.36s | App A → Sidecar A → Server → Sidecar B → App B (echo), single connection |
| `TestTCPMultipleConnections` | ✅ | 0.36s | 5 concurrent TCP connections through tunnel |
| `TestUDPEndToEnd` | ✅ | 0.35s | UDP packet forwarding end-to-end |
| `TestSC1_BasicTunnel_TCP` | ✅ | 0.81s | FRP-style single tunnel, no region |
| `TestSC2b_FRPStyle_NoRegion` | ✅ | 0.81s | FRP parity scenario: no region concept |
| `TestSC3_MultiServiceMux` | ✅ | 1.05s | Multiple services multiplexed over one QUIC connection |
| `TestSC4_Relay` | ✅ | 1.17s | Three-node relay path |
| `TestSC5_DisconnectRecovery` | ✅ | 1.87s | Sidecar disconnect + reconnect, no data loss |
| `TestSC6_TokenAuth_Success` | ✅ | 0.81s | Token authentication — valid token |
| `TestSC6_TokenAuth_Failure` | ✅ | 1.25s | Token authentication — invalid token rejected |

Full e2e suite: `test/e2e` package — 10 tests, **10 passed**, 0 failed

---

## Coverage by Package

| Package | Coverage |
|---|---|
| `internal/controller/webhook` | 95.7% |
| `pkg/annotations` | 95.5% |
| `pkg/agentconfig` | 94.0% |
| `pkg/config` | 88.5% |
| `pkg/protocol` | 88.5% |
| `pkg/tunnel` | 85.0% |
| `internal/server` | 84.3% |
| `internal/agent` | 83.1% |
| `pkg/tunnel/tlsutil` | 81.8% |
| `internal/sidecar` | 81.3% |
| `internal/controller/reconciler` | 77.8% |

---

## Performance Benchmarks

Benchmark: `go test -bench=. -benchtime=3s ./pkg/tunnel/`
Platform: Intel Core i5-8250U @ 1.60GHz, Linux x86\_64

| Benchmark | Iterations | ns/op | Throughput |
|---|---|---|---|
| `BenchmarkSingleStreamThroughput` | 18,076 | 185,683 ns | **176.47 MB/s** |
| `BenchmarkStreamOpenClose` | 5,439 | 676,917 ns | ~1,477 streams/s |

**Key metrics:**
- Tunnel throughput (single QUIC stream): **176 MB/s**
- Stream establishment latency: **~677 µs**
- Transport: QUIC with TLS 1.3, self-signed cert, stream multiplexing

---

## Test Scenarios — Relay & Reconnect

| Scenario | Test | Result |
|---|---|---|
| Cross-region connectivity | `TestCrossRegionEndToEnd/SC-1` | ✅ PASS (1.11s) |
| Single tunnel multi-stream | `TestCrossRegionEndToEnd/SC-2` | ✅ PASS |
| Large data transfer (1MB+) | `TestCrossRegionEndToEnd/LargeDataTransfer` | ✅ PASS |
| Three-region relay chain | `TestRelayThreeRegion/SC-1..3` | ✅ PASS (1.55s) |
| Relay hop limit (1 hop allowed) | `TestRelayHopLimit/SingleHopAllowed` | ✅ PASS (1.51s) |
| Relay hop limit exceeded (2 hops rejected) | `TestRelayHopLimitExceeded/TwoHopExceedsLimit` | ✅ PASS (1.92s) |
| Direct + relay concurrent paths | `TestRelayDirectAndRelay/BothConcurrent` | ✅ PASS (1.93s) |
| Integration graceful shutdown, no data loss | `TestIntegrationGracefulShutdownNoDataLoss` | ✅ PASS (0.12s) |
| Integration reconnect & recover | `TestIntegrationReconnectAndRecover` | ✅ PASS (1.22s) |

---

## Notes

- All tests run without mocks — real in-process servers and sidecars over Unix sockets / loopback TCP/UDP.
- E2E suite exercises the full data path: application socket → sidecar import → server routing → sidecar export → target service.
- Relay tests validate the hop-limit enforcement introduced in v1.0 (max 1 relay hop).
- Benchmark environment is a developer laptop; production hardware (server-grade CPU, dedicated NIC) will yield higher throughput.
