# V2-NET-4 validation

Scope: [OutpostNet#52](https://github.com/outpostos/OutpostNet/issues/52),
[ADR-0008 D3 and acceptance 1 (replication/delivery)](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0008-node-identity-and-origin.md).
Based on merged NET-3 (`bbe47c4`) and NET-1b (`b642e95`). State protocol remains
3.0; delivery remains 1.0. No durable format change.

## Red → green

1. `go test ./pkg/delivery -run '^TestCertificateHTTPReplication$'` initially
   failed because `pkg/state/httpauth` and the certificate configuration were
   absent. The real HTTPS push/pull test passed after the shared policy and state
   adapter implementation.
2. `go test ./pkg/delivery -run '^TestCertificateHTTPDeliveryHandoff$'` initially
   failed on missing delivery `AuthMode` / `Identity` config fields. After delivery
   integration, both `cert` and `token+cert` durable handoffs passed. The test now
   also closes/reopens the receiver before Receive/Ack and receipt return.
3. `go test ./pkg/delivery -run '^TestCertificateHTTPRejectsDeliveryBatchBeforeAdmission$'`
   reproduced a functional failure: **HTTP 200 instead of 403** for a batch with a
   valid prefix and a foreign destination. Full-batch identity/destination checks
   now reject it before admitting the prefix, with a visible claim counter.
4. `go test ./pkg/delivery -run '^TestCertificateHTTPConfigurationFailsClosed$'`
   reproduced acceptance of a state handler whose engine belonged to another
   node. Certificate-mode construction now rejects that mismatch before starting a listener.

## Acceptance mapping

All new tests use public adapters, real HTTP/TLS and real stores/boxes; they do
not invoke private handler helpers or mock certificate verification.

| Requirement | Automated evidence |
| --- | --- |
| Acceptance 1: missing/foreign certificates rejected and counted on replication and delivery | `TestCertificateHTTPRejectsUntrustedAndForgedSources` exercises push, pull and delivery in both certificate modes, checks exact counter deltas and no admission; checks weak TLS/plain listener defense too |
| Acceptance 1: certificate/source mismatch rejected with 403 and counted | Same test covers forged body source, forged header and missing source on all three endpoints |
| Envelope source, receipt sender and destination checked before any admission | `TestCertificateHTTPRejectsDeliveryBatchBeforeAdmission`, `TestCertificateHTTPRejectsForgedEnvelopeAndReceipt` |
| Client mutual TLS / trusted group | `TestCertificateHTTPReplication`, `TestCertificateHTTPDeliveryHandoff`, `TestCertificateHTTPClientsRejectForeignServers` (client counters checked) |
| Response/410 metadata bound to server certificate | `TestCertificateHTTPBindsResponseIdentity`: push/pull 200 and 410; delivery certificate differs from mapped destination |
| `token`, `token+cert`, `cert` transition | Existing token suites; both certificate modes in handoff and rejection tests; wrong token returns 401 in `token+cert` |
| Fail-closed config, plaintext and redirect refusal | `TestCertificateHTTPConfigurationFailsClosed`, `TestCertificateHTTPClientsRefuseDowngradeAndRedirect` |
| Concurrent operation and restart | `TestCertificateHTTPConcurrentReplication` (16 concurrent HTTPS pushes), handoff test reopens durable Inbox before consumption and authenticated receipt return |
| No protocol bump beyond coordinated 3.0 | `TestCertificateHTTPReplication` checks state version; existing protocol compatibility tests remain in the full suite |
| Documentation | `docs/http-node-auth.md`, state README, delivery guide, node certificate guide, architecture and root README |

## Gates

Commands are run from the isolated worktree. Short temporary paths avoid the
existing macOS Unix-socket path limit. Package concurrency is limited for these
resource-intensive suites; no test is excluded.

```sh
make fmt
go build ./...
go vet ./...
TMPDIR=/tmp go test -p 2 -race -count=1 ./...
go test -race -count=1 ./pkg/delivery -run '^TestCertificateHTTP'
GOOS=linux GOARCH=amd64 make build-agent
git diff --check
```

Formatting, `go build ./...`, `go vet ./...`, the Linux agent cross-build,
focused certificate race tests, Markdown target checks and `git diff --check`
passed. The first complete race run exposed an existing token-mode fixture that
uses a different handler/engine node ID. The new engine identity check is now
limited to certificate modes to preserve token compatibility. The focused legacy
HTTP test and certificate suite pass after that fix. An intermediate two-package
race run hit an unchanged `TestTunnelManagerServerMode` QUIC dial timeout;
`go test -race -count=1 ./internal/agent` passed on recheck (4.554s), and the
fresh full run also passed that package. The final complete command
`TMPDIR=/tmp go test -p 2 -race -count=1 ./...` passed every package, including
state (252.743s), delivery (277.989s), tunnel (15.679s), and e2e (4.404s).
Build, vet, focused regression/certificate race tests, Linux agent build,
format/diff checks, Markdown targets and changed-file sensitive-path/token scans
also passed on this implementation.

Supplemental Linux VM check: cross-compiled the delivery test binary with
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c ./pkg/delivery`, transferred it
through e5box to **outpost-node1**, and ran `-test.run=^TestCertificateHTTP -test.v`.
All 10 top-level tests passed. Removed the disposable binary from node1 and the
staging copy on e5box. No other VM was accessed. This supplemental binary does
not use the race detector; the local race gate does.

## Limits

Host product wiring/configuration belongs to V2-MESH-2 / V2-LET-2. Full-stack
RANGE-1/RANGE-2 certificate regressions and 2+2 partition validation belong to
V2-RANGE-3/5. No product daemons, system clocks or VM network rules are changed
by these library tests. The existing NET-3 verifier supplies date-independent
chain/URI verification; NET-4 does not change tunnel behavior. Future-HLC recovery
and its storage changes remain NET-1c. Compromised trusted nodes can still forge
forwarded writer metadata; per-entry signatures and revocation remain Phase 5.

## Publication

Rebase onto latest `origin/main` is required immediately before opening the PR.
The PR will remain unmerged; GitHub check links are recorded in its description.
