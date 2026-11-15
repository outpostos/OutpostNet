# NET-15 local validation

Issue: [OutpostNet#66](https://github.com/outpostos/OutpostNet/issues/66).
Epic: outpostos/OutpostOS#30. Baseline: `01135a3`.
Specification: accepted architecture v0.2 D2/D3, ADR-0007 and ADR-0008.

## Acceptance evidence

| Criterion | Automated evidence |
| --- | --- |
| Forged node-scoped registration and lease increase the exposed rejection counter | `TestDirectoryManagementRejectsForgedWriters`: a real service Host consumes the Mesh REST/SSE boundary fixture; Unix-socket `writer_rejected` advances 0 → 1 → 2. Also verifies method rejection, absence from the TCP health surface, and reset/reinitialized lease status after host restart. |
| Rejections visible in observation stream | `TestDirectoryObservationAndLeaseRecovery`: each forged input separately advances both endpoint and `service_directory_writer_rejected_total`; HTTP/SSE events pass strict Range protojson and stream sequence validation. |
| Lease expiry and recovery visible | Same test uses the executable's actual 30s default TTL; missing → valid → expired → valid appears in endpoint and metrics. `lease_expired` reaches 1; sequence progress alone restores real TCP application access. Reading status throughout does not renew the lease. A sequence above 2^53 survives exact JSON string decoding. |
| Read-only, local-only, payload-free | Management test verifies POST/PUT/PATCH/DELETE return 405. Lifecycle test checks directory JSON and metric JSON exclude payload/incarnation/token markers. `TestDirectoryStatusUnavailableGatewayIsStaleAndPrivate` supplies a credential-bearing upstream URL and secret-bearing error body; exported data stays safe and stale. All nine counters appear in endpoint and timeline. |
| Directory disabled | `TestDirectoryStatusWithoutMesh`: configured=false, no lease facts and zero counters, without enabling Mesh. |
| Local gates and CI | Local gates pass; commands and complete race package results are in [validation.txt](artifacts/net15/validation.txt). Hosted CI is pending PR publication. |
| No VM changes | All work and tests run locally; only Linux cross-compilation is performed. V2-RANGE-3 owns deployed re-verification. |

The service Host, Unix HTTP server, directory implementation, observation
server, event log and local TCP application are real. The Mesh REST/SSE source
is an external-boundary fixture using the existing recorded wire format; this
is host acceptance, not a claim of a new multi-node Mesh/mTLS deployment trial.

## Red → green

Before endpoint implementation:

```text
go test -race -count=1 ./internal/servicehost -run '^TestDirectoryManagementRejectsForgedWriters$'
FAIL: GET /v1/directory: status 404
```

After the safe management projection was added, that command passed (1.760s).
Before observation wiring:

```text
go test -race -count=1 ./internal/servicehost -run '^TestDirectoryObservationAndLeaseRecovery$'
FAIL: no event for initial directory counters within 15s
```

After directory metrics and lease validity were wired, both tests passed
together (33.807s). Additional failure/redaction tests then passed (3.528s).
The final complete suite includes the restart and application refusal checks.

## Gate environment

Local Go: 1.26.1, macOS amd64. Run tests with a short `TMPDIR` to accommodate
macOS Unix-socket path limits, as already documented by V2-NET-2. The initial
broader run with the system temporary directory failed unchanged agent tests
`TestAgentObservationReportsTunnelPeer` and
`TestObservationListenFailureDoesNotStopAgent` at socket bind (`invalid argument`).
Reproducing the agent package confirmed the same long-path failures. The final
suite uses `TMPDIR=/tmp`, with two-package concurrency, no exclusions and no
changed timeouts. Those initial failures are not counted as passing gates.

Required commands:

```sh
make fmt
go build ./...
make vet
TMPDIR=/tmp GOFLAGS=-p=2 make test
GOOS=linux GOARCH=amd64 make build-agent build-service
git diff --check
```

No Mesh contract or generated bindings changed, so buf/generated-code gates do
not apply. No production dependency pin, routing policy, lease TTL or writer
validation policy changed. Counters remain process-local; raw errors and resume
cursors remain excluded from public surfaces. See the [operator guide](directory-observation.md)
for field semantics and V2-RANGE-3's measurement procedure.
