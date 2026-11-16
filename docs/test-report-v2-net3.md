# V2-NET-3 validation

Date: 2026-11-14. Scope: OutpostNet#51, ADR-0008 D1/D2/D3 (tunnel)/D5;
OutpostOS#30. The tests generate fresh disposable credentials, never fixture
secrets. No system clock was changed. Production processes were not replaced.

## Acceptance mapping

| Requirement | Automated evidence |
| --- | --- |
| ADR-0008 acceptance 1, tunnel: no certificate / wrong group rejected and counted | `TestCertificateTunnelRejectsUnauthenticatedAndForgedPeers`: `missing_certificate`, `foreign_group`; no accepted reverse registration |
| Acceptance 1, tunnel: certificate identity differs from claim | Same test: `forged_handshake_claim`; `claim_rejected` increments; `TestCertificateTunnelRejectsMissingDeclarationBeforeServiceTraffic` rejects absent claims before acknowledging or registering |
| Routing uses authenticated destination identity | Same rejection test: `wrong_directory_destination`; a valid group node at the wrong directory address is refused |
| ADR-0008 acceptance 3: 1970 / +10 years | `TestCertificateTLSWithSkewedClocks`: real QUIC TLS handshakes and application traffic with TLS clocks set to 1970 and more than ten years ahead; each peer reports one validity warning |
| Acceptance 3: warnings visible | `TestNodeVerificationIgnoresWallClockAndWarns`: verifies counter = 2 and warning log text |
| D1 CA, issuance, inspect/list and loading | `TestCertificateCLIProvisionInspectAndLoad`, `TestCertificateCLIRejectsInvalidNodeAndLoadsIssuedCredentials`: issue, inspect/list, 0600 keys, refuse overwrite, load credentials, refuse mismatching local identity / corrupt CA |
| D2 chain and identity validation | `TestIdentityRejectsUntrustedMalformedAndWrongUsage`: missing/untrusted/malformed certificates, missing/empty/duplicate node URIs, local mismatch and wrong TLS usage |
| D5 same-group service authorization | `TestCertificateTunnelAuthorizesBothDirectionsWithoutToken`: forward and reverse echo; authenticated connections authorize all streams before route handling |
| Configuration and observability | `TestCertificateConfigFailsClosed`; `TestServiceHostsUseGroupCertificatesAndExposeSecurityCounters`: two real service hosts load disk credentials, exchange application traffic and query `GET /v1/security` |
| Concurrent streams and reconnect | `TestCertificateReconnectAndConcurrentStreams`: red before tunnel monitor fix, green after; five race-detector repetitions of this and the clock test pass |

The immediate address-update test exposed an existing monitor startup race:
`UpdateServerAddr` cleared the connection before the monitor first read it, so
the monitor exited instead of reconnecting. The monitor now redials a cleared
connection until its context is cancelled. No state/delivery implementation was
changed.

## Red → green record

Executed failing tests before the corresponding implementation:

- `go test ./pkg/tunnel/tlsutil`: `undefined: tlsutil.NewIdentity`; after adding
  credentials and verification, pass.
- `go test ./cmd/outpostnet-agent/app -run TestCertificateCLI -count=1`:
  `unknown command "cert"`; after CLI implementation, pass.
- `go test ./pkg/tunnel -run TestCertificateTunnel -count=1`: missing `Identity`
  and `ExpectedPeerID` configuration; after authenticated tunnel wiring, pass.
- `go test ./internal/servicehost -run TestServiceHostsUseGroup -count=1`:
  missing `CAFile`; after configuration/host wiring and security endpoint, pass.
- A trailing-whitespace CA PEM test failed with `invalid group CA PEM`; trimming
  only trailing whitespace before decoding made it pass.
- `go test -race ./pkg/tunnel`: reconnect test failed with
  `AcceptedPeers = [], want [node-a]`; after the monitor fix, pass.

## Gates and VM run

Local platform: macOS amd64, Go 1.26.1 (module targets Go 1.25).
Use `TMPDIR=/tmp` on macOS: the default temporary directory causes two existing
agent tests to exceed the platform's Unix socket path-length limit.

```sh
export TMPDIR=/tmp GOPRIVATE=github.com/outpostos GONOSUMDB=github.com/outpostos
make fmt
go build ./...
go vet ./...
go test -race -count=1 -p=1 ./...
go test -race -count=1 ./test/e2e
GOOS=linux GOARCH=amd64 make build-agent
```

Formatting/diff, build, vet, Linux agent build and targeted race checks passed.
All packages passed local race testing. In the serial full-suite run, every
package except `test/e2e` passed (delivery: 343.181s; state: 184.824s; tunnel:
16.223s). `TestSC3_MultiServiceMux` hit a legacy QUIC dial timeout; the complete
e2e package passed on rerun (4.361s). No tests were skipped or disabled.

Earlier parallel local runs encountered existing macOS socket-path limits,
a delivery process-crash fixture emitting extra output, an observation readiness
timeout, and a legacy QUIC dial timeout. Relevant package reruns passed. The
supplemental e5box parallel run passed the new certificate/host tests and build/vet,
but its existing cold-cache test expired during setup and state/delivery suites
hit their ten-minute timeouts. Those failures are recorded here rather than
claimed as a clean supplemental Linux gate. No unrelated implementation was
changed to work around them; GitHub CI is checked separately before handoff.

An additional Linux amd64 run on **outpost-node1** passed all `TestCertificate*`
tests present at the time (bidirectional access, rejection matrix, skewed TLS
clocks, reconnect/concurrency). Built locally with
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c ./pkg/tunnel`, copied the disposable
test binary through the dev box, ran `-test.run TestCertificate -test.v`, and
removed the binary. The later missing-declaration test additionally passed locally
under `-race`. No other VM was touched.

## Boundaries

V2-NET-4 owns HTTPS replication/delivery and `auth_mode`. V2-RANGE-3 owns the
mixed/cert full-stack RANGE-1/RANGE-2 regression and source-record filtering;
V2-RANGE-5 owns 2+2 partition validation. Those are not claimed here. No revocation,
per-service ACL or cross-group authorization is added. Legacy deployments without
`tls.ca_file` retain their previous behavior, explicitly marked by
`certificate_auth: false`.
