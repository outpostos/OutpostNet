# OS time source (V2-NET-1d, Net#63)

The agent now constructs its process clock with
`state.NewSystemTimeReference(ctx, referenceConfig, sourceConfig)`. This polls the
local Linux time service without root or clock changes. No time service is
required for startup. The constructor returns the shared `*TimeReference` and an
idempotent stop function that cancels and joins the poller. Cancel the host context
or call stop on shutdown. The first observation runs before the constructor
returns; subsequent polls run outside the reference lock and timestamp reads.

Embedding hosts (Mesh/Let) use the same constructor shown in
[time-reference.md](time-reference.md), share the returned pointer across stores,
and pass `reference.Now` to their clock callbacks. Their dependency upgrades and
constructor call sites live in their own repositories; they are not changed by
this Net PR. Injected clocks and custom time sources keep using the explicit
`NewTimeReference` / `UpdateSource` boundary.

The optional [group chrony/NTS generator](group-time-sync.md) configures the OS
service; this read-only adapter and its quality mapping remain unchanged.

## Configuration

`OSTimeSourceConfig` is an embedding API, not a new agent YAML section. The agent
uses its defaults. No shell, sudo, service control, network time query or mutation
command is executed by the adapter.

| Setting | Default | Constraint / purpose |
| --- | --- | --- |
| `PollInterval` | 5s | 1s–15s; delay after a completed probe, no overlapping polls |
| `CommandTimeout` | 1s | 1ms–5s per command; a poll uses at most three commands |
| `Runner` | cancellable local process | Injectable command boundary; must honor context cancellation |
| `KernelStatus` | Linux `adjtimex` with `modes=0` | Read-only maxerror and `STA_UNSYNC` check, used only for timesyncd |

The default runner uses the host PATH, `LC_ALL=C`, a 16 KiB output limit and
bounded process/pipe shutdown. It reports generic `error` / `unavailable` states
instead of exposing raw command errors, stderr or local executable paths.
Unsupported platforms report no source. Existing `ClockBudget` (2s pairwise),
`DriftPPM` (50) and `SourceTTL` (30s) retain their
[reference semantics](time-reference.md). Configure these on the shared reference,
not separately on its consumers.

## Source selection and quality

Chrony is preferred: `chronyc -c tracking` supplies upstream identity, signed
system offset, leap/stratum and an error bound computed as
`abs(system offset) + root dispersion + root delay/2`.
This is the [chrony tracking error formula](https://chrony-project.org/doc/4.6/chronyc.html#tracking).
Leap insertion/deletion is permitted; unsynchronized, zero/invalid stratum and
local/orphan mode are not synchronized sources. Malformed/nonfinite/overflowing
reports fail closed. A valid chrony report is authoritative, including loss;
it cannot be overridden by a stale kernel synchronization flag.

When chrony cannot be queried, the adapter tries `timedatectl show` and
`timedatectl show-timesync --all`. It requires an identified timesyncd server,
a non-ignored NTP packet with valid stratum/leap and a nonzero packet count,
`NTP=yes`, `NTPSynchronized=yes`, and kernel synchronization with a nonnegative
maxerror bound. Startup without a packet is unsynchronized. The
[timesyncd properties](https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.timesync1.html)
identify the source; `timedatectl show` alone cannot establish either its identity
or its uncertainty. Timesyncd has no current signed system-offset property, so
`OffsetKnown=false`; the kernel maxerror supplies uncertainty without inventing
an offset. No working source or query failure is nonfatal and observable.

| Observed source | Resulting local quality |
| --- | --- |
| Synchronized, recent observation; error plus elapsed drift ≤ half ClockBudget | `SYNCED` |
| Source lost/unavailable/malformed, or observation older than SourceTTL; previous synchronization still within half budget | `HOLDOVER` |
| No prior synchronization, or error plus elapsed drift exceeds half budget | `UNSYNCED` |
| Source restored with an acceptable bound | `SYNCED`, resetting the step baseline |

Loss reports never reset the last successful synchronization time or drift bound.
Peer time may supply a reference while local quality remains `UNSYNCED`.
Observations are process-local; a restart requires a fresh source observation.

## Status and observation

`TimeStatus.Source` records the latest source identity, state (`synchronized`,
`unsynchronized`, `unavailable`, `error`), uncertainty and optional signed offset.
`SourceAge` measures its age with the local monotonic clock. Store/engine
`ClockStatus` and `ReplicationStatus.Clocks` include the same `TimeSource` for the
local node. Source synchronization alone does not imply quality within budget.

On agents with a node ID, query the management Unix socket:

```sh
curl --unix-socket "$OUTPOSTNET_SOCKET" http://localhost/v1/time
```

`Agent.TimeReference()` exposes the same pointer after startup. Agent observation
samples it once a second into existing `MetricSample` events: `net_time_quality`
(value 1, with `quality`, `source`, `source_state` labels),
`net_time_source_uncertainty_seconds` for synchronized source observations, and
`net_time_source_offset_seconds` when an offset was measured. These are timeline
events, not new Snapshot fields. Unknown measurements are omitted. Embedding
hosts can map the public status into their existing event/lease producers.

## Acceptance evidence

The [fixture provenance](../pkg/state/testdata/time-source/README.md) distinguishes
real captures from synthetic variants. Node1 was read only. Its chrony capture
produces `SYNCED` with 18.252361ms uncertainty in the agent's management response;
this verifies the adapter/host assembly locally, not a deployment on the VM.

| Net#63 acceptance / scope | Evidence |
| --- | --- |
| 1: real synchronized capture | `TestOSTimeSourceCapturedChrony`, `TestAgentOSTimeSourceStatus` |
| 1: captured source loss, holdover then drift expiry, restart | `TestOSTimeSourceCapturedSourceLossEntersHoldover` (upstream systemd loss capture) |
| 1: missing binary, loss timer, recovery | `TestOSTimeSourceLossAndRecovery` |
| Pure parser; unsynchronized/malformed/local/nonfinite/overflow cases | `TestOSTimeSourceRejectsUntrustedChrony`, `TestOSTimeSourceTimesyncRequiresErrorBound`, `TestOSTimeSourceTimesyncStartupAndFallback` |
| Bounded commands; no masked chrony loss; cancellation and join | `TestOSTimeSourceCommandTimeoutAndNoMaskedLoss`, `TestOSTimeSourceProductionReferencePollsAndStops` |
| Shared host/store source status and Range event mapping | `TestOSTimeSourceProductionReferencePollsAndStops`, `TestAgentOSTimeSourceStatus`, `TestTimeSourceObservationUsesExistingMetricEvents` |
| 2: live chrony stop/start on VM | Deferred to [OutpostRange#51](https://github.com/outpostos/OutpostRange/issues/51) by the [architect's instruction](https://github.com/outpostos/OutpostNet/issues/63#issuecomment-6888153483); no VM changes or deployment |
| 3: local gates and CI | Commands below; PR checks record CI results |

```sh
TMPDIR=/tmp go test -race -count=1 ./pkg/state ./internal/agent ./internal/observation \
  -run 'TestOSTimeSource|TestAgentOSTimeSourceStatus|TestTimeSourceObservation'
go build ./...
go vet ./...
go mod tidy -diff
TMPDIR=/tmp go test -p 1 -race -count=1 ./...
GOOS=linux GOARCH=amd64 make build-agent
GOOS=linux GOARCH=amd64 go build ./...
make fmt
git diff --check
```

Red→green: parser and host/observation tests first failed on missing APIs; the
cancellation test then caught probes continuing after cancellation; malformed
source tests caught lowercase local mode being trusted and startup without a
timesync sample being misclassified as a parser error. All pass after their fixes.
No gossip algorithms, protocol version, durable format or Mesh contracts change.

Native Linux validation on e5box passed on a fresh archive of rebased product
commit `aeee927`: `go build ./...`, `go vet ./...`, the complete
`GOMAXPROCS=2 GOTMPDIR=/dev/shm TMPDIR=/dev/shm go test -race -count=1 -timeout=30m ./...`
(state 124.502s, delivery 134.780s, tunnel 15.038s), and
`GOOS=linux GOARCH=amd64 make build-agent`. No tests were excluded. The subsequent
test-only fixture isolation change (`8ba19ef`) also passed the focused Linux race
suite, including the default subprocess runner and NET-14 regressions. Native Mac
build/vet and the focused race suite passed. The initial complete Mac run hit a
QUIC dial timeout in `TestRelayDirectAndRelay`; its isolated race rerun passed.
That complete Mac run passed state (193.375s), delivery (248.122s) and tunnel
(16.605s); no other package failed. The final module metadata promotes the
already-pinned `golang.org/x/sys` to a direct dependency for the read-only syscall;
no dependency version changes, and `go mod tidy -diff` is clean.
The Linux gates ran on the development host, without changing any VM.
