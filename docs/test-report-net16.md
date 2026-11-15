# NET-16 local acceptance evidence

Scope: OutpostNet issue #70, architecture v0.2 D10 limit 3; epic
outpostos/OutpostOS#30. Tests use local stores and authenticated loopback HTTP.
No VMs were accessed; deployed D10 re-verification belongs to a later run.

| Requirement / acceptance criterion | Evidence |
| --- | --- |
| Archive exactly the final purge set, including full byte-identical values, tombstones, original position/identity and peer/horizon | `TestResyncArchivesOnlyPurgedEntriesAndPersistsCounts`: local binary value, local tombstone and forwarded write are archived; a held key and newer-than-horizon write remain live and absent from the archive. |
| Count local-origin vs forwarded entries per writer and triggering peer; survive restart/compaction | Same test checks 2 local + 1 forwarded, writer attribution, compact/reopen and preserved relative archive path. `TestMemoryReplicaCanRetainResyncArchives` checks opt-in memory-store archives and status ownership. |
| Archive failure aborts the purge with store unchanged | `TestResyncArchiveFailureLeavesStoreUnchanged`: create, write, short write, file fsync, close, archive-directory fsync, data-directory fsync, and purge-log refusal. Checks entries, status/position, floor/horizon and durable reopen. |
| Pre-retention warning, configurable fraction, monotonic elapsed time | `TestResyncLossWarningBeforeRetentionWithFrozenWallClock` checks custom 50% threshold, single warning per outage and recovery clearing. `TestResyncWarningFractionValidation` rejects invalid/NaN/infinite fractions. |
| Warning during backoff without ordinary replication ticks | `TestResyncWarningsRunDuringLongBackoff`: real `Run`, one-hour replication/backoff intervals, 10% warning threshold on a two-second retention. |
| Actual multi-process over-retention split/reunion | `TestResyncLossAcrossProcesses`: separate HTTP server process, real partition failure, default 80% warning before one-second retention, reunion after expiry; archive has exactly 2 local + 1 forwarded entries, byte-identical payloads/tombstone, no archive replay through subsequent anti-entropy. |
| MetricSample event representation | `TestResyncLossMetricSamples`: existing event body, scope/writer/peer/local-origin labels, counters, relative latest path and warning; maps through `state.ReplicationMetrics` and Net reporter. |
| GC compatibility argument and manual operator recovery | `docs/state-tombstone-gc.md` NET-16 section; `docs/resync-loss.md`. No purge predicate or wire change. |
| AC2: 03b resurrection suite and all existing GC tests unchanged | Included in full `go test -race -count=1 ./...`; existing GC test files are unchanged. |
| AC3: local gates and CI | Commands below; GitHub check result recorded on the PR. |

Red → green slices: the archive/counter test initially failed because LocalStatus
had no resync status; the warning slice failed because peer warning facts and
configuration did not exist; the metric-event slice failed because the reporter
had no replication adapter. Each passed after its implementation. The first full
state regression run caught a changing read-time duration in backoff status;
recording elapsed time only on failure/warning transitions restored the existing
status invariants without modifying those tests.

Reproduce from the repository root:

```sh
# On macOS, use a short temporary directory for existing Unix-socket tests.
export TMPDIR="${OUTPOST_TMPDIR:?set a short writable temporary directory}"
export GOTOOLCHAIN=go1.25.0
go test -race -count=1 ./pkg/state -run 'TestResyncArchive|TestResyncLoss|TestResyncWarning'
go test -race -count=1 ./internal/observation -run '^TestResyncLossMetricSamples$'
go build ./...
go vet ./...
go test -race -count=1 ./...
GOOS=linux GOARCH=amd64 make build-agent
make fmt
git diff --exit-code
```

The format check is run after committing; it must leave the worktree unchanged.
Final local gates use Go 1.25.0 on macOS amd64, matching the CI version in
go.mod; Linux amd64 is cross-compiled. An initial Go 1.26.1 full run hit the
existing macOS Unix-socket path-length limit in the default system temporary
directory. The final run uses a shorter TMPDIR. GitHub CI runs
format/vet/race tests/build-agent on Linux amd64.

## Remaining dependency boundary

The Net library and event adapter are ready for embedding hosts. Mesh and Let
observation producers are separate repositories; standalone Net agent/service
processes own no state store. Activating the requested Mesh/Let MetricSample
streams requires consumer dependency upgrades and sampler wiring after this Net
PR merges, following OutpostOS AGENTS.md's rule that private cross-repository
dependencies be merged before consumers pin their SHA. No local `replace`,
unmerged consumer dependency, consumer repository edit, or invented standalone
Net replication facts are included. This portion of requirement 1 remains open;
NET-16 must not be closed as fully integrated by this Net PR alone.
