# Local clock recovery (V2-NET-1c)

Design: [ADR-0006 D5 and acceptance 8](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md).
This procedure repairs one `pkg/state` directory while its embedding host is
stopped. Mesh and Let own their directories; the tunnel agent does not own them.

## Alarm and runtime protection

Store/engine `ClockStatus()` and `ReplicationStatus.Clocks` mark the local node
`ClockSuspect` when its issued/restored HLC exceeds its reference by more than
the five-minute safety valve. This applies even to a SYNCED node and is independent
of peer blame. Recovery emits a local warning; status reads also detect an HLC
left ahead after a running clock correction. The HLC alarm stays set for that
store instance. Hosts map these snapshots to their observation streams.

`TimeReference` compares system wall-time advancement with local monotonic elapsed
time. A forward excess greater than five minutes is rejected: reference-based
timestamps continue from the last accepted baseline plus elapsed time. Local writes
continue, and `TimeStatus` exposes `StepRejected`, `StepRejections` and
`ClockSuspect`. The counter counts distinct rejection episodes. A long interval
without clock reads is allowed when wall and monotonic clocks advance together;
backward corrections remain subject to HLC monotonicity.

A qualified `UpdateSource` observation confirms the system clock and resets the
baseline, including when leaving UNSYNCED. An operator can explicitly confirm a
corrected clock through `reference.ConfirmSystemClock()`. The rejected offset is
latched until confirmation or until the system clock returns within `ClockBudget`
of the extrapolated baseline. Merely waiting until it falls inside the five-minute
valve does not accept it. Peer-reference bootstrap remains available and does not
adopt a rejected system step. Confirmation does not rewrite an existing HLC.

Create and share a `TimeReference` for host code and stores as described in
[time-reference.md](time-reference.md). Stores with no explicit physical clock
also create a guarded reference. An explicitly supplied legacy `PhysicalClock`
retains its own behavior; use `TimeReference` to get monotonic step protection.
Detection is process-local: it cannot detect a clock jump while the process is
stopped or establish the correctness of its initial clock.

## Offline command

Correct/establish the reference time, stop the host embedding the directory, and
keep a backup of the stopped directory if its contents require investigation.
Build the tool with `make build-state-reset`, then run it for each affected state
directory:

```sh
build/outpostnet-state-reset --data-dir "${STATE_DIR:?}"
```

By default the command uses the corrected system clock. An operator with an
independently confirmed reference can pass a current RFC3339 timestamp:

```sh
build/outpostnet-state-reset --data-dir "${STATE_DIR:?}" \
  --reference-time "${CONFIRMED_REFERENCE_TIME:?}"
```

Library hosts can use `state.ResetDurableClock(ctx, state.ClockResetConfig{Dir:
stateDir, Reference: establishedReference})`. Neither entry HLCs nor unauthenticated
network time are suitable reference sources. An incorrect reference can discard
valid state. The command requires an existing manifest and takes the same exclusive
directory lock as the host; an open directory is refused.

The command deletes **all current entries** whose HLC wall time exceeds
`reference + MaxClockOffsetMS`, including forwarded values and tombstones. It
prints JSON containing `Purged` (entry count), `Rebuilt`, `ReferenceMS`, and `Floor`
only after success. Entries exactly at the valve remain. Repeating a successful
reset purges zero additional entries at the same reference.

One synced atomic log replacement removes historical entry/batch records,
snapshot HLC headers and purge records as recovery sources. The reset HLC starts
at the reference or the highest retained valid entry/horizon, whichever is later.
The manifest's node and policy identities remain intact. Opaque domain policies
are not imported: offline recovery follows committed local positions, without
reinterpreting their conflict order. Change/position counters never regress; reset
allocates a new position and raises the floor beyond every previous cursor.
Subscribers must take a fresh snapshot. Refusals for purged keys are removed;
other peer acknowledgement/refusal gates remain conservative.

Restart the host with the same identity and corrected/shared reference. Run its
normal anti-entropy loop to retrieve older healthy peer values; Mesh/Let republish
their own reconstructible keys. Wrong-clock writes that peers never accepted are
lost intentionally. Removing a future tombstone allows the healthy peer's older
live value to return. The state store is not a historical audit log.

## Future GC horizon and upgrade

The first D5-aware open checks the recovered local/adopted GC horizons against the
reference. A future horizon refuses startup with `ErrClockRebuildRequired`; a
successful check records `clock_recovery_version: 1` in the existing manifest.
No state protocol or durable record format version changes. Establish the clock
before this check; a host starting far behind can supply an established reference
before opening its stores. Samples/source observations are still process-local.

The reset command always checks horizons, even after the one-time upgrade check.
A future horizon cannot be fixed by a partial purge. Explicitly authorize a full
state clear with:

```sh
build/outpostnet-state-reset --data-dir "${STATE_DIR:?}" --rebuild
```

This clears **every entry**, local/adopted horizons and peer progress/refusals,
preserves the manifest identity and counters, and raises the consumer floor.
Restart and rebuild from healthy peers with normal snapshot anti-entropy/resync;
only restart the affected host once healthy peers are available to restore its
state. `--rebuild` uses this full clear only when the horizon is actually future;
otherwise it performs the ordinary future-entry purge. This accepted recovery
exception permits the local horizon to return to zero. D4 prevents new future
horizons; this check catches legacy data. Downgrading the binary cannot retain
the upgrade check or runtime step protection.

The tool does not clear `pkg/delivery` Inbox/Outbox directories. Existing envelopes
retain their expiry; the receiver rejects illegal future expiry through MaxExpiry
admission. Expired envelopes follow the existing delivery lifecycle. Applications
must resend when appropriate.

## Reproduction and evidence

| Requirement | Evidence |
| --- | --- |
| ADR-0006 #8: +1-year write, corrected clock, reset, exact count, fresh write accepted and old versions cannot win | `TestClockResetRecovery`: ordinary/compacted durable logs, restart twice, forwarded entry and tombstone, local alarm, healthy peer acceptance and authenticated HTTP anti-entropy restores older live value |
| No future HLC recovery source remains; identities/counters/floor preserved | `TestClockResetRecovery`, `TestClockResetBoundaryAndRetry`, `TestClockResetPreservesOpaqueCommittedPolicy` |
| Future purge horizon refuses upgrade/partial reset; explicit rebuild clears state | `TestClockResetFutureHorizonUpgrade` |
| Atomic failure behavior, no false purge success | `TestClockResetFailureRecovery`: short write, temp sync, rename, directory sync, reopen; real log recovery after every failure |
| Step rejection, extrapolation, confirmation, leaving UNSYNCED baseline | `TestClockStepDetection`, `TestClockStepBoundaryAndElapsedTime`, `TestClockStepProtectsStoreWrites` (memory/durable) |
| Offline CLI, directory lock and argument/result handling | `TestResetCommand`, `TestClockResetRecovery` |

```sh
go test -race -count=1 ./pkg/state ./cmd/outpostnet-state-reset -run '^Test(Clock|ResetCommand)'
make fmt
go build ./...
go vet ./...
TMPDIR=/tmp go test -p 1 -race -count=1 ./...
GOOS=linux GOARCH=amd64 make build-agent build-state-reset
git diff --check
```

Red→green: reset/CLI tests initially failed for absent APIs; step detection first
adopted the jump, then accepted a previously rejected offset once it slipped inside
the valve. The final tests require continued rejection and monotonic extrapolation.
Disk failure cases use the existing real-log fault/compaction seams. These are
local store/HTTP and injected-clock tests, with no VM clock modification, deployment
or power-loss guarantee. `TMPDIR=/tmp` avoids existing Mac Unix-socket path limits;
`-p 1` serializes the full race suite's packages on a shared machine.
