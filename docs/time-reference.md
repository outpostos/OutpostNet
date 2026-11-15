# Time reference (V2-NET-1b)

Design: [ADR-0006 D1/D2/D6](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md).
`pkg/state.TimeReference` is a concurrency-safe, process-local clock shared by
stores and host code. It never changes the operating-system clock and needs no
central service. Local writes continue when time is unknown.

## Embedding

Create one reference per host process, then share it across keyspaces and delivery:

```go
reference, err := state.NewTimeReference(state.TimeReferenceConfig{})
if err != nil { return err }
store, err := state.NewDurableStore(state.DurableStoreConfig{
    Dir: stateDir, NodeID: nodeID, TimeReference: reference,
    TombstoneRetention: retention,
})
if err != nil { return err }
outbox, err := delivery.NewOutbox(delivery.Config{
    Dir: outboxDir, NodeID: nodeID, TimeReference: reference, Transport: transport,
})
if err != nil { return err }
// Supply the same TimeReference to the Inbox. Hosts generate expiry with:
expiry := reference.Now().Add(ttl)
```

The reference implements `state.PhysicalClock` (`NowMS`) and provides `Now` for
existing Mesh `Config.Clock`, Let `cfg.Now` and delivery `Config.Clock` callbacks.
The explicit store `TimeReference` option also attaches gossip sampling and the
independent retention clock automatically. Configure `PhysicalClock` or
`TimeReference`, and delivery `Clock` or `TimeReference`, exclusively. Without a
reference, existing injectable clocks keep their behavior; handshake quality is
`UNSYNCED`. There is no implicit global clock or automatic product-host wiring.
Mesh/Let integration belongs to V2-MESH-5 / V2-LET-4.

The host's trusted time-source adapter (for example, a chrony monitor) periodically
calls `reference.UpdateSource(state.TimeSourceStatus{Synchronized: true,
Uncertainty: measuredErrorBound})`. Report source loss with an unsynchronized
observation. Net derives quality; it does not infer synchronization from successful
replication or the system clock's apparent date. No OS-specific source probe is
installed by this library. Hosts must supply honest uncertainty and an oscillator
drift bound suitable for their hardware.

| Setting | Default | Meaning |
| --- | --- | --- |
| `ClockBudget` | 2s | Pairwise clock error budget / UNSYNCED agreement width |
| `DriftPPM` | 50 | Worst-case local oscillator drift, parts per million |
| `SourceTTL` | 30s | Maximum age of a `SYNCED` source observation |
| `MaxSampleRTT` | 2s | Maximum accepted full round trip |
| `SampleTTL` | 30s | Maximum age of a reachable peer sample |
| `SystemClock` | Unix milliseconds | Actual local physical clock |
| `MonotonicNow` | `time.Now` | Local elapsed-time clock; preserve its monotonic component |

`SYNCED` requires a recent synchronized source observation whose uncertainty plus
elapsed-time drift is within **half** the pairwise budget. Two qualified nodes
therefore stay within the full budget. Losing or ceasing to report the source enters
`HOLDOVER` while that estimate remains within half the budget; afterwards quality is
`UNSYNCED`. Repeated loss reports never restart the holdover timer. Both `SYNCED`
and `HOLDOVER` use the system clock and never self-suspect from peer samples.

An `UNSYNCED` reference prefers the median of fresh `SYNCED`/`HOLDOVER` peers; one
is enough. Otherwise it needs at least two distinct `UNSYNCED` peers, with a strict
majority inside one `ClockBudget` interval, and uses that majority's median.
No qualifying samples means `TimeUnknown`, system time, and no clock-suspect
attribution to either side. Admission still enforces the five-minute safety valve
and records refusals. With a selected reference, a system/reference difference
exceeding that valve marks this node suspect, without blaming the healthy peer.

Peer send time is advanced by half the measured RTT, then by local monotonic
elapsed time until the next sample. Slow/invalid samples withdraw the old sample;
failed exchanges withdraw it immediately; otherwise samples expire at `SampleTTL`.
`ObservePeer` is the adapter seam for other authenticated transports; identify a
peer by its stable authenticated node ID, never by multiple address aliases.

## Wire and trust boundary

State protocol **3.0** replaces 2.0. Push and pull requests and responses carry
`sent_physical_ms` (actual system clock, never HLC or a relayed reference) and
`time_quality`. Pull responses and 410 responses also identify the answering
`node_id`, so aliases cannot vote twice. 410 responses carry protocol and policy
metadata and a time sample, learned **before** guarding the resync horizon. This
allows a node one year behind to bootstrap through a forced snapshot resync.
Samples participate in push/digest/snapshot envelope byte budgets.

Only responses to locally initiated, successful authenticated exchanges establish
a reference: they have a locally measured RTT. Inbound request samples are carried
as metadata but cannot establish a round-trip bound on their own. Quiet nodes
still sample peers during periodic anti-entropy; a node serving only inbound
traffic must also run its gossip loop. Custom `Syncer` / `SnapshotSyncer` adapters
must preserve responder identity, samples and `ResyncRequiredError` metadata and
must authenticate the exchange before returning them.

In legacy HTTP `token` mode, responder identity remains self-declared.
[V2-NET-4 certificate modes](http-node-auth.md) bind response and 410 identities
to the authenticated server certificate before exposing time samples or horizons.
State protocol remains 3.0 for the coordinated upgrade. A valid group peer falsely
claiming good time can still bias an `UNSYNCED` node; this is the ADR's accepted
limit.

Stop all hosts embedding state in a keyspace, upgrade their pinned Net dependency,
and restart the whole group. Mixed majors are rejected (requests: HTTP 426;
responses including 410: version validation). The durable log format is unchanged.
Time-source observations and samples are not persisted: restart begins `UNSYNCED`
and needs fresh observations. Durable HLC monotonicity remains intact; an already
future HLC is not rewritten when the reference changes (V2-NET-1c handles reset).

## Retention, expiry and observation

HLC generation, admission, horizon guards and the GC upper bound use the same
reference. Tombstone retention uses the independent clock
`min(system, reference)`. This can only delay collection relative to either clock
alone, preserving the [GC argument](state-tombstone-gc.md). In particular, a system
clock one year ahead cannot collect a reference-stamped tombstone before its
reference retention elapses. Peer acknowledgement/refusal gates remain required.

Delivery uses the shared reference for admission, expiry and its clock reads.
Generate `Envelope.ExpiryTime` with `reference.Now().Add(ttl)` too. Existing stored
expiry values are never rewritten: envelopes that become expired after a reference
switch expire normally, and delivery status/counters expose those outcomes.
Business applications supplying absolute timestamps from their own clocks remain
responsible for them. A wrong-clock future envelope is not repaired here; existing
MaxExpiry admission rules apply. No new clock-reset command is added.

`TimeStatus()` reports quality, unknown/reference selection, local self-suspicion,
system/reference milliseconds, and accepted/discarded sample counters.
Store/engine `ClockStatus()` and `ReplicationStatus.Clocks` additionally expose local
quality/unknown/reference fields alongside the existing per-writer entry and
per-peer horizon counters. Hosts poll these snapshots and map transitions to their
observation event stream and node leases; this library adds no Range dependency.
Self-suspicion from a reference clears when a valid local source returns. Existing
writer/horizon observations retain their process-local lifetime.

## Acceptance evidence

All tests use public boundaries, real memory/durable stores and local HTTP peers,
with injected clocks. They do not claim a VM deployment or cross-component Range
acceptance (those are separate tickets).

| ADR-0006 / issue criterion | Automated evidence |
| --- | --- |
| 3: one-year-behind node completes forced 410 resync; horizon bounded | `TestTimeReferenceBehindNodeCompletes410Resync` (memory and durable, real HTTP, new writes accepted by healthy peer, local-only self-suspicion) |
| 7: system +1 year cannot collect a reference-young tombstone | `TestTimeReferenceDrivesHLCAndConservativeRetention`, `TestTimeReferenceRetentionWaitsForSlowerSystemClock` (both stores; each direction delays collection) |
| 9a: trusted A, ahead B; A does not self-suspect, B counted | `TestTimeReferenceSyncedReceiverBlamesAheadWriterOnly` (SYNCED and HOLDOVER) |
| 9b: behind UNSYNCED A, SYNCED B; A self-suspects and converges | `TestTimeReferenceBehindNodeCompletes410Resync` |
| 9c: two UNSYNCED nodes remain time unknown without blame | `TestTimeReferenceTwoUnsyncedPeersDoNotBlameEachOther` (also two addresses for the same peer) |
| Quality, median/majority, monotonic extrapolation, RTT/expiry bounds | `TestTimeReferenceSourceQualityAndPeerSelection`, `TestTimeReferenceSourceErrorFitsPairwiseClockBudget`, `TestTimeReferenceConsensusRTTAndExpiry`, `TestTimeReferenceSlowHTTPResponseCannotSupplyReference`, `TestTimeReferenceRejectsOverflowAndAcceptsEpoch` |
| Handshake fields, physical rather than reference time, major upgrade | `TestTimeReferenceWireMetadataAndProtocolUpgrade`; existing byte-budget / protocol / horizon tests |
| Delivery reference admission/expiry, existing values, restart | `TestTimeReferenceDeliveryAdmissionExpiryAndRestart` |
| Shared concurrent reference | `TestTimeReferenceConcurrentHostUse` under `-race` |
| Net#55 follow-up: only clock_offset pins the refusal gate | `TestTimeReferencePermanentRejectionDoesNotPinGC`; existing clock-refusal tests |

Reproduce from the repository root:

```sh
go test -race -count=1 ./pkg/state ./pkg/delivery -run '^TestTimeReference'
go build ./...
go vet ./...
TMPDIR=/tmp go test -p 1 -race -count=1 ./...
GOOS=linux GOARCH=amd64 make build-agent
make fmt
git diff --check
```

`TMPDIR=/tmp` avoids the Mac Unix-socket path limit in existing host tests.
Red→green evidence: the reference/retention/delivery slices first failed because
the APIs were absent; the resync slice first refused the healthy horizon as one
year in the future; the unknown-time slice blamed the only peer; aliases initially
established a false majority; permanent rejection initially pinned GC; the overflow
slice initially accepted a wrapping timestamp. Each corresponding test passes
after its implementation slice. No Mesh contract changes or generated files occur.

On shared machines, `-p 1` serializes packages while retaining the complete race
suite. Initial parallel runs encountered timing-sensitive existing tests:
`TestProcessCrashBoundaries` (Mac child output contained trailing test-harness
text after JSON), `TestStreamRecoversAfterEarlyReturns` (one QUIC dial timeout),
and `TestColdStartFromCache` (Linux cache fixture expired before its assertion).
Isolated rechecks passed; the crash test also passed repeated checks on the
unchanged main baseline. These tests and their product code are unchanged here.
