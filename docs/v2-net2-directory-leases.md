# V2-NET-2 directory leases: implementation and validation

Specification: [OutpostNet#50](https://github.com/outpostos/OutpostNet/issues/50),
[ADR-0007 D3/D6](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0007-node-lease.md),
[ADR-0008 D4](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0008-node-identity-and-origin.md).
Epic: outpostos/OutpostOS#30. The architect authorized Mesh entity API fake
updates in the latest issue comment; the missing writer fixture is corrected.

## Behavior on this branch

The adapter streams `filter.entityIdPrefix=urn:outpostos:` and dispatches
`serviceRegistration` and `nodeLease` locally. Mesh component filters compose
by AND, so requiring both components would incorrectly exclude both entity types.
Net retains minimal JSON types and has no Mesh module dependency.

Both record types require `originNodeId` to match the node described by the
record. Lease node identity comes from `urn:outpostos:node-lease:<node_id>`;
instance node identity comes from `serviceRegistration.nodeId`. Missing or
mismatched writers are rejected and counted in `Directory.Stats().WriterRejected`.
Rejected replacement versions evict the previously accepted record. This is
consumer filtering, not a cryptographic attestation of forwarded records.

A live registration carrying `registrarIncarnation` is returned only while its
node lease has the same incarnation and the local monotonic elapsed time since
sequence progress is at most `LeaseTTL`. Readiness, weight, TCP endpoint and
reachability remain enforced by `service.Select`; a lease cannot bypass them.
No wall timestamp in a lease-backed record affects this decision. Decimal
ProtoJSON uint64 sequences are decoded without floating-point conversion.
Duplicate or lower sequence values in the same incarnation do not renew trust.
A new incarnation resets the sequence baseline and invalidates old instances.

The default TTL is 30s. Hosts can configure `meshdirectory.Config.LeaseTTL`,
`LeaseTTLByLinkClass` and a concurrency-safe `NodeLinkClass(nodeID)` callback.
Class names are supplied by the host's link policy; unknown classes fall back
to `LeaseTTL`. Nonpositive class overrides and negative default TTLs are rejected.
The existing service executable uses the default TTL; this scoped change adds
no CLI/YAML wiring or automatic inference from replication latency.

`MonotonicNow` defaults independently to `time.Now` and retains its monotonic
reading. `Now` remains the legacy-expiry/diagnostic wall clock; injecting a
wall-time reference must not replace `MonotonicNow`.

Every sweep (default 250ms) emits one notification and increments `LeaseExpired`
when a lease expires. Reads enforce the deadline between sweeps. Expiration
retains registrations and leases, so progress after a partition restores
resolution without re-registration. Legacy expiry still removes expired
records and increments `Expired`.

Snapshots replace registrations and leases atomically on completion and treat
each lease as just seen. Cache V2 stores validated writers, incarnations and
sequences, but never serializes a monotonic timestamp. Cold loading also treats
leases as just seen and stays stale until a snapshot completes. Both paths may
trust an old lease for one extra TTL, as required by ADR-0007. Cache replacements
are serialized to preserve snapshot/delta ordering.

Legacy registrations without an incarnation retain wall-clock expiry support
for the transition version. They still need matching writer metadata from an
upgraded Mesh gateway. No-expiry records without an incarnation are rejected;
pre-originNodeId gateways and V1 caches cannot establish S2 ownership. Upgrade
Mesh before the adapter, and deploy lease-publishing Let together with or after
this adapter. An old TTL-only adapter does not expire new no-expiry registrations.

## Validation evidence

The first red test, `TestLeaseBackedRegistrationRequiresLease`, returned
`a/READY` without any lease. It passed after the adapter stopped accepting
unbacked no-expiry registrations. The next red test,
`TestLeaseMakesNoExpiryRegistrationResolvable`, returned no instance with a
valid lease; it passed after implementing lease consumption.

Passing focused commands:

```sh
go test -race ./internal/meshdirectory ./pkg/service -count=1
# Point OUTPOSTMESH_GATEWAY_BIN at a gateway built from merged Mesh main.
OUTPOSTMESH_GATEWAY_BIN=<gateway-binary> go test -race -tags integration \
  ./internal/meshdirectory -run TestRealMesh -count=1 -v
```

The real gateway used Mesh commit `924ebde` (merged V2-MESH-1). Both real-gateway
tests passed: legacy registration/withdrawal and two no-expiry instances that
expire after renewal stops and recover on lease progress. The latter also
checks +60s consumer wall time and a forged placement whose writer is supplied
by the real gateway, rather than trusted from the publisher's input.

Automated behavior coverage:

| Requirement | Evidence |
| --- | --- |
| ADR-0007 acceptance 2, directory portion | `TestLeaseExpiryUsesOnlySequenceAndMonotonicElapsed`, `TestLeaseExpiryNotifiesOnceAndRenewalRestores`, `TestRealMeshLeaseDirectory`: all instances stop resolving after lease progress stops; renewal alone restores them. |
| Acceptance 3, directory portion | `TestLeaseExpiryUsesOnlySequenceAndMonotonicElapsed`: offsets -60s, 0, +60s and subsequent correction produce the same expiry and recovery; publisher expiry is deliberately invalid and ignored for lease-backed records. |
| Sequence and incarnation | Sequences above 2^53, duplicate/regressed values, incarnation reset, lease deletion, retained records and reachability filtering. |
| Writer filtering | `TestWriterFilterRejectsLeaseAndInstanceVersions`, `TestGoldenMeshStreamWithoutWriterIsRejected`, real-gateway forged placement; rejects counted for leases, new and legacy registrations, and missing writers. |
| Snapshot/cache | `TestLeaseSnapshotReplacesRecordsAtomically`, `TestLeaseTTLByLinkClassAndSnapshotRefresh`, `TestLeaseCacheRestartIsJustSeenAndStillExpires`; existing reconnect, fallback and cache tests. |
| Legacy compatibility and failures | Existing TTL/readiness/endpoint/weight tests plus `TestLeaseIncarnationAndLegacyTransition`, invalid lease replacements and invalid TTL configuration. |

## Resumed red → green

Before changing the service-host fake, all six affected tests failed again:
`TestDirectoryExportsFollowRegistrations`, `TestStaticExportWinsOverDirectory`,
`TestLogicalEntryThroughDirectoryExport`, `TestLogicalEntryFollowsDirectory`,
`TestLogicalFailoverOnlyBeforeBytes`, and
`TestLogicalFailureReasonsCounted/all_unreachable`. The fake now emits
`originNodeId` equal to the registration node. The same focused race command
passes (5.529s). Production writer filtering remains enforced.

The Linux full race run also reproduced a pre-existing cold-cache fixture race:
`TestColdStartFromCache` observed only `a/READY` because its two-second record
expired during setup. The test now uses the existing injected wall-clock port,
asserts both records first, then advances time to check expiry. Ten repetitions
pass under the race detector (4.309s), with the original expiry assertion intact.

The two real Mesh gateway integration tests passed again against merged Mesh
`924ebde`: legacy registration/withdrawal, no-expiry lease expiration/recovery,
+60s consumer wall offset, and gateway-derived writer rejection (4.172s).

## Gate and deployment record

macOS uses a short `TMPDIR` to avoid the unchanged Unix-socket fixture path
limit. The first full macOS race run passed every package except delivery's
unchanged process-crash fixture: a SIGKILL child emitted additional test output
(`invalid character 'P' after top-level value`). The focused crash test then
passed (2.277s). The final complete run passed with two-package concurrency; no test was
excluded (delivery 238.880s, state 211.902s).

The first e5box run used temporary stores on the host's rotational root disk;
state and delivery reached the unchanged ten-minute timeout. The corrected
cold-cache test passed separately. The final complete Linux run passed with temporary
stores on the host's SSD and two-package concurrency, retaining normal fsync
and the original test timeouts (delivery 196.073s, state 180.396s). These initial
failures are not counted as passing gates. No Mesh contract or binding is
changed, so buf/generated-code gates do not apply to this Net-only consumer
update.

Final passing commands on macOS (Go 1.26.1) and Linux amd64 (Go 1.26.2):

```sh
make fmt
go build ./...
make vet
# Set TMPDIR to a short path on a fast local disk.
GOFLAGS=-p=2 make test   # go test -race -count=1 ./...; all packages
GOOS=linux GOARCH=amd64 make build-agent
git diff --check
```

The final format check also leaves the committed tree unchanged. See the
[sanitized gate record](artifacts/v2-net2/validation.txt) for complete race
package results, failing-first fixture evidence and real-gateway test results.

At validation time, the shared reference VMs were running V2-RANGE-1 fault trials. This
work has made only read-only inspections there; no product unit, configuration,
clock or trial state has been changed. Actual Let-stop and the three-VM RANGE-1
regression remain pending; automated adapter results above do not claim that
full-stack deployment acceptance. ADR-0007 assigns the joint deployment
validation to V2-RANGE-3. This PR supplies the directory implementation needed
for that run; it does not claim all field acceptance is complete.
