# V2-NET-2 directory leases: implementation and blocked validation

Specification: [OutpostNet#50](https://github.com/outpostos/OutpostNet/issues/50),
[ADR-0007 D3/D6](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0007-node-lease.md),
[ADR-0008 D4](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0008-node-identity-and-origin.md).
Epic: outpostos/OutpostOS#30. This branch is not ready for deployment or a PR:
the full test gate is blocked by an existing fixture outside the authorized scope.

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

`go build ./...` and `go vet ./...` passed on macOS. The full
`go test -race -count=1 ./...` run failed in the service-host tests listed below;
it also encountered a macOS Unix-socket path-length failure in the unchanged
`internal/server.TestConnectExporterDisconnected` fixture. Raw logs remain
local; only sanitized excerpts are recorded here.

## Blocker and remaining acceptance

The architect limits changes to `internal/meshdirectory`, `pkg/service` and
their docs. `internal/servicehost/mesh_fake_test.go`, function `reg.json`,
emits legacy registrations with no `originNodeId`. Enforcing ADR-0008 correctly
rejects them; six existing tests fail:

- `TestDirectoryExportsFollowRegistrations`: route does not appear.
- `TestStaticExportWinsOverDirectory`: conflict warning absent.
- `TestLogicalEntryThroughDirectoryExport`: no backend.
- `TestLogicalEntryFollowsDirectory`: no backend.
- `TestLogicalFailoverOnlyBeforeBytes`: no backend.
- `TestLogicalFailureReasonsCounted/all_unreachable`: registrations rejected.

Representative diagnostic:

```text
rejecting node-scoped directory record: writer mismatch
node_id=node-b writer_node_id=""
```

The fixture needs to emit `originNodeId` equal to the registration's node.
That file is outside the authorized paths. It has not been modified, and the
production filter has not been weakened to accommodate it. Implementation
stopped at this scope boundary as instructed.

The full gate, Linux gates, hosted CI and actual three-VM RANGE-1/Let-stop
regression remain unverified. VM inspection was read-only; no product binary,
configuration, service or clock on any VM was changed. No PR has been opened.
