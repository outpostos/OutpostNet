# Service directory status (NET-15)

`outpostnet-service` exposes its own Mesh directory view at **GET
`/v1/directory`**, on the existing `management.socket_path` Unix socket
(default `/run/outpostnet/service.sock`). This read-only endpoint is available
with observation disabled too. It is not registered on the health or
observation TCP listeners, or on `outpostnet-agent`. Mutation methods return
405. The socket's existing filesystem permissions govern local access.

```sh
SERVICE_SOCKET=/run/outpostnet/service.sock
curl --fail --silent --unix-socket "$SERVICE_SOCKET" http://localhost/v1/directory | jq .
```

Example, after a rejected writer and an expired lease:

```json
{
  "configured": true,
  "synced": true,
  "connected": true,
  "stale": false,
  "stats": {
    "connects": 1,
    "snapshots": 1,
    "deltas": 2,
    "malformed": 0,
    "expired": 0,
    "lease_expired": 1,
    "writer_rejected": 1,
    "cache_writes": 0,
    "cache_write_errors": 0
  },
  "leases": [{
    "node_id": "node-b",
    "state": "expired",
    "lease_seq": "42",
    "last_advance_age_seconds": 31.2,
    "lease_ttl_seconds": 30
  }]
}
```

| Field | Meaning |
| --- | --- |
| `configured` | `mesh.gateway_url` enabled this host's directory. Without it the response is 200 with false flags, zero counters and `leases: []`; it is not a healthy synchronized directory. |
| `synced` | This directory has applied a complete Mesh snapshot in this process. Cache-only startup is unsynchronized. |
| `connected` | The Mesh SSE connection is currently open, including while its initial snapshot is arriving. |
| `stale` | Waiting for the first snapshot or disconnected; retained records still follow their ordinary lease/legacy expiry rules. |
| `stats.connects` | Successful HTTP stream connections (not failed connection attempts). |
| `stats.snapshots` | Complete snapshots applied. |
| `stats.deltas` | Processed delta envelopes, including versions rejected by writer validation. |
| `stats.malformed` | Malformed stream events or records skipped by validation. |
| `stats.expired` | Legacy wall-clock-expiring registrations removed by maintenance. |
| `stats.lease_expired` | Lease expiry transitions detected by maintenance, counted once until sequence progress or snapshot refresh. |
| `stats.writer_rejected` | Registrations or leases discarded because their writer is missing or differs from the described node (S2). Replayed rejected versions can be counted again. |
| `stats.cache_writes` | Successfully completed directory-cache writes. |
| `stats.cache_write_errors` | Failed directory-cache writes. |
| `leases[].node_id` | Node with an accepted lease or a retained lease-backed registration; rows are sorted by node ID. Legacy-only registrations have no lease row. |
| `leases[].state` | `valid`: last accepted sequence advance is within the effective TTL; `expired`: beyond it; `missing`: a registration needs a lease but none is accepted. This describes the node lease, not instance readiness or reachability. |
| `leases[].lease_seq` | Current accepted sequence, as a decimal string to preserve uint64 precision; absent when missing. |
| `leases[].last_advance_age_seconds` | Local monotonic elapsed seconds since sequence progress; absent when missing. Repeated/regressed sequences do not reset it. Snapshot/cache initialization treats a lease as just seen, as in ADR-0007. |
| `leases[].lease_ttl_seconds` | Effective TTL for that node (the executable currently uses the 30s default). |

All counters describe this process and reset on restart. The read computes lease
state at request time without expiring, renewing or deleting records; the
maintenance expiry counter may follow the state change by one sweep (normally
250ms). Recovery needs only sequence progress, not instance re-registration.
During startup, before the configured directory is published, the endpoint
returns 503. Upstream unavailability instead returns 200 with `stale: true`.

Only numeric counters, flags, node IDs and lease timing metadata are exported.
Entity payloads, endpoints, registrar incarnation, resume cursors and raw
connection errors are excluded. In particular, `Stats.LastConnectError` can
contain upstream response text or credentials and is never serialized here.

## Observation timeline

When the host has a directory and observation is enabled, it samples these
counters as the existing **109 `MetricSample`** events on the
`COMPONENT_OUTPOSTSERVICE` stream. Sampling runs initially and once per second while the process is scheduled.
Directory notifications do not trigger extra samples: counter increments between
ticks are coalesced into the next cumulative value, including rejection and expiry
counters. The management endpoint still reports current values immediately.
The existing bounded log, slow-subscriber and gap behavior applies. Metrics
are events; the observation Snapshot has no generic metric field.

Each `stats.<name>` maps to `service_directory_<name>_total`, with no labels.
Lease samples are:

| Metric | Value and labels |
| --- | --- |
| `service_directory_lease_valid` | 1 for `valid`, 0 otherwise; labels `lease_node_id` and `state` (`valid`, `expired`, `missing`). |
| `service_directory_lease_last_advance_age_seconds` | Monotonic age, label `lease_node_id`; omitted for missing leases. |
| `service_directory_lease_ttl_seconds` | Effective TTL, label `lease_node_id`. |

Events already carry source node, component, sequence and time. Metric values
use the existing double field; use the endpoint's integer counters and string
sequence when exact uint64 precision is needed. Removed nodes stop producing
lease samples; the endpoint's current rows are authoritative for membership.
No Mesh or Range contract/dependency upgrade is required.

## V2-RANGE-3 evidence collection

On each tested Service host, record `/v1/directory` immediately before and
after each forged registration and forged lease trial. Compare
`.stats.writer_rejected` on that same host and process: each rejected input
must advance it. Preserve both JSON responses with the refusal probe and
product versions; `/v1/security` contains tunnel counters and is not the
directory rejection oracle. Raw journal counting is not a substitute.

Collect the Service observation event stream through range-agent as usual.
Filter `metricSample.name == "service_directory_writer_rejected_total"`
and confirm the same increase. Read from the pre-trial snapshot sequence or
subscribe before injection; metrics are not present in snapshot fields.

For Let-stop trials, poll the hosting node's lease row until `expired`, observe
`lease_expired` advancing and routing stopping, then verify a sequence advance
returns the row to `valid` and routing recovers without re-registering the
instance. A forged lease replacing an accepted version can produce `missing`
because S2 filtering evicts that version; it never renews trust. Allow the
normal replication and sampling delay; correlate by process identity and
event sequence, not by equal request/stream timestamps.

NET-15 validates these surfaces locally. VM re-verification belongs to
[V2-RANGE-3](https://github.com/outpostos/OutpostRange/issues/52).
