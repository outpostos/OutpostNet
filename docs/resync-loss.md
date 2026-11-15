# Resync loss archives and early warnings (NET-16)

When a peer requires snapshot resync, the final purge removes entries held only
locally whose HLC is at or below that peer's guarded GC horizon. This includes
unreplicated writes after an overlong split. It also includes forwarded values
and tombstones. Normal LWW replacements and ordinary tombstone collection are
not counted as resync loss.

## Archive before purge

`DurableStore` writes each selected entry to a new
`archive/resync-purge-<UTC>-<unique>.jsonl` under its data directory. Every line
contains `key`, the complete base64 `value` or `deleted` tombstone, `version`,
`change_id`, the original local `position`, the triggering `peer` (credentials
redacted), and the adopted peer `horizon`. JSON/base64 preserves arbitrary value
bytes. Files are created with mode 0600; a newly created archive directory is 0700.

The file, archive directory, and data directory are synced before the single
purge record is committed. Archive create/write/short-write/fsync/close or
directory-sync failure returns an error: the final purge changes no entries,
position, floor, horizon, HLC, counters, or latest path. Snapshot pages already
merged before the final purge retain their existing ordinary merge semantics.
An error is visible through the exchange error/log and peer replication status.

A crash or purge-log failure after archiving can leave an archive of an attempted
purge that did not commit. A retry can archive those entries again. Only committed
purges increment counters or update `LatestArchivePath`; do not count JSONL lines
across files as committed losses. Archives are operator evidence, never read
back by startup, served to peers, or automatically deleted. Budget disk space
for them; disk exhaustion blocks the purge instead of silently losing evidence.

`MemoryStore` has no data directory. Its counters last for the store lifetime;
set `MemoryStoreConfig.ArchiveDir` to retain file archives, otherwise its state
and purge evidence are volatile. Production persistent state uses `DurableStore`.

## Status and observation

`Store.LocalStatus().Resync`, also present in `GossipEngine.ReplicationStatus().Local`,
contains:

| Field | Meaning |
| --- | --- |
| `PurgedLocalOrigin` | Committed removals with `Version.WriterNodeID == store node ID`. |
| `PurgedForwarded` | Committed removals written by another node. |
| `ByWriter` | Counts grouped by writer, triggering peer, and local-origin flag. |
| `LatestArchivePath` | Latest committed archive path, relative to the data directory. |

Durable purge records commit these counts with removals; replay and compaction
preserve them without reading archives. Existing format-1 directories remain
readable and start with zero counts for purges that predate NET-16. Older binaries
ignore the optional fields; running/compacting with an older binary can lose
counter history. The state wire protocol and all purge decisions are unchanged.
Per-entry warning logs name the key, writer, local-origin flag, peer, horizon,
and relative archive path; they do not include the value.

Hosts map `state.ReplicationMetrics(scope, status)` to their existing 109
`MetricSample` events (no Range dependency in the state library):

| Name | Value / labels |
| --- | --- |
| `state_resync_purged_entries_total` | Committed count; `scope`, `writer_node_id`, `peer`, `local_origin`. |
| `state_resync_latest_archive` | 1; `scope`, relative `path`. Omitted before any archive commits. |
| `state_peer_unreachable_seconds` | Elapsed outage measured at the latest failure/warning; `scope`, `peer`. |
| `state_resync_loss_limit_approaching` | 0 or 1; `scope`, `peer`, `warning="resync loss limit approaching"`. |
| `state_resync_loss_warnings_total` | Warning transitions this engine process; `scope`, `peer`. |

`internal/observation.Reporter.UpdateReplication` supplies the Net event adapter.
Standalone agent/service processes embed no state replicas and therefore do not
emit invented replication loss facts. Mesh and Let own their embedded replicas
and observation streams: they must upgrade their pinned Net dependency **after
this Net change is merged** and emit these samples on their own sampling ticks,
including every relevant store (Mesh entities/tasks/objects; Let metadata).
The Net PR provides the API and adapter; it does not activate those producers.

## Early warning

`GossipConfig.ResyncLossWarningFraction` defaults to **0.8**; a nonzero value
must be strictly between 0 and 1. With GC enabled, a peer without a successful
exchange for that fraction of `TombstoneRetention` gets the warning log
`resync loss limit approaching`. Peer status reports `UnreachableFor`,
`ResyncLossLimitApproaching`, and `ResyncLossWarnings`. `UnreachableFor` is
measured on failure and warning transitions; read-only sampling and skipped
backoff rounds do not advance it. This is a conservative
exchange-failure signal: transport, apply, and persistence failures all count;
it is not a claim about the peer's global connectivity or actual lost writes.

Elapsed time is local monotonic time, independent of status wall-clock jumps.
The outage starts at the first exchange failure observed in this process and
clears on a successful exchange. Each outage warns once; the warning counter
survives recovery but resets when the engine restarts. `Run` checks separately
from replication/backoff loops, at most one second apart (or one tenth of the
warning threshold for short retentions). Manual round users must continue
calling `SyncOnce`/`AntiEntropyOnce` before expiry, including during backoff.
GC disabled (`TombstoneRetention == 0`) means no retention warning. Custom store
wrappers should forward `TombstoneRetention() time.Duration` to enable it.

Keep retention greater than the longest planned split plus the clock safety
budget. The warning does not change membership, GC acknowledgement, routing,
write admission, or resync decisions.

## Inspect and deliberately republish

Resolve `LatestArchivePath` against that store's data directory. Inspect without
opening the archive as a store:

```sh
jq -c '{key, deleted, version, change_id, position, peer, horizon}' "${ARCHIVE_PATH:?}"
jq -r 'select(.key == "example/key") | .value' "$ARCHIVE_PATH" | base64 --decode > recovered-value.bin
```

The decode command uses GNU base64 (`-D` on macOS). Preserve the archive as evidence.
Compare the recovered value with current domain state, check authorization and
expiry, and decide whether it is still valid. In Mesh/Let, republish through the
owning domain API so its validation and merge rules run. Do not POST archive
entries to state push endpoints or reuse old HLC/ChangeID/position: they remain
below the horizon and can represent a legitimate cluster-side deletion.

A generic store owner can decode a chosen line and deliberately create a new
local mutation after review:

```go
var record struct { state.Entry }
if err := json.Unmarshal(selectedLine, &record); err != nil { return err }
// Only after operator/domain review; Put/Delete generate a fresh identity/HLC.
if record.Deleted {
    _, err = store.Delete(ctx, record.Key)
} else {
    _, err = store.Put(ctx, record.Key, record.Value)
}
return err
```

There is no automatic restore or republish command. A later D10 VM run must
validate the deployed Mesh/Let producer upgrades; NET-16 uses local processes.
