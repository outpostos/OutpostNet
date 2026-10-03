# pkg/state: tombstone GC and durable log compaction (ticket 03b)

Status: implemented on `feat/four-layer-03b-tombstone-gc`. Evidence is local
unit and fault-injection tests on one machine; it is not a production
multi-node or power-loss validation.

## Root cause

`storeCore.entries` keeps the latest entry for every key ever written,
including tombstones, and `DurableStore` only appends to `state.log`. A
consumer whose keys keep changing (Mesh entity/object/task IDs with TTL, Let
metadata) therefore grows memory and the log without bound, and cold start
replays the whole history. Tombstones cannot simply be dropped: in a
replicated keyspace an absent key is indistinguishable from "never seen", so
any peer still holding an older value would re-introduce it through push or
digest anti-entropy (resurrection).

## Model

Three pieces of per-store state are added. They are persisted by
`DurableStore` and survive compaction.

| State | Meaning |
|---|---|
| GC horizon (`Timestamp`) | Highest HLC of any entry this store removed (collected tombstone or resync purge), or adopted from a peer during resync. Only grows. |
| GC floor (`uint64`) | Highest local position of any collected tombstone, or a fresh local position allocated by a resync purge that removed values. A consumer position below it may have missed a removed tombstone or still hold a purged value. Only grows. |
| Replicated flag | Set when a `GossipEngine` is built on the store. Direct local GC is refused afterwards; GC must go through the engine's peer gate. |

### Collection condition

A tombstone `t` is collected only when all of the following hold:

1. **Retention**: `t.Version.HLC.WallMS <= physicalNow - TombstoneRetention`.
   `TombstoneRetention == 0` (the default) disables GC entirely.
2. **Peer acknowledgement** (replicated stores only, via
   `GossipEngine.CollectTombstones`): `t.Position <= min(push progress of every
   peer currently returned by the PeerProvider)`. Push progress only advances
   after the peer acknowledged the push (durably for `DurableStore`), and a
   non-transient acknowledgement means the peer now holds `t` or something
   newer for that key.
3. **Local-only stores** (no engine): `Store.CollectTombstones` uses the
   retention condition alone.

Collection is one atomic step under the commit lock: the removal (and, for
`DurableStore`, a single `purge` record carrying the new horizon, floor and the
exact identities removed) is committed before the entries leave memory.

### Why tombstone removal cannot resurrect a key

* **Horizon rejection (hard guarantee at the collecting node).** `Apply` of an
  incoming entry whose key is *absent* locally and whose HLC is `<= horizon` is
  refused: values with `BeyondGCHorizonError`, tombstones as a no-op (absent
  already means deleted). Any older value of a collected key has an HLC lower
  than the collected tombstone, hence `<= horizon`, so it can never be
  re-applied. Keys that are present are ordered by normal LWW; a present entry
  is always newer than any tombstone that was collected for that key.
* **Horizon handshake (lagging peers are forced to resync).** Push and digest
  pull requests carry the requester's horizon (`gc_horizon`, omitted when zero).
  A responder whose horizon is higher refuses the incremental exchange with
  `ResyncRequiredError` (HTTP `410 Gone` + `gc_horizon`). The requester then
  performs a snapshot resync from that peer instead of exchanging increments.
  A pushed entry that the receiver refuses individually (`beyond_gc_horizon`
  rejection, receiver horizon in `PushResponse.gc_horizon`) also triggers a
  resync.
* **Snapshot resync.** The lagging node pages through the peer's full state
  (`PullRequest{SincePosition, Limit}` without digest, never refused), applies it
  with normal LWW, and then atomically (one `purge` record) removes every local
  entry whose HLC is `<= peer horizon` and whose key the peer does not have, and
  adopts the peer horizon. Those entries are exactly the ones the peer would
  refuse: either values of keys deleted and collected cluster-side, or writes
  that failed to reach the peer within the retention window. Both are dropped
  explicitly (logged per key), never silently merged back. Snapshot pages hold
  256 entries and are cursored by the peer's `next_since_position`. When the
  purge removes entries, it allocates a fresh local position that becomes the
  GC floor, and observers receive a tombstone-shaped notice (`Deleted: true`,
  the removed entry's version and ChangeID, the new position) for each purged
  key, so consumers that held the value re-read or re-snapshot.
* **Consumers.** `CheckConsumerPosition(p)` returns `NeedsSnapshot` when
  `0 < p < floor`, because the position may precede a removed tombstone.

### Trade-off: peer acknowledgement vs. retention

| Option | Safety | Bounded growth | Cost |
|---|---|---|---|
| Ack gate only | Only against peers in the current PeerProvider list; unknown, removed or restored-from-backup nodes can still resurrect | No: one dead peer blocks GC forever | none |
| Retention + horizon + forced resync | Against every peer, including unknown ones, at every node that has the horizon | Yes | a peer lagging longer than retention loses its own unreplicated writes older than the horizon, explicitly |

Chosen: **both, with retention + horizon as the safety mechanism and the ack
gate as an efficiency guard.** The horizon is what makes resurrection impossible
at a collecting node; the ack gate avoids pushing healthy-but-slow listed peers
into full resyncs. A peer that is permanently gone blocks GC until it is removed
from the PeerProvider (an explicit membership decision); when it returns, the
handshake forces it to resync.

Residual risk (documented, not hidden): a node whose horizon is still lower than
the cluster's (for example a brand new node that has only talked to another
lagging node) may serve stale values locally until it exchanges with any peer
that has a higher horizon. It cannot reintroduce them into any node whose horizon
covers them, and the first such exchange forces its resync.

## Compaction (`DurableStore`)

`Compact(ctx)` rewrites `state.log` as the current snapshot:

1. Hold the commit lock (no entry mutation) and the log lock (no append or
   peer-progress save) for the whole rewrite; the entry map read lock is held
   only while entries are copied.
2. Write `state.log.compact` (through the same `durableLog` wrapper used by
   appends): a `snapshot` header record (change/position counter, HLC, horizon,
   floor), then one `entry` record per current entry (tombstones not yet
   collected included), then one `peer_progress` record per peer. Sequences
   continue after the current `nextSeq`, so they never regress.
3. `fsync` the temp file, close it.
4. `rename` it over `state.log`.
5. `fsync` the directory.
6. Reopen the append handle on the new file; swap it in.

| Crash / failure point | Disk after crash | Recovery / behavior |
|---|---|---|
| During temp write or before temp fsync | old `state.log` + partial `state.log.compact` | Open deletes the temp file; old log is authoritative. Runtime failure: temp removed, store stays writable. |
| After temp fsync, before rename | old log + complete temp | Same: temp is never trusted, it is deleted. |
| Temp write / short write / temp fsync fails | old log | Temp removed, error returned, store stays writable. |
| Rename fails | old log (+ temp) | Temp removed, store stays writable on old log. |
| After rename, before directory fsync | new log (or, after power loss, possibly the old one) | Either file is a complete, consistent state; counters restored from the header or the entries. |
| Directory fsync fails | as above | Store becomes fail-closed (`ErrDurableStoreUnwritable`): a later append could land in a file whose directory entry is not durable. Reopen recovers. |
| Reopening the append handle fails | new log | Fail-closed, same as above. |

Counters: the header restores the position/ChangeID counter and HLC even when
the entries that carried the highest values were collected, so recovered stores
never reuse a `ChangeID`, regress a position, or emit an older HLC.

`CompactIfNeeded(ctx)` compacts when `CompactionMinRecords > 0`, the log holds at
least that many records, and at least twice the number of records a fresh
snapshot would contain. With GC enabled, the log is then bounded by
`max(CompactionMinRecords, 2 * (live entries + uncollected tombstones + peers + 1))`
records, and cold start replays at most that many.

## Configuration

| Field | Default | Effect |
|---|---|---|
| `MemoryStoreConfig.TombstoneRetention` / `DurableStoreConfig.TombstoneRetention` | 0 (off) | Minimum tombstone age before collection. |
| `DurableStoreConfig.CompactionMinRecords` | 0 (off) | Enables `CompactIfNeeded` and engine-driven automatic compaction. `Compact` is always available. |

`GossipEngine.Run` performs `CollectTombstones` and `CompactIfNeeded` on the
anti-entropy tick when they are enabled. Stores without an engine call
`CollectTombstones` / `CompactIfNeeded` themselves.

## Impact on consumers and wire

* Default behavior is unchanged: with both settings zero no entry is removed,
  no new record type is written, and no new wire field is sent.
* Wire additions are optional JSON fields (`gc_horizon` in push/pull requests and
  in push responses; `gc_horizon` in the 410 error body) and a new rejection
  reason `beyond_gc_horizon`. Peers that do not send `gc_horizon` are treated as
  horizon zero: once a node has a non-zero horizon it refuses their incremental
  exchange with 410. Enabling GC therefore requires every peer of the keyspace to
  run this version.
* `ListEntries`/`ChangesSince`/`Get` no longer return collected tombstones; a
  consumer must treat "absent" as "deleted" and honor `NeedsSnapshot`.
* `TombstoneRetention` must exceed the longest expected peer partition plus
  `MaxClockOffsetMS`; otherwise partitioned peers are forced into resync and
  lose their unreplicated writes older than the horizon.
* Durable directories written by this version may contain `purge` and `snapshot`
  records; older binaries refuse them as unknown record types rather than
  misreading them.
* Out of scope: epoch/lineage guards (03a) and narrowing `CompareAndSet`'s
  append-under-lock scope (still a known bound).
