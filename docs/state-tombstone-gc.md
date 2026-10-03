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
   after the peer answered the push (durably for `DurableStore`). This gate is
   an efficiency guard, not a proof that the peer holds `t`: every entry that is
   not rejected as *transient* advances the cursor, including non-transient
   rejections (for example a `ValidateApply` failure or `beyond_gc_horizon`).
   Safety does not depend on it; it comes from retention plus the GC horizon
   below.
3. **Local-only stores** (no engine): `Store.CollectTombstones` uses the
   retention condition alone.

The `PeerProvider` of a replicated store must return the **configured
membership** of the keyspace, not the set of peers that are currently alive or
reachable: a peer missing from the list is not asked to acknowledge anything. If
the provider returns no peers at all, `GossipEngine.CollectTombstones` refuses
with `ErrTombstoneGCNoPeers` and collects nothing; there is no retention-only
fallback for a replicated store, because an empty list cannot be told apart
from a transient isolation.

Collection is one atomic step under the commit lock: the removal (and, for
`DurableStore`, a single `purge` record carrying the new horizon, floor and the
exact identities removed) is committed before the entries leave memory.

### Why tombstone removal cannot resurrect a key

* **Horizon rejection (hard guarantee at the collecting node).** Every merge
  of a remote entry (push, digest pull and snapshot resync alike) whose key is
  *absent* locally and whose HLC is `<= horizon` is refused: values with
  `BeyondGCHorizonError`, tombstones as a no-op (absent already means deleted).
  Any older value of a collected key has an HLC lower than the collected
  tombstone, hence `<= horizon`, so it can never be re-applied at a node that
  collected it or adopted a horizon covering it. Keys that are present are
  ordered by normal LWW; a present entry is always newer than any tombstone that
  was collected for that key. There is no bypass for resync: a peer's horizon
  is a single maximum timestamp and says nothing about which deletes that peer
  saw, so a peer with a higher horizon can still hold a value this node deleted
  and collected.
* **Horizon handshake (stale peers are forced to resync).** Push and digest
  pull requests carry the requester's horizon (`gc_horizon`, omitted when zero).
  A responder refuses the incremental exchange with `ResyncRequiredError`
  (HTTP `410 Gone` + `gc_horizon`) when its horizon is higher, or when a digest
  pull's digest holds a live value for a key the responder no longer has at or
  below its horizon (the requester still holds something deleted and collected
  here, even if the horizons are equal and its push cursor already passed that
  value). The requester then performs a snapshot resync from that peer instead
  of exchanging increments. A pushed entry that the receiver refuses
  individually (`beyond_gc_horizon` rejection, receiver horizon in
  `PushResponse.gc_horizon`) also triggers a resync of the pusher.
* **Snapshot resync.** The resyncing node pages through the peer's full state
  (`PullRequest{SincePosition, Limit, MaxBytes}` without digest, never refused)
  and applies each page as it arrives through normal `Apply` (LWW plus the local
  horizon rejection above; only the set of snapshot keys is kept across pages).
  Then it atomically (one `purge` record) removes every local entry whose HLC is
  `<= peer horizon` and whose key the peer does not have, and adopts the peer
  horizon, raising its HLC to at least that horizon (the same as replaying the
  purge record after a restart). Those entries are exactly the ones the peer
  would refuse: either values of keys deleted and collected cluster-side, or
  writes that failed to reach the peer within the retention window. Both are
  dropped explicitly (logged per key), never silently merged back. A failed page
  aborts the resync before the purge; the pages already applied are ordinary
  merges. Snapshot pages hold at most 256 entries and at most `max_bytes` of
  encoded response (`HTTPSyncer` sends its `MaxResponseBytes`); a single entry
  that cannot fit fails the resync with `PullEntryTooLargeError` naming the key,
  its size and the limit. Pages are cursored by the peer's
  `next_since_position`. When the purge removes entries, it allocates a fresh
  local position that becomes the GC floor, and observers receive a
  tombstone-shaped notice (`Deleted: true`, the removed entry's version and
  ChangeID, the new position) for each purged key, so consumers that held the
  value re-read or re-snapshot.
* **Consumers.** `CheckConsumerPosition(p)` returns `NeedsSnapshot` when
  `0 < p < floor`, because the position may precede a removed tombstone.

### Trade-off: peer acknowledgement vs. retention

| Option | Safety | Bounded growth | Cost |
|---|---|---|---|
| Ack gate only | Only against peers in the current PeerProvider list; unknown, removed or restored-from-backup nodes can still resurrect | No: one dead peer blocks GC forever | none |
| Retention + horizon + forced resync | At every node whose horizon covers the deleted value: refused from every peer, listed or not, on push, pull and resync. A node still holding the value is forced to resync once it pushes it or offers it in a digest to such a node | Yes | a peer lagging longer than retention loses its own unreplicated writes older than the horizon, explicitly |

Chosen: **both, with retention + horizon as the safety mechanism and the ack
gate as an efficiency guard.** The horizon is what makes resurrection impossible
at a collecting node; the ack gate avoids pushing healthy-but-slow listed peers
into full resyncs. A peer that is permanently gone blocks GC until it is removed
from the PeerProvider (an explicit membership decision); when it returns, the
handshake forces it to resync.

Residual risks (documented, not hidden):

* A node whose horizon does not cover a deleted value and that does not hold the
  value's tombstone (for example a brand new node that has only resynced from a
  stale holder) may serve and pass on that stale value among nodes whose horizon
  does not cover it either, until it exchanges with a node whose horizon does.
  It cannot reintroduce the value into any node whose horizon covers it, and its
  first such exchange (a refused push, or a digest pull answered with 410) forces
  its resync.
* A stale holder is only cleaned up when it initiates an exchange with a
  covering node. A node that never pushes or pulls keeps serving the stale value
  locally.
* The node that resyncs is the one whose horizon is lower or whose push was
  refused, not necessarily the one that is behind. Its purge drops every local
  entry at or below the peer horizon that the peer lacks, including its own
  writes that did not reach that peer within the retention window. This only
  happens when a partition outlasted `TombstoneRetention`, which the
  configuration rule below excludes.
* Resync reads the peer's snapshot in pages without a consistent cut. If the
  peer collects or purges a key while the requester is paging, the requester can
  keep its older value of that key after this resync (the key was not on any
  page, and that value's HLC is above the horizon read with the first page). It
  is removed at the next refused exchange with that peer (the next `410` or
  `beyond_gc_horizon`), not immediately.

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
  no new record type is written, and the GC fields (`gc_horizon`) are never sent
  because the horizon stays zero. One wire difference is independent of GC: a
  position pull with `Limit > 0` whose page comes back full now carries
  `more: true` and `next_since_position`. Both are optional fields that older
  receivers ignore.
* Wire additions are optional JSON fields (`gc_horizon` in push/pull requests and
  in push and pull responses; `gc_horizon` in the 410 error body; `max_bytes` in
  pull requests, sent only by `HTTPSyncer.PullSnapshot` for paged position
  pulls; `more` / `next_since_position` in pull responses) and a new rejection
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
