# pkg/state: tombstone GC and durable log compaction (tickets 03b, 03d)

Status: on `main` — 03b (PR #8, `59aa33d`) and the 03d requester horizon check
and batched resync apply (PR #11, `e8f9de0`); 10-D1 (PR #13, `5d28562`) added
push-round replication status and left the GC gate unchanged. Consumers enable
GC in 03c (OutpostLet PR #61, OutpostMesh PR #45), off by default. Evidence is
local unit and fault-injection tests on one machine; it is not a production
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
   peer currently returned by the PeerProvider)`. This is the acknowledged
   position only; the push-round fields of `ReplicationStatus`
   (`OutboundRoundCaughtUp`, `OutboundRoundTime`, ticket 10-D1) never gate
   collection. Push progress only advances
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
  (HTTP `410 Gone` + `gc_horizon`) in two cases:
  1. **Unvouched lower horizon**: the requester's horizon is lower than the
     responder's *and* the responder cannot vouch that the requester received
     every tombstone the responder removed (see "Requester horizon check" below;
     ticket 03d). A lower horizon alone no longer forces a resync: in a healthy
     cluster nodes collect the same tombstones on different ticks, and the one
     that has not collected yet still holds the tombstones.
  2. **Digest holds a collected value**: a digest pull's digest holds a live
     value for a key the responder no longer has at or below its horizon (the
     requester still holds something deleted and collected here, even if the
     horizons are equal and its push cursor already passed that value).

  The requester then performs a snapshot resync from that peer instead of
  exchanging increments. A pushed entry that the receiver refuses individually
  (`beyond_gc_horizon` rejection, receiver horizon in `PushResponse.gc_horizon`)
  also triggers a resync of the pusher.
* **Snapshot resync.** The resyncing node pages through the peer's full state
  (`PullRequest{SincePosition, Limit, MaxBytes}` without digest, never refused)
  and merges each page as it arrives with the same rules as `Apply` (validation,
  HLC offset, LWW plus the local horizon rejection above; only the set of
  snapshot keys is kept across pages). The accepted entries of a page are
  committed together (ticket 03d, 03b F4): `DurableStore` writes them as one
  `batch` record with one fsync, so a resync costs one fsync per page plus one
  for the purge instead of one per entry (522 entries: 523 fsyncs before, 4
  after). A page is all or nothing: an entry that fails validation or the HLC
  offset check aborts the page before anything is written, the page becomes
  visible only after its record is durable, a failed write is rolled back, and
  a crash in the middle of the record leaves an unterminated line that recovery
  discards, so a cold start sees whole pages only.
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

### Requester horizon check (ticket 03d)

03b refused every incremental exchange from a requester whose horizon was lower.
Horizons only say "entries at or below this HLC were removed here"; a lower
requester horizon does not mean the requester holds a value those removals
suppress. Every node collects a tombstone only after all members acknowledged
it, but each node does so on its own maintenance tick, so in a healthy cluster
horizons are unequal most of the time and the old rule forced full snapshot
resyncs back and forth. `TestHealthyStaggeredGCNeverForcesResync` (TTL load,
staggered collection, every member acknowledging) measured with the 03b rule:
392 resyncs in 600 rounds with two in-memory replicas, 882 with three, 72 and
162 in 120 rounds with two and three `DurableStore` replicas. With the rule
below: 0 in all four once every node has a horizon (see "Healthy cluster").
`TestPartitionBeyondRetentionStillForcesResync` shows a
node partitioned and dropped from the membership for longer than the retention
is still resynced when it rejoins, nothing is resurrected, and the healthy load
afterwards is back to 0. Its first push after the rejoin carries only a fresh
key and is refused by the request-level lower-horizon check (asserted: the push
gets `ResyncRequiredError`, the node's horizon is non-zero and covers the
responder's adopted horizon, so the refusal comes from the unacknowledged
floor), not by the digest check or a per-entry rejection.

**Rule.** Responder `R` (horizon `H_R`, GC floor `F_R`, adopted horizon `A_R`)
answers a push or digest pull from requester `Q` (horizon `H_Q`, node ID
`SourceNodeID`) with 410 for a lower horizon only when `H_Q < H_R` and `Q` is not
*vouched*. `Q` is vouched when both hold:

* **Acknowledged every own removal**: `R`'s push progress towards the address
  that answered as node `Q` is `>= F_R`. Every tombstone `R` collected has a
  local position `<= F_R`, so a push `Q` answered carried it (or `Q` already had
  a newer entry, or its own horizon covered it). `R` learns which node answers
  at an address from the optional `node_id` of the push response and keeps it
  next to the push progress (`DurableStore` persists it in the `peer_progress`
  record). A requester `R` never pushed to, or whose responses carry no
  `node_id` (a 03b node), is not vouched.
* **No unvouched adopted removals**: `H_Q >= A_R`, where `A_R` is the highest
  peer horizon `R` adopted through a snapshot resync. `R` cannot vouch for
  removals another node performed, so for those the 03b comparison applies.
* **Not a zero horizon**: `H_Q` is non-zero, or `F_R` is zero. A zero horizon
  means `Q` never collected or adopted anything, which is also what a node looks
  like whose data directory was wiped and that came back with the same node ID
  at the same address: the push progress it inherits was acknowledged by the
  state that is gone. Such a requester is refused on its first exchange and
  adopts `H_R` (`TestWipedRequesterWithSameIdentityIsRefusedOnFirstExchange`).
  The cost in a healthy cluster is one resync per node, once in its lifetime:
  when the first node of a fresh cluster collects, every node that has not
  collected yet adopts its horizon instead; a horizon never returns to zero.

The digest check (case 2 above) and the per-entry `beyond_gc_horizon` rejection
are unchanged and apply to vouched requesters as well.

**Why this cannot resurrect a key.** The no-resurrection guarantee does not
rest on vouching. It rests on three mechanisms, none of which vouching skips:

1. **Apply horizon rejection.** Every entry merged by a push, a digest pull or a
   snapshot resync goes through the receiver's horizon rejection and LWW, so a
   node whose horizon covers a deleted value, or that holds the newer
   tombstone, never accepts that value. A 410 never merges an entry anywhere; it
   only makes the requester replace increments by a snapshot resync, and when
   it is skipped the exchange proceeds as an ordinary push or digest pull.
2. **Range-complete digest check.** Every anti-entropy round sends every peer
   contiguous digest intervals covering the whole key space (see the invariant
   below). Within each interval the responder answers 410 if the requester
   holds a live value `<= H_R` for a key `R` no longer has, vouched or not.
   That is what removes a stale value from a requester that still holds it.
3. **Per-entry rejection.** A pushed value refused as `beyond_gc_horizon`
   makes the pusher resync from the receiver.

Vouching is an optimisation that only removes resyncs the 03b rule forced on
requesters that have nothing to clean up. It is not a proof that a vouched
requester holds no stale value: a requester whose state was restored from an
older backup, that refused a tombstone non-transiently, or whose address was
taken over can be vouched and still hold one. Such a value is cleaned up by
mechanism 2 on the requester's next anti-entropy round (or by mechanism 3 on its
next push of that value), not by a 410 on its first exchange. The vouching rule
is kept no weaker than its stated scope: a removal `R` performed on another
node's behalf (adopted through a resync, whether or not it raised `H_R`) is never
vouched (`H_Q >= A_R`), a zero horizon is never vouched once `R` removed
anything, and an address that stops sending `node_id` stops vouching.
The 03b regression tests (three resurrection scenarios, the digest 410 and the
resync refusal) run unchanged; with vouching forced on for every identified
requester they fail only on their "requester was resynced" assertions, never by
resurrecting a key, which is the evidence that safety does not depend on it.

**Invariant: each round covers all keys at every peer.** A round submits a
complete local digest to each peer, optionally split into sorted contiguous
intervals `[start_key, end_key)`. The first starts at the empty string (the
minimum key); the last has no upper bound. Each intervening end equals the
next start: the lower boundary is included, the upper boundary is excluded.
Thus a boundary key belongs to exactly the following interval. Every local key,
including tombstones, is included in exactly one interval's digest. Sampling,
truncation, skipping peers, and splitting coverage across rounds are forbidden.

Mechanism 2 remains complete: any stale local value belongs to one interval;
when that interval reaches the collecting responder, its live digest record
is checked and yields 410, even at an inclusive lower boundary. A record at an
exclusive upper boundary is checked by the following interval. The responder
checks only the declared interval, regardless of the diff continuation cursor;
continuations repeat the complete interval digest, so response paging never
hides a collected value. Request records outside the interval are invalid.

An interval containing no requester keys still declares its bounds and sends
an empty digest. It is a digest exchange, not a position snapshot: the horizon
handshake still runs. There is no held value for mechanism 2 to reject in that
interval, but responder keys absent from the requester must still be returned
by the diff. This includes keys before the requester's first key, between its
keys, and after its last key. An empty requester sends one unbounded interval.

Diff responses page in key order inside the interval using `More` and an
exclusive last-key cursor. The requester finishes all continuations before the
next interval. Only completion of every interval records pull success. Any
failed page, apply failure, or 410/resync invalidates the round; the next round
starts at the first interval with a fresh complete digest. Already applied
pages remain ordinary horizon-checked merges, not a successful partial round.
Concurrent writes are ordinary later state changes: a round's digest is a
local snapshot, not a consistent distributed cut; the next round covers them.
`LocalDigest`, `AntiEntropyOnce` and `checkRequesterHoldsCollected` implement
this invariant.

**Healthy cluster.** With stable membership, every member acknowledged every
collected tombstone before any node collected it (the GC gate), no node holds a
value below another node's horizon for a key the other lacks, no push entry is
refused and no resync adopts anything. Once every member has a non-zero horizon,
every lower-horizon request comes from a vouched member, so no 410 is sent and
no resync runs. Before that, each member that has not collected yet resyncs
once from the first collector (the zero-horizon condition above):
`TestHealthyStaggeredGCNeverForcesResync` measures 1 (two replicas) and 2 (three
replicas) such resyncs in the first 24 rounds and 0 in the remaining 576 (memory)
or 96 (`DurableStore`) rounds.

**Residual (unchanged class).** Vouching relies on push progress, which is keyed
by peer address: if a different node takes over an address, the node ID learned
from its first push response replaces the old one (or clears it, if the response
carries no `node_id`) and it inherits that
address's progress, exactly as the push cursor already does. A requester whose
directory was wiped has horizon zero and is not vouched (see the rule). A
requester restored from an older backup that already had a non-zero horizon
`>= A_R` is still vouched by the address progress: until it exchanges with a
node that refuses it, it accepts stale values for keys deleted and collected
after the backup (from a lagging node outside the membership, say) and may pass
them on to other nodes whose horizon does not cover them. It is cleaned up
through the digest check or a refused push rather than by a 410 on its first
push. Both are lineage problems tracked by 03a.

**NET-8 upgrade.** Digest range paging changes the state wire protocol from
1.0 to 2.0. Mixed old/new clusters are unsupported: request majors mismatch
with HTTP 426, and the client rejects old response majors. Stop all hosts of
a keyspace, upgrade their pinned Net dependency together and restart them.
This includes Mesh, Let and Range test consumers as applicable; it does not
change the durable log format. The following 03b/03d compatibility discussion
is historical and applies only within protocol 1.0.

**Historical mixed 03b/03d versions.** No request field changes; the push response gains the
optional `node_id`. A 03b responder ignores vouching and keeps refusing lower
horizons, which a 03d requester handles by resyncing as before. A 03d responder
never vouches a 03b requester (no `node_id` in its push responses), so it applies
the 03b rule to it. That includes a node downgraded from 03d to 03b at the same
address: the first push response without `node_id` clears the address's node
mapping in memory and in the durable `peer_progress` record (a record without
`peer_node_id` clears it on replay too), so the old node ID is not vouched again,
also after a restart or compaction
(`TestPushResponseWithoutNodeIDClearsTheAddressMapping`). A mixed cluster is therefore as safe as 03b and only pairs
with a 03b node keep the resync churn.

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

NET-8 supersedes the historical wire compatibility notes below: protocol 2.0
adds `start_key` / `end_key` and `after_key` to digest requests, and `next_key`
to diff responses. `MaxBytes` bounds digest responses too. See the cluster
upgrade requirement above; optional JSON fields do not make 1.0/2.0 mixed
operation safe. The remaining notes describe the earlier GC rollout.

* Default behavior is unchanged: with both settings zero no entry is removed,
  no new record type is written, and the GC fields (`gc_horizon`) are never sent
  because the horizon stays zero. One wire difference is independent of GC: a
  position pull with `Limit > 0` whose page comes back full now carries
  `more: true` and `next_since_position`. Both are optional fields that older
  receivers ignore.
* Wire additions are optional JSON fields (`gc_horizon` in push/pull requests and
  in push and pull responses; `gc_horizon` in the 410 error body; `max_bytes` in
  pull requests, sent only by `HTTPSyncer.PullSnapshot` for paged position
  pulls; `more` / `next_since_position` in pull responses; `node_id` in push
  responses, ticket 03d, sent regardless of GC) and a new rejection reason
  `beyond_gc_horizon`. Peers that do not send `gc_horizon` are treated as
  horizon zero: once a node has a non-zero horizon it refuses their incremental
  exchange with 410 unless it can vouch for them (see "Requester horizon
  check"). Enabling GC therefore requires every peer of the keyspace to run a
  version with GC support (03b or later); mixing 03b and 03d nodes is safe and
  only keeps the 03b resync behavior on pairs that involve a 03b node.
* `ListEntries`/`ChangesSince`/`Get` no longer return collected tombstones; a
  consumer must treat "absent" as "deleted" and honor `NeedsSnapshot`.
* `TombstoneRetention` must exceed the longest expected peer partition plus
  `MaxClockOffsetMS`; otherwise partitioned peers are forced into resync and
  lose their unreplicated writes older than the horizon.
* Durable directories written by this version may contain `purge`, `snapshot`
  and (03d) `batch` records; older binaries refuse them rather than misreading
  them. Optional record fields added by 03d (`peer_node_id` on `peer_progress`,
  `adopted` on `purge` and `snapshot`) are ignored by 03b binaries. See
  "Downgrading a durable directory from 03d to 03b" below.
* Out of scope: epoch/lineage guards (03a) and narrowing `CompareAndSet`'s
  append-under-lock scope (still a known bound).

### Downgrading a durable directory from 03d to 03b

**What 03b reports.** A 03b binary opening a directory whose log holds a `batch`
record (written by any snapshot resync under 03d) refuses to start with exactly:

```
replay durable state log line <N>: unknown durable record type "batch"
```

(measured: the 03b `NewDurableStore` at `59aa33d` opening a 03d directory after
one resync, `<N>` = 2). Nothing is replayed partially; the directory is left
untouched. The remedy is to start the 03d binary on that directory again, have
it run `Compact` (which rewrites every `batch` as ordinary `entry` records), and
retry the downgrade; the same directory after `Compact` opens under 03b without
error and with the same horizon (measured).

**Precondition: compact after writes have stopped.** `Compact` only rewrites
the records that exist when it runs. A 03d node that keeps gossiping after
`Compact` writes a new `batch` record on its next resync (any peer's 410, a
refused push or a digest refusal triggers one), and the directory is again
unreadable by 03b. The downgrade sequence is therefore: stop gossip on the node
(no `Run`, `SyncOnce` or `AntiEntropyOnce`, so no resync can start), call
`Compact`, exit without any further write, then start the 03b binary.

**Current limit: no operational entry point.** Nothing outside `pkg/state`
calls `Compact`: no CLI, agent flag or endpoint in this repository exposes it,
and the engine only runs `CompactIfNeeded`, which compacts on a record-count
threshold (`CompactionMinRecords`) while gossip keeps running, so it cannot
provide the sequence above. A "stop gossip, force `Compact`, exit" entry was
booked to ticket 11 by the 03d review and does not exist yet (checked at 11-D);
it must exist before GC is enabled anywhere a downgrade may be needed. Until
then the only downgrade path for a directory that holds `batch` records is to
make sure every peer holds this node's writes, stop it, clear its data
directory and start the 03b binary on the empty directory, which resyncs from
its peers under the 03b rule; writes the node had not replicated are lost on
that path. "Every peer holds this node's writes" means: stop the host's local
writes, then wait until `ReplicationStatus` reports `OutboundRoundCaughtUp`
with an `OutboundRoundTime` later than that point for every peer
(`pkg/state/README.md`). `CaughtUp` also proves it when it holds, but while
other nodes keep writing it is structurally false (10-D1) and may never be
reached.

**Why the format version is not raised.** The 03b binary is fixed, so only its
existing error paths can be chosen from. Raising the version of `batch` records
makes 03b report `replay durable state log line 2: unsupported durable record
format version 2`, which no longer names the record type; raising the manifest
version makes 03b refuse every 03d directory with `unsupported durable state
format version 2`, including compacted ones that it can read correctly, i.e. it
removes the downgrade path instead of explaining it. Both were measured against
`59aa33d`; neither is clearer than the current error, so the format stays at
version 1 and this section is the explanation.

**Round trip 03d → 03b → 03d.** 03b ignores `adopted` and `peer_node_id`, never
writes them, and its own resyncs record no adopted horizon. If 03b compacts the
directory (`Compact`, or `CompactIfNeeded` with `CompactionMinRecords > 0`), the
snapshot header loses `adopted` and the `peer_progress` records lose
`peer_node_id`. After upgrading back to 03d:

* Node mappings: every `peer_progress` record written by 03b has no
  `peer_node_id` and clears the address's mapping on replay, so the node vouches
  for nobody until a 03d push response names the node at that address again.
  Nothing is vouched on stale mappings.
* Adopted horizon: `A_R` is whatever the remaining 03d records carry, possibly
  zero, and removals adopted through resyncs under 03b are not reflected. Until
  the node's next resync raises it again, the node may vouch a requester for
  removals it adopted from another node. The safety mechanisms above are
  unaffected; the consequence is that such a requester, if it holds a stale
  value, is cleaned up by the digest check or a per-entry rejection rather than
  by a 410 on its first exchange.
* Handling: no action is needed for safety. To restore the strict rule right
  away, make sure every peer holds this node's writes (as in "Current limit"
  above: local writes stopped, then `OutboundRoundCaughtUp` with a later
  `OutboundRoundTime` for every peer), stop it,
  clear its data directory and restart it: with horizon zero it is refused on
  its first exchange (rule "Not a zero horizon"), resyncs and records the peer
  horizon as adopted, which makes it apply the 03b comparison to every lower
  horizon until it collects on its own.
