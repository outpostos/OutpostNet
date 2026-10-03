# OutpostNet state primitives

`pkg/state` provides generic state replication primitives. Entries carry opaque
bytes and tombstones, not Let PodBinding, Mesh Entity/Task, Kubernetes, or
tunnel-specific types.

Public consumer surface:

- `Store` is the common seam implemented by `MemoryStore` and `DurableStore`.
- `Put`, `Delete`, `Get`, `ListEntries`, and `Observe` for local state.
- `CompareAndSet` for atomic conditional mutation by `EntryIdentity`.
- `LocalStatus` reports local recovery readiness and current local position.
- `CheckConsumerPosition` distinguishes resumable positions from positions that
  require a snapshot or resync. It is local-position scoped; it is not a global
  "all peers synced" claim.
- `GossipEngine.SyncOnce`, `AntiEntropyOnce`, and `Run` for peer exchange.
- `HTTPSyncer` and `NewHTTPHandler` for authenticated JSON/HTTP transport.
- `GossipEngine.ReplicationStatus` for a read-only, concurrency-safe view of
  local readiness and per-peer exchange results (see below).

Identity model:

- `ChangeID` identifies the original write and is preserved when forwarded.
- `StateVersion` orders conflicting states through HLC plus writer identity.
- `Entry.Position` is a local consumption cursor and is not serialized on the wire.

Durable store:

- `NewDurableStore(DurableStoreConfig{Dir, NodeID, ...})` opens a file-backed
  store with the same `Store` interface as `MemoryStore`.
- The directory contains a manifest, an append-only `state.log`, and a lock file.
  The manifest pins format version, node identity, and policy ID. Opening with a
  different node or policy fails. Concurrent opens of the same directory fail
  with `ErrDurableStoreLocked`.
- Local writes and accepted remote applies are appended to `state.log` and the
  log file is synced before the entry becomes visible or observers are notified.
  A failed append/sync returns an error; the failed entry is not readable and is
  not announced. Local counters may have gaps after failures, but committed
  counters are not reused.
- A failed or short append is rolled back: the log is truncated to its pre-append
  offset and best-effort synced, so the file keeps no byte the store did not
  confirm and a later cold start still opens the directory. The discarded durable
  sequence number is reused, which is safe because its bytes are gone — recovery
  derives the next sequence from the records that are actually on disk, so no
  sequence can collide in the log. Change counters are never reused.
- If that rollback itself fails, the store becomes fail-closed: it keeps serving
  already confirmed reads, but every `Put`, `Delete`, `CompareAndSet`, `Apply`,
  and `SavePeerProgress` returns `ErrDurableStoreUnwritable` and no further byte
  is written. A restart re-runs recovery, which repairs the torn tail.
- Recovery restores values, tombstones, HLC state, committed change counters, and
  local positions into a fresh store instance. A backward or frozen physical
  clock after restart advances from the recovered HLC instead of reusing an old
  `ChangeID` or regressing `StateVersion`.
- Recovery distinguishes an unconfirmed tail from confirmed corruption. A record
  is written by a single `Write` that ends in `\n`, so an unterminated final line
  (even one that happens to parse) was never confirmed: recovery truncates it,
  emits one warning, and opens the directory. Corrupt JSON or a regressed
  sequence in a *terminated* record is confirmed corruption and refuses the
  directory, as do corrupt manifests and unsupported format versions. A tail that
  cannot be truncated is also refused, with `ErrDurableLogTruncated`. The store
  never silently clears the directory or falls back to memory.
- `LocalStatus().Ready` is true while the replica serves local reads and false
  after `Close`, and `CheckConsumerPosition` then reports `NeedsSnapshot` instead
  of resuming. `Ready` stays a local-readiness signal; it never claims that peers
  were caught up. Recovery runs to completion before a store constructor returns,
  so a store that could not recover is not published at all.
- `DurableStore` also persists gossip peer push progress. `GossipEngine` advances
  that progress only after a peer acknowledges the push and the local progress
  record is durably saved. If progress saving fails, the sync returns an error
  and recovery can safely replay the unconfirmed entries.

Tombstone GC and compaction (opt-in, see `docs/state-tombstone-gc.md`):

- `TombstoneRetention` (both stores, default 0 = off) enables tombstone
  collection. Local stores call `CollectTombstones`; stores attached to a
  `GossipEngine` must collect through `GossipEngine.CollectTombstones`, which
  additionally waits until every current peer acknowledged the tombstone. Its
  `PeerProvider` must return the configured membership, not the live peers; an
  empty peer list is refused with `ErrTombstoneGCNoPeers`.
- Collected keys raise the store's GC horizon. `Apply`, snapshot resync
  included, refuses an absent key at or below the horizon. Peers with a lower
  horizon, or whose digest still holds a value collected here, are refused with
  `ResyncRequiredError` (HTTP 410) and resync from a snapshot. A consumer position
  below the GC floor reports `NeedsSnapshot`.
- `DurableStore.Compact` rewrites the log as a snapshot through a synced temp
  file, rename and directory sync; `CompactionMinRecords` (default 0 = off)
  enables `CompactIfNeeded`. A directory sync failure after rename makes the
  store fail-closed.

Replication status (`GossipEngine.ReplicationStatus`):

- One `PeerReplicationStatus` per peer the `PeerProvider` currently returns:
  `AcknowledgedPosition` (persisted `PeerProgressStore` value when available),
  `OutboundCaughtUp` (`AcknowledgedPosition >= Local.CurrentPosition`, with the
  local position read first so the claim can only be conservative),
  `LastPullTime` (start of the latest fully applied pull), `InboundCaughtUp`
  (a pull was applied and no exchange failed after it started), `CaughtUp`,
  `LastSuccessTime`, `LastError`/`LastErrorTime`/`LastErrorOperation` (kept
  after recovery), `ConsecutiveFailures` and `NextAttemptTime` (backoff).
- A round that skips a peer in backoff is not a pull. A snapshot resync clears
  backoff but does not move `LastPullTime`; the next pull does.
- These are local facts of this process: times are not persisted, so a fresh
  process reports no pull yet (`InboundCaughtUp=false`) while the persisted
  acknowledged position survives. Same semantics as OutpostMesh
  `GetReplicationStatus` (ticket 06); hosts map it to their own reporting
  (for example OutpostRange `ReplicationProgress`) instead of wrapping the
  Syncer to re-derive it.
- Credentials never appear: peer addresses have their password redacted, and
  `HTTPSyncer` errors (returned, logged and reported) show the redacted URL and
  replace its bearer token with `[REDACTED]` even if a peer echoes it.

Policies:

- `MergePolicy` is the single source for Store merge ordering and Gossip digest
  comparison. `NewGossipEngine` rejects an explicitly configured policy whose
  `ID()` differs from the store policy.
- Push and pull requests and responses carry that stable policy ID. Peers that
  omit it or use a different policy are rejected before state is applied.
- Digest metadata is `map[string]json.RawMessage`, so domain adapters can add
  summaries without adding Let- or Mesh-specific fields to the Net wire type.

HTTP adapter:

- Handlers require a non-empty bearer token and `X-Outpost-Node-ID`.
- Request body size is bounded by `MaxRequestBytes` (default 1 MiB).
- Client response body size is bounded by `MaxResponseBytes` (default 1 MiB) for
  both success and error responses; oversized responses fail instead of being
  decoded as truncated JSON.
- Protocol major-version mismatch returns HTTP 426.

Boundaries:

- `DurableStore` is a state-replica durability mechanism, not a permanent event
  history or audit log. Incremental reads use current latest state by local
  position; callers that cannot validate a position must take a snapshot/resync.
- Each committed durable record is written and `fsync`ed before visibility. The
  manifest uses a temp file, rename, file sync, and best-effort directory sync.
  This distinguishes process-crash recovery from stronger storage-controller or
  full power-loss guarantees; the package does not claim protection beyond what
  the filesystem honors for synced file data and directory metadata.
- A rollback can only remove bytes the filesystem lets it truncate, and recovery
  can only repair a tail the filesystem reports. Failure modes that leave a
  truncated record *inside* confirmed history, or that lose already synced data,
  are outside what this store can detect; epoch and lineage guards for a
  directory that was deleted or replaced underneath the node are tracked
  separately.
- `CompareAndSet` and `Apply` append while holding the store's entry lock, so a
  slow `fsync` can delay concurrent readers of that store; `Put` and `Delete`
  append outside it. This lock scope is a known bound, not a correctness claim;
  ticket 03b evaluated it and left it unchanged (still open). `Compact` holds the
  commit lock and the log lock for the whole rewrite, so writers wait for it,
  while readers are blocked only while the entry map is copied.
- It does not import or adapt OutpostLet legacy wire formats; that remains ticket
  04.
