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
  included, refuses an absent key at or below the horizon. A peer whose digest
  still holds a value collected here, or whose horizon is lower and that this
  node cannot vouch for (it did not acknowledge every tombstone removed here,
  is below a horizon adopted from another peer, or has horizon zero while this
  node removed something), is refused with
  `ResyncRequiredError` (HTTP 410) and resyncs from a snapshot. A lower horizon
  alone, as during staggered collection in a healthy cluster, is not refused.
  Resync commits each snapshot page as one durable record (one fsync per page).
  A consumer position below the GC floor reports `NeedsSnapshot`.
- `DurableStore.Compact` rewrites the log as a snapshot through a synced temp
  file, rename and directory sync; `CompactionMinRecords` (default 0 = off)
  enables `CompactIfNeeded`. A directory sync failure after rename makes the
  store fail-closed.

Replication status (`GossipEngine.ReplicationStatus`):

- One `PeerReplicationStatus` per peer the `PeerProvider` currently returns:
  `AcknowledgedPosition` (persisted `PeerProgressStore` value when available),
  `OutboundCaughtUp` (`AcknowledgedPosition >= Local.CurrentPosition`, with the
  local position read first so the claim can only be conservative),
  `OutboundRoundCaughtUp` and `OutboundRoundTime` (see below),
  `LastPullTime` (start of the latest fully applied pull), `InboundCaughtUp`
  (a pull was applied and no exchange failed after it started), `CaughtUp`,
  `LastSuccessTime`, `LastError`/`LastErrorTime`/`LastErrorOperation` (kept
  after recovery), `ConsecutiveFailures` and `NextAttemptTime` (backoff).
- `OutboundCaughtUp` is an instantaneous "nothing left to push" fact. The
  local position also grows with every applied peer entry (Lamport merge), and
  the acknowledged position only moves on this node's push rounds, so under
  sustained writes anywhere in the connected set it is structurally false on a
  healthy cluster. `OutboundRoundCaughtUp` judges push rounds instead: each
  round reads the local position before collecting its changes and is caught
  up when the peer acknowledged every change up to that position. A round with
  nothing past the acknowledged position counts too (the peer holds everything
  this store can push, even when a resync purge left the local position on a
  floor no entry carries). It holds when `OutboundCaughtUp` holds, or when the
  latest push round was caught up and no exchange with the peer failed after
  that round started; a round skipped by backoff is not a round.
  `OutboundRoundTime` is the start of the latest caught-up round, kept after
  later failures: its age shows how fresh the claim is and, while the flag is
  false, since when the peer has been behind. Neither field gates tombstone GC
  (that still uses the acknowledged position only), and `CaughtUp` remains
  `OutboundCaughtUp && InboundCaughtUp`.
- Reading the push-round fact: with no local writes a partitioned peer has
  nothing to be pushed, so `OutboundCaughtUp` and therefore
  `OutboundRoundCaughtUp` stay true; the flag only turns false for a
  partitioned peer once there are local changes it has not acknowledged.
  Reachability is `InboundCaughtUp` and `LastError`/`LastErrorTime`, not this
  flag. Under sustained writes anywhere in the connected set `CaughtUp` is
  structurally false for the same reason as `OutboundCaughtUp`; to establish
  "every peer holds this node's writes", stop local writes and wait for
  `OutboundRoundCaughtUp` with an `OutboundRoundTime` after that point on
  every peer.
- A round that skips a peer in backoff is not a pull. A snapshot resync clears
  backoff but does not move `LastPullTime`; the next pull does.
- These are local facts of this process: times are not persisted, so a fresh
  process reports no pull yet (`InboundCaughtUp=false`) while the persisted
  acknowledged position survives. Same semantics as OutpostMesh
  `GetReplicationStatus` (ticket 06); hosts map it to their own reporting
  (for example OutpostRange `ReplicationProgress`) instead of wrapping the
  Syncer to re-derive it. Mapped today by OutpostLet (`outpostlet.metadata`)
  and OutpostMesh (`outpostmesh.entities`), including the push-round fields
  as Range `outbound_round_caught_up` / `outbound_round_time`; see
  `docs/observation.md` section 4. OutpostNet's own processes do not run a
  `GossipEngine` and report no replication status.
- Credentials never appear: every pkg/state log line (debug through error) and
  every error returned by `SyncOnce`, `AntiEntropyOnce`, `ReplicationStatus` or
  `HTTPSyncer` names a peer by its address with the password masked
  (`user:password@host` and `scheme://user:password@host` alike). Per-peer
  errors from any `Syncer` have the password removed even if the Syncer quotes
  the raw address (the original error stays reachable through `errors.As`), and
  `HTTPSyncer` replaces its bearer token and the URL password with `[REDACTED]`
  even if a peer echoes them in an error body. The helpers live in
  `redact.go`.

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
- `protocol_version` is the object `{"major":M,"minor":N}`; a body that does
  not decode (for example a bare integer) is a 400, and a major-version
  mismatch returns HTTP 426.

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
- It does not import or adapt OutpostLet legacy wire formats. OutpostLet's
  `/metadata/v1` (protocol v3) adapter lives in OutpostLet
  (`pkg/services/metadata/legacy_wire.go`, ticket 04) together with its removal
  condition; this package only speaks `/state/v1`.
