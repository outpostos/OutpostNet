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

Push paging and write limits (NET-7):

- `GossipConfig.MaxPushBytes` defaults to 768 KiB of encoded JSON request,
  including its envelope. Push walks local positions in order, acknowledging
  and saving progress after each page. `MaxPushPagesPerRound` defaults to 16
  per peer; remaining pages wait for the next round so other peers get a turn.
  A transport or progress-save failure resumes from the last saved position.
  A transient rejection stops at the confirmed prefix; `clock_offset` refusals
  are skipped and tracked separately for GC/vouching (ADR-0006 D3/D4). A 410 or a
  `beyond_gc_horizon` rejection still triggers snapshot resync.
- A capped round only reports `OutboundRoundCaughtUp` when acknowledgements
  cover the position read at its start (or there is nothing left to replicate).
  Writes arriving during the round do not extend that target.
- Both `MemoryStoreConfig.MaxEntryBytes` and `DurableStoreConfig.MaxEntryBytes`
  default to 512 KiB. This bounds the **complete JSON encoding of an Entry**:
  base64 value, escaped key, writer/change identity and version metadata count;
  the node-local position does not. `Put` and successful-condition
  `CompareAndSet` reject larger entries with `errors.Is(err, ErrEntryTooLarge)`
  before persistence, visibility or notification. Tombstone mutations use the
  same limit. Zero selects the default; negative limits are invalid.
- `Apply` and durable recovery retain larger existing/remote entries for
  compatibility. Each such entry is sent alone, even if it exceeds
  `MaxPushBytes`. A warning and the per-peer `OversizedPushEntries` counter
  report each attempted send (including retries); the counter resets with the
  engine. If a legacy entry also exceeds the peer's HTTP request limit, that
  page fails visibly and cannot advance until the operator raises the limit.
- Construction requires `MaxPushBytes >= MaxEntryBytes + envelope overhead`
  (including the largest GC horizon encoding) and strictly less than the HTTP
  request limit. `HTTPSyncerConfig.MaxRequestBytes` declares that peer limit
  (default 1 MiB); `NewGossipEngine` checks it, and `NewHTTPHandler` checks its
  own `MaxRequestBytes` against the engine. Configure the sender for the
  smallest peer limit; limits are not negotiated on the wire. Other Syncers
  can expose `MaxRequestBytes() int64`, otherwise the 1 MiB default is assumed.
- Both stores implement the optional `BytePageStore.ChangesSinceBytes` seam.
  It seeks a maintained position index and copies only page payloads;
  page reads do not sort or materialize the remaining tail. Existing custom
  `Store` implementations remain compatible and are read one entry at a time.
  Wrappers should forward `BytePageStore` and `MaxEntryBytes() int64` when they
  change the store's default limit. Byte budgets count encoded entries plus
  commas, excluding the array; a first oversized entry is returned alone.

Digest anti-entropy paging (NET-8, ADR-0004 D2):

- `GossipConfig.MaxDigestBytes` defaults to 768 KiB of **complete encoded
  request**, including the range, digest, horizon and continuation envelope.
  It must be positive and strictly below both the syncer's declared peer
  request limit and the local handler's limit. Zero selects the default.
- Each peer receives a fresh, complete local digest every round, sorted into
  contiguous `[start_key, end_key)` intervals. `start_key` is explicitly
  present (a Go `*string`); the first is the empty string. Empty `end_key`
  means no upper bound. An empty requester sends one unbounded digest interval,
  which still runs the GC handshake; it is never mistaken for a snapshot.
- Every interval includes all local keys in its bounds, including tombstones.
  Responders check only those bounds for collected values (410) and diff,
  returning keys the requester lacks as well as newer entries. The GC check
  repeats over the entire interval on each continuation, including keys before
  the diff cursor. Sampling, truncation and spreading coverage across rounds
  are forbidden; the proof is in [tombstone GC](../../docs/state-tombstone-gc.md).
- Diff pages are sorted by key, capped at 256 entries by default and at
  `PullRequest.MaxBytes` (default 1 MiB). `HTTPSyncer.PullSnapshot` caps that
  budget at `MaxResponseBytes`, including JSON escaping, the `more` /
  `next_key` envelope and the HTTP newline. The requester repeats the complete
  interval digest with exclusive `after_key = next_key` until `more` is false,
  then advances to the next digest interval. `HTTPSyncer.Pull` drains digest
  continuations for callers using its entries-only convenience API.
- A failed transfer, invalid continuation, apply error or 410/resync abandons
  the round; the next attempt starts from the first interval with a fresh
  digest. Already applied pages are ordinary merges; `LastPullTime` advances
  only after every interval finishes. Clock-ahead entries are per-entry
  refusals and do not fail the round or back off their forwarding peer.
  Resync alone never counts as a pull.
- A single digest record/envelope that cannot fit fails explicitly with
  `DigestEntryTooLargeError`; a diff entry that cannot fit fails with
  `PullEntryTooLargeError`. No oversized request is sent. A remote continuation
  key larger than the reserved local key budget can also cause an explicit
  digest size failure; raise the digest and peer limits in that case.
- Custom Syncers used for anti-entropy must implement `SnapshotSyncer` to return
  paging metadata for both digest and snapshot pulls. Missing support fails
  visibly. The built-in stores use the optional `ReplicationReadStore` described below.
  Custom stores without it retain the compatible `ListEntries` fallback.

Replication memory and ordered reads (NET-11):

- Both stores implement optional `ReplicationReadStore`: `DigestRange` returns
  owned metadata in key order within `[start, end)` (empty end is unbounded),
  `HasKey` checks presence without copying a value, and `DigestDiffPage` selects
  differences with the store policy and copies only values fitting the encoded
  response. Existing `Store` implementations remain source-compatible. Wrappers
  should forward this interface to keep the optimization.
- Key and position B-tree indexes contain only key/position references. Every
  committed Put, CAS, Apply, resync batch, recovery and purge updates both indexes
  under the entry lock; failed persistence never publishes index changes.
  Updating an entry costs O(log key count); each push page seeks in O(log key
  count) and visits only the page candidates it encodes.
- Anti-entropy takes a complete ordered metadata snapshot per peer, then splits
  it into the same NET-8 complete intervals. It never copies stored values to
  build a digest. Responders snapshot shallow immutable references only inside
  the requested interval; policy comparison and encoding run after unlocking,
  and only transmitted values are copied. No per-page full-store sorting occurs.
  Policy callbacks must treat entries as immutable, as for merge comparison.
  Returned metadata (including raw JSON) and response values belong to callers.
- Per-round allocation is O(key metadata + encoded digest requests + transmitted
  differences), rather than O(retained value bytes times digest/diff pages).
  Metadata snapshots are still O(key count), and a full-space diff may scan all
  keys; this change does not promise constant-memory digests or Merkle trees.
  Large policy metadata and actual differing payloads still cost their encoded
  size. Custom stores using the fallback retain their previous memory costs.
- `TestReplicationMemoryCeiling` exercises two replicas with 7,500 x 28 KiB values
  plus 200,000 small keys, two peer exchanges per round and push pages. With a
  soft Go memory limit it asserts sampled peak `HeapInuse` <= 2.5 times the
  combined retained value bytes (1,076,200,000 bytes) and allocation <= 1.5 GiB
  per round. Benchmarks compare 28/56 KiB values and 1k–200k push tails; see
  [NET-11 measurements](../../docs/replication-memory-net11.md) for commands,
  before/after results and measurement limits.

**Protocol upgrade:** NET-8 raises `StateProtocolVersion` from 1.0 to **2.0**.
New/old majors reject each other (HTTP 426 for requests; response validation
also rejects an old peer). Mixed clusters are unsupported. Stop all hosts
embedding `pkg/state`, upgrade their pinned Net dependency and restart the
whole keyspace together (including Mesh, Let and Range test consumers where
applicable). Durable log format is unchanged. Consumer dependency upgrades and
three-VM RANGE-2 acceptance are separate tickets.

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
  (that uses the non-refused acknowledgement prefix), and `CaughtUp` remains
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

Clock refusal safety (V2-NET-1a, ADR-0006 D3/D4):

- All three incoming paths (push, digest, resync snapshot) count and skip entries
  beyond the five-minute HLC safety valve. Snapshot keys are held before apply;
  accepted entries still commit as one atomic batch. Within-valve ahead entries
  remain eligible and are marked. No forwarding peer backoff is charged for an
  entry refusal. Missing/newer keys are reoffered by every anti-entropy round.
- Push cursors pass clock refusals. Per-peer refused-position sets persist with
  durable progress and survive compaction. GC and vouching use the prefix before
  the earliest unresolved refusal; accepting that write or a replacement releases
  it. `ReplicationStatus.Peers` reports the raw `AcknowledgedPosition`,
  `RefusedPositions`, `LowestRefusedPosition` and safe `GCConfirmedPosition`.
  Outstanding refusals suppress outbound caught-up/round caught-up claims.
- Every incoming peer GC horizon is guarded against the same physical reference
  as HLC admission, including requests, responses, 410s and all snapshot pages;
  a future horizon is never used to purge or raise HLC. Collection also excludes
  tombstones beyond that reference plus the safety valve.
- `ReplicationStatus.Clocks`, store/engine `ClockStatus()` and warnings expose
  `clock_suspect`: per-writer refusal/ahead counts and per-peer horizon refusal
  counts. They are process-local observations, reset on recreation. Hosts map
  these facts into their observation stream; this change adds no Range dependency.
- Peer-derived time references/quality and the separate retention clock are
  V2-NET-1b; local future-HLC cleanup is V2-NET-1c. Protocol 2.0 is unchanged in
  this ticket. See [GC compatibility and acceptance evidence](../../docs/state-tombstone-gc.md#adr-0006-d3d4-clock-refusal-compatibility-v2-net-1a).
