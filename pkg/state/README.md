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
- Recovery restores values, tombstones, HLC state, committed change counters, and
  local positions into a fresh store instance. A backward or frozen physical
  clock after restart advances from the recovered HLC instead of reusing an old
  `ChangeID` or regressing `StateVersion`.
- Corrupt manifests, unsupported format versions, corrupt log records, and
  incomplete tail records fail explicitly. The store does not silently clear the
  directory or fall back to memory.
- `DurableStore` also persists gossip peer push progress. `GossipEngine` advances
  that progress only after a peer acknowledges the push and the local progress
  record is durably saved. If progress saving fails, the sync returns an error
  and recovery can safely replay the unconfirmed entries.

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
- It does not import or adapt OutpostLet legacy wire formats; that remains ticket
  04.
