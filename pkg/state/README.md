# OutpostNet state primitives

`pkg/state` is the ticket 02 in-memory state replication slice. It is generic:
entries carry opaque bytes and tombstones, not Let PodBinding, Mesh Entity/Task,
Kubernetes, or tunnel-specific types.

Public consumer surface:

- `MemoryStore.Put`, `Delete`, `Get`, `ListEntries`, and `Observe` for local state.
- `MemoryStore.CompareAndSet` for atomic conditional mutation by `EntryIdentity`.
- `GossipEngine.SyncOnce`, `AntiEntropyOnce`, and `Run` for peer exchange.
- `HTTPSyncer` and `NewHTTPHandler` for authenticated JSON/HTTP transport.

Identity model:

- `ChangeID` identifies the original write and is preserved when forwarded.
- `StateVersion` orders conflicting states through HLC plus writer identity.
- `Entry.Position` is a local consumption cursor and is not serialized on the wire.

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

- This package intentionally keeps state in memory. Disk persistence and
  cold-start recovery are ticket 03.
- It does not import or adapt OutpostLet legacy wire formats; that remains ticket
  04.
