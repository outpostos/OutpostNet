# Durable delivery (`pkg/delivery`, NET-9)

Design: [ADR-0004 D3](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0004-paged-replication-and-reliable-delivery.md).

This is an embeddable library for direct peer delivery, separate from `pkg/state`.
It stores every accepted envelope rather than folding intermediate values into
the latest state. It has no Mesh, Let, Service, Range or HTTP dependency. The host
supplies an authenticated `Transport`. `MemoryTransport` connects real durable
boxes in one process for tests; HTTP, peer resolution and metrics belong to NET-10.

## Contract

`Send(ctx, Envelope)` assigns `MessageID{SourceNodeID, SourceIncarnation, Seq}`
and the payload's SHA-256 `Checksum`. It returns an ID only after the entire
envelope is appended and synced. The incarnation is a random 128-bit value
persisted when the directory is initialized. Reopening preserves it and the
sequence, including after compaction or collection. A fresh directory gets a
new incarnation. Never clone an active source directory into another writer.

Envelopes carry destination node identity, logical mailbox, category, P0–P3
priority, optional business idempotency key and correlation ID, required expiry,
opaque payload, checksum and optional `WantHandoffReceipt`. Destination, mailbox
and category must be nonempty. The checksum covers the payload; it is not a
signature. Transport authentication must protect metadata and bind a receipt's
`ReceiverNodeID` to the authenticated peer.

`Inbox.Accept` returns one result per input, in input order:

| Result | Meaning |
| --- | --- |
| `accepted` | Envelope, dedup identity and pending delivery are committed together |
| `duplicate` | The ID already exists; no second delivery is queued |
| `rejected_expired` | Arrival is at or after expiry plus allowed clock skew |
| `rejected_invalid` | Invalid fields, destination, checksum or envelope size |
| `full` | No admission; retry after backoff |

All new accepted entries of a batch use one newline-terminated log record and
one fsync. If persistence fails, the method returns an error and no result list;
the sender must infer no acknowledgement. Result lengths, IDs, ordering and
codes are validated before the Outbox releases any payload. A response lost
after acceptance causes a safe resend with the same ID.

The Outbox transitions from `PENDING` to `DELIVERED` after durable acceptance,
`REJECTED` after invalid rejection, or `EXPIRED` at local expiry or expired
rejection. Payloads are released on those transitions; status metadata remains
until collection. `Status(id)` returns the persisted state or `ErrNotFound` after
collection. `DELIVERED` means durable reception, not consumer processing.

`Receive(ctx, mailbox)` returns a copied envelope, including expiry, and leases
it within this process. Concurrent receivers cannot lease the same entry.
`Nack(id)` releases a lease; restart releases all leases. After its own durable
transaction completes, the consumer calls `Ack(ctx, id)`. Ack durably removes
the payload while retaining dedup metadata. A crash before Ack redelivers; a
crash after successful Ack does not. Ack is idempotent while metadata exists.

With `WantHandoffReceipt`, Ack also atomically queues a durable receipt. Inbox
`Flush`/`Run` sends receipts back through the same `Transport`; Outbox persists
`HANDED_OFF`. Receipt response loss and restarts are safe. A receipt may arrive
before the original acceptance response; the later response cannot regress
`HANDED_OFF`. Receipts expire with the retained metadata and do not prove that
an external device acted successfully.

## Host lifecycle and limits

Use separate directories for each Inbox and Outbox. Constructors finish recovery
before returning. A nonblocking directory lock excludes concurrent processes;
node/kind/version mismatch, a missing manifest or log, and corrupt confirmed
records refuse startup. No fallback to memory occurs. A crash during first
initialization can leave a manifest without a log; no message was accepted, and
the host must replace that incomplete directory rather than reuse its identity.

Run each box's `Run(ctx, reportError)` in a host-owned goroutine. Cancel and join
it during shutdown, then `Close`. Errors are reported and retried; a nil reporter
logs them. Fatal write errors and close terminate Run. Alternatively call
`Flush(ctx)` and `Collect(ctx)` explicitly. Status queries alone do not perform
expiry maintenance. Close wakes blocked Receive calls.

Each Flush makes at most one byte-limited batch per destination, in deterministic
destination order. Within each destination it takes a prefix sorted by priority
(P0 first), then source sequence. It checks expiry again before sending each
peer's batch. Failures and `full` responses cause per-peer exponential backoff;
other peers can still advance. Retry timing is in memory and resets on restart.
Transport implementations must honor context cancellation and bound their own
network calls. Run polls at `RetryMin`; it has no hidden goroutine of its own.

| Config field | Default | Meaning |
| --- | --- | --- |
| `QuotaBytes` | 64 MiB per box | Live encoded entries, including retained metadata |
| `MaxEnvelopeBytes` | 512 KiB | JSON-encoded envelope, including base64 payload and metadata |
| `MaxBatchBytes` | 768 KiB | JSON-encoded Batch; must be at least `MaxEnvelopeBytes + 32` |
| `MaxExpiry` | 72 hours | Maximum future expiry accepted by Send |
| `ClockSkew` | 0 | Nonnegative arrival tolerance and metadata retention extension |
| `RetryMin` / `RetryMax` | 100 ms / 30 s | Per-destination exponential backoff bounds |
| `Clock` | `time.Now` | Injectable clock, safe for concurrent calls |

Quota charges reserve enough metadata for Ack/receipt transitions even with
empty payloads. Outbox fullness returns `ErrOutboxFull` without accepting or
allocating a committed sequence. Inbox fullness returns `full` without writing
that entry; other entries in the batch can still be accepted. Releasing payloads
frees space, but retained status/dedup records also consume quota. Budget storage
for the retention window and ingress rate, and retry rejected admission explicitly.

Receive never returns an envelope at or beyond its actual expiry, even during
the clock skew tolerance window. Consumers must check expiry and execution
conditions again immediately before acting. Collect discards expired Inbox
payloads and keeps dedup until expiry plus skew. It marks pending Outbox entries
EXPIRED, retaining newly expired statuses for that maintenance pass, then removes
terminal records at expiry plus skew on later maintenance. Dedup guarantees assume
the configured clock skew bound; rolling clocks outside it can violate the window.

## Persistence and failure boundary

Each directory holds `manifest.json`, `delivery.log` and `box.lock`. Commits
append one JSON transaction plus newline and fsync before publishing changes.
A partial append or failed sync is rolled back to the previous committed offset.
If rollback fails, the box returns `ErrUnwritable` and refuses further writes.
That failed operation is ambiguous: a complete record might survive recovery;
the caller must use business idempotency when retrying an operation with no ID.

Recovery truncates an unterminated final record, including otherwise valid JSON
without its newline. Terminated corruption, bad checksums on pending payloads,
invalid status/identity and regressed counters refuse opening. Filesystem fault
tests use real Write/Sync/Truncate wrappers, matching `pkg/state`'s approach.

`Compact(ctx)` writes an equivalent snapshot to a temp file, syncs it, renames it
over the log, syncs the directory and reopens the append handle. A failure before
rename preserves the old writable log; a failure after rename fails closed.
Uninstalled compaction temp files are discarded on open. Hosts should schedule
Collect and Compact to reclaim physical disk: live-entry quota does not bound
append history, and compaction needs temporary space for a second live snapshot.
Compaction holds the commit lock, so reads and writes wait during the rewrite.

Guarantees depend on filesystem fsync and atomic rename behavior. They cover
process crashes, not storage that loses synced data, directory replacement,
restoring stale backups, or power-loss behavior beyond filesystem guarantees.

## Consumer idempotency

The consumer's processed-ID record and business changes must commit in its own
transaction. This library cannot atomically commit a separate business database.
For example (pseudocode; keep processed IDs for your business retry horizon):

```go
delivery, err := inbox.Receive(ctx, "commands")
if err != nil { return err }
if !time.Now().Before(delivery.ExpiryTime) {
    return inbox.Ack(ctx, delivery.MessageID) // discard stale work explicitly
}
err = database.Transaction(func(tx Tx) error {
    key := delivery.MessageID // full tuple; optionally use a business idempotency key
    if tx.AlreadyProcessed(key) { return nil }
    if err := tx.ApplyBusinessChange(delivery.Payload); err != nil { return err }
    return tx.RecordProcessed(key)
})
if err != nil {
    _ = inbox.Nack(delivery.MessageID)
    return err
}
return inbox.Ack(ctx, delivery.MessageID)
```

Delivery is at least once, with bounded deduplication. There is no global order,
cross-envelope total order, exactly-once external action, business execution
receipt, multi-hop store-and-forward, priority bandwidth reservation, or runtime
HTTP integration in NET-9. Connected relay transport may carry a direct-peer
exchange; the library itself never forwards via another node's durable queue.

Acceptance evidence and reproducible commands: [NET-9 test report](delivery-test-report.md).
