# Durable delivery (`pkg/delivery`)

Design: [ADR-0004 D3](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0004-paged-replication-and-reliable-delivery.md).

This is an embeddable library for direct peer delivery, separate from `pkg/state`.
It stores every accepted envelope rather than folding intermediate values into
the latest state. It has no Mesh, Let, Service or Range dependency. The host supplies a
`Transport`: `MemoryTransport` connects durable boxes for tests; `HTTPTransport`
uses the host's existing replication membership and identity mapping.

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
receipt, multi-hop store-and-forward, priority bandwidth reservation, or automatic product
wiring. Connected relay transport may carry a direct-peer
exchange; the library itself never forwards via another node's durable queue.

Acceptance evidence and reproducible commands: [NET-9 test report](delivery-test-report.md).

## Embedding in a gateway process (NET-10)

Reuse the authenticated state replication listener, `state.PeerProvider`, and
its address-to-node identity mapping. `HTTPTransport` scans the current provider
on each attempt; it does not discover nodes or add routes. Ambiguous mappings
fail visibly. Unknown nodes return `ErrUnknownPeer`, increment the Outbox's
unknown-peer and failure counters, and stay PENDING with exponential backoff.
Address changes take effect at the next attempt. Supply concurrency-safe provider
and identity callbacks; update them together from the host's existing directory.

The host constructs the following alongside its existing state handler (error
checks shown; `dataDir`, `nodeID`, `token`, `peers`, `peerIdentity`, and `mux` are
host-owned configuration and dependencies):

```go
wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{
    NodeID: nodeID, AuthToken: token,
    PeerProvider: peers, PeerIdentity: peerIdentity,
    MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
})
if err != nil { return err }
outbox, err := delivery.NewOutbox(delivery.Config{
    Dir: filepath.Join(dataDir, "delivery", "outbox"), NodeID: nodeID,
    Transport: wire,
})
if err != nil { return err }
inbox, err := delivery.NewInbox(delivery.Config{
    Dir: filepath.Join(dataDir, "delivery", "inbox"), NodeID: nodeID,
    Transport: wire,
})
if err != nil { _ = outbox.Close(); return err }
handler, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{
    NodeID: nodeID, AuthToken: token, Inbox: inbox, Outbox: outbox,
    MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
})
if err != nil { _ = inbox.Close(); _ = outbox.Close(); return err }
mux.Handle(delivery.BatchPath, handler) // alongside /state/v1/push and /state/v1/pull
```

Use persistent, separate Inbox/Outbox directories; do not put these under a
per-process temporary directory in production. Run both `Run(ctx, reportError)`
loops in host-owned goroutines. On shutdown stop HTTP admission, cancel and join
both loops, then close both boxes. Report errors through the host logger; repeated
unknown/unreachable destinations are observable failures, never successful
admission or message deletion. The HTTP client defaults to a 10-second timeout
(and adds that default to a supplied client with no timeout), honors context
cancellation, and refuses redirects. The host owns HTTP server timeouts and TLS.

`go test -run '^ExampleHTTPTransport$' ./pkg/delivery` executes the complete
bidirectional example: two independent durable nodes, HTTP acceptance,
consumer Receive/Ack and receipts through the same endpoint. This is an embedding
example; Mesh configuration and runtime wiring are a separate ticket.

### HTTP wire and security boundary

`POST /delivery/v1/batch` takes JSON:

```json
{"protocol_version":{"major":1,"minor":0},"source":"node-a","envelopes":[],"receipts":[]}
```

Envelope/receipt schemas are the public `Batch` types. Responses carry
`protocol_version`, receiver `node_id`, and ordered `envelopes` / `receipts`
results from `BatchResult`. Delivery protocol **1.0** is independent of state
protocol 2.0. A mismatched request major returns 426; an incompatible response,
wrong receiver node, malformed JSON, result ID/order/length or code rejects the
whole acknowledgement. No payload is released on transport failure.

Authentication matches the state HTTP boundary: required Bearer token,
`X-Outpost-Node-ID` equal to request `source`, no self requests. Every envelope's
source and every receipt's `receiver_node_id` must match that source. These
checks run over the entire batch before any persistence. Wrong tokens return
401; identity mismatches return 403. A handler can expose only Inbox or Outbox;
a request needing an absent box returns 503. Persistence failures return 500
with a generic message, never an acknowledgement. A mixed batch may commit one
box before the other fails; resending is safe because both paths deduplicate.

As with state replication, a shared token authenticates membership, and node
identity is self-declared: any token holder can claim a different node ID. This
does not implement per-node credentials or cross-group authorization. Reuse the
host's TLS/protected listener; plain HTTP does not provide confidentiality.
Errors never include remote response bodies, bearer tokens or complete peer
URLs. HTTP client errors use the same credential redaction utility as state.
The library adds no per-request logger; default Run logging receives those
sanitized errors.

### Byte budgets

Both HTTP configurations default request and response caps to 1 MiB. These
include the protocol/source/receiver wrapper. The transport splits a Batch into
pages bounded by both the exact encoded request and the largest legal response;
this also pages receipt batches. `MaxBatchBytes` continues to bound one box's
Flush batch, independently of the HTTP caps. Choose HTTP caps large enough for
`MaxEnvelopeBytes` plus the encoded wrapper (including escaped node IDs), and
for at least one result. There is no limit negotiation; declare the smallest
peer caps on the transport. A single item that cannot fit fails visibly and
stays PENDING. Oversized envelopes are refused earlier by Send.

The handler reads at most its request cap before returning 413, including
chunked bodies and trailing whitespace. Before persistence it also reserves
the worst-case result size; an unrepresentable response returns 413. The client
bounds all response bodies. If a later page fails, the box retains the entire
Flush batch; retries of earlier committed pages return duplicate. HTTP success
means durable acceptance only; Ack and HANDED_OFF retain their separate meaning.

### Local observation

`inbox.DeliveryStatus()` / `outbox.DeliveryStatus()` return copied, sorted
per-peer facts without network calls, maintenance or credentials. Outbox peers
are destinations; Inbox peers are envelope sources (receipt destinations).
Pending counts/bytes include encoded envelopes, with separate receipt counts
and bytes. Queue age uses persisted enqueue time, survives recovery and
compaction, and clamps backward clock jumps to zero. Pre-NET-10 records lack an
enqueue timestamp: their age is unknown (zero timestamp), not reconstructed
from expiry. Gauges reflect stored records until Collect/Flush maintenance.

Counters reset on reopen. Send success/failure counts actual Flush batch
attempts, not skipped backoff polls; `full` counts as a failed attempt. Unknown
peer counts resolution failures. Receiver counters count duplicate, expired,
invalid and full admission results, including retries; Outbox also counts receipt
admission results and committed rejections. Local committed expiry
also increments expiry. Outbox full counts Send refusals. Counters do not claim
business execution or network-wide delivery. See [observation.md](observation.md)
for mapping these facts to the existing Range MetricSample event stream.
