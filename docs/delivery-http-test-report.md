# NET-10 acceptance evidence

Issue: [OutpostNet #40](https://github.com/outpostos/OutpostNet/issues/40).
Design: [ADR-0004 D3](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0004-paged-replication-and-reliable-delivery.md).
Base: `0ba33d6` (merged NET-9); macOS amd64, Go 1.26.1.
Tests use real HTTP connections and durable files, with faults at the HTTP boundary.

| Criterion | Evidence |
| --- | --- |
| 1. Failure first; HTTP Send → durable receive → Receive → Ack → HANDED_OFF | First commit `3e6d315` contains `TestHTTPDurableHandoff`, run before implementation: undefined transport/handler APIs. The same race test passes with the implementation. `ExampleHTTPTransport` executes both directions between two independent nodes, including receipts. |
| 2. Wrong token → 401; source mismatch rejected; sender stays PENDING | `TestHTTPAuthenticationKeepsPending` checks actual 401/403 responses and an empty receiver. `TestHTTPIdentityProtocolAndResponseValidation` also rejects forged envelope sources and receipt receivers before persistence, request protocol mismatch, bad response node/version/IDs/codes and missing results. |
| 3. Byte paging; oversized Send refused; receiver 413 leaves sender PENDING | `TestHTTPWireBytePaging` sends eight envelopes with a 1,200-byte request / 600-byte response cap, measures every actual body and verifies every ID and handoff. `TestHTTPReceiverByteLimitKeepsPending` checks actual 413 and Send's `ErrEnvelopeTooLarge`. `TestHTTPResponseBudgetRefusesBeforePersistence` verifies receiver response-cap refusal leaves its Inbox empty. |
| 4. Connection interruption / response lost after durable commit; retry duplicate; one consumer delivery | `TestHTTPConnectionAndResponseLoss` hijacks and closes real HTTP connections before or after invoking the durable handler. Both leave PENDING, retry to DELIVERED, and produce one consumer delivery. The committed-response-loss case closes/reopens the receiver's directory before retry and checks its duplicate counter is exactly one. |
| 5. Unknown/unreachable destination, visible counters, backoff, recovery | `TestHTTPUnknownPeerBackoffRecoveryAndObservation` uses the real PeerProvider/identity seam, an injected clock and an unreachable HTTP address. It checks queue count/bytes/age, one unknown count, no new attempt during backoff, two total failures, eventual delivery and zero pending. |
| 6. No token in logs or observation | `TestHTTPNoCredentialOrPeerURLInErrorsOrLogs` captures slog, returned errors and status for remote credential echo, client errors, invalid JSON and redirects. Tokens, URL passwords and full URLs must be absent; redirect targets receive zero calls. `internal/observation.TestDeliveryMetricSamplesUseLocalFacts` maps real pending/failed box status into existing MetricSample events, checks values/labels and secret-free proto JSON. Existing state redaction regressions cover the shared utility. |
| Local CI, race, docs and PR mapping | Commands below; `delivery.md` documents mounting/lifecycle/configuration, wire protocol, security boundary, limits, status and failure semantics. `observation.md` documents the existing event-stream mapping. The PR repeats this acceptance mapping. |

Additional admission tests exercise ordered accepted / duplicate / invalid /
expired / full results and recovery after quota release. Status tests verify
Outbox-full counts, expiry counts and queue age across compaction/reopen, with
process counters reset. Existing durability, concurrency, SIGKILL and random
10k-message tests continue to cover the core library and its new timestamp
metadata; no persistence is replaced with a mock.

## Red → green and reproduction

Initial red command:

```sh
go test -race -count=1 -run '^TestHTTPDurableHandoff$' ./pkg/delivery
```

It failed with undefined `HTTPTransport`, `NewHTTPTransport`, `HTTPHandlerConfig`
and `NewHTTPHandler`. The subsequent byte-paging test failed with
`delivery HTTP request exceeds byte limit` before wire paging was implemented.
Status and observation tests were run red with missing `DeliveryStatus` and
`UpdateDelivery` APIs before those implementations.

Focused final checks:

```sh
go test -race -count=1 -run 'TestHTTP|TestDeliveryStatus|ExampleHTTPTransport' ./pkg/delivery
go test -race -count=1 -run TestDeliveryMetricSamplesUseLocalFacts ./internal/observation
```

Repository gates:

```sh
make fmt
git diff --exit-code
go build ./...
make vet
TMPDIR=/tmp GOFLAGS=-p=1 go test -race -count=1 -timeout=30m ./...
GOOS=linux GOARCH=amd64 make build-agent
```

The short macOS temporary directory avoids existing Unix-socket pathname limits.
Package serialization avoids local QUIC handshake contention while retaining
all tests and their internal concurrency. The longer timeout accommodates the
existing fsynced 10k-message scenario under the race detector. A separate full
delivery race run passed in 301 seconds.

## Scope and limits

Delivery 1.0 is independent of the state protocol. Shared-token authentication
and self-declared node identity retain the state transport's security boundary;
this does not introduce per-node credentials or automatic TLS. Hosts reuse their
protected replication listener and own TLS and server timeouts.

Queue enqueue time is additive durable metadata, preserved through compaction;
old NET-9 records without it report unknown age. Gauges derive from storage and
counters reset on reopen. MetricSample uses the existing event timeline; the
contract has no generic metric snapshot field. Agent/service do not construct
boxes and therefore emit no delivery facts by themselves. Embedding hosts own
runtime wiring and their observation sampling tick.

No Mesh/Let dependency updates, gateway deployment, multi-hop forwarding,
priority bandwidth scheduling or three-VM 50k RANGE-2 acceptance is included.
No VMs were modified. The PR is for review and is not merged by the implementer.
