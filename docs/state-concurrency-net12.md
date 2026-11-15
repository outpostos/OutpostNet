# Concurrent gossip rounds (NET-12)

Issue: [OutpostNet#46](https://github.com/outpostos/OutpostNet/issues/46).
Design constraints: [ADR-0004 D1/D2](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0004-paged-replication-and-reliable-delivery.md)
and [ADR-0006 D1–D4](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md).
Baseline: `fc1fb2b`, including Net#55/#57/#58.

`GossipEngine` now explicitly guarantees overlapping push and digest rounds,
inbound handlers and status reads. Each round method is non-reentrant. `Run`
uses independent loops, one for each method, and joins both on cancellation.
Maintenance still follows pull rounds. Custom dependencies and callbacks must
support concurrent use; syncer calls must honor their context. Hosts using
manual loops must not also start `Run`.

No request, response, version, paging, merge, refusal, GC or durable-record
semantics change. State protocol remains 3.0 and durable format remains 1.
Push progress, refusal confirmation and exchange status retain their existing
locking. Backoff reflects completion order; conservative catch-up claims use
round start times and retain failures that occurred after those starts.

## Acceptance evidence

The tests use public engine/store APIs and real authenticated HTTP peers.
Transport hooks hold exchanges open or inject disconnects; channels determine
completion order. Tests never invoke the same round method reentrantly.

| Criterion / path | Automated evidence |
| --- | --- |
| Godoc contract on engine and both round methods | `pkg/state/gossip.go`; host usage in `pkg/state/README.md` |
| Push proceeds while a digest is held open | `TestConcurrentGossipPushDoesNotWaitForDigest`; `TestGossipRunPushDoesNotWaitForDigest` also proves the new periodic behavior |
| Independent periodic loops and cancellation joins | `TestGossipRunPullDoesNotWaitForPushAndJoins` holds push open, observes an inbound value through periodic pull, then holds transport cleanup until Run's join is checked |
| Concurrent push/pull, handlers, writes and push-triggered 410 resync | `TestConcurrentGossipResyncUnderWrites`: memory/durable, three HTTP peers; hold the push snapshot while pull finishes its own 410 resync, then overlap all six round methods, writes and status reads |
| Convergence, monotonic cursors and status | Resync test verifies all 48 writes on every replica and no collected-key resurrection; `progressReader` checks cursor monotonicity and refusal barriers; quiescent rounds compare payloads/identities and inbound/outbound round status |
| Failure/backoff race | `TestConcurrentGossipFailureAndBackoff`: push/pull failures, each finishing before/after the other method's success; assert retained error history, success/failure times, conservative inbound status, shared backoff skips and cursor ownership |
| Refusal sets and `peer_progress` under concurrent push/pull | `TestConcurrentGossipRefusalProgressRecovery`: two refused tombstones, partial digest confirmation racing push progress, healthy writes, GC blocked by the remaining refusal, durable compaction/reopen preserving cursor and refusal, subsequent acceptance releasing the final barrier |
| Shared time reference bookkeeping after Net#57/#58 | `TestConcurrentGossipTimeReference`: memory/durable, concurrent exchanges/handlers/writes/status; a behind UNSYNCED node learns its SYNCED peer, reports self-suspicion and converges |
| New race tests pass 20 repetitions | Focused command below |
| No protocol change; full local/CI gates | Commands below; PR records final CI checks |

Raw `OutboundCaughtUp` can remain false after resync allocates a floor with no
entry to push. Tests honor the existing distinction: completed rounds can report
`OutboundRoundCaughtUp`, while `CaughtUp` remains the conjunction of the raw
outbound and conservative inbound facts. Resync alone never advances `LastPullTime`.
Outstanding refusals suppress outbound claims and exclude the lowest refused
position from the inclusive GC prefix.

## Reproduction

```sh
go test -race -count=20 ./pkg/state -run 'TestConcurrentGossip|TestGossipRun'
make fmt
git diff --check
go build ./...
go vet ./...
GOMAXPROCS=2 TMPDIR=/tmp go test -p 1 -race -count=1 -timeout=30m ./...
GOOS=linux GOARCH=amd64 make build-agent
```

The short temporary directory avoids existing Mac Unix-socket path limits;
package serialization bounds contention in the full suite without skipping tests.

Red → green: before changing `Run`, the new held-digest test failed with
`Run push waited for held-open digest` after its two-second deadline. With
independent loops it passes. The manual overlap cases document behavior already
supported by the baseline; no extra locking or protocol changes were needed.

These are local library and HTTP tests. No VM deployment, operating-system clock
changes, power-loss validation, cross-repository dependency upgrades or Range
2+2 scenario are part of NET-12. The contract permits one push and one pull at a
time; it does not add parallel rounds of the same kind or parallelize peers
within either loop.
