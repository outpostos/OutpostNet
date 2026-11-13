# NET-9 acceptance evidence

Issue: [OutpostNet #37](https://github.com/outpostos/OutpostNet/issues/37).
Design: [ADR-0004 D3](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0004-paged-replication-and-reliable-delivery.md).
Environment: macOS 13.7.8, amd64, Go 1.26.1; in-process transport and real files.

## Acceptance mapping

| Criterion | Status | Automated evidence in `pkg/delivery` |
| --- | --- | --- |
| 1. Red first; Send → Receive → Ack → optional HANDED_OFF | Pass | First commit `606c7fa` contains the failing flow test (`undefined: NewMemoryTransport`, `NewOutbox`, `NewInbox`, etc.). `TestSendReceiveAckHandoff`; `TestReceiptRaceAndLossSurviveRestart` |
| 2. Crash durability and incomplete tails | Pass | `TestCrashRecoveryAtSendAcceptAndAck`; `TestFailedDiskOperationsNeverAcknowledge` (short Write, failed Sync, failed rollback for Send/Accept/Ack); `TestInboxBatchUsesOneSyncAndTornTailIsNotConfirmed`; `TestProcessCrashBoundaries` (actual SIGKILL after Send, Accept, Receive and Ack) |
| 3. Deduplication and expiry + skew cleanup | Pass | `TestExpiryDedupAndCollection`: 50 repeated two-envelope batches, one delivery, duplicate after Ack, cleanup with injected clock |
| 4. Sender/arrival expiry and expiry on delivery | Pass | `TestExpiryDedupAndCollection`; `TestInboxNeverDeliversExpiredQueuedEnvelope`; `TestSlowDestinationDoesNotCauseExpiredSendToNext`; `TestFlushKeepsNewExpiredStatusWhileAnotherMessageIsLive`; `TestEnvelopeValidation` |
| 5. Outbox refusal; Inbox full → backoff → space release → success | Pass | `TestQuotaBackoffAndPayloadRelease`; `TestPriorityByteBatchesAndTransportErrors` |
| 6. At least 10k envelopes, randomized restarts and transport failures | Pass | `TestRandomCrashesAndTransportFailures10000`, deterministic seed 37: 10,000 unique messages, each Acked exactly once and ultimately HANDED_OFF; no expired delivery |
| 7. Compaction preserves statuses, pending deliveries and dedup | Pass | `TestCompactionPreservesStatusPendingDedupAndSequence`; `TestReceiptRaceAndLossSurviveRestart` also compacts a pending receipt; `TestCompactionFailuresPreserveRecoverableState` covers temp write/sync, rename, directory sync and reopen |

The random test sends 10,000 unique messages, injects disconnects and response
loss after durable acceptance, and randomly reopens both directories without
any flush/checkpoint on Close. It requires nonzero failures, lost responses and
restarts on both sides. Every message must reach HANDED_OFF and be Acked exactly
once; returned deliveries must still be valid. The consumer does not crash
between processing and Ack in this scenario. Separate crash-image, real-file
fault and SIGKILL tests cover pre-Ack redelivery and post-Ack non-redelivery.

Additional tests cover concurrent Run/Send/Receive/Ack/Close, byte-prefix ordering,
malformed response rejection, corrupt confirmed records, directory locking,
identity mismatch and source incarnation/sequence recovery. Boundary tests in
`pkg/service` now include `pkg/delivery` in the generic Net / no-Range checks.
`pkg/state` is unchanged by NET-9.

## Reproduction and gates

```sh
make fmt
git diff --exit-code
go build ./...
go vet ./...
go test -race -count=1 -timeout=30m ./...
GOOS=linux GOARCH=amd64 make build-agent
```

On macOS set `TMPDIR` to a short temporary directory first. The default macOS
temporary path plus existing agent test names exceeds the Unix-socket path
limit; two existing agent tests fail to bind with the default path and pass with
a short one. No product change is needed. The longer test timeout accommodates
10k real fsynced messages and recovery under the race detector on local storage.

Build, vet, format and Linux agent cross-build: pass. Full repository race gate:
pass (the first complete run's delivery package, including the 10k scenario,
completed in 294.7 seconds).
Targeted durability, lifecycle, concurrency, receipt, compaction and SIGKILL race
tests: pass. The additional subprocess and compaction/receipt assertions were
also run separately with `-race -count=1` after the full suite was launched.

## Boundaries

This evidence covers the library with its in-memory transport, process deaths
and injected filesystem failures. It does not claim VM/network HTTP integration,
power-loss/storage-controller guarantees or exactly-once external actions.
HTTP/peer resolution/metrics remain NET-10; the three-node 50k delivery scenario
belongs to RANGE-2. The consumer transaction example and retention/quota semantics
are documented in [delivery.md](delivery.md).
