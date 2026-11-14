# NET-13 delivery throughput evidence

Issue: [OutpostNet #53](https://github.com/outpostos/OutpostNet/issues/53).
Design: [ADR-0004 D3](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0004-paged-replication-and-reliable-delivery.md).

## Change and durability boundary

Ready Inbox envelopes use a balanced tree per mailbox, ordered by priority and
source sequence. A subtree's latest expiry lets Receive skip expired prefixes
in O(log n), including after restart. Leasing removes the entry; Nack inserts
it again. Ack, expiry collection and replay update the indexes alongside the
committed entries. Acked records remain available for dedup without participating
in Receive. Outbox send queues, pending receipts and expiry maintenance have
separate indexes, so Flush does not scan retained records.

The size-bounded page builder encodes each selected item once and accounts for
JSON brackets and commas. It preserves one prefix per destination per Flush,
byte caps, deterministic destination order and retry/backpressure semantics.

Concurrent Ack calls share transactions of at most 256 requests. Every caller
waits for the fsync outcome. There is no batching timer and no new persistent
format: records remain version-1 transactions understood by existing recovery.
The commit mutex still covers append, fsync and publication, preserving rollback
and compaction ordering; Ack work per transaction is bounded by group size.
Send still syncs before returning, and Inbox Accept already uses one fsync for
all newly accepted entries in a batch. The VM producer is sequential and also
syncs its own successful-Send evidence; Send group commit would not remove that
serial workload. Its measured admission rate is sufficient for this scenario.

## Regression and profile evidence

The first commit, `b853a9a`, records the failing tests before implementation:
Receive read 5,000 / 50,000 queued entries on the first call, and 64 concurrent
Acks used 64 fsyncs. The initial CPU profile identified `ordered` (scan/sort)
as a major cost (38.08% cumulative samples, including fixture construction).
A subsequent expired-prefix regression failed with 12,800,720 allocated bytes
for a single Receive before the balanced mailbox index was added.

Mac amd64, Go 1.26.1; identical 50k pending + 50k retained-Ack fixture:

| Receive + Nack benchmark | Before | Final |
| --- | ---: | ---: |
| Time per operation | 63,878,443 ns | 2,595 ns |
| Allocated bytes per operation | 69,534,580 | 440 |
| Allocations per operation | 32 | 3 |

Full Receive+Ack drains under `-race` (real journal writes, fsync omitted only
in this algorithm scaling test):

| Pending + retained Acked | Envelopes/s | Allocated bytes/envelope |
| --- | ---: | ---: |
| 5,000 + 5,000 | 503 | 7,194 |
| 50,000 + 50,000 | 466 | 7,038 |

Per-envelope time at 50k is **1.08x** the 5k cost, below 3x. Machine-independent
CI guards enforce clock-read work and a 64 KiB/envelope allocation budget.
The expired-prefix test bounds one Receive to 64 KiB even with 50k expired
entries before the live entry. Full-page Inbox/Outbox Flush tests include 50k
retained terminal records and enforce a 128 MiB allocation cap (including the
race detector's JSON/pool overhead), exact wire byte limits and a filled page.
The original quadratic builders exceed this budget by orders of magnitude.

Real-file durability tests separately retain fsync. Concurrent-Ack tests block
a real fsync and require no early success, shared syncs, and no redelivery after
reopening. `TestConcurrentAcksCrashMidGroup` SIGKILLs a subprocess during a
63-Ack transaction, once after a torn append and once after fsync but before
publication/return. Recovery redelivers every torn-group entry, and none of the
synced group. Existing short-write, failed-sync, failed-rollback, compaction,
expiry, quota, HTTP, 10k random-restart stress and SIGKILL suites remain enabled.
Mixed priority/mailbox, Nack, compaction and restart ordering is also covered.

```sh
go test -race -count=1 -run 'TestInboxDrainWorkBudget|TestReceiveSkipsExpired|TestFlushAllocation|TestConcurrentAcks|TestReadyOrder' -v ./pkg/delivery
go test -run '^$' -bench BenchmarkReceiveBacklog -benchtime=2s ./pkg/delivery
go test -run '^$' -bench BenchmarkReceiveBacklog -benchtime=2s -cpuprofile=before.cpu ./pkg/delivery
go tool pprof -top before.cpu
```

For the baseline profile, use the first test commit; for final numbers, use the
implementation commit. Profiles include constructor/fixture CPU, while benchmark
timings exclude setup. The scaling test isolates algorithm growth from storage
latency; the VM trial below includes all real library and consumer fsyncs.

## VM reproduction

Use committed Range tooling from `outpostos/OutpostRange@1275b0c`:
`tools/phase2/delivery.py`, `judge.py`, and `delivery-echo/main.go`. Build the
unchanged main file from the Net worktree, resolving Net imports directly from
that module; no local `replace` directive or consumer dependency change is used:

```sh
git -C "$RANGE" show 1275b0c:tools/phase2/delivery-echo/main.go > "$TRIAL/main.go"
GOOS=linux GOARCH=amd64 go build -o "$TRIAL/delivery-echo" "$TRIAL/main.go"
```

On e5box copy the operator/judge, binary and the wrapper below into a fresh trial
directory. The existing Range management network must reach the three reference
VMs and their product stack must be active. Keep token files on the VMs. The
operator verifies sender/receiver hostnames before changing anything. This run
only changes the store and executable locations to preserve historical trials:

```python
import pathlib, delivery
delivery.BASE = '/var/lib/range2/net13-delivery-final'
delivery.BIN = '/opt/range1/bin/net13-delivery-echo'
delivery.run(pathlib.Path('evidence'), pathlib.Path('delivery-echo'))
```

The workload retains seed 46, 50k envelopes, 256-byte payloads, 5% 30-second
expiry / remaining one-hour expiry, 256 KiB initial inbox then 512 MiB, all
successful-Send evidence before sender crash, sender/receiver SIGKILL, five
consumer SIGKILLs, a delivery-port-only partition and the unchanged 30-minute
drain deadline. The consumer durably records processed IDs before Ack. The
small consumer delays during crash injection and the zero-delay final consumer
are the committed Range settings. Product Net/Mesh/Let services and range-agent
continue running throughout; node3 supplies the existing logical-service path.

## Final VM result (implementation `134d4e4`)

**Pass.** The committed Range judge accepts the complete, retained per-message
records. [Summary](artifacts/net13-delivery/summary.json),
[verdict](artifacts/net13-delivery/verdict.json), and
[hash manifest](artifacts/net13-delivery/manifest.json) accompany gzip-compressed
successful-Send, consumer, sender-status and fault JSONL in
[`artifacts/net13-delivery/`](artifacts/net13-delivery/).

- 50,000 successful Sends; all **47,500 unexpired Sends** processed and Acked,
  with sender state HANDED_OFF. No missing unexpired IDs.
- Zero expired deliveries; 47,502 processing records contain exactly two
  duplicates, both accepted by the judge's recorded crash-window checks.
- 47,500 HANDED_OFF / 2,483 EXPIRED / 17 DELIVERED. The 17 are short-expiry
  messages durably received before expiry, then expired without handoff;
  DELIVERED correctly describes durable reception rather than processing.
- One sender SIGKILL, one receiver SIGKILL, five consumer SIGKILLs; observed
  inbox-full count 361 with 1,205 sender-pending envelopes. The scoped partition
  lasted 38.307 seconds, including remote command completion.
- Final capture and judgment completed **220.252 seconds after partition
  clearance**, versus the unchanged 1,800-second budget and 900-second target.
  The entire trial took 375.448 seconds, including initial Send admission and
  faults. Consumer handoff averaged **179.57 unique envelopes/s** across its
  264.519-second interval, including crash delays and the partition.

Replay exact hashes and the unchanged Range judge locally:

```sh
PYTHONPATH="$RANGE/tools/phase2" python3 docs/artifacts/net13-delivery/replay.py
```

Only outpost-node1 (.119), outpost-node2 (.155) and outpost-node3 (.218) were
accessed. Existing product services and agents stayed active. Test units were
stopped, the dedicated installed test binary removed, and the scoped firewall
rule cleared; normal qdiscs and all five service states are recorded in
[restoration.json](artifacts/net13-delivery/restoration.json).
A fresh 120-second logical-service probe passes **600/600** connections, all
through node3: [probe summary](artifacts/net13-delivery/restoration-summary.json)
and [individual connections](artifacts/net13-delivery/restoration-clients.jsonl).
Both exploratory and final delivery stores remain on the VMs for inspection;
historical RANGE-2 stores were not changed. The final binary hash is in the
summary. Existing Mesh/Let dependency pins are unchanged: their upgrade and the
complete RANGE-2 run-9 acceptance remain separate work.

## Acceptance mapping and gates

| NET-13 criterion | Evidence |
| --- | --- |
| 1. 50k pending + 50k retained; cost within 3x; CI budget; envelopes/s | Scaling table above: 1.08x per-envelope cost; clock/allocation budgets in `TestInboxDrainWorkBudget`, expired-prefix and Flush allocation regressions |
| 2. Existing durability/crash/edge/stress race suites, concurrent group crash | Full `pkg/delivery` race suite passes; `TestConcurrentAcksShareDurableCommit`, `TestConcurrentAcksCrashMidGroup`; existing actual SIGKILL, faulted-file and 10k-restart suites enabled |
| 3. VM 50k Range tooling, all faults, timely unexpired handoff, no expired delivery, restore | Complete raw records, hashes, passing replay above; 220.252 s after partition, 179.57 handoffs/s; service restoration evidence |
| 4. Local/CI gates and docs | Commands below; `delivery.md` documents indexes, batching, lock and durability boundary; hosted checks attached to the PR |

```sh
make fmt
git diff --exit-code
go build ./...
make vet
TMPDIR=/tmp GOFLAGS=-p=1 go test -race -count=1 -timeout=30m ./...
GOOS=linux GOARCH=amd64 make build-agent
```

Mac build, vet, format and Linux cross-build pass. The complete Mac race run
passes delivery (222.741 s), state (158.250 s) and every other package except
an existing QUIC loopback handshake timeout in `TestMultiplexNoHOLBlocking`.
The unchanged tunnel package then passes its complete race rerun (14.970 s).
Linux validation runs the CI-equivalent commands on the same implementation,
including the entire repository race suite; its result is recorded with the PR.
No tunnel code or test deadlines were changed.
