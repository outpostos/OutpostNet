# NET-11 replication allocation evidence

Issue: outpostos/OutpostNet#44; parent outpostos/OutpostOS#6; ADR-0004 D1/D2.
Baseline: `ba847e5`, with measurement-only commit `56e8843`.
Measured locally on darwin/amd64, Go 1.26.1, Intel Core i5-7360U, GOMAXPROCS=4.
Times are diagnostic samples on a shared developer machine, not SLOs.

## Workload and reproducible commands

`BenchmarkReplicationRound` seeds two independent MemoryStores, each with 7,500
large values plus 200,000 one-byte values (207,500 keys). The local engine makes
two peer exchanges against the independent replica under separate peer addresses;
this exercises per-peer digest reconstruction without retaining three full stores.
The transport calls the real responder synchronously and retains no request history.
Both replicas already agree, isolating retained-payload overhead from necessary
transmission of differing values. HTTP buffer allocations are outside this benchmark;
the existing NET-7/NET-8 tests separately exercise bounded real HTTP transport.

```sh
# Run in the measurement-only baseline, then the implementation checkout.
go test ./pkg/state -run '^$' -bench '^BenchmarkReplicationRound$' -benchtime=1x -count=1
# Implementation samples below use 3x; bytes/op are stable across sample counts.
go test ./pkg/state -run '^$' -bench '^BenchmarkReplicationRound$' -benchtime=3x -count=1
go test ./pkg/state -run '^$' -bench '^BenchmarkPushPageTail$' -benchtime=3x -count=1
go test ./pkg/state -run '^TestReplicationMemoryCeiling$' -count=1 -v
```

Set `GOPRIVATE=github.com/outpostos GONOSUMDB=github.com/outpostos` for private
module access. Benchmarks exclude seeding and run GC before timing each dataset.
Large payloads are 28 KiB and 56 KiB: retained value bytes **per replica** are
215,240,000 and 430,280,000 respectively, including the small keys.

## Anti-entropy: allocations per two-peer round

| Per-replica values | Before bytes/op | After bytes/op | Reduction | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: |
| 215,240,000 B | 19,235,951,072 | 672,125,440 | 96.5% | 11.874 s | 1.501 s |
| 430,280,000 B | 36,019,742,856 | 670,726,869 | 98.1% | 13.951 s | 1.231 s |

Doubling retained payload increased baseline allocation by 87.2%; optimized
allocation changed by -0.2%. Remaining work scales with key metadata, digest
encoding and actual differences. It does not copy the retained payload to
produce metadata or an empty diff. Values remain immutable internally and only
returned response values are copied. Metadata returned by policies is detached
too, including policies deriving raw JSON from an entry's value.

## Push: a 4 KiB page at the beginning of the tail

| Tail entries | Before ns/op | After ns/op | Before bytes/op | After bytes/op |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 218,581 | 25,997 | 130,216 | 15,344 |
| 10,000 | 2,932,270 | 28,261 | 1,138,005 | 15,344 |
| 100,000 | 31,171,097 | 32,142 | 11,222,357 | 15,536 |
| 200,000 | 63,334,431 | 53,305 | 22,421,013 | 15,344 |

The maintained position index replaces sorting the entire tail. Reads seek in
O(log keys), snapshot at most 128 shallow references per chunk, and encode/copy
only page candidates outside the read lock. This is O(log keys + page work),
not O(tail log tail). A 200x longer tail no longer creates a proportional
allocation or sorting cost. The existing B-tree dependency becomes direct;
no new module version or durable/wire format is introduced.

## Memory regression and correctness

The original red test (`56e8843`) reports 19,332,478,680 allocated bytes per
round, failing the 1 GiB allocation guard. With a soft memory limit its sampled
peak HeapInuse is 856,014,848 bytes; the peak alone did not reject the baseline,
because GC aggressively reclaimed its tens of gigabytes of transient copies.

The optimized test also sends two push pages per peer per round, then runs
anti-entropy, for two rounds. A normal run reports:

```text
stored_values=430480000 allocation_per_round=684611152
peak_HeapInuse=773447680 ceiling=1076200000
PASS
```

The ceiling is **2.5 x the combined values in both replicas**, including both
stores, their metadata/indexes and replication garbage in the measured process.
The soft Go memory limit is 80% of that ceiling. Sampling runs every millisecond
and at the end of the workload; it measures HeapInuse, not process RSS, and can
miss shorter spikes. This is a reproducible regression guard, not a production
RSS or three-VM Mesh measurement. Both peak and allocation guards run in the
ordinary test suite, including under the race detector.

`TestReplicationReadsPreservePolicyAndOwnership` compares optimized and legacy
Store-only fallback digests/diffs with a value-dependent policy in both stores,
including CAS, tombstones, compaction/reopen, bounds, continuation and cancellation.
`TestReplicationReadsDuringMutation` covers concurrent mutation/metadata/diff/push
reads. `TestDigestPageCannotExposeUnsentValuesThroughSliceCapacity` prevents
unselected values from being exposed through the response slice's spare capacity.
Existing NET-7/NET-8 paging, 03b resurrection, GC/resync and durable fault tests are
unchanged. Consumer dependency upgrades and RANGE-2 VM acceptance remain separate.
