# Independent timer-driven peer pushes (NET-HOL-1)

Issue: [OutpostLet#130](https://github.com/outpostos/OutpostLet/issues/130).
Baseline: Net `0394e49`, Let `76d5a29`.
Constraints: architecture v0.2 P1/D4/D10, ADR-0004 D1/D2, ADR-0006
D3/D4 and ADR-0012 D6. The ticket explicitly authorizes this Net change.

## Diagnosis: confirmed push head-of-line blocking

At the baseline, `SyncOnce` calls `pushTo` sequentially for each peer. `Run`
waits for that whole method before reading another push tick. Even if the
reachable peer is first in the list, a write committed after its visit waits
for the remaining peers' requests before another round can visit it.
Backoff cannot skip a request already in flight. Its existing deadline is
anchored to round start, so a 10s request may also consume its backoff period;
NET-HOL-1 preserves that behavior.

Let's production `assembleMetadataReplication` passes a nil client to
`metadata.NewWireSyncer`, which creates `http.Client{Timeout: 10*time.Second}`.
`httpauth.Auth.Client` copies it and clones `http.DefaultTransport` for
certificate authentication without changing the timeout. The underlying
transport's TCP dial timeout is **30s**, TLS handshake timeout **10s**. The
**effective request deadline is 10s**, including connection establishment,
TLS and response reading (unless an earlier caller deadline cancels it).
Net's own default HTTPSyncer uses the same 10s client default. No timeout is
changed by this fix. Reproduce the transport defaults with
`go doc net/http.DefaultTransport`; inspect Net `pkg/state/httpauth/auth.go`
and Let `metadata_assembly.go` / `pkg/services/metadata/legacy_wire.go` at the
pinned baselines.

[Range PR #85](https://github.com/outpostos/OutpostRange/pull/85), fetched
read-only at `091ff779544f4bd27bba117837e37a7af619548b`, supplies the sealed
D10 evidence. Its archive SHA-256 is
`7a8474ece210bbdd8281b01eea78a288078b28c8816905c8653bd00a6c603060`.
[Extracted writer visits](artifacts/net-hol-1/writer-push-visits.jsonl) retain
the original time, monotonic clock and start position, add UTC, and replace
peer addresses with product node IDs. Product node0 is physical node2.

| UTC on 2026-11-22 | Writer node1 push visit | Start position |
| --- | --- | ---: |
| 17:55:19.932635 | reachable node0 | 918978 |
| 17:55:19.936079 | unreachable node3 | 918978 |
| 17:55:23.899 | epoch 27 committed locally | 918980 |
| 17:55:29.936866 | unreachable node4 | 918985 |
| 17:55:39.937121 | reachable node0 again | 918990 |

Read-only node1 journal queries independently show metadata push
`Client.Timeout exceeded while awaiting headers` for node3 at
**17:55:29.936421085Z**, then node4 at **17:55:39.936834715Z**.
[Filtered journal evidence](artifacts/net-hol-1/node1-metadata-timeouts.jsonl)
contains only metadata push failures with peer addresses replaced by node IDs.
The read command was `ssh e5box` followed by
`ssh ubuntu@192.168.122.119 'sudo journalctl --since "2026-11-22 17:55:00 UTC" --until "2026-11-22 17:55:45 UTC" --no-pager -o cat --grep="state gossip"'`;
only JSON metadata `operation=push` records were retained. Both observed waits
are 10s. The push path to the uncut peer is therefore
blocked by the other peers, including across subsequent ticks.

The archive reports 15.176s of disagreement and 40ms of local adoption work.
Node0's observed adoption precedes its first covering traced push; these
records cannot identify its first causal delivery among concurrent pull/relay
and push. The confirmed finding is the blocked push path, not an assignment
of that first delivery to a particular recorded exchange. No referee rule,
election logic or T_handover is changed.

## Implementation and preserved semantics

- On each existing push tick, `Run` claims idle peers outside their existing
  shared push/pull backoff and starts their push rounds concurrently. A busy
  peer skips ticks; its completion does not trigger another round. Claims
  cover every page, resync, progress checkpoint and round-status update.
- A peer has at most one unfinished push round. A stalled peer holds only its
  own slot, so healthy peers can push new writes on subsequent ticks. Work is
  bounded by the number of peer addresses; no goroutine is queued per busy tick.
- `SyncOnce` also fans out, then joins all started exchanges and aggregates
  their errors. A local page/refusal-read failure cancels its other exchanges
  and joins them before returning the local error. Manual round calls remain
  non-reentrant. Hosts with serial manual loops still wait at the call boundary;
  production Let uses `Run`.
- `Run` joins all in-flight work on cancellation. The independent pull loop,
  maintenance, default/configured intervals, page cap, backoff calculation,
  cursor ownership, durable acknowledgement/refusal checkpoint, GC/vouching,
  and caught-up target semantics retain their existing rules. The state
  protocol stays 3.0 and durable format stays 1.

## Regression and validation

`TestGossipRunPushIsolatesBlackholedPeers` uses **four real engines**, real
memory/durable stores and authenticated HTTP adapters. All four replicas
receive a baseline write. Two peer servers then accept connections and hold
pushes without returning responses; the healthy peer is deliberately first
in the list. Pull is set to one hour so it cannot mask a broken push path.
After a blackhole is entered, three successive writer writes must each reach
the healthy store within **one configured 200ms interval + 100ms processing
allowance**. Both blackholes must have exactly one outstanding request across
those ticks, no premature failure and no cursor movement past the baseline;
the healthy durable acknowledgement must advance independently.

This catches both sequential peer pushes and concurrent fanout followed by a
global barrier between periodic rounds. Timer durations are scaled for a fast
local regression; no product interval or handover threshold is changed.

[Red output](artifacts/net-hol-1/red.txt) records the final regression against
the baseline engine using Go's source overlay. Both stores fail on the first
post-blackhole write. [Green output](artifacts/net-hol-1/green.txt) includes
actual delivery timings and these additional checks:

| Requirement | Evidence |
| --- | --- |
| Healthy peer progresses during manual `SyncOnce` | `TestGossipSyncOncePeersProgressIndependently`: two held failures precede the healthy peer; its durable cursor advances before either failure completes |
| Join/error aggregation/backoff/no overlap | Same test: duplicate peer address, both errors returned, failed cursors stay at zero, next round skips failures and advances healthy peer |
| Local-store failure cancels and joins transport cleanup | `TestGossipSyncOnceLocalFailureCancelsAndJoins` |
| Per-peer page cap/caught-up semantics | `TestPushPageBudgetGivesOtherPeersATurn`: concurrent-safe per-peer visit counts replace the old serial-order assumption |
| GC fixture concurrency | `loopNet` clock-injection count is atomic; the startup churn harness matches every resync to an actual zero-horizon refusal and bounds concurrent first-adoption requests, retaining zero resyncs after warm-up |
| Push/pull overlap, failure ordering, 410/resync, refusal/GC recovery, clocks, shutdown joins | Existing `TestConcurrentGossip*` and `TestGossipRun*` regressions, plus full race suite |

Reproduction (Go, org module access; use a short temporary directory on Mac):

```sh
# Expected failure on the baseline, without modifying the checkout:
sh docs/artifacts/net-hol-1/red.sh
# Current implementation:
go test -race -count=1 -v ./pkg/state -run 'TestGossipRunPushIsolatesBlackholedPeers|TestGossipSyncOnce'
go test -race -count=20 ./pkg/state -run 'TestGossipRun|TestGossipSyncOnce|TestConcurrentGossip|TestPushPageBudgetGivesOtherPeersATurn'
make fmt
git diff --exit-code
go build ./...
go vet ./...
GOMAXPROCS=2 TMPDIR=/tmp go test -p 1 -race -count=1 -timeout=30m ./...
GOOS=linux GOARCH=amd64 make build-agent
# Linux with chrony installed, private loopback test daemons using -x only:
OUTPOSTNET_CHRONY_TEST=1 go test -race -count=1 -v ./internal/chrony
```

Validation results are recorded in [gates](artifacts/net-hol-1/gates.md).
VM access was read-only. No deployments, VM configuration changes, dependency
pin upgrades in Let/Mesh, Range qualification rerun, D10 acceptance claim or
referee changes are included. The original duplicate observation remains a
separate architect/referee question. Concurrent pushes can both receive a 410 for horizon zero before either
startup resync completes; this permits extra already-started first-adoption
snapshots. Every request/response and GC admission rule is unchanged. The
GC harness now verifies their cause and bounded startup count, while requiring
zero resyncs after warm-up. Review and merge precede consumer rollout.
