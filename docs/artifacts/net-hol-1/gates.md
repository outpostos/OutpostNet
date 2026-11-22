# NET-HOL-1 validation

Baseline Net `0394e49`; local Mac Go 1.26.1, native Linux Go 1.26.2.
The [source hashes](source-hashes.json) match all changed Go files byte for byte
between the isolated worktree and Linux validation directory.

| Check | Result |
| --- | --- |
| Baseline red, final four-engine regression | Expected failure on memory and durable stores: [red](red.txt), reproducible with `sh docs/artifacts/net-hol-1/red.sh` |
| Final blackhole/manual-error/local-cancellation regressions | PASS, `go test -race -count=1 -v ./pkg/state -run 'TestGossipRunPushIsolatesBlackholedPeers\|TestGossipSyncOnce'`: [green](green.txt) |
| Final concurrency regressions ×20 | PASS, `go test -race -count=20 ./pkg/state -run 'TestGossipRun\|TestGossipSyncOnce\|TestConcurrentGossip\|TestPushPageBudgetGivesOtherPeersATurn'`: 87.715s, [result](repeat.txt) |
| GC resurrection/churn/partition regressions | PASS, `go test -race -count=1 ./pkg/state -run 'TestClockAhead03bResurrectionSuite\|TestHealthyStaggeredGCNeverForcesResync\|TestPartitionBeyondRetentionStillForcesResync'`: 72.650s, [result](gc.txt) |
| Mac/Linux `go build ./...`, `go vet ./...` | PASS |
| Formatting / whitespace | `make fmt` and `git diff --check` pass; committed-tree format check recorded with the PR |
| Linux real chrony/NTS CI gate | PASS, `OUTPOSTNET_CHRONY_TEST=1 go test -race -count=1 -v ./internal/chrony`: 33.119s |
| Mac full race suite | PASS, state 168.912s / delivery 217.328s: [output](mac-full-race.txt); final manual-cancellation guard and assertion also pass in the focused ×20 run |
| Linux full race suite and native agent build | PASS: [full output](linux-full-race.txt); native `make build-agent` exits 0 |
| Linux agent cross-build on Mac | PASS, `GOOS=linux GOARCH=amd64 make build-agent` |

Initial validation exposed the serial-only page-order fixture,
a non-atomic `loopNet.clockInjections` counter and a startup churn assumption
of exactly one snapshot per adopting node. The fixtures now count pages per
peer, use an atomic injection counter, and match each startup snapshot to a
zero-horizon refusal with a bound on already-started concurrent requests.
Steady healthy churn still requires **zero resyncs after warm-up**. GC
admission, cursor/refusal rules and product code outside push scheduling are
unchanged. The final canceled-manual-round assertion was also rerun explicitly.

A Mac-created tar overlay also contained AppleDouble `._*.go` metadata,
which Go itself ignores but the repository boundary scanner parses. That
Linux run passed state (107.136s) and failed the boundary scan. Metadata files
were removed only from the private validation directory before the clean
full gate; no repository boundary rule was changed.

An earlier Linux full run used the host's slow temporary storage; it was
stopped before completion. Superseded runs were stopped when final test
changes arrived. Final native validation uses private test/build temporary
files on tmpfs (`TMPDIR=/dev/shm GOTMPDIR=/dev/shm`) and
`GOMAXPROCS=2 go test -race -count=1 -timeout=30m ./...`.
Mac uses the same test flags and `TMPDIR=/tmp` for short Unix socket paths.
Real NTS uses private loopback ports, Unix sockets and chronyd `-x` virtual
clocks; it does not alter a host clock or service.

Only node1's journal was read from the VM group, through the authorized dev
host. No VM configuration, deployment, store, election, handover threshold or
referee rule was changed. Let/Mesh rollout and Range requalification remain
outside this Net PR. The PR is non-draft and must not be merged by the implementer.
