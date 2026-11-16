# V2-NET-17 validation

Scope: [Net#76](https://github.com/outpostos/OutpostNet/issues/76),
[ADR-0006 D7](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md).
The final branch uses certificate SAN issuance from merged [PR #77](https://github.com/outpostos/OutpostNet/pull/77),
with additional CLI IPv6/identity regression coverage. Renderer/CLI/CI originated
in `b46bf4b`; subsequent commits record portability fixes and the main merge.
No VM configuration, service or system clock was changed. Node1 was queried only
with read-only `chronyc -v`, `tracking`, and `sources -n` (client version 4.5).
Native Linux process evidence uses chrony 4.2 built with `+NTS` on the development
host; it is not evidence that node1 has NTS configured.

## Acceptance mapping

| Criterion / scope | Evidence |
| --- | --- |
| 1: orphan, NTS, group CA, access, makestep, initial cert-time relaxation | `TestRenderGroupNTSGolden`: literal `group.conf` |
| 1: optional gpsd SHM/SOCK with PPS | Same golden test: `shm-pps.conf`, `sock-pps.conf`; both also parsed by real `chronyd -p` |
| 1: SANs alongside existing URI; compatibility | `TestCertificateCLIWithNTSSANs`: IPv4/IPv6/DNS hostname checks, URI extraction, existing identity loader, inspect output, bad SAN rejection; existing certificate tests unchanged |
| 1: real parser | `TestChronyRealNTSGroupAndForeignCA`: actual generated group and GNSS configs parsed; CI's explicit chrony installation and opt-in Linux step |
| 2: local multiprocess NTS | Same real test: group source selected, NTS authdata cookies present; foreign-CA source attempted but has zero cookies; exact access rules checked with `chronyc accheck` |
| D7 date boundary | Real server certificate starts one year in the future; initial NTS still succeeds with `nocerttimecheck 1`, without changing any clock |
| Re-render/reload on changes | `TestChronyCLIWatchAppliesPeerAndCertificateChangesAndRetries`: peer replacement removes old access, same-path credential content change triggers apply, unchanged input stays quiet, rejected parser candidate preserves installed config, restart failure retries |
| Net peer-set input | `TestChronyCLIRendersLiveNetPeerSet`: real Unix HTTP `/v1/peers`, pending peer included, failed API rejected; file mode uses the same Net peer array |
| Unsafe configuration | `TestRenderRejectsUnsafeInput`, `TestRenderOnlyGroupSourcesAndDeterministic`, `TestChronyCLIRenderAndRejectInvalidCredentials` |
| Service ordering | `deploy/chrony/` render/watch units, chrony dependency drop-in, time-ready gate before `time-sync.target`, required K3s/Let consumer drop-in; `systemd-analyze verify` on temporary copies passed (only the example agent executable substituted with `/usr/bin/true`) |
| Existing time-source adapter | Real follower `tracking` passes unchanged `state.ParseChronyTracking`; local orphan leader remains unsynchronized; full existing source/host tests run in the race suite |
| 3: gates and HTTPS | Commands and results below; final GitHub checks linked from the PR |

## Red then green

The SAN CLI regression was added before implementing SAN support, then adapted
to the merged `--san` interface from PR #77. The renderer golden
suite first failed because the package had no implementation. Render/watch CLI
tests first failed with `unknown command "chrony"`; the live-directory slice first
failed with `unknown flag: --socket`. Each passed after its implementation.

Real-process fixture failures exposed distribution AppArmor restrictions on the
installed executable's temporary-file access, chrony's strict command-socket
directory permissions, and its default root startup check. The fixture now uses
a private executable copy, mode 0700 private directories, the current user, and
`-U -x` with high loopback ports. It passes as both root and an unprivileged user.
No installed binary, AppArmor policy, service, or system clock is changed.

CI's chrony 4.8 introduced a 300s default orphan activation delay. The initial
30s process test correctly observed authenticated but unsynchronized responses.
The fixture now parses the original generated policy first and then shortens only
its orphan delay (if supported), like its port/poll overrides. Production retains
chrony's default; the startup example allows 720s before failing. The adjusted
fixture passes on both 4.2 and locally built 4.8 without installing the 4.8 build.

An initial Linux archive included Apple resource-fork files (`._*.go`); existing
source-boundary tests rejected them. Generated metadata was removed only from the
isolated validation copy, and the full suite was rerun from a Python tarfile
archive without Mac metadata. These files were never in the branch.

## Results

After integrating main, the combined branch passed native Linux build, vet,
module tidiness, complete race tests with real NTS enabled, and the agent build
(delivery 139.369s, state 126.500s, tunnel 14.936s). Mac build/vet, cross-build and
focused race tests also passed after the merge. The earlier complete Mac/Linux
runs below preceded main's SAN/reconnect changes; an additional complete Mac run
is recorded in the PR. The complete Mac race suite passed, including delivery
182.533s, state 182.974s, and tunnel 15.177s. The complete native Linux race suite
(with the real NTS test enabled) passed: delivery 138.029s, state 127.656s, and
tunnel 15.435s. No tests were excluded. Build, vet, module tidiness, formatting,
Linux agent cross-build/native build, and unit verification passed.

One additional focused Mac watcher run timed out waiting for the first apply;
the complete Mac/Linux runs and five repeated focused race runs passed. A test-only
follow-up captures command termination/output after joining the watcher, so any
future timeout has diagnostic evidence. The follow-up passed focused race checks.

## Reproduction

From the worktree root on Mac:

```sh
go build ./...
go vet ./...
go mod tidy -diff
make fmt
git diff --check
TMPDIR=/tmp go test -p 1 -race -count=1 ./...
GOOS=linux GOARCH=amd64 make build-agent
```

On Linux with chrony +NTS installed:

```sh
go build ./...
go vet ./...
go mod tidy -diff
GOMAXPROCS=2 GOTMPDIR=/dev/shm TMPDIR=/tmp OUTPOSTNET_CHRONY_TEST=1 \
  go test -race -count=1 -timeout=30m ./...
make build-agent
OUTPOSTNET_CHRONY_TEST=1 go test -race -count=1 -v ./internal/chrony
```

No Mesh contract or generated files change. VM wrong-clock K3s boot, certificate
removal/rotation faults, and multi-node wall-clock agreement (ADR acceptance 11–14)
remain V2-RANGE-2 work. GNSS templates are parsed, without claiming receiver or
PPS hardware acceptance. NTS agreement without an external source does not prove
absolute UTC accuracy.
