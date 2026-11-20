# Group chrony/NTS (V2-NET-17)

This implements [ADR-0006 D7](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md): chrony corrects the **system** clock before K3s/Let validate TLS. Outpost's process time reference remains a separate safety net. No Outpost process changes the system clock itself.

## Provision and render

Use chrony 4.2 or later built with `+NTS` (`chronyd -v`).
Issue each node a certificate with its actual NTS endpoint IPs and/or DNS names:

```sh
outpostnet-agent cert issue --ca credentials/group.pem --ca-key credentials/group.key \
  --node-id node-a --san 192.0.2.1 --san 2001:db8::1 --san node-a.example.net \
  --cert credentials/node-a.pem --key credentials/node-a.key
```

The node URI and both TLS usages are preserved. Existing URI-only certificates
remain usable for tunnels/state/delivery but must be reissued for NTS serving.
Keep the issuer key offline. Give the chrony service user read access to **only**
its node key (e.g. root-owned directory with a dedicated chrony group and mode
0640 key); no permissions or credentials are installed by the command.
Configure the local `node_id`, `tls.ca_file`, `tls.cert_file`, `tls.key_file` in
agent YAML. Relative credential paths follow the agent's working directory.
The renderer validates the group chain, matching node URI/key, and presence of
server SANs. Remote names must match their certificates; chrony verifies them.

The bootstrap peer file uses the same full array as Net `PUT /v1/peers`:

```json
[{"node_id":"node-b","addr":"192.0.2.2:4242"},
 {"node_id":"node-c","addr":"node-c.example.net:4242"}]
```

Publish the same peer set to the file (atomic rename) and the Net management API.
Tunnel ports are discarded for time service: all group nodes use UDP 123 and TCP
4460 by default. Select group-wide non-default ports with `--ntp-port`/`--ntske-port`
(all nodes must use the same values); the fragment then emits the matching
daemon-level `port`/`ntsport` and per-source overrides. This node is omitted; every
other node becomes an NTS `server`. All resolved IP addresses become exact `allow`
rules; no subnet or wildcard is accepted.
DNS is refreshed on every render; failure preserves the last applied config and
reports an error. **Use IP peer endpoints for boot without DNS.** An unresolved
DNS peer makes the bootstrap renderer fail and prevents chrony from starting;
the renderer does not skip peers or relax its security policy.

```sh
# Render only; no service control or clock changes.
outpostnet-agent chrony --config agent.yaml --peers peers.json
# Read the actual live Net directory instead (includes pending peers).
outpostnet-agent chrony --config agent.yaml --socket /run/outpostnet/outpostnet.sock
# Validate with chronyd -p, atomically install, restart the full chrony config.
outpostnet-agent chrony --config agent.yaml --peers peers.json \
  --output /etc/chrony/conf.d/outpostnet.conf --apply
# Continue monitoring peer data, DNS results, CA/certificate/key contents (5s).
outpostnet-agent chrony --config agent.yaml --peers peers.json \
  --output /etc/chrony/conf.d/outpostnet.conf --apply --watch
# Group-wide non-default NTP/NTS-KE ports (every node must match).
outpostnet-agent chrony --config agent.yaml --peers peers.json \
  --ntp-port 1234 --ntske-port 4461
```

`--watch` also supports `--socket` for deployments whose peer authority is the
running Net directory. The file source allows synchronization **before** Net or
K3s starts; do not make bootstrap time depend on a service waiting for time.
An unavailable socket is an error, never an empty peer set. Use `[]` explicitly
for a single node. `--interval` controls polling; `--unit chronyd.service` adapts
service naming. Watch mode retries failed updates and writes errors to stderr.
Unchanged input does not restart chrony. A rejected candidate does not replace
the installed fragment. Restart failure is reported and retried; the candidate
stays installed, so operator recovery can restart the service. Credential rotations
should atomically switch a prepared credential directory and retain the old
credentials until the update succeeds. Concurrent direct edits to credentials
are not a transaction.

## Policy and boundaries

The fragment emits `local stratum 10 orphan`, `makestep 1.0 1`, and
`nocerttimecheck 1`: one large initial correction, and certificate date relaxation
only until the first clock update. Chain and endpoint-name validation remain
required. Only the group CA is trusted (`nosystemcert`, `ntstrustedcerts 1`,
`certset 1`). `authselectmode require` excludes accidental unauthenticated NTP
sources. There is no option to add plaintext NTP sources.

`deny all` and exact peer `allow` rules restrict **both** NTP and NTS-KE. NTS
clients authenticate servers; NTS does not provide mutual node-certificate
authentication for clients. The IP access list provides the client boundary.
The fragment must be the only owner of chrony source and access policy: remove
distribution pools, DHCP `sourcedir`, `initstepslew`, other allow rules, and other
includes that could override it. The [base example](../deploy/chrony/chrony.conf)
includes only the generated fragment and a drift file. `--apply` parses the
fragment, not an arbitrary parent config; validate the full deployed parent too:

```sh
chronyd -p -f /etc/chrony/chrony.conf
chronyc sources -n
chronyc authdata -n
chronyc tracking
chronyc accheck 192.0.2.2
```

Full policy changes use `systemctl restart` because `reload sources` only reloads
source files and cannot remove old access rules or reload the full server config.
A restart begins a new initial-update window. Routine changes do not grant
unlimited steps within a running daemon. Persisted NTS cookies are deliberately
not configured; CA rotation starts fresh key establishment.

Orphan convergence provides agreement without absolute UTC. **Known limit:** if
the whole group restarts together and a wrong-clock node has the lowest orphan
reference ID, that node can become the orphan reference and the group can converge
to its wrong time. An RTC makes this unlikely; the boot gate's LOCAL dwell cannot
establish absolute UTC or prevent this election outcome. The existing
[V2-NET-1d adapter](os-time-source.md) continues reporting local/orphan tracking
as unsynchronized; followers can report synchronized chrony with an identified
upstream and bounded error. This ticket changes no quality policy.

Directive semantics: [chrony 4.2 manual](https://chrony-project.org/doc/4.2/chrony.conf.html)
and [chronyc commands](https://chrony-project.org/doc/4.2/chronyc.html).

## Optional receiver

No refclock is emitted by default. On nodes with a configured receiver:

```sh
outpostnet-agent chrony --config agent.yaml --peers peers.json --gnss-shm 0 --pps /dev/pps0
# Or gpsd SOCK; choose the socket for the installed gpsd/receiver version:
outpostnet-agent chrony --config agent.yaml --peers peers.json \
  --gnss-sock /run/chrony.clk.ttyS0.sock --pps /dev/pps0
```

Both templates mark GNSS preferred and lock optional PPS to GNSS for full seconds.
Start chrony before gpsd for SOCK. Provision device/socket access and gpsd SHM
ownership; SHM is suitable only with trusted local writers. Tune receiver latency
in deployment before using it for accuracy acceptance. These are templates, not
claims of tested GNSS hardware. GNSS can be jammed/spoofed; group NTS sources remain.

## Service ordering example

[deploy/chrony](../deploy/chrony/) supplies a bootstrap renderer, watcher, chrony
drop-in, finite [time-ready script](../deploy/chrony/outpostnet-time-ready) before `time-sync.target`, and consumer
drop-in. Adapt paths and distribution service names, install the bootstrap
peer file/credentials, create the fragment directory, then install/enable units.
Apply the consumer drop-in to **both** `k3s.service` and the deployed Let service.
Install Python 3 and the script alongside the units:

```sh
sudo install -D -m 0755 deploy/chrony/outpostnet-time-ready /usr/local/libexec/outpostnet-time-ready
```

The gate polls `chronyc -n -c tracking` every 2s for at most 720s (including
query time, measured with a monotonic clock). It passes for a synchronized
source with absolute remaining correction ≤0.5s, or a local orphan reference
(`7F7F0101`, normal leap status) observed continuously for **at least 60s** by
default (`--local-dwell`). The dwell must allow at least several iburst/NTS-KE
rounds so reachable authenticated peers can correct a wrong boot clock. Chrony
4.2–4.5 reports LOCAL/Normal immediately when unsynchronized with `local` enabled;
the gate's dwell is what prevents that first report from releasing a wrong-clock
node on chrony <4.8. The rule applies to every chrony version, independently of
chrony's own orphan activation delay. A poll showing a source clears the LOCAL
dwell and uses the normal correction rule, including waiting for a large
correction to fall to ≤0.5s. An invalid report, absent reference or failed query
also clears the dwell; separated LOCAL intervals do not accumulate.
Source authentication relies on the
rendered group-only NTS / `authselectmode require` policy above; configured local
GNSS refclocks remain usable. Do not run the gate against an unrelated chrony
configuration. Orphan readiness means local agreement, does not prove UTC
accuracy, and does not promote the orphan to `SYNCED` in Outpost's time adapter.

**Default timeout action: start consumers and report.** This implements the
architect's proposal for ADR-0006 D7, **pending LF's ruling**. A timeout prints a
warning to stderr (the service journal), explicitly reports that time readiness
is unconfirmed, and exits 0 so consumers may start. Third-party TLS can still
fail if the system clock remains wrong. To keep consumers down,
create `/etc/outpostnet/time-ready.env` containing:

```ini
OUTPOSTNET_TIME_TIMEOUT_ACTION=fail
```

`fail` reports the timeout and exits 1; the consumer drop-in's `Requires` plus
`After` then blocks K3s/Let. `start` explicitly selects the default. Invalid
options fail with exit 2. The gate uses `Wants=chrony.service` so its timeout
policy also applies when chrony fails to start. Missing/failed/hung queries and
malformed tracking never count as readiness. `After=time-sync.target` alone
does not wait for a correction. Inspect `journalctl -u outpostnet-time-ready`;
a successful unit can mean either readiness or the reported timeout policy.
The same environment file can configure the minimum continuous LOCAL dwell in
seconds, for example (60s is also the default):

```ini
OUTPOSTNET_TIME_LOCAL_DWELL=60
```

Choose a dwell long enough for several iburst/NTS-KE rounds on the deployment's
links. It must fit within the gate's timeout for LOCAL to pass; it does not extend
the timeout or change the configured timeout action. Dwell values must be finite
and greater than zero.
For manual checks, the script supports `--timeout`, `--interval`, `--local-dwell`,
`--on-timeout start|fail`, `--chronyc`, and a local command `--socket`.
After changing policy, use `systemctl daemon-reload` and restart the gate before
retrying blocked consumers; it is a oneshot with `RemainAfterExit=yes`.

This startup gate does not stop workloads on later source loss. At runtime the
existing time-reference quality/holdover reporting continues. Do not tie the
chrony unit to an agent that itself waits for K3s. No units are installed or
services restarted just by building or running render-only mode.

## Verification

```sh
TMPDIR=/tmp go test -race -count=1 ./cmd/outpostnet-agent/app ./internal/chrony ./pkg/tunnel/tlsutil
# Linux, chrony +NTS installed; high loopback ports, private files, -x virtual clocks:
OUTPOSTNET_CHRONY_TEST=1 go test -race -count=1 -v ./internal/chrony
```

Golden tests cover orphan/NTS/CA/access/step/date policy, both GNSS templates and
non-default NTP/NTS-KE ports.
CLI tests cover SAN issuance and inspect, key mismatch, live peer API, watch
replacement, unchanged input, certificate-content changes, non-default port flags
and parse/restart failure retry. The Linux test parses generated configs and runs
three isolated chronyd processes: two under the group CA and a foreign-CA server. It asserts a
group NTS source is selected with cookies despite a future notBefore, foreign key establishment gets no
cookies, non-peer access is denied, and the unchanged time-source parser accepts
actual follower tracking. Test processes use a private executable copy to keep
distribution AppArmor path rules from restricting temporary test files; the
installed daemon/profile remain untouched. CI installs chrony and runs this test.
The same three-process fixture checks that the orphan leader passes as
`local orphan reference` after a shortened test dwell, while the NTS follower
passes as `synchronized source` with the default dwell longer than its timeout.
It then disables the foreign process's local reference and
checks that it waits until the bound and fails. `TestTimeReady*` exercises both
timeout actions (including the default), correction/leap boundaries, invalid
reports/options and missing, failed or hung queries through the script CLI.
Scripted tracking tests cover LOCAL followed by large then small source correction
(only the last passes as `synchronized source`), sustained LOCAL past its dwell,
and interruption/reset of the continuous LOCAL interval.

The process fixture shortens polling and, where supported, orphan activation
delay; it parses the original generated policy unchanged first. Production keeps
the distribution's orphan activation default.

The tests never change the host clock or VM services. Wrong-clock K3s boot,
VM certificate removal, and four-node orphan convergence (ADR acceptance 11–14)
remain the V2-RANGE-2 continuation.
