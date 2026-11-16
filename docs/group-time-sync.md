# Group chrony/NTS (V2-NET-17)

This implements [ADR-0006 D7](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md): chrony corrects the **system** clock before K3s/Let validate TLS. Outpost's process time reference remains a separate safety net. No Outpost process changes the system clock itself.

## Provision and render

Use chrony 4.2 or later built with `+NTS` (`chronyd -v`).
Issue each node a certificate with its actual NTS endpoint IPs and/or DNS names:

```sh
outpostnet-agent cert issue --ca credentials/group.pem --ca-key credentials/group.key \
  --node-id node-a --ip 192.0.2.1 --ip 2001:db8::1 --dns node-a.example.net \
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
4460. This node is omitted; every other node becomes an NTS `server`. All resolved
IP addresses become exact `allow` rules; no subnet or wildcard is accepted.
DNS is refreshed on every render; failure preserves the last applied config and
reports an error. Prefer IP endpoints for disconnected bootstrap without DNS.

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

Orphan convergence provides agreement without absolute UTC. The existing
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
drop-in, finite `chronyc waitsync` gate before `time-sync.target`, and consumer
drop-in. Adapt paths and distribution service names, install the bootstrap
peer file/credentials, create the fragment directory, then install/enable units.
Apply the consumer drop-in to **both** `k3s.service` and the deployed Let service.
`Requires` plus `After` on `outpostnet-time-ready.service` prevents TLS consumers
from starting when synchronization times out. `After=time-sync.target` alone
does not wait for a correction. The wait checks remaining correction ≤0.5s and
fails after about 720s (allowing newer chrony's 300s orphan activation delay); it does not prove UTC accuracy in orphan mode.

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

Golden tests cover orphan/NTS/CA/access/step/date policy and both GNSS templates.
CLI tests cover SAN issuance and inspect, key mismatch, live peer API, watch
replacement, unchanged input, certificate-content changes and parse/restart
failure retry. The Linux test parses generated configs and runs three isolated
chronyd processes: two under the group CA and a foreign-CA server. It asserts a
group NTS source is selected with cookies despite a future notBefore, foreign key establishment gets no
cookies, non-peer access is denied, and the unchanged time-source parser accepts
actual follower tracking. Test processes use a private executable copy to keep
distribution AppArmor path rules from restricting temporary test files; the
installed daemon/profile remain untouched. CI installs chrony and runs this test.

The process fixture shortens polling and, where supported, orphan activation
delay; it parses the original generated policy unchanged first. Production keeps
the distribution's orphan activation default.

The tests never change the host clock or VM services. Wrong-clock K3s boot,
VM certificate removal, and four-node orphan convergence (ADR acceptance 11–14)
remain the V2-RANGE-2 continuation.
