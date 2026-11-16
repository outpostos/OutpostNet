# Group time sync: chrony + NTS (V2-NET-17, ADR-0006 D7)

Design: [ADR-0006 D7](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0006-time-base.md).
The Outpost time reference (D1–D6) only covers Outpost code. Third-party
components such as K3s and TLS read the operating-system clock, so a node that
boots a year behind cannot validate certificates even though Outpost itself has
a correct reference. ADR-0006 D7 fixes the OS clock in three layers:

1. GNSS where a receiver exists (optional refclock template below);
2. chrony + NTS inside the group, in orphan mode, configured from the Net peer
   set and the node certificate;
3. the existing Outpost reference as a safety net (V2-NET-1d still reads chrony
   state; see [os-time-source.md](os-time-source.md)).

`outpostnet-agent chrony render` renders layer 2 and the optional layer-1
template. It never writes unauthenticated NTP sources unless asked.

## Provision the certificate with SANs

NTS clients verify the server certificate by the address they dial, so the node
certificate must carry that IP address or DNS name in addition to the usual
`urn:outpostos:node:<node_id>` URI:

```sh
outpostnet-agent cert issue --ca group.pem --ca-key group.key \
  --node-id node-a --san node-a.edge.local --san 10.0.0.5 \
  --cert node-a.pem --key node-a.key
```

Certificates without SANs stay valid for tunnels; they just cannot be verified
as NTS servers. `cert inspect` prints `dns=`/`ip=` when SANs are present. See
[node-certificates.md](node-certificates.md).

## Render the fragment

```sh
outpostnet-agent chrony render \
  --node-id node-a --cert node-a.pem --key node-a.key --ca group.pem \
  --allow 10.0.0.0/24 \
  --output /etc/chrony/conf.d/outpostnet.conf
```

`--peer NODE_ID=HOST[:PORT]` adds a group peer explicitly. To read the peer set
from a running agent instead, point at its management socket; the `addr` host is
used and the QUIC port is ignored (NTP uses `--ntp-port`, default 123):

```sh
outpostnet-agent chrony render ... \
  --peers-socket /run/outpostnet/outpostnet.sock \
  --allow 10.0.0.0/24 --output /etc/chrony/conf.d/outpostnet.conf
```

`GET /v1/peers` returns `{"node_id":"…","addr":"host:port"}`. The local node is
skipped. Explicit `--peer` entries win over the directory on `node_id`.

### Split sources for reload without restart

Pass `--sources-output /etc/chrony/sources.d/outpostnet.sources` to write the
`server … nts` lines to a chrony `*.sources` file and emit `sourcedir` in the
main fragment. `chronyc reload sources` then applies peer changes without
restarting chronyd. Certificate changes still require a restart because chrony
loads the NTS server certificate only once.

```sh
outpostnet-agent chrony render ... \
  --output /etc/chrony/conf.d/outpostnet.conf \
  --sources-output /etc/chrony/sources.d/outpostnet.sources
chronyc reload sources
```

With split sources, keep access rules static: pass `--allow` with the group
subnet. Otherwise an added peer also rewrites the main fragment (the renderer
warns when a peer relies only on a derived per-address `allow`).

## What the fragment contains

| Directive | Purpose |
| --- | --- |
| `ntsservercert` / `ntsserverkey` / `ntsport` | This node serves NTS-KE on 4460/tcp with its group certificate. |
| `ntsdumpdir` | Persist NTS server keys so a restart does not invalidate cookies. |
| `nosystemcert` + `ntstrustedcerts 1 <CA>` | Trust only the group CA for NTS peers. |
| `server <peer> nts iburst certset 1` | Poll every group peer over NTS with the group CA set. |
| `local stratum 10 orphan` | Converge inside the group when no external source exists. |
| `makestep 1.0 3` | Step a badly wrong boot clock in the first three updates. |
| `nocerttimecheck 1` | Skip NTS certificate date checks until the first clock update, matching ADR-0008 (validity is not a security boundary). |
| `allow <peer/subnet>` | Serve NTP only to group peers and explicit operator networks. No `allow` means chrony serves nobody. |
| `refclock SHM/SOCK` + `refclock PPS` | Optional GNSS template; rendered only when requested. |

`certset 1` references the group CA set, so no other CA can authenticate a time
source. There is no pool or plain `server` line. A host-level
`server`/`pool`/`allow 0/0` in `/etc/chrony/chrony.conf` still applies, so remove
those from the node's main configuration when deploying this fragment.

### GNSS template

Only for nodes with a receiver. gpsd feeds chrony through SHM or its Unix
socket; PPS locks a kernel pulse source to it:

```sh
# SHM + PPS
--gnss-shm 0 --gnss-pps /dev/pps0
# gpsd socket without PPS
--gnss-sock /run/chrony/gpsd.sock --gnss-refid NMEA
```

The gpsd source is marked `noselect` when PPS is present, and the PPS refclock
is preferred over network sources while it is reachable. Without a receiver,
omit the flags entirely.

## Install into chrony

The fragment is a normal include. Confirm the host `chrony.conf` has a confdir
for it (the stock Debian/Ubuntu package does):

```text
confdir /etc/chrony/conf.d
sourcedir /etc/chrony/sources.d
```

Keep the certificate and key readable by the chrony user. Distributions ship an
AppArmor profile that confines chronyd to `/etc/chrony/**`, `/var/lib/chrony`
and `/run/chrony`; placing credentials elsewhere causes
`Could not open … Permission denied` on start. For example:

```sh
install -d -m 0750 -g _chrony /etc/chrony/credentials
install -m 0640 -g _chrony node-a.pem node-a.key group.pem /etc/chrony/credentials/
```

### Firewall

Restrict both ports to group peers. Example nftables rules for subnet
`10.0.0.0/24`:

```nft
table inet outpostnet_time {
  chain input {
    type filter hook input priority filter; policy accept;
    udp dport 123 ip saddr 10.0.0.0/24 accept
    udp dport 123 drop
    tcp dport 4460 ip saddr 10.0.0.0/24 accept
    tcp dport 4460 drop
  }
}
```

### Re-render on peer or certificate change

Peers come from the Net peer set, so render after the directory changes and
reload the sources. A systemd timer keeps this converging without restarting
chrony on every change:

```ini
# /etc/systemd/system/outpostnet-chrony-render.service
[Unit]
Description=Render OutpostNet chrony/NTS fragment
After=outpostnet-agent.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/outpostnet-agent chrony render \
  --node-id node-a \
  --cert /etc/chrony/credentials/node-a.pem \
  --key /etc/chrony/credentials/node-a.key \
  --ca /etc/chrony/credentials/group.pem \
  --peers-socket /run/outpostnet/outpostnet.sock \
  --allow 10.0.0.0/24 \
  --output /etc/chrony/conf.d/outpostnet.conf \
  --sources-output /etc/chrony/sources.d/outpostnet.sources
ExecStart=/usr/bin/chronyc reload sources
Restart=no
```

```ini
# /etc/systemd/system/outpostnet-chrony-render.timer
[Unit]
Description=Re-render OutpostNet chrony/NTS fragment

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s

[Install]
WantedBy=timers.target
```

A certificate rotation rewrites `ntsservercert`, which chrony reads once, so
restart chronyd after rotating credentials (`systemctl restart chrony`).
Cookie persistence through `ntsdumpdir` keeps clients working across the
restart.

## Service ordering before K3s and Let

The point of D7 is that the system clock is corrected before K3s validates TLS.
`chrony.service` is ordered `Before=time-sync.target`, and `chrony-wait.service`
runs `chronyc waitsync` before the same target. Order dependents after the
target and pull in the wait unit:

```ini
# /etc/systemd/system/k3s.service.d/10-outpostnet-time.conf
[Unit]
Wants=time-sync.target chrony-wait.service
After=time-sync.target
```

Then `systemctl daemon-reload && systemctl enable --now chrony-wait.service`.
Use the same drop-in for Let. On a node with no reachable group peer,
`chrony-wait` times out after three minutes and startup continues; Outpost's
own reference still keeps replication safe.

## V2-NET-1d: the time-source adapter keeps working

Rendering does not change how the agent reads chrony. `pkg/state` still prefers
`chronyc -c tracking`, treats a valid local/orphan report as unsynchronized, and
reports quality on the management socket (`GET /v1/time`) and as
`net_time_*` metric events. Verify after deploying:

```sh
curl --unix-socket /run/outpostnet/outpostnet.sock http://localhost/v1/time
chronyc -c tracking
```

## Evidence

Golden tests cover the fragment (NTS, group CA, orphan, makestep, cert-time
relaxation, access rules, GNSS SHM/SOCK/PPS, split sources). A real
`chronyd -p` parse check runs whenever chronyd is installed (Linux CI); it
parses both the main fragment and an included `.sources` file.

```sh
go test -race -count=1 ./pkg/chronyconf ./cmd/outpostnet-agent/app \
  ./pkg/state ./internal/agent ./internal/observation \
  -run 'Chrony|Render|OSTimeSource|TimeSource|AgentOSTimeSourceStatus'
```

The multi-process NTS sync test (two chronyd instances under the group CA, plus
foreign-CA rejection) and VM acceptance ADR-0006 11–14 are tracked separately;
see [os-time-source.md](os-time-source.md) and the V2-RANGE-2 continuation.
