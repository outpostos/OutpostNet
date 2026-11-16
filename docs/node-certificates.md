# Node certificates (V2-NET-3)

The QUIC tunnel implements [ADR-0008 D1–D3, D5](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0008-node-identity-and-origin.md).
Each group provisions its own CA offline; each node receives its own key pair and
certificate with exactly one URI SAN `urn:outpostos:node:<node_id>`.
No online CA, DNS, time source, or revocation service is needed to connect.

## Provision offline

Use the existing agent binary (also available with `go run ./cmd/outpostnet-agent`):

```sh
mkdir credentials
outpostnet-agent cert ca --cert credentials/group.pem --key credentials/group.key
outpostnet-agent cert issue --ca credentials/group.pem --ca-key credentials/group.key \
  --node-id node-a --cert credentials/node-a.pem --key credentials/node-a.key
outpostnet-agent cert issue --ca credentials/group.pem --ca-key credentials/group.key \
  --node-id node-b --cert credentials/node-b.pem --key credentials/node-b.key
outpostnet-agent cert list credentials
outpostnet-agent cert inspect credentials/node-a.pem
```

The tool creates ECDSA P-256 keys, a twenty-year CA, and ten-year node certificates
(within the issuer's window), usable as both TLS client and server. Output files
must not exist. Keys are created with mode `0600`; inspect/list print only public
certificate details. `list` reads `.pem`/`.crt` certificates, nonrecursively; keep
private keys in `.key` files. Distribute only the group **public** CA and each
node's own certificate/key to that node. Keep the CA private key offline.

### Optional IP/DNS SANs (NTS server verification)

Tunnel identity is the URI SAN above and needs no DNS or IP names. When the node
also serves NTS to group peers (ADR-0006 D7), embed the names those peers use to
verify it, alongside the node identity:

```sh
outpostnet-agent cert issue --ca credentials/group.pem --ca-key credentials/group.key \
  --node-id node-a --san node-a.edge.local --san 10.0.0.5 \
  --cert credentials/node-a.pem --key credentials/node-a.key
```

`--san` is repeatable and accepts an IP address or a DNS name. Names are validated
(RFC 1123 labels; empty and duplicate values are rejected) and canonicalized
before being written as IP/DNS SANs. They do not change tunnel authentication, so
certificates without SANs and already-issued credentials stay valid. `cert inspect`
appends `dns=` and `ip=` when those SANs are present.

See [group chrony/NTS](group-time-sync.md) for the renderer, GNSS templates and
startup ordering for K3s/Let.

## Configure both peers

Both `outpostnet-agent` and `outpostnet-service` accept:

```yaml
node_id: node-a
# Existing tunnels, peers, exports and imports are configured as usual.
tls:
  ca_file: credentials/group.pem
  cert_file: credentials/node-a.pem
  key_file: credentials/node-a.key
```

Use the node-b credentials and `node_id: node-b` on the other peer. `tls.ca_file`
activates strict certificate mode for all configured and peer-directory tunnels:
missing/invalid files, an untrusted local leaf, or a configured `node_id` that
fails to match the local certificate abort startup. `tls.insecure: true` is
incompatible with certificate mode. Both cert/key paths must be supplied together.
TLS changes require a restart. No self-signed or token-only fallback occurs when
a CA is configured. CA bundles may contain multiple CA certificates during an
operator-managed CA rotation; every listed CA is trusted for service access.

An optional `auth.token`, if configured, must also match. Empty tokens work in
certificate mode, including reverse streams. Without `tls.ca_file`, the existing
legacy TLS/token behavior remains for standalone deployments; it does not provide
node authentication. Upgrade both ends to certificate mode together. The HTTP
replication/delivery adapters have separate `AuthMode` configuration; see
[HTTP node authentication (V2-NET-4)](http-node-auth.md) for setup and migration.

## Authentication and service authorization

The custom `VerifyPeerCertificate` validates the chain to the local group CA,
the TLS client/server usage and the unique nonempty node URI. Per ADR-0008,
`x509.VerifyOptions.CurrentTime` is the leaf's `NotBefore`, independent of the
system clock; actual out-of-window dates increment a counter and log a warning,
without refusing the peer. The issuing tool ensures the CA/leaf windows overlap.
Validity is not a security boundary. Revocation is deferred to Phase 5; rotate
the group CA for emergency removal.

On the accepting side, TLS requests a client certificate and the custom verifier
**rejects and counts** its absence (`RequestClientCert` lets the verifier observe
missing certificates). Session tickets are disabled so each connection verifies
its certificate. The first QUIC stream must finish the token/identity handshake;
the compatibility node declaration must equal the verified leaf identity before
acknowledgement or registration. Missing, malformed and mismatching declarations
are rejected. The accepting side registers reverse channels using the certificate
identity. Dynamic dialers also check the expected peer-directory `node_id`, so
an address pointing to another valid group node fails visibly.

Every subsequent service stream, including confirmed streams, relay ingress and
reverse streams, belongs to that authenticated connection. The initial policy is
"all nodes trusted by the local group CA may open service streams". Authorization
happens before route lookup or backend dialing. Static region labels are not node
IDs: static tunnels authenticate group membership without asserting a destination
node ID. Cross-group authorization and per-service access lists remain Phase 5.

Library callers use `tlsutil.LoadIdentity` / `NewIdentity` and set `Identity` on
`tunnel.ServerConfig` / `ClientConfig` (takes precedence over `TLS`). Dynamic
clients set `ExpectedPeerID`; `NodeID` is the compatibility claim, defaulting to
the local certificate identity. Share an Identity across a node's connections to
aggregate its atomic counters. Use `Identity.TLSConfig` for the reusable custom
verifier; using only a TLS config does not enable the tunnel's claim handshake.

## Local evidence

Both binaries expose process-lifetime counters on their management Unix socket:

```sh
curl --unix-socket /run/outpostnet/service.sock http://localhost/v1/security
```

```json
{"certificate_auth":true,"certificate_rejected":0,"claim_rejected":0,"validity_warnings":0}
```

`certificate_rejected` counts missing certificates, invalid chains/usages and
invalid SAN identities observed by the verifier. `claim_rejected` counts rejected
handshake claims and unexpected destination identities. `validity_warnings`
counts accepted peer certificates outside their date window. Warnings are also
logged; credentials are never logged. TLS failures that happen before the peer
sends a certificate are ordinary connection failures, not certificate rejections.
Counters reset on process restart; `certificate_auth: false` identifies legacy
mode. This endpoint is read-only and does not extend the Range contract.

Reproducible coverage:

```sh
go test -race -count=1 ./pkg/tunnel/... ./cmd/outpostnet-agent/app ./internal/servicehost
```

`TestCertificateTunnelRejectsUnauthenticatedAndForgedPeers` covers missing/foreign
certificates, forged handshake claims, destination mismatches and counters.
`TestCertificateTunnelAuthorizesBothDirectionsWithoutToken` covers forward/reverse
service streams; `TestCertificateReconnectAndConcurrentStreams` covers replacement
connections and concurrent streams. `TestCertificateTLSWithSkewedClocks` uses real
QUIC/TLS with injected TLS clocks at 1970 and more than ten years ahead, transfers
TCP application bytes and checks both peers' validity counters, without changing
the machine clock. `TestNodeVerificationIgnoresWallClockAndWarns` also checks
warning logs. `TestServiceHostsUseGroupCertificatesAndExposeSecurityCounters`
loads disk credentials through the actual host and reads the Unix-socket endpoint.
