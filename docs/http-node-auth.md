# Replication and delivery node authentication (V2-NET-4)

Implements [ADR-0008 D3](https://github.com/outpostos/OutpostOS/blob/main/docs/adr/0008-node-identity-and-origin.md)
for the embeddable `pkg/state` and `pkg/delivery` HTTP adapters. Credentials and
the offline verifier come from [NET-3 node certificates](node-certificates.md).
Mesh and Let host configuration/wiring are V2-MESH-2 and V2-LET-2; this library
change does not enable certificates in those processes automatically.

## Authentication modes

All four HTTP configs (`state.HTTPSyncerConfig`, `state.HTTPHandlerConfig`,
`delivery.HTTPTransportConfig`, `delivery.HTTPHandlerConfig`) accept `AuthMode`
and `Identity`. Hosts map their `auth_mode` setting to `httpauth.Mode`:

| `AuthMode` | Required credentials | Transport |
| --- | --- | --- |
| empty / `httpauth.Token` (`token`) | Nonempty `AuthToken`; source remains self-declared | Legacy HTTP or host-protected HTTPS |
| `httpauth.TokenCert` (`token+cert`) | Both nonempty `AuthToken` **and** a valid group node `Identity` | HTTPS mutual TLS |
| `httpauth.Cert` (`cert`) | Valid group node `Identity`; token is ignored and not sent | HTTPS mutual TLS |

`token+cert` is conjunctive: token-only nodes cannot enter a certificate listener.
A group can mix `token+cert` and `cert` nodes while every sender that contacts a
`token+cert` receiver still uses `token+cert` and the matching token. Configure
senders before changing receivers to `cert`, then remove token settings from
senders. Alpha deployments may upgrade the entire group directly to `cert`.
Standalone `token` support remains for one transition version; it offers no
per-node identity guarantee. No automatic downgrade or authentication fallback
occurs. Supplying an `Identity` with token-only/default mode fails construction,
so an omitted mode cannot silently discard configured credentials.

An unknown mode, missing required credentials or a local certificate node ID
that differs from `NodeID` fails construction. Certificate-mode state handlers also require the
engine's node ID to match. Certificate clients use HTTPS for bare `host:port`
addresses and refuse explicit HTTP or URL userinfo before contacting the peer.
Both clients refuse redirects, including same-host redirects. Supplied clients
are copied; their timeout is retained (zero defaults to ten seconds). In
certificate modes the transport must be a standard `*http.Transport` without
custom TLS dialers; its settings are cloned and its TLS config is replaced by
the group verifier. Custom RoundTrippers remain supported in token mode.

## Host setup

Load each node's own key pair and the group's public CA using
`tlsutil.LoadIdentity(certFile, keyFile, caFile, nodeID, logger)`; keep the CA key
offline. Share that `*tlsutil.Identity` among the node's adapters to aggregate
security counters. For example, with host-owned `identity`, `nodeID`, `engine`,
`inbox`, `outbox`, `peers` and `peerIdentity`:

```go
syncer, err := state.NewHTTPSyncer(state.HTTPSyncerConfig{
    NodeID: nodeID, AuthMode: httpauth.Cert, Identity: identity,
    PeerIdentity: peerIdentity, // optional expected responder node for state
})
if err != nil { return err }
_ = syncer // supply to the host's GossipEngine constructor
stateHandler, err := state.NewHTTPHandler(state.HTTPHandlerConfig{
    NodeID: nodeID, Engine: engine, AuthMode: httpauth.Cert, Identity: identity,
})
if err != nil { return err }
wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{
    NodeID: nodeID, AuthMode: httpauth.Cert, Identity: identity,
    PeerProvider: peers, PeerIdentity: peerIdentity,
})
if err != nil { return err }
_ = wire // supply to the durable Inbox/Outbox constructors
handler, err := delivery.NewHTTPHandler(delivery.HTTPHandlerConfig{
    NodeID: nodeID, Inbox: inbox, Outbox: outbox,
    AuthMode: httpauth.Cert, Identity: identity,
})
if err != nil { return err }
mux := http.NewServeMux()
mux.Handle(state.PushPath, stateHandler)
mux.Handle(state.PullPath, stateHandler)
mux.Handle(delivery.BatchPath, handler)
server := &http.Server{
    Addr: listenAddr, Handler: mux,
    TLSConfig: httpauth.TLSConfig(identity, true),
    ReadHeaderTimeout: 5 * time.Second,
}
return server.ListenAndServeTLS("", "") // certificates are in TLSConfig
```

Import `github.com/outpostos/OutpostNet/pkg/state/httpauth` alongside the existing
state/delivery packages. Peer provider and identity callbacks must be safe for
concurrent calls. Hosts own listener lifetimes, resource limits and local
management endpoints. HTTP ALPN uses `http/1.1`; the tunnel's `outpostnet` ALPN is
not used on this listener. Verification requires no DNS identity, online CA or
external clock: group chain, TLS key usage and exactly one node URI SAN are
checked with the NET-3 verifier, independently of certificate dates. Date-window
violations still warn and count. TLS session tickets are disabled.

Handlers also verify the certificate at each request boundary using their own
group CA. A handler mounted accidentally on plaintext or on a TLS listener that
does not verify client certificates fails closed; forwarded identity headers do
not authenticate a node. Deploy direct HTTPS rather than terminating TLS at an
HTTP proxy that discards the original peer certificate.

## Identity checks and counters

Before engine calls or Inbox/Outbox admission:

- State push/pull `SourceNodeID` must equal the TLS certificate node ID and
  `X-Outpost-Node-ID`; self-replication is rejected.
- Delivery request `source`, header, and every envelope's
  `message_id.source_node_id` must equal the TLS certificate node ID. Every
  `destination_node_id` must be this receiver. Any mismatch rejects the entire
  batch with 403, including an otherwise valid prefix.
- A receipt's `receiver_node_id` must equal the sending certificate node ID;
  its message must originate on this receiving node. Receipt message source
  denotes the **original** message source, not the receipt sender.

Source/certificate mismatches return 403. Invalid/missing tokens return 401.
A missing or foreign-group certificate normally fails the TLS handshake, before
an HTTP response exists. On a weak/plain listener the handler returns 403.

State success and 410/resync responses bind their existing `node_id` to the
server certificate before exposing entries, time samples or GC horizons. Optional
`HTTPSyncerConfig.PeerIdentity` also checks the directory expectation. Delivery
checks both response `node_id` and mapped destination against the certificate
before accepting acknowledgements. Directory checks happen on responses;
all valid group nodes are trusted peers at this phase.

Read `identity.Stats()` through the host's local management API:

| Counter | Meaning |
| --- | --- |
| `CertificateRejected` / `certificate_rejected` | Missing/foreign/malformed certificates or TLS usage rejected by verifier; also handler checks on weak listeners |
| `ClaimRejected` / `claim_rejected` | Body/header/certificate/response/destination mismatch |
| `ValidityWarnings` / `validity_warnings` | Accepted certificate outside its date window at verification time |

Counters are atomic and reset with the Identity/process. Validity may be checked
at handshake and at each HTTP request, so warning counts denote verification
observations, not unique connections. Token refusals use HTTP 401; they are not
certificate refusals. These libraries do not start their own management listener
or extend the Range observation contract. Existing tunnel management endpoints
cover their own process Identity; embedding hosts must expose the shared HTTP
Identity's stats themselves. Neither credentials nor incoming claim text is added
to certificate/claim warning logs.

State protocol stays **3.0** (NET-1b's coordinated upgrade); existing responder
identity fields suffice. Delivery stays **1.0**, and durable formats are unchanged.
NET-1c's clock recovery is separate. Third-party state entries are still forwarded
without per-entry signatures: an authenticated compromised group node can forge
writer metadata. Revocation, cross-group ACLs, signatures and the Phase 3 unified
transport remain outside this ticket. Full RANGE-1/RANGE-2 certificate regressions
and 2+2 partition acceptance belong to V2-RANGE-3/5 after host wiring.

## Reproduce acceptance

```sh
go test -race -count=1 ./pkg/delivery -run '^TestCertificateHTTP'
```

The suite uses real local HTTPS peers and public state/delivery APIs. It covers
missing and foreign credentials, forged sources and receipts, destination
mismatch before admission, TLS client verification, responder/410 identity,
plaintext and redirect refusal, configuration rejection, concurrent replication,
and durable delivery/reopened Inbox/Ack/receipt handoff in both certificate modes.
Legacy token suites remain part of the full race gate. See the
[validation report](test-report-v2-net4.md) for red→green and gate evidence.
