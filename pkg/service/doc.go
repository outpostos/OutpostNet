// Package service is the OutpostService domain module: stable service
// identities, stable local entries and service exports.
//
// Responsibilities (four-layer refactor, ticket 09):
//
//   - Ref / Target: the stable service identity "<service>@<node_id>" an
//     application depends on, and the routing target handed to Net.
//   - Entry: a stable local listener. Every accepted connection asks Net for
//     a fresh stream to the target, so endpoint changes beneath the identity
//     (peer address/port change, replaced instance) are picked up by new
//     connections without touching the application. In-flight connections
//     are NOT migrated.
//   - RegisterExports / ApplyExports: which service names are exported to
//     which local addresses, including in-place address changes.
//
// The package reaches Net only through two small ports injected by a host:
// StreamOpener (open a bidirectional stream to a Target) and RouteTable
// (AddRoute/RemoveRoute, satisfied by *tunnel.Server). NetOpener adapts a Net
// host that opens streams in transport terms (NetStreams: peer node_id or
// tunnel region + stream name) to StreamOpener. It must not import
// pkg/tunnel or any internal/* host; boundary_test.go enforces this and also
// enforces that the generic Net packages never import this package.
//
// Cross-node protocol support: TCP only. UDP over the QUIC tunnel is not
// implemented; ValidateConfig rejects udp entries and exports. See
// docs/service-layer.md.
package service
