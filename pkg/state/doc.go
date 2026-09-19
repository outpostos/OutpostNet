// Package state provides domain-agnostic replicated state primitives for
// OutpostNet consumers.
//
// A MemoryStore stores opaque byte values and tombstones, exposes local
// submit/read/observe operations, and supports atomic CompareAndSet by entry
// identity. GossipEngine exchanges entries through a Syncer using incremental
// local positions and digest anti-entropy. Entry.ChangeID identifies the
// original write, Entry.Version orders conflicting state, and Entry.Position is
// local to each receiving node.
package state
