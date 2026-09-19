// Package state provides domain-agnostic replicated state primitives for
// OutpostNet consumers.
//
// MemoryStore and DurableStore implement the Store interface for opaque byte
// values and tombstones, expose local submit/read/observe operations, and
// support atomic CompareAndSet by entry identity. DurableStore restores entries,
// tombstones, HLC state, local positions, and gossip peer progress from disk.
// GossipEngine exchanges entries through a Syncer using incremental local
// positions and digest anti-entropy. Entry.ChangeID identifies the original
// write, Entry.Version orders conflicting state, and Entry.Position is local to
// each receiving node.
package state
