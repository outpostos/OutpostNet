package protocol

import "sync/atomic"

// StreamAllocator generates unique stream IDs. Used by the Server.
type StreamAllocator struct {
	next atomic.Uint32
}

// NewStreamAllocator creates an allocator starting at stream ID 1.
func NewStreamAllocator() *StreamAllocator {
	a := &StreamAllocator{}
	a.next.Store(0) // first Add will return 1
	return a
}

// Allocate returns the next unique stream ID. Never returns 0.
func (a *StreamAllocator) Allocate() uint32 {
	for {
		id := a.next.Add(1)
		if id != 0 {
			return id
		}
		// Wrapped around to 0, skip it.
	}
}
