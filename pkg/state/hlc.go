package state

import (
	"fmt"
	"sync"
	"time"
)

// MaxClockOffsetMS bounds how far a received HLC wall time may be ahead of the
// local physical clock.
const MaxClockOffsetMS int64 = int64(5 * time.Minute / time.Millisecond)

// Timestamp is a hybrid logical clock timestamp used for state-version order.
type Timestamp struct {
	WallMS  int64  `json:"wall_ms"`
	Logical uint16 `json:"logical"`
}

// Compare returns -1, 0, or 1 when t is respectively older than, equal to, or
// newer than other.
func (t Timestamp) Compare(other Timestamp) int {
	if t.WallMS < other.WallMS {
		return -1
	}
	if t.WallMS > other.WallMS {
		return 1
	}
	if t.Logical < other.Logical {
		return -1
	}
	if t.Logical > other.Logical {
		return 1
	}
	return 0
}

// PhysicalClock supplies the physical component for an HLC.
type PhysicalClock interface {
	NowMS() int64
}

// PhysicalClockFunc adapts a function into a PhysicalClock.
type PhysicalClockFunc func() int64

// NowMS returns the current physical time in milliseconds.
func (f PhysicalClockFunc) NowMS() int64 { return f() }

// SystemPhysicalClock reads Unix time in milliseconds.
type SystemPhysicalClock struct{}

// NowMS returns the current Unix time in milliseconds.
func (SystemPhysicalClock) NowMS() int64 { return time.Now().UnixMilli() }

// ClockOffsetError reports a remote HLC beyond the accepted future skew.
type ClockOffsetError struct {
	RemoteWallMS   int64
	PhysicalNowMS  int64
	OffsetMS       int64
	MaxClockOffset int64
}

func (e *ClockOffsetError) Error() string {
	return fmt.Sprintf("remote HLC wall time exceeds max clock offset: remote_wall_ms=%d physical_now_ms=%d offset_ms=%d max_offset_ms=%d",
		e.RemoteWallMS, e.PhysicalNowMS, e.OffsetMS, e.MaxClockOffset)
}

type hlcClock struct {
	mu       sync.Mutex
	physical PhysicalClock
	last     Timestamp
}

func newHLCClock(physical PhysicalClock) *hlcClock {
	if physical == nil {
		physical = SystemPhysicalClock{}
	}
	return &hlcClock{physical: physical}
}

func (c *hlcClock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.physical.NowMS()
	if pt > c.last.WallMS {
		c.last = Timestamp{WallMS: pt}
		return c.last
	}
	c.last = incrementTimestamp(c.last.WallMS, c.last.Logical)
	return c.last
}

func (c *hlcClock) Update(remote Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.physical.NowMS()
	if err := checkFutureTimestamp(remote, pt); err != nil {
		return c.last, err
	}

	local := c.last
	wall := maxInt64(local.WallMS, remote.WallMS, pt)
	switch {
	case wall == local.WallMS && wall == remote.WallMS:
		c.last = incrementTimestamp(wall, maxUint16(local.Logical, remote.Logical))
	case wall == local.WallMS:
		c.last = incrementTimestamp(wall, local.Logical)
	case wall == remote.WallMS:
		c.last = incrementTimestamp(wall, remote.Logical)
	default:
		c.last = Timestamp{WallMS: wall}
	}
	return c.last, nil
}

// Current returns the last issued or restored timestamp.
func (c *hlcClock) Current() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

func (c *hlcClock) Restore(ts Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ts.Compare(c.last) > 0 {
		c.last = ts
	}
}

func incrementTimestamp(wall int64, logical uint16) Timestamp {
	if logical == ^uint16(0) {
		return Timestamp{WallMS: wall + 1}
	}
	return Timestamp{WallMS: wall, Logical: logical + 1}
}

func maxInt64(values ...int64) int64 {
	max := values[0]
	for _, value := range values[1:] {
		if value > max {
			max = value
		}
	}
	return max
}

func maxUint16(a, b uint16) uint16 {
	if a > b {
		return a
	}
	return b
}
