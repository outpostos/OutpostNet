// Package observation is the OutpostNet producer side of the OutpostRange
// observation contract (proto outpostrange.observation.v1alpha1): a bounded
// event log, a Reporter holding the last reported Net and Service facts, and
// the localhost-only snapshot/SSE endpoint read by range-agent.
//
// Observation never gates the product: recording is non-blocking, the event
// log is a bounded ring, slow subscribers are dropped, and the endpoint's
// failures are logged by the host and never returned to it. Only this
// package (and the hosts wiring it) imports OutpostRange; pkg/state,
// pkg/tunnel and pkg/service stay free of it. See docs/observation.md.
package observation

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultEventBufferSize is how many events the log retains for resume.
	DefaultEventBufferSize = 4096
	// subscriberBuffer is how far a subscriber may lag before it is dropped.
	subscriberBuffer = 1024
	seqOrdinalBits   = 20
	seqOrdinalMask   = (uint64(1) << seqOrdinalBits) - 1
)

// ErrEventGap means after_seq is in this process epoch but older than the
// retained window; the reader must take a new snapshot.
var ErrEventGap = errors.New("observation: after_seq is older than the retained event log")

// EventLog numbers events for one (node_id, component) stream and keeps the
// most recent ones. seq = (start UnixMilli << 20) + ordinal, as in the Range
// contract. All methods are safe for concurrent use and never block on
// subscribers.
type EventLog struct {
	nodeID    string
	component observationv1alpha1.Component
	now       func() time.Time
	logger    *slog.Logger

	mu       sync.Mutex
	epoch    uint64
	lastSeq  uint64
	events   []*observationv1alpha1.Event
	capacity int
	dropped  uint64
	subs     map[uint64]chan *observationv1alpha1.Event
	nextSub  uint64
}

// NewEventLog creates a log whose epoch is the current time.
func NewEventLog(nodeID string, component observationv1alpha1.Component, capacity int, now func() time.Time, logger *slog.Logger) *EventLog {
	if capacity <= 0 {
		capacity = DefaultEventBufferSize
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	epoch := uint64(now().UTC().UnixMilli()) << seqOrdinalBits
	return &EventLog{
		nodeID:    nodeID,
		component: component,
		now:       now,
		logger:    logger,
		epoch:     epoch,
		lastSeq:   epoch,
		capacity:  capacity,
		subs:      make(map[uint64]chan *observationv1alpha1.Event),
	}
}

// Append stamps event with node_id, component, the next seq and node_time,
// retains it, and offers it to subscribers. The caller sets the body and must
// not modify event afterwards.
func (l *EventLog) Append(event *observationv1alpha1.Event) *observationv1alpha1.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastSeq++
	seq := l.lastSeq
	if seq&seqOrdinalMask == seqOrdinalMask {
		l.logger.Warn("observation event ordinal reached its epoch limit", "seq", seq)
	}
	component := l.component
	event.NodeId = l.nodeID
	event.Component = &component
	event.Seq = &seq
	event.NodeTime = timestamppb.New(l.now().UTC())

	if len(l.events) == l.capacity {
		l.events[0] = nil
		l.events = l.events[1:]
		l.dropped++
	}
	l.events = append(l.events, event)
	for id, ch := range l.subs {
		select {
		case ch <- event:
		default:
			delete(l.subs, id)
			close(ch)
			l.logger.Warn("observation subscriber too slow; closing its stream", "subscriber", id)
		}
	}
	return event
}

// CurrentSeq is the seq of the latest appended event (the epoch base when
// none was appended yet).
func (l *EventLog) CurrentSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastSeq
}

// Dropped counts events evicted from the retained window.
func (l *EventLog) Dropped() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropped
}

// SubscribeAfter returns the retained events with seq > afterSeq and a
// channel for later ones. The channel is closed when the subscriber falls
// behind or cancel is called. An afterSeq from an earlier epoch receives the
// whole retained window (a legal restart transition for the reader).
func (l *EventLog) SubscribeAfter(afterSeq uint64) ([]*observationv1alpha1.Event, <-chan *observationv1alpha1.Event, func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.events) > 0 {
		earliest := l.events[0].GetSeq()
		if afterSeq >= l.epoch && afterSeq < earliest-1 {
			return nil, nil, nil, fmt.Errorf("%w: after_seq=%d earliest_retained_seq=%d", ErrEventGap, afterSeq, earliest)
		}
	}
	var backlog []*observationv1alpha1.Event
	for _, event := range l.events {
		if event.GetSeq() > afterSeq {
			backlog = append(backlog, event)
		}
	}
	l.nextSub++
	id := l.nextSub
	ch := make(chan *observationv1alpha1.Event, subscriberBuffer)
	l.subs[id] = ch
	return backlog, ch, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if ch, ok := l.subs[id]; ok {
			delete(l.subs, id)
			close(ch)
		}
	}, nil
}
