package observation

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Component names the stream a host reports on. The layer of each fact is
// given by the event body, not by the component.
type Component int

const (
	// ComponentNet is a standalone OutpostNet process (outpostnet-agent).
	ComponentNet Component = iota + 1
	// ComponentService is an OutpostService process (outpostnet-service).
	ComponentService
)

func (c Component) proto() observationv1alpha1.Component {
	switch c {
	case ComponentNet:
		return observationv1alpha1.Component_COMPONENT_OUTPOSTNET
	case ComponentService:
		return observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE
	default:
		return observationv1alpha1.Component_COMPONENT_UNSPECIFIED
	}
}

// Reporter holds the facts last reported on one stream and appends an event
// only when a fact changes. Updates and snapshots share one lock, so a
// snapshot's seq is exactly the last event it already reflects.
type Reporter struct {
	nodeID    string
	component observationv1alpha1.Component
	now       func() time.Time
	log       *EventLog

	mu       sync.Mutex
	netPeers map[string]*observationv1alpha1.NetPeerChanged
	services map[string]*observationv1alpha1.ServiceAccessChanged
}

// ReporterConfig configures a Reporter.
type ReporterConfig struct {
	NodeID          string
	Component       Component
	EventBufferSize int
	Now             func() time.Time
	Logger          *slog.Logger
}

// NewReporter creates a reporter with an empty state.
func NewReporter(cfg ReporterConfig) *Reporter {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	component := cfg.Component.proto()
	return &Reporter{
		nodeID:    cfg.NodeID,
		component: component,
		now:       now,
		log:       NewEventLog(cfg.NodeID, component, cfg.EventBufferSize, now, cfg.Logger),
		netPeers:  make(map[string]*observationv1alpha1.NetPeerChanged),
		services:  make(map[string]*observationv1alpha1.ServiceAccessChanged),
	}
}

// Log returns the reporter's event log.
func (r *Reporter) Log() *EventLog { return r.log }

// Snapshot returns every current fact (Net peers in field 15, Service entries
// in field 20) and the seq of the last event it reflects.
func (r *Reporter) Snapshot() *observationv1alpha1.Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	seq := r.log.CurrentSeq()
	component := r.component
	snap := &observationv1alpha1.Snapshot{
		NodeId:    r.nodeID,
		Component: &component,
		Seq:       &seq,
		NodeTime:  timestamppb.New(r.now().UTC()),
	}
	for _, key := range sortedKeys(r.netPeers) {
		snap.NetPeers = append(snap.NetPeers, proto.Clone(r.netPeers[key]).(*observationv1alpha1.NetPeerChanged))
	}
	for _, key := range sortedKeys(r.services) {
		snap.ServiceAccess = append(snap.ServiceAccess, proto.Clone(r.services[key]).(*observationv1alpha1.ServiceAccessChanged))
	}
	return snap
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}
