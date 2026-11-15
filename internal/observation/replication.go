package observation

import (
	"github.com/outpostos/OutpostNet/pkg/state"
	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
)

// UpdateReplication emits facts from a replica embedded in this process as
// existing MetricSample events. Standalone agents have no state stores and do
// not call it. Embedding hosts use state.ReplicationMetrics in their own streams.
func (r *Reporter) UpdateReplication(scope string, status state.ReplicationStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sample := range state.ReplicationMetrics(scope, status) {
		value := sample.Value
		r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_MetricSample{MetricSample: &observationv1alpha1.MetricSample{Name: sample.Name, Value: &value, Labels: sample.Labels}}})
	}
}
