package observation

import (
	"context"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
)

// UpdateTime reports local source status on the existing metric event timeline.
// These samples are events, not additional Snapshot fields or a new contract.
func (r *Reporter) UpdateTime(s state.TimeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	labels := map[string]string{"quality": string(s.Quality), "source": s.Source.Source, "source_state": s.Source.State}
	samples := []struct {
		name  string
		value float64
	}{
		{"net_time_quality", 1},
	}
	if s.Source.Synchronized {
		samples = append(samples, struct {
			name  string
			value float64
		}{"net_time_source_uncertainty_seconds", s.Source.Uncertainty.Seconds()})
	}
	if s.Source.OffsetKnown {
		samples = append(samples, struct {
			name  string
			value float64
		}{"net_time_source_offset_seconds", s.Source.Offset.Seconds()})
	}
	for _, sample := range samples {
		value := sample.value
		r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_MetricSample{MetricSample: &observationv1alpha1.MetricSample{Name: sample.name, Value: &value, Labels: labels}}})
	}
}

// RunTimeSampler samples quality (including holdover expiry) independently of
// source polling so a command timeout cannot hide a transition from observation.
func (r *Reporter) RunTimeSampler(ctx context.Context, ref *state.TimeReference) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		r.UpdateTime(ref.TimeStatus())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
