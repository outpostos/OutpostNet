package observation

import (
	"github.com/outpostos/OutpostNet/internal/meshdirectory"
	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
)

// UpdateDirectory emits the host's safe directory snapshot as MetricSample
// events on the existing bounded timeline. There is no entity data or raw
// upstream error text in this projection, and no Range contract change.
func (r *Reporter) UpdateDirectory(s meshdirectory.Status) {
	r.mu.Lock()
	defer r.mu.Unlock()
	emit := func(name string, value float64, labels map[string]string) {
		r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_MetricSample{MetricSample: &observationv1alpha1.MetricSample{
			Name: "service_directory_" + name, Value: &value, Labels: labels,
		}}})
	}
	for _, sample := range []struct {
		name  string
		value uint64
	}{
		{"connects_total", s.Stats.Connects},
		{"snapshots_total", s.Stats.Snapshots},
		{"deltas_total", s.Stats.Deltas},
		{"malformed_total", s.Stats.Malformed},
		{"expired_total", s.Stats.Expired},
		{"lease_expired_total", s.Stats.LeaseExpired},
		{"writer_rejected_total", s.Stats.WriterRejected},
		{"cache_writes_total", s.Stats.CacheWrites},
		{"cache_write_errors_total", s.Stats.CacheWriteErrors},
	} {
		emit(sample.name, float64(sample.value), nil)
	}
	for _, lease := range s.Leases {
		valid := 0.0
		if lease.State == "valid" {
			valid = 1
		}
		emit("lease_valid", valid, map[string]string{"lease_node_id": lease.NodeID, "state": lease.State})
		emit("lease_ttl_seconds", lease.TTLSeconds, map[string]string{"lease_node_id": lease.NodeID})
		if lease.LastAdvanceAgeSeconds != nil {
			emit("lease_last_advance_age_seconds", *lease.LastAdvanceAgeSeconds, map[string]string{"lease_node_id": lease.NodeID})
		}
	}
}
