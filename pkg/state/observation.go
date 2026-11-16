package state

import "strconv"

// ReplicationMetric is a local observation sample independent of any telemetry
// protocol. Hosts map these to their existing MetricSample event timeline.
// Samples describe only this embedded replica, never cluster-wide loss.
type ReplicationMetric struct {
	Name   string
	Value  float64
	Labels map[string]string
}

// ReplicationMetrics maps committed resync losses and current outage warnings.
// scope identifies the embedded keyspace (for example outpostlet.metadata).
// Counts survive durable restart; warning counts cover this engine process.
func ReplicationMetrics(scope string, s ReplicationStatus) []ReplicationMetric {
	var samples []ReplicationMetric
	for _, c := range s.Local.Resync.ByWriter {
		samples = append(samples, ReplicationMetric{Name: "state_resync_purged_entries_total", Value: float64(c.Entries), Labels: map[string]string{"scope": scope, "writer_node_id": c.WriterNodeID, "peer": c.Peer, "local_origin": strconv.FormatBool(c.LocalOrigin)}})
	}
	if path := s.Local.Resync.LatestArchivePath; path != "" {
		samples = append(samples, ReplicationMetric{Name: "state_resync_latest_archive", Value: 1, Labels: map[string]string{"scope": scope, "path": path}})
	}
	for _, p := range s.Peers {
		approaching := float64(0)
		if p.ResyncLossLimitApproaching {
			approaching = 1
		}
		samples = append(samples,
			ReplicationMetric{Name: "state_peer_unreachable_seconds", Value: p.UnreachableFor.Seconds(), Labels: map[string]string{"scope": scope, "peer": p.Peer}},
			ReplicationMetric{Name: "state_resync_loss_limit_approaching", Value: approaching, Labels: map[string]string{"scope": scope, "peer": p.Peer, "warning": "resync loss limit approaching"}},
			ReplicationMetric{Name: "state_resync_loss_warnings_total", Value: float64(p.ResyncLossWarnings), Labels: map[string]string{"scope": scope, "peer": p.Peer}},
		)
	}
	return samples
}
