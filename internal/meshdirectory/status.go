package meshdirectory

import "sort"

// Counters is the safe numeric projection of Stats. Raw connection errors and
// resume cursors are deliberately excluded: upstream text can contain secrets.
type Counters struct {
	Connects         uint64 `json:"connects"`
	Snapshots        uint64 `json:"snapshots"`
	Deltas           uint64 `json:"deltas"`
	Malformed        uint64 `json:"malformed"`
	Expired          uint64 `json:"expired"`
	LeaseExpired     uint64 `json:"lease_expired"`
	WriterRejected   uint64 `json:"writer_rejected"`
	CacheWrites      uint64 `json:"cache_writes"`
	CacheWriteErrors uint64 `json:"cache_write_errors"`
}

// Status reports only this process's directory view, without entity payloads.
type Status struct {
	Configured bool          `json:"configured"`
	Synced     bool          `json:"synced"`
	Connected  bool          `json:"connected"`
	Stale      bool          `json:"stale"`
	Stats      Counters      `json:"stats"`
	Leases     []LeaseStatus `json:"leases"`
}

// LeaseStatus reports the locally accepted lease for a node. A missing lease
// has no sequence or age; payloads and registrar incarnation are not exported.
type LeaseStatus struct {
	NodeID                string   `json:"node_id"`
	State                 string   `json:"state"` // valid, expired, or missing
	Seq                   uint64   `json:"lease_seq,string,omitempty"`
	LastAdvanceAgeSeconds *float64 `json:"last_advance_age_seconds,omitempty"`
	TTLSeconds            float64  `json:"lease_ttl_seconds"`
}

// Status returns a detached, read-only snapshot suitable for local management.
func (d *Directory) Status() Status {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s := d.Stats()
	status := Status{
		Configured: true, Synced: d.synced, Connected: d.connected, Stale: d.stale,
		Stats: Counters{
			Connects: s.Connects, Snapshots: s.Snapshots, Deltas: s.Deltas,
			Malformed: s.Malformed, Expired: s.Expired, LeaseExpired: s.LeaseExpired,
			WriterRejected: s.WriterRejected, CacheWrites: s.CacheWrites, CacheWriteErrors: s.CacheWriteErrors,
		},
		Leases: []LeaseStatus{},
	}
	nodes := make(map[string]struct{}, len(d.leases))
	for node := range d.leases {
		nodes[node] = struct{}{}
	}
	for _, in := range d.entities {
		if in.Incarnation != "" {
			nodes[in.NodeID] = struct{}{}
		}
	}
	mono := d.cfg.MonotonicNow()
	for node := range nodes {
		ttl := d.leaseTTL(node)
		view := LeaseStatus{NodeID: node, State: "missing", TTLSeconds: ttl.Seconds()}
		if lease, ok := d.leases[node]; ok {
			age := max(0, mono.Sub(lease.Seen).Seconds())
			view.Seq, view.LastAdvanceAgeSeconds = lease.Seq, &age
			view.State = "valid"
			if mono.Sub(lease.Seen) > ttl {
				view.State = "expired"
			}
		}
		status.Leases = append(status.Leases, view)
	}
	sort.Slice(status.Leases, func(i, j int) bool { return status.Leases[i].NodeID < status.Leases[j].NodeID })
	return status
}
