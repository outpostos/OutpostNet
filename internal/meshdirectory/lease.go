package meshdirectory

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/outpostos/OutpostNet/pkg/service"
)

const leasePrefix = "urn:outpostos:node-lease:"

type registration struct {
	service.Instance
	OriginNodeID string
	Incarnation  string
}

type nodeLease struct {
	OriginNodeID string    `json:"origin_node_id"`
	Seq          uint64    `json:"lease_seq"`
	Incarnation  string    `json:"registrar_incarnation"`
	Seen         time.Time `json:"-"` // Never persist a process-local monotonic timestamp.
	expired      bool
}

type wireNodeLease struct {
	Seq         *uint64 `json:"leaseSeq,string"` // ProtoJSON uint64 must not pass through float64.
	Incarnation string  `json:"registrarIncarnation"`
}

type snapshot struct {
	instances map[string]registration
	leases    map[string]nodeLease
}

func newSnapshot() *snapshot {
	return &snapshot{instances: map[string]registration{}, leases: map[string]nodeLease{}}
}

// Caller holds d.mu. Class names belong to the host's link policy.
func (d *Directory) leaseTTL(node string) time.Duration {
	if d.cfg.NodeLinkClass != nil {
		if ttl, ok := d.cfg.LeaseTTLByLinkClass[d.cfg.NodeLinkClass(node)]; ok {
			return ttl
		}
	}
	return d.cfg.LeaseTTL
}

func (d *Directory) valid(in registration, wall, mono time.Time) bool {
	if in.Incarnation == "" {
		return !in.Expiry.IsZero() && wall.Before(in.Expiry)
	}
	lease, ok := d.leases[in.NodeID]
	return ok && lease.Incarnation == in.Incarnation && mono.Sub(lease.Seen) <= d.leaseTTL(in.NodeID)
}

// decodeRecord checks both legacy and lease-backed ownership. Missing writer
// metadata is rejected too: transition support does not relax S2.
func (d *Directory) decodeRecord(e *wireEntity) (registration, *nodeLease, error) {
	in, ok, err := toInstance(e)
	if err != nil {
		return registration{}, nil, err
	}
	node := ""
	if ok {
		node = in.NodeID
	}
	if e.NodeLease != nil {
		if ok || !strings.HasPrefix(e.EntityID, leasePrefix) {
			return registration{}, nil, errors.New("invalid node lease entity ID or mixed components")
		}
		node = strings.TrimPrefix(e.EntityID, leasePrefix)
	}
	if !ok && e.NodeLease == nil {
		return registration{}, nil, nil
	}
	if node == "" || e.OriginNodeID != node {
		d.count(func(s *Stats) { s.WriterRejected++ })
		d.logger.Warn("rejecting node-scoped directory record: writer mismatch", "entity_id", e.EntityID, "node_id", node, "writer_node_id", e.OriginNodeID)
		return registration{}, nil, nil
	}
	if e.IsLive == nil || !*e.IsLive {
		return registration{}, nil, nil
	}
	if l := e.NodeLease; l != nil {
		if l.Seq == nil || *l.Seq == 0 || l.Incarnation == "" {
			return registration{}, nil, errors.New("incomplete node lease")
		}
		return registration{}, &nodeLease{OriginNodeID: node, Seq: *l.Seq, Incarnation: l.Incarnation}, nil
	}
	if e.ServiceRegistration.RegistrarIncarnation == "" && in.Expiry.IsZero() {
		return registration{}, nil, errors.New("legacy registration without expiry or registrar incarnation")
	}
	return registration{Instance: in, OriginNodeID: node, Incarnation: e.ServiceRegistration.RegistrarIncarnation}, nil, nil
}

func (d *Directory) handle(event, data string, pending *snapshot) *snapshot {
	if event == "error" {
		d.malformed("mesh stream error", nil, data)
		return pending
	}
	var msg wireResponse
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		d.malformed("undecodable event", err, data)
		return pending
	}
	switch msg.Phase {
	case "STREAM_PHASE_SNAPSHOT":
		if pending == nil {
			pending = newSnapshot()
		}
		if msg.Entity == nil {
			d.malformed("snapshot without entity", nil, data)
			return pending
		}
		in, lease, err := d.decodeRecord(msg.Entity)
		if err != nil {
			d.malformed("bad snapshot entity", err, data)
		} else if lease != nil {
			pending.leases[lease.OriginNodeID] = *lease
		} else if in.ServiceName != "" {
			pending.instances[msg.Entity.EntityID] = in
		}
		return pending
	case "STREAM_PHASE_SNAPSHOT_COMPLETE":
		if pending == nil {
			pending = newSnapshot()
		}
		// A full snapshot is explicitly just-seen, even when seq hasn't advanced.
		mono := d.cfg.MonotonicNow()
		for node, lease := range pending.leases {
			lease.Seen = mono
			pending.leases[node] = lease
		}
		d.mu.Lock()
		d.entities, d.leases = pending.instances, pending.leases
		d.synced, d.stale, d.dirty = true, false, true
		d.cursor = msg.Cursor
		d.mu.Unlock()
		d.count(func(s *Stats) { s.Snapshots++ })
		d.logger.Info("service directory snapshot applied", "instances", len(pending.instances), "leases", len(pending.leases))
		d.saveCacheIfDirty(true)
		d.notify()
		return nil
	case "STREAM_PHASE_DELTA":
		if pending != nil {
			d.malformed("delta before snapshot completed", nil, data)
			return pending
		}
		e := msg.Entity
		if e == nil || e.EntityID == "" {
			d.malformed("delta without entity", nil, data)
			return nil
		}
		remove := msg.EventType == "ENTITY_EVENT_TYPE_DELETED" || msg.EventType == "ENTITY_EVENT_TYPE_EXPIRED" || (e.IsLive != nil && !*e.IsLive)
		// Deletions carry the last visible entity. Check its ownership as well.
		in, lease, err := d.decodeRecord(e)
		if err != nil {
			d.malformed("bad delta entity", err, data)
		}
		mono := d.cfg.MonotonicNow()
		d.mu.Lock()
		delete(d.entities, e.EntityID)
		if strings.HasPrefix(e.EntityID, leasePrefix) {
			node := strings.TrimPrefix(e.EntityID, leasePrefix)
			if remove || lease == nil {
				delete(d.leases, node)
			} else {
				old, exists := d.leases[node]
				if !exists || old.Incarnation != lease.Incarnation || lease.Seq > old.Seq {
					lease.Seen = mono
					d.leases[node] = *lease
				}
			}
		} else if !remove && err == nil && in.ServiceName != "" {
			d.entities[e.EntityID] = in
		}
		if msg.Cursor != "" {
			d.cursor = msg.Cursor
		}
		d.dirty = true
		d.mu.Unlock()
		d.count(func(s *Stats) { s.Deltas++ })
		d.notify()
		return nil
	default:
		d.malformed("unknown phase "+msg.Phase, nil, data)
		return pending
	}
}
