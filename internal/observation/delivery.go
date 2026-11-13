package observation

import (
	"github.com/outpostos/OutpostNet/pkg/delivery"
	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"
)

// UpdateDelivery samples boxes embedded in this process into the existing
// bounded MetricSample event timeline. Hosts call it on their sampling tick;
// no new exporter or dependency is introduced into the delivery library.
// Metric samples are timeline events, not fields of the contract's Snapshot.
func (r *Reporter) UpdateDelivery(statuses ...delivery.DeliveryStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range statuses {
		for _, p := range s.Peers {
			samples := []struct {
				name  string
				value float64
			}{
				{"delivery_pending_envelopes", float64(p.PendingEnvelopes)},
				{"delivery_pending_bytes", float64(p.PendingBytes)},
				{"delivery_pending_receipts", float64(p.PendingReceipts)},
				{"delivery_pending_receipt_bytes", float64(p.PendingReceiptBytes)},
				{"delivery_send_success_total", float64(p.Counters.SendSuccess)},
				{"delivery_send_failure_total", float64(p.Counters.SendFailure)},
				{"delivery_unknown_peer_total", float64(p.Counters.UnknownPeer)},
				{"delivery_duplicate_total", float64(p.Counters.Duplicate)},
				{"delivery_expired_total", float64(p.Counters.Expired)},
				{"delivery_rejected_total", float64(p.Counters.Rejected)},
				{"delivery_inbox_full_total", float64(p.Counters.InboxFull)},
				{"delivery_outbox_full_total", float64(p.Counters.OutboxFull)},
			}
			if !p.OldestPendingTime.IsZero() {
				samples = append(samples, struct {
					name  string
					value float64
				}{"delivery_oldest_pending_seconds", p.OldestPendingAge.Seconds()})
			} else if p.PendingEnvelopes+p.PendingReceipts == 0 {
				samples = append(samples, struct {
					name  string
					value float64
				}{"delivery_oldest_pending_seconds", 0})
			}
			for _, sample := range samples {
				value := sample.value
				r.log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_MetricSample{MetricSample: &observationv1alpha1.MetricSample{Name: sample.name, Value: &value, Labels: map[string]string{"peer_node_id": p.PeerNodeID, "box": s.Kind}}}})
			}
		}
	}
}
