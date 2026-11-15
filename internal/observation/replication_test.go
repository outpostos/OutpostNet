package observation

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestResyncLossMetricSamples(t *testing.T) {
	r := testReporter(32)
	start := r.Log().CurrentSeq()
	status := state.ReplicationStatus{
		Local: state.LocalStatus{Resync: state.ResyncStatus{PurgedLocalOrigin: 2, PurgedForwarded: 1, LatestArchivePath: "archive/resync-purge-test.jsonl", ByWriter: []state.ResyncPurgeCounter{{WriterNodeID: "local", Peer: "peer", LocalOrigin: true, Entries: 2}, {WriterNodeID: "third", Peer: "peer", Entries: 1}}}},
		Peers: []state.PeerReplicationStatus{{Peer: "peer", UnreachableFor: 8 * time.Second, ResyncLossLimitApproaching: true, ResyncLossWarnings: 1}},
	}
	r.UpdateReplication("example.state", status)
	events, _, cancel, err := r.Log().SubscribeAfter(start)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	seen := map[string]bool{}
	for _, event := range events {
		m := event.GetMetricSample()
		if m == nil || m.Labels["scope"] != "example.state" {
			t.Fatalf("bad metric: %+v", event)
		}
		switch m.Name {
		case "state_resync_purged_entries_total":
			if m.Labels["writer_node_id"] == "local" {
				if m.Labels["local_origin"] != "true" || m.GetValue() != 2 {
					t.Fatal(m)
				}
			} else if m.Labels["writer_node_id"] == "third" {
				if m.Labels["local_origin"] != "false" || m.GetValue() != 1 {
					t.Fatal(m)
				}
			} else {
				t.Fatal(m)
			}
			if m.Labels["peer"] != "peer" {
				t.Fatal(m)
			}
		case "state_resync_latest_archive":
			if m.Labels["path"] != "archive/resync-purge-test.jsonl" || m.GetValue() != 1 {
				t.Fatal(m)
			}
		case "state_resync_loss_limit_approaching":
			if m.GetValue() != 1 || m.Labels["warning"] != "resync loss limit approaching" {
				t.Fatal(m)
			}
		case "state_peer_unreachable_seconds":
			if m.GetValue() != 8 {
				t.Fatal(m)
			}
		case "state_resync_loss_warnings_total":
			if m.GetValue() != 1 {
				t.Fatal(m)
			}
		default:
			t.Fatalf("unknown metric: %s", m.Name)
		}
		seen[m.Name] = true
	}
	if len(events) != 6 || len(seen) != 5 {
		t.Fatalf("missing metric samples: %+v", seen)
	}
}
