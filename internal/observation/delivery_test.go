package observation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/delivery"
	"github.com/outpostos/OutpostNet/pkg/state"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestDeliveryMetricSamplesUseLocalFacts(t *testing.T) {
	const token = "observation-secret-token"
	wire, err := delivery.NewHTTPTransport(delivery.HTTPTransportConfig{NodeID: "a", AuthToken: token, PeerProvider: state.NewStaticPeerProvider(nil), PeerIdentity: func(string) string { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	out, err := delivery.NewOutbox(delivery.Config{Dir: t.TempDir(), NodeID: "a", Transport: wire})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	_, err = out.Send(context.Background(), delivery.Envelope{DestinationNodeID: "b", Mailbox: "commands", Category: "test", Payload: []byte(token), ExpiryTime: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(context.Background()); err == nil {
		t.Fatal("unknown destination succeeded")
	}
	r := testReporter(128)
	r.UpdateDelivery(out.DeliveryStatus())
	events, _, cancel, err := r.Log().SubscribeAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	found := map[string]float64{}
	for _, e := range events {
		data, err := protojson.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), token) {
			t.Fatal("secret in observation")
		}
		m := e.GetMetricSample()
		if m == nil {
			t.Fatal("expected existing MetricSample contract")
		}
		if m.Labels["peer_node_id"] != "b" || m.Labels["box"] != "outbox" {
			t.Fatalf("labels: %v", m.Labels)
		}
		found[m.Name] = m.GetValue()
	}
	if found["delivery_pending_envelopes"] != 1 || found["delivery_send_failure_total"] != 1 || found["delivery_unknown_peer_total"] != 1 || found["delivery_pending_bytes"] <= 0 {
		t.Fatalf("metrics: %v", found)
	}
	// Gauges/counters and observations must never include opaque message data.
	raw, _ := json.Marshal(out.DeliveryStatus())
	if strings.Contains(string(raw), token) {
		t.Fatal("secret in status")
	}
}
