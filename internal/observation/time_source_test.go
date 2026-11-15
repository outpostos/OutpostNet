package observation

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestTimeSourceObservationUsesExistingMetricEvents(t *testing.T) {
	r := testReporter(16)
	start := r.Log().CurrentSeq()
	r.UpdateTime(state.TimeStatus{Quality: state.TimeHoldover, Source: state.TimeSourceStatus{Source: "chrony:ntp.example", State: "synchronized", Synchronized: true, Uncertainty: time.Millisecond}})
	events, _, cancel, err := r.Log().SubscribeAfter(start)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(events) != 2 {
		t.Fatalf("time metrics: %v", events)
	}
	quality := events[0].GetMetricSample()
	if quality.GetName() != "net_time_quality" || quality.GetValue() != 1 || quality.GetLabels()["quality"] != "HOLDOVER" || quality.GetLabels()["source_state"] != "synchronized" {
		t.Fatal(quality)
	}
	if events[1].GetMetricSample().GetName() != "net_time_source_uncertainty_seconds" {
		t.Fatal(events[1])
	}
}
