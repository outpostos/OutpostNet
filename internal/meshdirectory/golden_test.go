package meshdirectory

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/service"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The golden transcripts are real gateway output: the local decoder must read
// them exactly as Mesh writes them.
func TestGoldenMeshStream(t *testing.T) {
	recordedAt := time.Date(2026, 11, 13, 7, 43, 34, 0, time.UTC)
	s := newSSEServer(t,
		func(r *http.Request, send func(string)) {
			send(fixture(t, "stream-golden.sse"))
		},
		func(r *http.Request, send func(string)) {
			send(fixture(t, "stream-fallback.sse"))
		})
	clock := recordedAt
	now := func() time.Time { return clock }
	d, err := New(Config{GatewayURL: s.srv.URL, Now: now, Logger: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	// Drive one connection by hand so the clock stays at recording time.
	if _, err := d.follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, fr := d.Instances("echo")
	if uids(got) != "uid-b/READY,uid-c/READY" || !fr.Synced {
		t.Fatalf("after golden stream = %v %+v", uids(got), fr)
	}
	b := got[0]
	want := service.Instance{ServiceName: "echo", InstanceID: "uid-b", NodeID: "node-a", Endpoints: []service.Endpoint{{Protocol: "tcp", Port: 18080}}, Health: service.HealthReady, Live: true, Version: "1.0.0"}
	if b.ServiceName != want.ServiceName || b.NodeID != want.NodeID || b.Version != want.Version || len(b.Endpoints) != 1 || b.Endpoints[0] != want.Endpoints[0] || b.EffectiveWeight() != 100 || b.Expiry.IsZero() {
		t.Fatalf("uid-b = %+v", b)
	}
	if st := d.Stats(); st.Malformed != 0 || st.Snapshots != 1 || st.Deltas != 4 {
		t.Fatalf("stats = %+v", st)
	}
	// Reconnect: the gateway answered the stale cursor with a fresh snapshot
	// (uid-c already gone) and then the EXPIRED delta of uid-c.
	if _, err := d.follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.Instances("echo"); uids(got) != "uid-b/READY" {
		t.Fatalf("after fallback = %v", uids(got))
	}
	if st := d.Stats(); st.Malformed != 0 || st.Snapshots != 2 || st.Deltas != 5 {
		t.Fatalf("stats after fallback = %+v", st)
	}
}
