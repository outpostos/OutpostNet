package meshdirectory

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
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
// them exactly as Mesh writes them. Pre-originNodeId gateways are rejected
// by S2; transition registrations must be read through an upgraded gateway.
func TestGoldenMeshStreamWithoutWriterIsRejected(t *testing.T) {
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
	if len(got) != 0 || !fr.Synced {
		t.Fatalf("after golden stream = %v %+v", uids(got), fr)
	}
	if st := d.Stats(); st.Malformed != 0 || st.Snapshots != 1 || st.Deltas != 4 || st.WriterRejected != 5 {
		t.Fatalf("stats = %+v", st)
	}
	// Reconnect: the gateway answered the stale cursor with a fresh snapshot
	// (uid-c already gone) and then the EXPIRED delta of uid-c.
	if _, err := d.follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.Instances("echo"); len(got) != 0 {
		t.Fatalf("after fallback = %v", uids(got))
	}
	if st := d.Stats(); st.Malformed != 0 || st.Snapshots != 2 || st.Deltas != 5 || st.WriterRejected != 7 {
		t.Fatalf("stats after fallback = %+v", st)
	}
}
