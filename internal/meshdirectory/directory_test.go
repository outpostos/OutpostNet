package meshdirectory

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/service"
)

// sseServer scripts one handler per connection. Each handler gets the
// request (to inspect resumeCursor) and a writer that flushes every event.
type sseServer struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	conns    []func(r *http.Request, send func(string))
	requests []*http.Request
	hold     chan struct{} // closed at cleanup to release held connections
}

func newSSEServer(t *testing.T, conns ...func(r *http.Request, send func(string))) *sseServer {
	s := &sseServer{t: t, conns: conns, hold: make(chan struct{})}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(func() { close(s.hold); s.srv.CloseClientConnections(); s.srv.Close() })
	return s
}

func (s *sseServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != streamPath || r.Header.Get("Accept") != "text/event-stream" ||
		r.URL.Query().Get("filter.requiredComponents") != streamFilter {
		s.t.Errorf("unexpected request %s %s accept=%q", r.Method, r.URL, r.Header.Get("Accept"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	i := len(s.requests)
	s.requests = append(s.requests, r)
	var h func(*http.Request, func(string))
	if i < len(s.conns) {
		h = s.conns[i]
	}
	s.mu.Unlock()
	if h == nil {
		// No more scripted connections: refuse, like an unavailable gateway.
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f := w.(http.Flusher)
	f.Flush()
	h(r, func(ev string) {
		fmt.Fprint(w, ev)
		f.Flush()
	})
}

func (s *sseServer) request(i int) *http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i >= len(s.requests) {
		return nil
	}
	return s.requests[i]
}

// holdOpen keeps a connection open until the test ends.
func (s *sseServer) holdOpen(r *http.Request) {
	select {
	case <-s.hold:
	case <-r.Context().Done():
	}
}

func entityJSON(id, svc, uid, health string, live bool, expiry time.Time, weight int) string {
	w := ""
	if weight >= 0 {
		w = fmt.Sprintf(`, "weight":%d`, weight)
	}
	return fmt.Sprintf(`{"entityId":%q, "isLive":%t, "createdTime":"2026-11-13T08:00:00Z", "expiryTime":%q, "provenance":{"integrationName":"outpostlet"}, "serviceRegistration":{"serviceName":%q, "instanceId":%q, "nodeId":"node-a", "endpoints":[{"port":{"protocol":"ENDPOINT_PROTOCOL_TCP", "port":18080}%s}], "healthState":%q}}`,
		id, live, expiry.UTC().Format(time.RFC3339Nano), svc, uid, w, health)
}

func snap(entity, cursor string) string {
	return fmt.Sprintf("event: snapshot\ndata: {\"phase\":\"STREAM_PHASE_SNAPSHOT\", \"entity\":%s, \"cursor\":%q}\n\n", entity, cursor)
}

func complete(cursor string) string {
	return fmt.Sprintf("event: snapshot-complete\ndata: {\"phase\":\"STREAM_PHASE_SNAPSHOT_COMPLETE\", \"cursor\":%q}\n\n", cursor)
}

func delta(eventType, entity, cursor string) string {
	return fmt.Sprintf("event: delta\ndata: {\"phase\":\"STREAM_PHASE_DELTA\", \"eventType\":%q, \"entity\":%s, \"cursor\":%q}\n\n", eventType, entity, cursor)
}

var far = time.Now().Add(time.Hour)

const ready = "SERVICE_HEALTH_STATE_READY"

func id(uid string) string { return "urn:outpostos:service-instance:node-a~echo~" + uid }

type harness struct {
	d       *Directory
	cancel  context.CancelFunc
	done    chan struct{}
	notices atomic.Int64
}

func startDir(t *testing.T, cfg Config) *harness {
	t.Helper()
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 50 * time.Millisecond
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = 20 * time.Millisecond
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{d: d, done: make(chan struct{})}
	d.Subscribe(func() { h.notices.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { d.Run(ctx); close(h.done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not stop")
		}
	})
	return h
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func uids(insts []service.Instance) string {
	var out []string
	for _, in := range insts {
		out = append(out, in.InstanceID+"/"+in.Health.String())
	}
	return strings.Join(out, ",")
}

func hasState(d *Directory, want string) func() bool {
	return func() bool {
		got, _ := d.Instances("echo")
		return uids(got) == want
	}
}

// Acceptance 1: snapshot, then delta add / update / delete.
func TestSnapshotThenDeltas(t *testing.T) {
	var s *sseServer
	s = newSSEServer(t, func(r *http.Request, send func(string)) {
		send(": heartbeat\n\n")
		send(snap(entityJSON(id("a"), "echo", "a", ready, true, far, 100), "c1"))
		send(snap(entityJSON("urn:outpostos:service-instance:node-a~other~x", "other", "x", ready, true, far, -1), "c1"))
		send(complete("c1"))
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", entityJSON(id("b"), "echo", "b", "SERVICE_HEALTH_STATE_NOT_READY", true, far, 5), "c2"))
		send(delta("ENTITY_EVENT_TYPE_OVERRIDDEN", entityJSON(id("b"), "echo", "b", ready, true, far, 5), "c3"))
		send(delta("ENTITY_EVENT_TYPE_DELETED", entityJSON(id("a"), "echo", "a", ready, false, far, 100), "c4"))
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", entityJSON(id("c"), "echo", "c", ready, true, far, -1), "c5"))
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", entityJSON(id("c"), "echo", "c", ready, false, far, -1), "c6"))
		s.holdOpen(r)
	})
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	eventually(t, "final state", hasState(h.d, "b/READY"))
	got, fr := h.d.Instances("echo")
	if !fr.Synced || fr.Stale || fr.AsOf.IsZero() {
		t.Fatalf("freshness = %+v", fr)
	}
	b := got[0]
	if b.NodeID != "node-a" || b.EffectiveWeight() != 5 || len(b.Endpoints) != 1 || b.Endpoints[0] != (service.Endpoint{Protocol: "tcp", Port: 18080}) || !b.Live {
		t.Fatalf("instance b = %+v", b)
	}
	if other, _ := h.d.Instances("other"); len(other) != 1 || other[0].Weight != nil {
		t.Fatalf("other = %+v", other)
	}
	st := h.d.Stats()
	if st.Snapshots != 1 || st.Deltas != 5 || st.Malformed != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if h.notices.Load() < 6 {
		t.Fatalf("notifications = %d, want one per change", h.notices.Load())
	}
}

// Acceptance 1: a snapshot is not visible until SNAPSHOT_COMPLETE, then
// replaces the directory at once.
func TestSnapshotAppliedAtomicallyOnComplete(t *testing.T) {
	release := make(chan struct{})
	s := newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(entityJSON(id("a"), "echo", "a", ready, true, far, -1), "c1"))
		send(snap(entityJSON(id("b"), "echo", "b", ready, true, far, -1), "c1"))
		<-release
		send(complete("c1"))
	})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	time.Sleep(100 * time.Millisecond)
	if got, fr := h.d.Instances("echo"); len(got) != 0 || fr.Synced || !fr.Stale {
		t.Fatalf("partial snapshot visible: %v %+v", uids(got), fr)
	}
	close(release)
	eventually(t, "snapshot", hasState(h.d, "a/READY,b/READY"))
}

// Acceptance 1: after a disconnect the stream resumes from the last cursor;
// replayed deltas are applied idempotently (nothing lost, nothing doubled).
func TestReconnectResumesFromCursor(t *testing.T) {
	var s *sseServer
	s = newSSEServer(t,
		func(r *http.Request, send func(string)) {
			send(snap(entityJSON(id("a"), "echo", "a", ready, true, far, -1), "c1"))
			send(complete("c1"))
			send(delta("ENTITY_EVENT_TYPE_PUBLISHED", entityJSON(id("b"), "echo", "b", ready, true, far, -1), "c2"))
			// connection drops here
		},
		func(r *http.Request, send func(string)) {
			if got := r.URL.Query().Get("resumeCursor"); got != "c2" {
				t.Errorf("resumeCursor = %q, want c2", got)
			}
			// Mesh replays only later deltas.
			send(delta("ENTITY_EVENT_TYPE_PUBLISHED", entityJSON(id("c"), "echo", "c", ready, true, far, -1), "c3"))
			send(delta("ENTITY_EVENT_TYPE_DELETED", entityJSON(id("a"), "echo", "a", ready, false, far, -1), "c4"))
			s.holdOpen(r)
		})
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	eventually(t, "resumed state", hasState(h.d, "b/READY,c/READY"))
	_, fr := h.d.Instances("echo")
	if fr.Stale || !fr.Synced {
		t.Fatalf("freshness after resume = %+v", fr)
	}
	if st := h.d.Stats(); st.Snapshots != 1 || st.Connects != 2 || st.LastResumeCursor != "c2" {
		t.Fatalf("stats = %+v", st)
	}
}

// Acceptance 1: an unusable cursor makes Mesh send a fresh snapshot, which
// replaces the directory (entries missing from it are gone).
func TestStaleCursorFallsBackToSnapshot(t *testing.T) {
	var s *sseServer
	s = newSSEServer(t,
		func(r *http.Request, send func(string)) {
			send(snap(entityJSON(id("a"), "echo", "a", ready, true, far, -1), "c1"))
			send(snap(entityJSON(id("b"), "echo", "b", ready, true, far, -1), "c1"))
			send(complete("c1"))
		},
		func(r *http.Request, send func(string)) {
			if r.URL.Query().Get("resumeCursor") != "c1" {
				t.Errorf("second connection did not resume")
			}
			send(snap(entityJSON(id("b"), "echo", "b", "SERVICE_HEALTH_STATE_DRAINING", true, far, -1), "n1"))
			send(snap(entityJSON(id("d"), "echo", "d", ready, true, far, -1), "n1"))
			send(complete("n1"))
			s.holdOpen(r)
		})
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	eventually(t, "fallback snapshot", hasState(h.d, "b/DRAINING,d/READY"))
	if st := h.d.Stats(); st.Snapshots != 2 {
		t.Fatalf("snapshots = %d", st.Snapshots)
	}
}

// Acceptance 1: malformed events are skipped and counted; the stream goes on.
func TestMalformedEventsSkipped(t *testing.T) {
	var s *sseServer
	s = newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(entityJSON(id("a"), "echo", "a", ready, true, far, -1), "c1"))
		send(complete("c1"))
		send("event: delta\ndata: {not json\n\n")
		send("event: delta\ndata: {\"phase\":\"STREAM_PHASE_BOGUS\"}\n\n")
		send("event: delta\ndata: {\"phase\":\"STREAM_PHASE_DELTA\", \"eventType\":\"ENTITY_EVENT_TYPE_PUBLISHED\"}\n\n")
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", `{"entityId":"urn:x:y:z", "serviceRegistration":{"serviceName":"echo"}, "expiryTime":"yesterday"}`, "c2"))
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", `{"entityId":"urn:x:y:z2", "serviceRegistration":{}}`, "c3"))
		send("event: error\ndata: {\"code\":\"Internal\"}\n\n")
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", entityJSON(id("b"), "echo", "b", ready, true, far, -1), "c4"))
		// A non-registration entity is ignored without counting as malformed.
		send(delta("ENTITY_EVENT_TYPE_PUBLISHED", `{"entityId":"urn:outpostos:track:t1", "isLive":true}`, "c5"))
		s.holdOpen(r)
	})
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	eventually(t, "valid delta after junk", hasState(h.d, "a/READY,b/READY"))
	if st := h.d.Stats(); st.Malformed != 6 {
		t.Fatalf("malformed = %d, want 6 (%+v)", st.Malformed, st)
	}
}

// Acceptance 2: an expired instance disappears locally and subscribers hear
// about it, without any event from Mesh.
func TestExpiryDropsInstanceAndNotifies(t *testing.T) {
	var s *sseServer
	soon := time.Now().Add(300 * time.Millisecond)
	s = newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(entityJSON(id("a"), "echo", "a", ready, true, soon, -1), "c1"))
		send(snap(entityJSON(id("b"), "echo", "b", ready, true, far, -1), "c1"))
		send(complete("c1"))
		s.holdOpen(r)
	})
	h := startDir(t, Config{GatewayURL: s.srv.URL})
	eventually(t, "snapshot", hasState(h.d, "a/READY,b/READY"))
	before := h.notices.Load()
	eventually(t, "expiry", hasState(h.d, "b/READY"))
	eventually(t, "expiry notification", func() bool { return h.notices.Load() > before })
	if st := h.d.Stats(); st.Expired != 1 {
		t.Fatalf("expired = %d", st.Expired)
	}
}

func writeCache(t *testing.T, path string, insts map[string]service.Instance) {
	t.Helper()
	d := &Directory{cfg: Config{CachePath: path, Now: time.Now, CacheInterval: time.Second}, entities: insts, synced: true, dirty: true}
	d.logger = testLogger()
	d.saveCacheIfDirty(true)
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

// Acceptance 3: with a cache and no Mesh, the directory answers at once,
// stays stale, still expires by TTL, and turns fresh after a snapshot.
func TestColdStartFromCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), CacheFileName)
	w := uint32(7)
	writeCache(t, path, map[string]service.Instance{
		id("a"): {ServiceName: "echo", InstanceID: "a", NodeID: "node-a", Endpoints: []service.Endpoint{{Protocol: "tcp", Port: 18080}}, Weight: &w, Health: service.HealthReady, Live: true, Expiry: far},
		id("x"): {ServiceName: "echo", InstanceID: "x", NodeID: "node-a", Endpoints: []service.Endpoint{{Protocol: "tcp", Port: 18080}}, Health: service.HealthReady, Live: true, Expiry: time.Now().Add(2 * time.Second)},
	})

	// Phase 1: Mesh unreachable.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	h := startDir(t, Config{GatewayURL: deadURL, CachePath: path})
	got, fr := h.d.Instances("echo")
	if uids(got) != "a/READY,x/READY" || !fr.Stale || fr.Synced {
		t.Fatalf("cold start = %v %+v", uids(got), fr)
	}
	if got[0].EffectiveWeight() != 7 {
		t.Fatalf("cached weight lost: %+v", got[0])
	}
	eventually(t, "cached instance expiry", hasState(h.d, "a/READY"))
	if _, fr := h.d.Instances("echo"); !fr.Stale {
		t.Fatal("directory claimed fresh without Mesh")
	}
	h.cancel()
	<-h.done

	// Phase 2: Mesh reachable; the snapshot replaces the cache.
	var s *sseServer
	s = newSSEServer(t, func(r *http.Request, send func(string)) {
		send(snap(entityJSON(id("b"), "echo", "b", ready, true, far, -1), "c1"))
		send(complete("c1"))
		s.holdOpen(r)
	})
	h2 := startDir(t, Config{GatewayURL: s.srv.URL, CachePath: path})
	if _, fr := h2.d.Instances("echo"); !fr.Stale {
		t.Fatal("not stale before first snapshot")
	}
	eventually(t, "fresh snapshot", hasState(h2.d, "b/READY"))
	if _, fr := h2.d.Instances("echo"); fr.Stale || !fr.Synced {
		t.Fatalf("freshness after snapshot = %+v", fr)
	}
	// The cache now holds the snapshot.
	eventually(t, "cache rewritten", func() bool {
		data, err := os.ReadFile(path)
		return err == nil && strings.Contains(string(data), `"instance_id": "b"`) && !strings.Contains(string(data), `"instance_id": "a"`)
	})
}

// Acceptance 3: a corrupt cache is ignored (with a warning), not fatal.
func TestCorruptCacheIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), CacheFileName)
	for _, content := range []string{"{not json", `{"version": 99, "instances": []}`} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		d, err := New(Config{GatewayURL: "http://127.0.0.1:1", CachePath: path})
		if err != nil {
			t.Fatalf("corrupt cache made New fail: %v", err)
		}
		if got, fr := d.Instances("echo"); len(got) != 0 || !fr.Stale {
			t.Fatalf("corrupt cache produced %v %+v", got, fr)
		}
	}
}

// Reconnects back off up to MaxBackoff and record the error.
func TestUnavailableGatewayRetries(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	h := startDir(t, Config{GatewayURL: srv.URL, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 40 * time.Millisecond})
	time.Sleep(400 * time.Millisecond)
	n := hits.Load()
	// 5,10,20,40,40,... ms: about 10-12 attempts in 400ms; never a hot loop.
	if n < 5 || n > 20 {
		t.Fatalf("attempts in 400ms = %d", n)
	}
	if st := h.d.Stats(); !strings.Contains(st.LastConnectError, "503") {
		t.Fatalf("last error = %q", st.LastConnectError)
	}
}

func TestNewRejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "127.0.0.1:8081", "ftp://x"} {
		if _, err := New(Config{GatewayURL: u}); err == nil {
			t.Errorf("New(%q) accepted", u)
		}
	}
}

func TestSubscribeCancel(t *testing.T) {
	d, _ := New(Config{GatewayURL: "http://127.0.0.1:1"})
	var n atomic.Int64
	cancel := d.Subscribe(func() { n.Add(1) })
	d.notify()
	cancel()
	d.notify()
	if n.Load() != 1 {
		t.Fatalf("notifications after cancel = %d", n.Load())
	}
}
