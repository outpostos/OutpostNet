package servicehost_test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeMesh is a minimal OutpostMesh gateway for host tests: it serves the
// SSE entity stream (snapshot, snapshot-complete, then deltas) of the
// ServiceRegistration entities set with register/withdraw. The stream wire
// format follows OutpostNet internal/meshdirectory testdata (recorded from a
// real gateway).
type fakeMesh struct {
	srv *httptest.Server

	mu      sync.Mutex
	state   map[string]string // entity id -> entity JSON
	subs    map[chan string]struct{}
	cursor  int
	streams int
}

func newFakeMesh(t *testing.T) *fakeMesh {
	t.Helper()
	m := &fakeMesh{state: map[string]string{}, subs: map[chan string]struct{}{}}
	m.srv = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(func() { m.srv.CloseClientConnections(); m.srv.Close() })
	return m
}

func (m *fakeMesh) url() string { return m.srv.URL }

func (m *fakeMesh) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1alpha1/entities:stream" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	ch := make(chan string, 64)
	m.mu.Lock()
	m.streams++
	ids := make([]string, 0, len(m.state))
	for id := range m.state {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	m.cursor++
	cur := m.cursor
	var snapshot []string
	for _, id := range ids {
		snapshot = append(snapshot, fmt.Sprintf("event: snapshot\ndata: {\"phase\":\"STREAM_PHASE_SNAPSHOT\", \"entity\":%s, \"cursor\":\"%d\"}\n\n", m.state[id], cur))
	}
	m.subs[ch] = struct{}{}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.subs, ch)
		m.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f := w.(http.Flusher)
	for _, ev := range snapshot {
		fmt.Fprint(w, ev)
	}
	fmt.Fprintf(w, "event: snapshot-complete\ndata: {\"phase\":\"STREAM_PHASE_SNAPSHOT_COMPLETE\", \"cursor\":\"%d\"}\n\n", cur)
	f.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			fmt.Fprint(w, ev)
			f.Flush()
		}
	}
}

func (m *fakeMesh) broadcast(eventType, entity string) {
	m.cursor++
	ev := fmt.Sprintf("event: delta\ndata: {\"phase\":\"STREAM_PHASE_DELTA\", \"eventType\":%q, \"entity\":%s, \"cursor\":\"%d\"}\n\n", eventType, entity, m.cursor)
	for ch := range m.subs {
		ch <- ev
	}
}

// reg is one ServiceRegistration as Let would publish it.
type reg struct {
	service, uid, node string
	port               int
	health             string // default READY
	live               bool
	expiry             time.Time // default +1h
	weight             int       // 0 = unset
}

func (r reg) id() string {
	return "urn:outpostos:service-instance:" + r.node + "~" + r.service + "~" + r.uid
}

func (r reg) json() string {
	h := r.health
	if h == "" {
		h = "SERVICE_HEALTH_STATE_READY"
	}
	exp := r.expiry
	if exp.IsZero() {
		exp = time.Now().Add(time.Hour)
	}
	w := ""
	if r.weight > 0 {
		w = fmt.Sprintf(`, "weight":%d`, r.weight)
	}
	return fmt.Sprintf(`{"entityId":%q, "isLive":%t, "createdTime":"2026-11-13T08:00:00Z", "expiryTime":%q, "provenance":{"integrationName":"outpostlet"}, "serviceRegistration":{"serviceName":%q, "instanceId":%q, "nodeId":%q, "endpoints":[{"port":{"protocol":"ENDPOINT_PROTOCOL_TCP", "port":%d}%s}], "healthState":%q}}`,
		r.id(), r.live, exp.UTC().Format(time.RFC3339Nano), r.service, r.uid, r.node, r.port, w, h)
}

// register publishes (or replaces) a live registration.
func (m *fakeMesh) register(r reg) {
	r.live = true
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[r.id()] = r.json()
	m.broadcast("ENTITY_EVENT_TYPE_PUBLISHED", r.json())
}

// withdraw publishes is_live=false, as Let does for a vanished Pod.
func (m *fakeMesh) withdraw(r reg) {
	r.live = false
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.state, r.id())
	m.broadcast("ENTITY_EVENT_TYPE_DELETED", r.json())
}

func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.Atoi(p)
	return n
}

// countingApp is a tagged line-echo application that counts accepted
// connections and can be killed (listener and every connection closed).
type countingApp struct {
	addr string
	ln   net.Listener

	mu       sync.Mutex
	accepted int
	conns    []net.Conn
}

func startCountingApp(t *testing.T, tag string) *countingApp {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := &countingApp{addr: ln.Addr().String(), ln: ln}
	t.Cleanup(a.kill)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			a.mu.Lock()
			a.accepted++
			a.conns = append(a.conns, c)
			a.mu.Unlock()
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					// echo with tag prefix, line oriented
					if _, err := c.Write(append([]byte(tag+":"), buf[:n]...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return a
}

func (a *countingApp) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.accepted
}

func (a *countingApp) kill() {
	a.ln.Close()
	a.mu.Lock()
	for _, c := range a.conns {
		c.Close()
	}
	a.mu.Unlock()
}
