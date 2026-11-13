package service

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeDir struct {
	mu    sync.Mutex
	insts []Instance
	fresh Freshness
}

func (d *fakeDir) Instances(string) ([]Instance, Freshness) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Instance(nil), d.insts...), d.fresh
}
func (d *fakeDir) Subscribe(func()) func() { return func() {} }

type fakePeers struct {
	mu    sync.Mutex
	calls []string
	errs  map[string]error
}

func (p *fakePeers) OpenConfirmedPeerStream(_ context.Context, nodeID, svc string) (io.ReadWriteCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, nodeID+"/"+svc)
	if err := p.errs[nodeID]; err != nil {
		return nil, err
	}
	a, b := net.Pipe()
	go func() { io.Copy(b, b) }()
	return a, nil
}

func live(id, node string) Instance {
	return Instance{ServiceName: "echo", InstanceID: id, NodeID: node, Endpoints: []Endpoint{{Protocol: "tcp", Port: 7000}}, Health: HealthReady, Live: true}
}

func newOpener(t *testing.T, dir *fakeDir, peers *fakePeers, dial func(context.Context, uint16) (io.ReadWriteCloser, error)) (*LogicalOpener, *[]Resolution) {
	t.Helper()
	var res []Resolution
	o, err := NewLogicalOpener(LogicalConfig{Self: "node-a", Directory: dir, Peers: peers, DialLocal: dial, OnResolve: func(r Resolution) { res = append(res, r) }})
	if err != nil {
		t.Fatal(err)
	}
	return o, &res
}

var logicalEcho = Target{Service: "echo", Logical: true}

func TestLogicalPrefersLocalInstance(t *testing.T) {
	dir := &fakeDir{insts: []Instance{live("remote", "node-b"), live("local", "node-a")}, fresh: Freshness{Synced: true}}
	peers := &fakePeers{}
	var dialed []uint16
	o, res := newOpener(t, dir, peers, func(_ context.Context, port uint16) (io.ReadWriteCloser, error) {
		dialed = append(dialed, port)
		a, _ := net.Pipe()
		return a, nil
	})
	s, err := o.OpenStream(context.Background(), logicalEcho)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rs := s.(*ResolvedStream)
	if !rs.Local || rs.Instance.InstanceID != "local" || len(dialed) != 1 || dialed[0] != 7000 || len(peers.calls) != 0 {
		t.Fatalf("resolved %+v dialed %v peers %v", rs, dialed, peers.calls)
	}
	if node, addr := rs.ResolvedBackend(); node != "node-a" || addr != "127.0.0.1:7000" {
		t.Fatalf("backend = %s %s", node, addr)
	}
	if r := (*res)[0]; r.Attempts != 1 || r.Reason != "" || r.Candidates != 2 {
		t.Fatalf("resolution = %+v", r)
	}
}

// Acceptance 3 (unit): a failed first candidate falls through to the next
// on the same connection; nothing was forwarded because OpenStream had not
// returned yet.
func TestLogicalFailsOverBeforeAnyByte(t *testing.T) {
	dir := &fakeDir{insts: []Instance{live("local", "node-a"), live("remote", "node-b")}, fresh: Freshness{Synced: true}}
	peers := &fakePeers{}
	o, res := newOpener(t, dir, peers, func(context.Context, uint16) (io.ReadWriteCloser, error) {
		return nil, errors.New("connection refused")
	})
	s, err := o.OpenStream(context.Background(), logicalEcho)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.(*ResolvedStream).Instance.InstanceID; got != "remote" {
		t.Fatalf("resolved %s", got)
	}
	r := (*res)[0]
	if r.Attempts != 2 || len(r.Errors) != 1 || !strings.Contains(r.Errors[0].Error(), "connection refused") {
		t.Fatalf("resolution = %+v", r)
	}
	if st := o.Stats(); st.Resolved != 1 || st.FailedOver != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// Acceptance 4: the three failure reasons.
func TestLogicalFailureReasons(t *testing.T) {
	cases := []struct {
		name   string
		dir    *fakeDir
		peers  *fakePeers
		reason string
		err    error
	}{
		{"empty fresh directory", &fakeDir{fresh: Freshness{Synced: true}}, &fakePeers{}, ReasonNoCandidate, ErrNoCandidate},
		{"only unusable instances", &fakeDir{insts: []Instance{func() Instance { i := live("x", "node-b"); i.Health = HealthDraining; return i }()}, fresh: Freshness{Synced: true}}, &fakePeers{}, ReasonNoCandidate, ErrNoCandidate},
		{"stale empty directory", &fakeDir{fresh: Freshness{Synced: true, Stale: true}}, &fakePeers{}, ReasonDirectoryStale, ErrDirectoryStale},
		{"never synced", &fakeDir{}, &fakePeers{}, ReasonDirectoryStale, ErrDirectoryStale},
		{"all unreachable", &fakeDir{insts: []Instance{live("b", "node-b"), live("c", "node-c")}, fresh: Freshness{Synced: true}},
			&fakePeers{errs: map[string]error{"node-b": errors.New("no route"), "node-c": errors.New("dial failed")}}, ReasonAllUnreachable, ErrAllUnreachable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, res := newOpener(t, tc.dir, tc.peers, nil)
			_, err := o.OpenStream(context.Background(), logicalEcho)
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want %v", err, tc.err)
			}
			if r := (*res)[0]; r.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q", r.Reason, tc.reason)
			}
			if st := o.Stats(); st.Failures[tc.reason] != 1 || st.Resolved != 0 {
				t.Fatalf("stats = %+v", st)
			}
		})
	}
}

// A stale directory is still used, and the success is counted as such.
func TestLogicalUsesStaleDirectory(t *testing.T) {
	dir := &fakeDir{insts: []Instance{live("b", "node-b")}, fresh: Freshness{Synced: false, Stale: true}}
	o, res := newOpener(t, dir, &fakePeers{}, nil)
	s, err := o.OpenStream(context.Background(), logicalEcho)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if !(*res)[0].Stale || o.Stats().ResolvedFromStale != 1 {
		t.Fatalf("stale success not recorded: %+v %+v", (*res)[0], o.Stats())
	}
}

func TestLogicalSkipsUnreachableNodes(t *testing.T) {
	dir := &fakeDir{insts: []Instance{live("b", "node-b"), live("c", "node-c")}, fresh: Freshness{Synced: true}}
	peers := &fakePeers{}
	o, err := NewLogicalOpener(LogicalConfig{Self: "node-a", Directory: dir, Peers: peers, Reachable: func(n string) bool { return n == "node-c" }})
	if err != nil {
		t.Fatal(err)
	}
	s, err := o.OpenStream(context.Background(), logicalEcho)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if len(peers.calls) != 1 || peers.calls[0] != "node-c/echo" {
		t.Fatalf("peer calls = %v", peers.calls)
	}
}

func TestLogicalAttemptTimeout(t *testing.T) {
	dir := &fakeDir{insts: []Instance{live("local", "node-a"), live("remote", "node-b")}, fresh: Freshness{Synced: true}}
	o, err := NewLogicalOpener(LogicalConfig{Self: "node-a", Directory: dir, Peers: &fakePeers{}, AttemptTimeout: 50 * time.Millisecond,
		DialLocal: func(ctx context.Context, _ uint16) (io.ReadWriteCloser, error) { <-ctx.Done(); return nil, ctx.Err() }})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	s, err := o.OpenStream(context.Background(), logicalEcho)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("hung local attempt took %s", d)
	}
}

func TestLogicalRejectsNonLogicalTarget(t *testing.T) {
	o, _ := newOpener(t, &fakeDir{}, &fakePeers{}, nil)
	if _, err := o.OpenStream(context.Background(), Target{Service: "echo", NodeID: "node-b"}); err == nil {
		t.Fatal("node-bound target accepted")
	}
	if _, err := NetOpener(nil).OpenStream(context.Background(), logicalEcho); err == nil {
		t.Fatal("NetOpener accepted a logical target")
	}
}

func TestLogicalTargetValidate(t *testing.T) {
	if err := (Target{Service: "echo", Logical: true}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Target{{Service: "Echo_Svc", Logical: true}, {Service: "echo", Logical: true, NodeID: "n"}, {Service: "echo", Logical: true, Region: "r"}} {
		if err := bad.Validate(); err == nil {
			t.Errorf("Validate accepted %+v", bad)
		}
	}
	if got := logicalEcho.String(); got != "echo (logical)" {
		t.Fatalf("String = %q", got)
	}
}
