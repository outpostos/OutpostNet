package observation

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	observationv1alpha1 "github.com/outpostos/OutpostRange/gen/go/outpostrange/observation/v1alpha1"

	"github.com/outpostos/OutpostNet/internal/observation/observationtest"
	"github.com/outpostos/OutpostNet/pkg/service"
)

func testReporter(capacity int) *Reporter {
	return NewReporter(ReporterConfig{NodeID: "node-a", Component: ComponentService, EventBufferSize: capacity})
}

func TestEventLogSequenceEpochRingAndGap(t *testing.T) {
	start := time.UnixMilli(1_790_899_200_000)
	log := NewEventLog("node-a", observationv1alpha1.Component_COMPONENT_OUTPOSTNET, 3, func() time.Time { return start }, nil)
	epoch := uint64(start.UnixMilli()) << 20
	if got := log.CurrentSeq(); got != epoch {
		t.Fatalf("empty log seq = %d, want epoch base %d", got, epoch)
	}
	for i := 0; i < 5; i++ {
		log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_NetPeerChanged{NetPeerChanged: &observationv1alpha1.NetPeerChanged{PeerAddress: "x"}}})
	}
	if got := log.CurrentSeq(); got != epoch+5 {
		t.Fatalf("seq = %d, want %d", got, epoch+5)
	}
	if log.Dropped() != 2 {
		t.Fatalf("dropped = %d, want 2", log.Dropped())
	}
	backlog, _, cancel, err := log.SubscribeAfter(epoch + 2)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if len(backlog) != 3 || backlog[0].GetSeq() != epoch+3 {
		t.Fatalf("backlog = %v", backlog)
	}
	if _, _, _, err := log.SubscribeAfter(epoch + 1); !errors.Is(err, ErrEventGap) {
		t.Fatalf("resume before the retained window: err = %v, want ErrEventGap", err)
	}
	// A seq from an earlier process epoch is a restart, not a gap.
	backlog, _, cancel, err = log.SubscribeAfter(epoch - 10)
	if err != nil || len(backlog) != 3 {
		t.Fatalf("earlier epoch: backlog=%d err=%v", len(backlog), err)
	}
	cancel()
}

func TestSlowSubscriberNeverBlocksAppend(t *testing.T) {
	log := NewEventLog("node-a", observationv1alpha1.Component_COMPONENT_OUTPOSTNET, 8, nil, nil)
	_, ch, cancel, err := log.SubscribeAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < subscriberBuffer+10; i++ {
			log.Append(&observationv1alpha1.Event{Body: &observationv1alpha1.Event_NetPeerChanged{NetPeerChanged: &observationv1alpha1.NetPeerChanged{}}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Append blocked on a subscriber that never reads")
	}
	n := 0
	for range ch {
		n++
	}
	if n != subscriberBuffer {
		t.Fatalf("slow subscriber got %d events before being closed, want %d", n, subscriberBuffer)
	}
}

func TestNetLinksReportOnlyChangesAndRemovals(t *testing.T) {
	r := testReporter(0)
	failedAt := time.Unix(1_790_000_000, 0)
	links := []NetLink{{PeerNodeID: "node-b", PeerAddress: "10.0.0.2:4433", State: LinkConnected}}
	r.UpdateNetLinks(links)
	r.UpdateNetLinks(links)
	if got := r.Log().CurrentSeq() & seqOrdinalMask; got != 1 {
		t.Fatalf("identical sample appended events: ordinal %d", got)
	}
	r.UpdateNetLinks([]NetLink{{PeerNodeID: "node-b", PeerAddress: "10.0.0.2:4433", State: LinkConnecting, LastError: "timeout", LastErrorTime: failedAt}})
	r.UpdateNetLinks([]NetLink{{PeerAddress: "10.0.0.9:4433"}}) // node-b removed; unmeasured state
	backlog, _, cancel, err := r.Log().SubscribeAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if len(backlog) != 4 {
		t.Fatalf("events = %v", backlog)
	}
	if p := backlog[1].GetNetPeerChanged(); p.GetState() != observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTING || p.GetLastError() != "timeout" || !p.GetLastErrorTime().AsTime().Equal(failedAt) {
		t.Fatalf("change event = %v", p)
	}
	if p := backlog[2].GetNetPeerChanged(); p.State != nil || p.GetPeerAddress() != "10.0.0.9:4433" {
		t.Fatalf("unmeasured link must not claim a state: %v", p)
	}
	if p := backlog[3].GetNetPeerChanged(); p.GetPeerNodeId() != "node-b" || p.GetState() != observationv1alpha1.PeerLinkState_PEER_LINK_STATE_DISCONNECTED {
		t.Fatalf("removal event = %v", p)
	}
	snap := r.Snapshot()
	if len(snap.GetNetPeers()) != 1 || snap.GetSeq() != r.Log().CurrentSeq() {
		t.Fatalf("snapshot = %v", snap)
	}
}

type fakeStream struct {
	reads []string
	end   error
}

func (s *fakeStream) Read(p []byte) (int, error) {
	if len(s.reads) == 0 {
		return 0, s.end
	}
	n := copy(p, s.reads[0])
	s.reads = s.reads[1:]
	return n, nil
}
func (s *fakeStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *fakeStream) Close() error                { return nil }

func TestServiceAccessFromRealOutcomes(t *testing.T) {
	r := testReporter(0)
	entry := r.ServiceEntry("echo@node-b")
	entry.SetEntryAddress("127.0.0.1:18080")
	backend := Backend{NodeID: "node-b", Address: "10.0.0.2:4433"}
	var next func() (io.ReadWriteCloser, error)
	opener := entry.Wrap(service.StreamOpenerFunc(func(context.Context, service.Target) (io.ReadWriteCloser, error) { return next() }), OpenerConfig{
		Resolve:   func(service.Target) (Backend, bool) { return backend, true },
		NoBackend: func(err error) bool { return strings.Contains(err.Error(), "not in directory") },
	})
	target := service.Target{Service: "echo", NodeID: "node-b"}
	drain := func(s io.ReadWriteCloser) {
		_, _ = io.Copy(io.Discard, s)
	}
	stateOf := func() *observationv1alpha1.ServiceAccessChanged { return r.Snapshot().GetServiceAccess()[0] }

	if got := stateOf(); got.State != nil || got.GetEntryAddress() != "127.0.0.1:18080" {
		t.Fatalf("before any connection = %v", got)
	}

	next = func() (io.ReadWriteCloser, error) { return &fakeStream{reads: []string{"hi"}, end: io.EOF}, nil }
	s, _ := opener.OpenStream(context.Background(), target)
	drain(s)
	if got := stateOf(); got.GetState() != observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_REACHABLE || got.GetBackendAddress() != backend.Address {
		t.Fatalf("after backend data = %v", got)
	}

	// The provider closes without data (its local backend is down).
	next = func() (io.ReadWriteCloser, error) { return &fakeStream{end: io.EOF}, nil }
	s, _ = opener.OpenStream(context.Background(), target)
	drain(s)
	if got := stateOf(); got.GetState() != observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_UNREACHABLE || !strings.Contains(got.GetLastError(), "before any backend data") {
		t.Fatalf("after empty close = %v", got)
	}

	// The entry closes first and nothing came back: no conclusion.
	seq := r.Log().CurrentSeq()
	next = func() (io.ReadWriteCloser, error) { return &fakeStream{end: net.ErrClosed}, nil }
	s, _ = opener.OpenStream(context.Background(), target)
	s.Close()
	drain(s)
	if r.Log().CurrentSeq() != seq {
		t.Fatal("a stream closed by the entry itself was reported")
	}

	// The same failure again only refreshes the error time, without an event.
	next = func() (io.ReadWriteCloser, error) { return &fakeStream{end: io.EOF}, nil }
	s, _ = opener.OpenStream(context.Background(), target)
	drain(s)
	if r.Log().CurrentSeq() != seq {
		t.Fatal("repeated identical failure appended an event")
	}

	next = func() (io.ReadWriteCloser, error) { return nil, errors.New(`peer "node-b" not in directory`) }
	if _, err := opener.OpenStream(context.Background(), target); err == nil {
		t.Fatal("expected open error")
	}
	if got := stateOf(); got.GetState() != observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_NO_BACKEND || got.GetBackendNodeId() != "" || got.GetBackendAddress() != "" {
		t.Fatalf("no backend = %v", got)
	}
}

func TestBackendNodeSwitchReportsPreviousBackend(t *testing.T) {
	r := testReporter(0)
	entry := r.ServiceEntry("db")
	entry.record(observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_REACHABLE, Backend{NodeID: "node-b", Address: "b:1"}, nil)
	entry.record(observationv1alpha1.ServiceAccessState_SERVICE_ACCESS_STATE_REACHABLE, Backend{NodeID: "node-c", Address: "c:1"}, nil)
	backlog, _, cancel, _ := r.Log().SubscribeAfter(0)
	cancel()
	if got := backlog[len(backlog)-1].GetServiceAccessChanged(); got.GetPreviousBackendNodeId() != "node-b" {
		t.Fatalf("switch event = %v", got)
	}
	if got := r.Snapshot().GetServiceAccess()[0]; got.GetPreviousBackendNodeId() != "" {
		t.Fatalf("snapshot state must not carry the one-off switch marker: %v", got)
	}
}

func TestServerIsLoopbackOnlyAndSpeaksTheRangeContract(t *testing.T) {
	r := testReporter(0)
	for _, addr := range []string{"0.0.0.0:0", "10.126.126.12:0", ":0"} {
		if _, err := NewServer(addr, r, nil); err == nil {
			t.Fatalf("non-loopback %q accepted", addr)
		}
	}
	srv, err := NewServer("127.0.0.1:0", r, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	url := "http://" + srv.Addr().String()
	r.UpdateNetLinks([]NetLink{{PeerNodeID: "node-b", PeerAddress: "b:1", State: LinkConnected}})
	sub, err := observationtest.Subscribe(ctx, url, observationv1alpha1.Component_COMPONENT_OUTPOSTSERVICE)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	r.UpdateNetLinks([]NetLink{{PeerNodeID: "node-b", PeerAddress: "b:1", State: LinkDisconnected, LastError: "boom", LastErrorTime: time.Now()}})
	if _, err := sub.WaitFor(5*time.Second, "disconnect", func(e *observationv1alpha1.Event) bool {
		return e.GetNetPeerChanged().GetState() == observationv1alpha1.PeerLinkState_PEER_LINK_STATE_DISCONNECTED
	}); err != nil {
		t.Fatal(err)
	}
	if first := sub.Events()[0]; first.GetSubscriptionSnapshot() == nil || first.GetSubscriptionSnapshot().GetNetPeers()[0].GetState() != observationv1alpha1.PeerLinkState_PEER_LINK_STATE_CONNECTED {
		t.Fatalf("first forwarded event = %v", first)
	}

	resp, err := http.Get(url + EventsPath + "?after_seq=abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad after_seq status = %d", resp.StatusCode)
	}
}
