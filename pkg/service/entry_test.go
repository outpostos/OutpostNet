package service_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/service"
)

// startTaggedServer starts a real TCP server that answers every line with
// "<tag>:<line>", so a client can tell which instance served it.
func startTaggedServer(t *testing.T, tag string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := io.WriteString(c, tag+":"+line); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln
}

func ask(t *testing.T, addr, msg string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(c, msg+"\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return strings.TrimSpace(line)
}

// tcpOpener resolves a Target to whatever TCP address is current and dials
// it for real. It stands in for the Net host in unit tests; the tunnel-backed
// two-node test lives in internal/servicehost.
type tcpOpener struct {
	addr  atomic.Value // string
	mu    sync.Mutex
	calls []service.Target
}

func (o *tcpOpener) OpenStream(ctx context.Context, tgt service.Target) (io.ReadWriteCloser, error) {
	o.mu.Lock()
	o.calls = append(o.calls, tgt)
	o.mu.Unlock()
	addr, _ := o.addr.Load().(string)
	if addr == "" {
		return nil, errors.New("no endpoint")
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

func startEntry(t *testing.T, tgt service.Target, opener service.StreamOpener) *service.Entry {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e := service.NewEntry(service.EntryConfig{Target: tgt, ListenAddr: "127.0.0.1:0"}, opener, nil)
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(3 * time.Second)
	for e.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("entry did not start listening")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return e
}

func TestEntryResolvesTargetPerConnection(t *testing.T) {
	v1 := startTaggedServer(t, "v1")
	v2 := startTaggedServer(t, "v2")
	op := &tcpOpener{}
	op.addr.Store(v1.Addr().String())

	tgt := service.Target{Service: "echo", NodeID: "node-b"}
	e := startEntry(t, tgt, op)
	entryAddr := e.Addr().String()

	if got := ask(t, entryAddr, "hello"); got != "v1:hello" {
		t.Fatalf("first answer = %q, want v1:hello", got)
	}
	op.addr.Store(v2.Addr().String())
	if got := ask(t, entryAddr, "hello"); got != "v2:hello" {
		t.Fatalf("answer after endpoint change = %q, want v2:hello", got)
	}
	if e.Addr().String() != entryAddr {
		t.Fatalf("entry address changed: %s -> %s", entryAddr, e.Addr())
	}

	op.mu.Lock()
	defer op.mu.Unlock()
	if len(op.calls) != 2 {
		t.Fatalf("opener calls = %d, want 2 (one per connection)", len(op.calls))
	}
	for _, c := range op.calls {
		if c != tgt {
			t.Fatalf("opener got target %+v, want %+v", c, tgt)
		}
	}
}

func TestEntryClosesClientWhenTargetUnavailable(t *testing.T) {
	op := &tcpOpener{} // no endpoint
	e := startEntry(t, service.Target{Service: "echo", NodeID: "node-x"}, op)

	c, err := net.Dial("tcp", e.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("expected the entry to close the client connection when no stream can be opened")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("entry kept an unroutable client connection open")
	}
}

func TestTargetValidation(t *testing.T) {
	cases := []struct {
		tgt service.Target
		ok  bool
	}{
		{service.Target{Service: "echo", NodeID: "node-b"}, true},
		{service.Target{Service: "echo", Region: "cloud"}, true},
		{service.Target{Service: "echo"}, true}, // legacy: any client tunnel with empty region
		{service.Target{NodeID: "node-b"}, false},
		{service.Target{Service: "echo", NodeID: "node-b", Region: "cloud"}, false},
		{service.Target{Service: strings.Repeat("x", 256), NodeID: "n"}, false},
	}
	for _, c := range cases {
		err := c.tgt.Validate()
		if (err == nil) != c.ok {
			t.Errorf("Validate(%+v) err=%v, want ok=%v", c.tgt, err, c.ok)
		}
	}
}

func TestParseRef(t *testing.T) {
	r, err := service.ParseRef("echo@node-b")
	if err != nil {
		t.Fatal(err)
	}
	if r.Service != "echo" || r.NodeID != "node-b" || r.String() != "echo@node-b" {
		t.Fatalf("ParseRef = %+v (%s)", r, r)
	}
	if r.Target() != (service.Target{Service: "echo", NodeID: "node-b"}) {
		t.Fatalf("Target() = %+v", r.Target())
	}
	for _, bad := range []string{"", "echo", "@n", "echo@", "a@b@c"} {
		if _, err := service.ParseRef(bad); err == nil {
			t.Errorf("ParseRef(%q) succeeded, want error", bad)
		}
	}
}

// TestEntryRunReturnsWithInFlightConnection: shutting an entry down must
// not hang on an application connection that stays open. After the shutdown
// grace the entry closes it and Run returns, so a host can wait for its
// entries before stopping the transport underneath them.
func TestEntryRunReturnsWithInFlightConnection(t *testing.T) {
	v1 := startTaggedServer(t, "v1")
	op := &tcpOpener{}
	op.addr.Store(v1.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := service.NewEntry(service.EntryConfig{Target: service.Target{Service: "echo", NodeID: "node-b"}, ListenAddr: "127.0.0.1:0"}, op, nil)
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for e.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("entry did not start listening")
		}
		time.Sleep(10 * time.Millisecond)
	}

	c, err := net.Dial("tcp", e.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	io.WriteString(c, "hi\n")
	if line, err := r.ReadString('\n'); err != nil || line != "v1:hi\n" {
		t.Fatalf("warmup = %q, %v", line, err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("Entry.Run did not return while an application connection stayed open")
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("in-flight connection still open after Run returned")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("in-flight connection still open after Run returned")
	}
}
