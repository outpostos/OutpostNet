package delivery

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Faults decorate genuine file operations, as in state/durable_store_fault_test.
type diskFault struct {
	short, syncFail, truncateFail bool
	syncs                         int
}
type faultyFile struct {
	logFile
	fault *diskFault
}

func (f *diskFault) wrap(l logFile) logFile { return &faultyFile{l, f} }
func (f *faultyFile) Write(p []byte) (int, error) {
	if f.fault.short {
		f.fault.short = false
		n, e := f.logFile.Write(p[:len(p)/2])
		if e != nil {
			return n, e
		}
		return n, io.ErrShortWrite
	}
	return f.logFile.Write(p)
}
func (f *faultyFile) Sync() error {
	f.fault.syncs++
	if f.fault.syncFail {
		f.fault.syncFail = false
		return errors.New("injected sync failure")
	}
	return f.logFile.Sync()
}
func (f *faultyFile) Truncate(n int64) error {
	if f.fault.truncateFail {
		return errors.New("injected truncate failure")
	}
	return f.logFile.Truncate(n)
}
func crashImage(t *testing.T, dir string) string {
	t.Helper()
	dst := t.TempDir()
	for _, name := range []string{"manifest.json", "delivery.log"} {
		b, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dst, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	return dst
}

func TestCrashRecoveryAtSendAcceptAndAck(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b"})
	id, err := o.Send(ctx, message(now))
	if err != nil {
		t.Fatal(err)
	}
	recoveredOut := mustOut(t, Config{Dir: crashImage(t, o.cfg.Dir), NodeID: "a"})
	if s, _ := recoveredOut.Status(id); s != Pending {
		t.Fatal(s)
	}
	wire := NewMemoryTransport()
	wire.Register("b", in, nil)
	recoveredOut.cfg.Transport = wire
	if err = recoveredOut.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	recoveredIn := mustIn(t, Config{Dir: crashImage(t, in.cfg.Dir), NodeID: "b"})
	d, err := recoveredIn.Receive(ctx, "m")
	if err != nil || d.MessageID != id {
		t.Fatalf("%+v %v", d, err)
	}
	// A lease is deliberately not persisted; pre-Ack crash redelivers.
	beforeAck := mustIn(t, Config{Dir: crashImage(t, recoveredIn.cfg.Dir), NodeID: "b"})
	d, err = beforeAck.Receive(ctx, "m")
	if err != nil || d.MessageID != id {
		t.Fatalf("%+v %v", d, err)
	}
	if err = beforeAck.Ack(ctx, id); err != nil {
		t.Fatal(err)
	}
	afterAck := mustIn(t, Config{Dir: crashImage(t, beforeAck.cfg.Dir), NodeID: "b"})
	noDelivery(t, afterAck)
	newID, err := recoveredOut.Send(ctx, message(now))
	if err != nil || newID.SourceIncarnation != id.SourceIncarnation || newID.Seq != id.Seq+1 {
		t.Fatalf("identity %+v %v", newID, err)
	}
	fresh := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
	freshID, err := fresh.Send(ctx, message(now))
	if err != nil || freshID.SourceIncarnation == id.SourceIncarnation {
		t.Fatalf("fresh identity %+v %v", freshID, err)
	}
}

func TestFailedDiskOperationsNeverAcknowledge(t *testing.T) {
	for _, kind := range []string{"outbox", "inbox", "ack"} {
		for _, mode := range []string{"short", "sync", "rollback"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				f := &diskFault{}
				cfg := Config{Dir: t.TempDir(), NodeID: "b", wrapLog: f.wrap}
				var b *box
				var action func() error
				id := external(time.Now(), 1).MessageID
				if kind == "outbox" {
					o := mustOut(t, cfg)
					b = o.box
					action = func() error { _, e := o.Send(ctx, message(time.Now())); return e }
				} else {
					in := mustIn(t, cfg)
					b = in.box
					if kind == "ack" {
						if _, e := in.Accept(ctx, []Envelope{external(time.Now(), 1)}); e != nil {
							t.Fatal(e)
						}
						action = func() error { return in.Ack(ctx, id) }
					} else {
						action = func() error {
							r, e := in.Accept(ctx, []Envelope{external(time.Now(), 1), external(time.Now(), 2)})
							if e != nil && r != nil {
								t.Fatal("returned acknowledgements on failure")
							}
							return e
						}
					}
				}
				if mode == "sync" {
					f.syncFail = true
				} else {
					f.short = true
				}
				f.truncateFail = mode == "rollback"
				if e := action(); e == nil {
					t.Fatal("disk failure returned success")
				}
				if mode == "rollback" {
					if e := action(); !errors.Is(e, ErrUnwritable) {
						t.Fatalf("did not fail closed: %v", e)
					}
				}
				image := crashImage(t, cfg.Dir)
				if kind == "outbox" {
					o := mustOut(t, Config{Dir: image, NodeID: "b"})
					if len(o.entries) != 0 {
						t.Fatal("unconfirmed send survived")
					}
				} else {
					in := mustIn(t, Config{Dir: image, NodeID: "b"})
					if kind == "ack" {
						d, e := in.Receive(ctx, "m")
						if e != nil || d.MessageID != id {
							t.Fatalf("failed Ack removed delivery: %v %v", d, e)
						}
					} else {
						noDelivery(t, in)
					}
				}
				_ = b
			})
		}
	}
}

func TestInboxBatchUsesOneSyncAndTornTailIsNotConfirmed(t *testing.T) {
	ctx := context.Background()
	f := &diskFault{}
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b", wrapLog: f.wrap})
	before := f.syncs
	if _, e := in.Accept(ctx, []Envelope{external(time.Now(), 1), external(time.Now(), 2)}); e != nil {
		t.Fatal(e)
	}
	if f.syncs-before != 1 {
		t.Fatalf("batch used %d syncs", f.syncs-before)
	}
	dir := crashImage(t, in.cfg.Dir)
	path := filepath.Join(dir, "delivery.log")
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, data[:len(data)-1], 0600); e != nil {
		t.Fatal(e)
	}
	repaired := mustIn(t, Config{Dir: dir, NodeID: "b"})
	noDelivery(t, repaired)
	info, e := os.Stat(path)
	if e != nil || info.Size() != 0 {
		t.Fatalf("tail not truncated: %v %v", info, e)
	}
	dir = crashImage(t, in.cfg.Dir)
	if e = os.WriteFile(filepath.Join(dir, "delivery.log"), []byte("broken\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = NewInbox(Config{Dir: dir, NodeID: "b"}); e == nil {
		t.Fatal("confirmed corruption accepted")
	}
}

func TestCompactionPreservesStatusPendingDedupAndSequence(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
	in := mustIn(t, Config{Dir: t.TempDir(), NodeID: "b"})
	wire := NewMemoryTransport()
	wire.Register("b", in, nil)
	o.cfg.Transport = wire
	id, e := o.Send(ctx, message(now))
	if e != nil {
		t.Fatal(e)
	}
	if e = o.Flush(ctx); e != nil {
		t.Fatal(e)
	}
	if e = in.Ack(ctx, id); e != nil {
		t.Fatal(e)
	}
	id2, e := o.Send(ctx, message(now))
	if e != nil {
		t.Fatal(e)
	}
	if e = o.Flush(ctx); e != nil {
		t.Fatal(e)
	}
	id3, e := o.Send(ctx, message(now))
	if e != nil {
		t.Fatal(e)
	}
	for _, b := range []*box{o.box, in.box} {
		if e = b.Compact(ctx); e != nil {
			t.Fatal(e)
		}
	}
	ro := mustOut(t, Config{Dir: crashImage(t, o.cfg.Dir), NodeID: "a"})
	ri := mustIn(t, Config{Dir: crashImage(t, in.cfg.Dir), NodeID: "b"})
	for _, id := range []MessageID{id, id2, id3} {
		a, _ := o.Status(id)
		b, _ := ro.Status(id)
		if a != b {
			t.Fatalf("status %s != %s", a, b)
		}
	}
	d, e := ri.Receive(ctx, "m")
	if e != nil || d.MessageID != id2 {
		t.Fatalf("pending %+v %v", d, e)
	}
	original := o.entries[id].Envelope
	original.Payload = []byte("data")
	r, e := ri.Accept(ctx, []Envelope{original})
	if e != nil || r[0].Code != Duplicate {
		t.Fatalf("dedup %v %v", r, e)
	}
	next, e := ro.Send(ctx, message(now))
	if e != nil || next.Seq != id3.Seq+1 {
		t.Fatalf("counter %+v %v", next, e)
	}
}

func TestDirectoryLockAndIdentity(t *testing.T) {
	o := mustOut(t, Config{Dir: t.TempDir(), NodeID: "a"})
	if _, e := NewOutbox(o.cfg); !errors.Is(e, ErrLocked) {
		t.Fatal(e)
	}
	if e := o.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := NewInbox(o.cfg); e == nil {
		t.Fatal("opened outbox as inbox")
	}
	cfg := o.cfg
	cfg.NodeID = "other"
	if _, e := NewOutbox(cfg); e == nil {
		t.Fatal("opened wrong node")
	}
}
