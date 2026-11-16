package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// Real process deaths complement the precise Write/Sync/Truncate crash images.
func TestProcessCrashBoundaries(t *testing.T) {
	if dir := os.Getenv("NET9_CRASH_DIR"); dir != "" {
		ctx := context.Background()
		stage := os.Getenv("NET9_CRASH_STAGE")
		var id MessageID
		if stage == "send" {
			o, e := NewOutbox(Config{Dir: dir, NodeID: "a"})
			if e != nil {
				t.Fatal(e)
			}
			id, e = o.Send(ctx, message(time.Now()))
			if e != nil {
				t.Fatal(e)
			}
		} else {
			in, e := NewInbox(Config{Dir: dir, NodeID: "b"})
			if e != nil {
				t.Fatal(e)
			}
			envelope := external(time.Now(), 1)
			id = envelope.MessageID
			if _, e = in.Accept(ctx, []Envelope{envelope}); e != nil {
				t.Fatal(e)
			}
			if stage == "lease" || stage == "ack" {
				if _, e = in.Receive(ctx, "m"); e != nil {
					t.Fatal(e)
				}
			}
			if stage == "ack" {
				if e = in.Ack(ctx, id); e != nil {
					t.Fatal(e)
				}
			}
		}
		if e := json.NewEncoder(os.Stdout).Encode(id); e != nil {
			t.Fatal(e)
		}
		if e := syscall.Kill(os.Getpid(), syscall.SIGKILL); e != nil {
			t.Fatal(e)
		}
		return
	}
	for _, stage := range []string{"send", "accept", "lease", "ack"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestProcessCrashBoundaries$")
			cmd.Env = append(os.Environ(), "NET9_CRASH_DIR="+dir, "NET9_CRASH_STAGE="+stage)
			data, err := cmd.Output()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("child did not crash: %v", err)
			}
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected process exit: %v", err)
			}
			var id MessageID
			if err = json.Unmarshal(data, &id); err != nil {
				t.Fatal(err)
			}
			if stage == "send" {
				o := mustOut(t, Config{Dir: dir, NodeID: "a"})
				if s, e := o.Status(id); e != nil || s != Pending {
					t.Fatalf("%s %v", s, e)
				}
			} else {
				in := mustIn(t, Config{Dir: dir, NodeID: "b"})
				if stage == "ack" {
					noDelivery(t, in)
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					d, e := in.Receive(ctx, "m")
					if e != nil || d.MessageID != id {
						t.Fatalf("%+v %v", d, e)
					}
				}
			}
		})
	}
}

// Crash inside a multi-Ack transaction, both with a torn append and after a
// completed fsync but before publication/returns. Recovery must be all-or-none.
type groupCrashFile struct {
	logFile
	stage string
}

func (f groupCrashFile) Write(data []byte) (int, error) {
	var tx transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return 0, err
	}
	if len(tx.Upserts) > 1 && tx.Upserts[0].Acked {
		if f.stage == "torn" {
			_, _ = f.logFile.Write(data[:len(data)/2])
		} else {
			_, _ = f.logFile.Write(data)
			_ = f.logFile.Sync()
		}
		_ = json.NewEncoder(os.Stdout).Encode(len(tx.Upserts))
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		panic("SIGKILL returned")
	}
	return f.logFile.Write(data)
}
func TestConcurrentAcksCrashMidGroup(t *testing.T) {
	const count = 64
	if dir := os.Getenv("NET13_GROUP_DIR"); dir != "" {
		var barrier *syncBarrierFile
		in, err := NewInbox(Config{Dir: dir, NodeID: "b", wrapLog: func(f logFile) logFile {
			barrier = &syncBarrierFile{logFile: groupCrashFile{f, os.Getenv("NET13_GROUP_STAGE")}, entered: make(chan struct{}), release: make(chan struct{})}
			return barrier
		}})
		if err != nil {
			t.Fatal(err)
		}
		barrier.armed.Store(true)
		go func() { _ = in.Ack(context.Background(), external(time.Now(), 1).MessageID) }()
		<-barrier.entered
		for i := 2; i <= count; i++ {
			go func(i int) { _ = in.Ack(context.Background(), external(time.Now(), uint64(i)).MessageID) }(i)
		}
		// The first fsync is blocked while every remaining call joins the next group.
		deadline := time.Now().Add(5 * time.Second)
		for {
			in.ackMu.Lock()
			n := len(in.acks)
			in.ackMu.Unlock()
			if n == count-1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Acks did not queue")
			}
			time.Sleep(time.Millisecond)
		}
		close(barrier.release)
		time.Sleep(10 * time.Second)
		t.Fatal("group did not crash")
	}
	for _, stage := range []string{"torn", "synced"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			in := mustIn(t, Config{Dir: dir, NodeID: "b"})
			envelopes := make([]Envelope, count)
			for i := range envelopes {
				envelopes[i] = external(time.Now(), uint64(i+1))
			}
			if _, err := in.Accept(context.Background(), envelopes); err != nil {
				t.Fatal(err)
			}
			if err := in.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestConcurrentAcksCrashMidGroup$")
			cmd.Env = append(os.Environ(), "NET13_GROUP_DIR="+dir, "NET13_GROUP_STAGE="+stage)
			data, err := cmd.Output()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("child did not crash: %v", err)
			}
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected exit: %v", err)
			}
			var grouped int
			if err = json.Unmarshal(data, &grouped); err != nil || grouped != count-1 {
				t.Fatalf("not a concurrent group: %s %v", data, err)
			}
			recovered := mustIn(t, Config{Dir: dir, NodeID: "b"})
			if stage == "synced" {
				noDelivery(t, recovered)
			} else {
				for i := 2; i <= count; i++ {
					e, err := recovered.Receive(context.Background(), "m")
					if err != nil || e.MessageID.Seq != uint64(i) {
						t.Fatalf("recovery %v %v", e.MessageID, err)
					}
				}
				noDelivery(t, recovered)
			}
		})
	}
}
