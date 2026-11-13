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
