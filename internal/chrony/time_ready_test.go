package chrony_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func timeReadyCommand(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, "python3", append([]string{"../../deploy/chrony/outpostnet-time-ready"}, args...)...)
}

func TestTimeReadyOrphan(t *testing.T) {
	dir := t.TempDir()
	client := filepath.Join(dir, "chronyc")
	// chronyc -c tracking: local refid, no source address, normal leap status.
	if err := os.WriteFile(client, []byte("#!/bin/sh\nprintf '%s\\n' '7F7F0101,,10,0,0,0,0,0,0,0,0,0,0,Normal'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := timeReadyCommand(ctx, "--chronyc", client, "--timeout", "10", "--interval", "0.1", "--on-timeout", "fail").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "ready: local orphan reference") {
		t.Fatalf("active orphan must pass: %s %v", out, err)
	}
}

func TestTimeReadyTimeoutActions(t *testing.T) {
	const noReference = "00000000,,0,0,0,0,0,0,0,0,0,0,0,Not synchronised"
	for _, action := range []string{"default", "start", "fail"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			args := []string{"--chronyc", fakeTimeClient(t), "--timeout", "0.2", "--interval", "0.03"}
			if action != "default" {
				args = append(args, "--on-timeout", action)
			}
			cmd := timeReadyCommand(ctx, args...)
			cmd.Env = append(os.Environ(), "TIME_READY_REPORT="+noReference)
			started := time.Now()
			out, err := cmd.CombinedOutput()
			if (err != nil) != (action == "fail") || ctx.Err() != nil {
				t.Fatalf("timeout action %s: %s %v", action, out, err)
			}
			if time.Since(started) < 200*time.Millisecond || !strings.Contains(string(out), "WARNING: timeout:") || strings.Contains(string(out), ": ready: ") {
				t.Fatalf("no reference must wait for the bound and report: %s", out)
			}
			message := "starting consumers; time readiness unconfirmed"
			if action == "fail" {
				message = "keeping consumers down"
			}
			if !strings.Contains(string(out), message) {
				t.Fatalf("missing action report: %s", out)
			}
		})
	}
}

func TestTimeReadyTracking(t *testing.T) {
	for _, tc := range []struct {
		name, report string
		ready        bool
	}{
		{"follower", "7F000002,127.0.0.2,11,1,0.5,0,0,0,0,0,0,0,0,Normal", true},
		{"negative correction", "7F000002,127.0.0.2,11,1,-0.5,0,0,0,0,0,0,0,0,Normal", true},
		{"slewing", "7F000002,127.0.0.2,11,1,0.5001,0,0,0,0,0,0,0,0,Normal", false},
		{"unsynchronized follower", "7F000002,127.0.0.2,11,1,0,0,0,0,0,0,0,0,0,Not synchronised", false},
		{"inactive orphan", "7F7F0101,,10,0,0,0,0,0,0,0,0,0,0,Not synchronised", false},
		{"orphan leap not normal", "7F7F0101,,10,0,0,0,0,0,0,0,0,0,0,Insert second", false},
		{"zero refid", "00000000,,10,0,0,0,0,0,0,0,0,0,0,Normal", false},
		{"invalid stratum", "7F000002,127.0.0.2,16,1,0,0,0,0,0,0,0,0,0,Normal", false},
		{"nonfinite correction", "7F000002,127.0.0.2,11,1,nan,0,0,0,0,0,0,0,0,Normal", false},
		{"truncated", "7F7F0101,,10", false},
		{"query error text", "506 Cannot talk to daemon", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			bound := "1"
			if tc.ready {
				bound = "10" // Allow subprocess startup under full-suite race load.
			}
			cmd := timeReadyCommand(ctx, "--chronyc", fakeTimeClient(t), "--timeout", bound, "--interval", "0.1", "--on-timeout", "fail")
			cmd.Env = append(os.Environ(), "TIME_READY_REPORT="+tc.report)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.ready || ctx.Err() != nil {
				t.Fatalf("ready=%v: %s %v", tc.ready, out, err)
			}
			if !tc.ready {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 1 || !strings.Contains(string(out), "WARNING: timeout:") {
					t.Fatalf("unready report must honor timeout policy: %s %v", out, err)
				}
			}
		})
	}
}

func fakeTimeClient(t *testing.T) string {
	t.Helper()
	client := filepath.Join(t.TempDir(), "chronyc")
	if err := os.WriteFile(client, []byte("#!/bin/sh\nprintf '%s\\n' \"$TIME_READY_REPORT\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestTimeReadyQueryFailures(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"unavailable", ""},
		{"query failure", "#!/bin/sh\nexit 1\n"},
		{"hung query", "#!/bin/sh\nexec sleep 10\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := filepath.Join(t.TempDir(), "chronyc")
			if tc.body != "" {
				if err := os.WriteFile(client, []byte(tc.body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := timeReadyCommand(ctx, "--chronyc", client, "--timeout", "0.3", "--interval", "0.05").CombinedOutput()
			if err != nil || !strings.Contains(string(out), "time readiness unconfirmed") || ctx.Err() != nil {
				t.Fatalf("query failure must honor bounded start/report policy: %s %v", out, err)
			}
		})
	}
}

func TestTimeReadyInvalidOptions(t *testing.T) {
	for _, args := range [][]string{{"--timeout", "0"}, {"--timeout", "nan"}, {"--interval", "-1"}, {"--interval", "inf"}, {"--on-timeout", "ignore"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := timeReadyCommand(ctx, args...).CombinedOutput()
		cancel()
		if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 || !strings.Contains(string(out), "error:") {
			t.Fatalf("invalid options %v must fail without starting consumers: %s %v", args, out, err)
		}
	}
}
