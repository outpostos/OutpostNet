package state_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestOSTimeSourceCapturedChrony(t *testing.T) {
	data, err := os.ReadFile("testdata/time-source/chrony-node1.csv")
	if err != nil {
		t.Fatal(err)
	}
	s, err := state.ParseChronyTracking(string(data))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Synchronized || s.Source != "chrony:202.118.1.130" || s.Offset != -276195*time.Nanosecond || s.Uncertainty != 18252361*time.Nanosecond {
		t.Fatalf("captured tracking: %+v", s)
	}
}

func TestOSTimeSourceRejectsUntrustedChrony(t *testing.T) {
	data, _ := os.ReadFile("testdata/time-source/chrony-node1.csv")
	for _, tc := range []struct {
		name, old, replacement string
		invalid                bool
	}{
		{"unsynchronized", "Normal", "Not synchronised", false},
		{"stratum-zero", ",2,", ",0,", false},
		{"local-mode", "CA760182", "7f7f0101", false},
		{"nan", "0.001447433", "NaN", true},
		{"infinite", "0.001447433", "+Inf", true},
		{"overflow", "0.001447433", "1e300", true},
		{"negative-dispersion", "0.001447433", "-1", true},
		{"unknown-leap", "Normal", "unknown", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := state.ParseChronyTracking(strings.Replace(string(data), tc.old, tc.replacement, 1))
			if s.Synchronized || (err != nil) != tc.invalid {
				t.Fatalf("untrusted source: %+v %v", s, err)
			}
		})
	}
	for _, output := range []string{"", "506 Cannot talk to daemon\n", string(data) + string(data)} {
		if s, err := state.ParseChronyTracking(output); err == nil || s.Synchronized {
			t.Fatalf("accepted malformed report: %+v %v", s, err)
		}
	}
}

func TestOSTimeSourceTimesyncStartupAndFallback(t *testing.T) {
	s, err := state.ParseTimesyncStatus("NTP=yes\nNTPSynchronized=no\n", "ServerName=ntp.example\n", time.Second, false)
	if err != nil || s.Synchronized || s.State != "unsynchronized" {
		t.Fatalf("startup without sample: %+v %v", s, err)
	}
	for _, interval := range []time.Duration{-time.Second, time.Millisecond, time.Hour} {
		if _, err := state.NewOSTimeSource(state.OSTimeSourceConfig{PollInterval: interval}); err == nil {
			t.Fatal("invalid polling interval accepted")
		}
	}
	show, _ := os.ReadFile("testdata/time-source/timedatectl-node1.txt")
	probe, _ := state.NewOSTimeSource(state.OSTimeSourceConfig{
		Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if name == "chronyc" {
				return nil, exec.ErrNotFound
			}
			if args[0] == "show" {
				return show, nil
			}
			return []byte("ServerName=ntp.example\nNTPMessage={ Leap=0, Stratum=2, Ignored=no PacketCount=34 }"), nil
		},
		KernelStatus: func() (time.Duration, bool, error) { return 4 * time.Millisecond, true, nil },
	})
	if s := probe.Poll(context.Background()); !s.Synchronized || s.Source != "systemd-timesyncd:ntp.example" {
		t.Fatal(s)
	}
}

func TestOSTimeSourceCommandTimeoutAndNoMaskedLoss(t *testing.T) {
	probe, _ := state.NewOSTimeSource(state.OSTimeSourceConfig{
		CommandTimeout: 10 * time.Millisecond,
		Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	start := time.Now()
	if s := probe.Poll(context.Background()); s.Synchronized || s.State != "unavailable" {
		t.Fatal(s)
	}
	if time.Since(start) > time.Second {
		t.Fatal("command deadlines not bounded")
	}
	data, _ := os.ReadFile("testdata/time-source/chrony-node1.csv")
	probe, _ = state.NewOSTimeSource(state.OSTimeSourceConfig{Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name != "chronyc" {
			t.Fatal("chrony loss masked by another probe")
		}
		return []byte(strings.Replace(string(data), "Normal", "Not synchronised", 1)), nil
	}})
	if s := probe.Poll(context.Background()); s.Synchronized || s.State != "unsynchronized" {
		t.Fatal(s)
	}
}

func TestOSTimeSourceProductionReferencePollsAndStops(t *testing.T) {
	data, _ := os.ReadFile("testdata/time-source/chrony-node1.csv")
	var calls atomic.Int32
	started := make(chan struct{})
	ref, stop, err := state.NewSystemTimeReference(context.Background(), state.TimeReferenceConfig{}, state.OSTimeSourceConfig{
		PollInterval: time.Second,
		Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if calls.Add(1) == 1 {
				return data, nil
			}
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if ref.TimeStatus().Quality != state.TimeSynced {
		t.Fatal(ref.TimeStatus())
	}
	store, err := state.NewMemoryStore(state.MemoryStoreConfig{NodeID: "local", TimeReference: ref})
	if err != nil {
		t.Fatal(err)
	}
	if s := store.ClockStatus(); len(s) != 1 || s[0].TimeSource.Source != "chrony:202.118.1.130" || s[0].TimeQuality != state.TimeSynced {
		t.Fatalf("host store source: %+v", s)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("no periodic probe")
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not cancel/join")
	}
	if calls.Load() != 2 {
		t.Fatal("probe continued after cancellation")
	}
}

func TestOSTimeSourceLossAndRecovery(t *testing.T) {
	data, _ := os.ReadFile("testdata/time-source/chrony-node1.csv")
	mono := time.Unix(1, 0)
	ref, _ := state.NewTimeReference(state.TimeReferenceConfig{MonotonicNow: func() time.Time { return mono }, ClockBudget: 2 * time.Second, DriftPPM: 1000})
	missing := false
	probe, err := state.NewOSTimeSource(state.OSTimeSourceConfig{Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "chronyc" && !missing {
			return data, nil
		}
		return nil, &exec.Error{Name: name, Err: exec.ErrNotFound}
	}})
	if err != nil {
		t.Fatal(err)
	}
	ref.UpdateSource(probe.Poll(context.Background()))
	if ref.TimeStatus().Quality != state.TimeSynced {
		t.Fatal(ref.TimeStatus())
	}
	missing = true
	mono = mono.Add(time.Second)
	ref.UpdateSource(probe.Poll(context.Background()))
	if s := ref.TimeStatus(); s.Quality != state.TimeHoldover || s.Source.Source != "none" || s.Source.State != "unavailable" {
		t.Fatal(s)
	}
	mono = mono.Add(1000 * time.Second)
	ref.UpdateSource(probe.Poll(context.Background()))
	if ref.TimeStatus().Quality != state.TimeUnsynced {
		t.Fatal(ref.TimeStatus())
	}
	missing = false
	ref.UpdateSource(probe.Poll(context.Background()))
	if ref.TimeStatus().Quality != state.TimeSynced {
		t.Fatal(ref.TimeStatus())
	}
}

func TestOSTimeSourceTimesyncRequiresErrorBound(t *testing.T) {
	show, _ := os.ReadFile("testdata/time-source/timedatectl-node1.txt")
	// A kernel synchronization flag alone (also set by chrony) is insufficient.
	s, err := state.ParseTimesyncStatus(string(show), "", time.Millisecond, true)
	if err == nil || s.Synchronized {
		t.Fatalf("invented source: %+v %v", s, err)
	}
	timesync := "ServerName=ntp.example\nServerAddress=192.0.2.1\nNTPMessage={ Leap=0, Stratum=2, Ignored=no, PacketCount=34 }\n"
	s, err = state.ParseTimesyncStatus(string(show), timesync, 5*time.Millisecond, true)
	if err != nil || !s.Synchronized || s.Uncertainty != 5*time.Millisecond || s.OffsetKnown || s.Source != "systemd-timesyncd:192.0.2.1" {
		t.Fatalf("timesync: %+v %v", s, err)
	}
	for _, changed := range []string{strings.Replace(timesync, "Ignored=no", "Ignored=yes", 1), strings.Replace(timesync, "Leap=0", "Leap=3", 1)} {
		s, err = state.ParseTimesyncStatus(string(show), changed, time.Millisecond, true)
		if err != nil || s.Synchronized {
			t.Fatalf("bad NTP state: %+v %v", s, err)
		}
	}
	s, err = state.ParseTimesyncStatus(string(show), timesync, time.Millisecond, false)
	if err != nil || s.Synchronized {
		t.Fatalf("kernel unsynced: %+v %v", s, err)
	}
}

func TestOSTimeSourceCapturedSourceLossEntersHoldover(t *testing.T) {
	tracking, _ := os.ReadFile("testdata/time-source/chrony-node1.csv")
	show, _ := os.ReadFile("testdata/time-source/timedatectl-node1.txt")
	lost, _ := os.ReadFile("testdata/time-source/timesync-source-loss.txt")
	mono := time.Unix(1, 0)
	ref, _ := state.NewTimeReference(state.TimeReferenceConfig{MonotonicNow: func() time.Time { return mono }, DriftPPM: 1000})
	good, _ := state.ParseChronyTracking(string(tracking))
	ref.UpdateSource(good)
	bad, err := state.ParseTimesyncStatus(string(show), string(lost), time.Millisecond, true)
	if err != nil || bad.Synchronized {
		t.Fatalf("source loss: %+v %v", bad, err)
	}
	mono = mono.Add(time.Second)
	ref.UpdateSource(bad)
	if s := ref.TimeStatus(); s.Quality != state.TimeHoldover || s.Source.State != "unsynchronized" {
		t.Fatal(s)
	}
	mono = mono.Add(1000 * time.Second)
	ref.UpdateSource(bad)
	if ref.TimeStatus().Quality != state.TimeUnsynced {
		t.Fatal(ref.TimeStatus())
	}
	restarted, _ := state.NewTimeReference(state.TimeReferenceConfig{})
	restarted.UpdateSource(bad)
	if restarted.TimeStatus().Quality != state.TimeUnsynced {
		t.Fatal("restart retained synchronization")
	}
}
