package state

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// TimeCommandRunner executes a local read-only command with cancellation. It
// must honor ctx; no shell or privilege escalation is used by the default runner.
type TimeCommandRunner func(context.Context, string, ...string) ([]byte, error)

// OSTimeSourceConfig configures bounded local probes. Zero values use defaults.
type OSTimeSourceConfig struct {
	Runner         TimeCommandRunner
	PollInterval   time.Duration // default 5s; range 1s..15s
	CommandTimeout time.Duration // default 1s; range 1ms..5s per command
	// KernelStatus reads adjtimex modes=0 on Linux. It returns maxerror and
	// whether STA_UNSYNC is clear. It must be concurrency-safe and nonblocking.
	KernelStatus func() (time.Duration, bool, error)
}

// OSTimeSource probes chrony first, then systemd-timesyncd. Poll failures are
// observations, never fatal. A valid chrony report is authoritative even when
// unsynchronized, so a stale kernel flag cannot mask chrony source loss.
type OSTimeSource struct{ cfg OSTimeSourceConfig }

func NewOSTimeSource(cfg OSTimeSourceConfig) (*OSTimeSource, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.CommandTimeout == 0 {
		cfg.CommandTimeout = time.Second
	}
	if cfg.PollInterval < time.Second || cfg.PollInterval > 15*time.Second || cfg.CommandTimeout < time.Millisecond || cfg.CommandTimeout > 5*time.Second {
		return nil, fmt.Errorf("invalid OS time source polling interval or command timeout")
	}
	if cfg.Runner == nil {
		cfg.Runner = runTimeCommand
	}
	if cfg.KernelStatus == nil {
		cfg.KernelStatus = kernelTimeStatus
	}
	return &OSTimeSource{cfg: cfg}, nil
}

func runTimeCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	if runtime.GOOS != "linux" {
		return nil, exec.ErrNotFound
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "LC_ALL=C", "LANG=C", "SYSTEMD_COLORS=0")
	cmd.WaitDelay = 100 * time.Millisecond
	// Reports are small. Limit memory even if a broken executable is noisy.
	out := &timeCommandOutput{}
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	if out.overflow {
		return nil, fmt.Errorf("time source output too large")
	}
	return out.data, nil
}

type timeCommandOutput struct {
	data     []byte
	overflow bool
}

func (b *timeCommandOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(b.data)+n > 16*1024 {
		b.overflow = true
		return n, nil
	}
	b.data = append(b.data, p...)
	return n, nil
}

func (s *OSTimeSource) command(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CommandTimeout)
	defer cancel()
	return s.cfg.Runner(ctx, name, args...)
}

// Poll returns the current source and error bound. It never returns raw command
// errors/output (which can contain machine paths or other host information).
func (s *OSTimeSource) Poll(ctx context.Context) TimeSourceStatus {
	if ctx.Err() != nil {
		return TimeSourceStatus{Source: "none", State: "unavailable"}
	}
	tracking, err := s.command(ctx, "chronyc", "-c", "tracking")
	if err == nil {
		status, parseErr := ParseChronyTracking(string(tracking))
		if parseErr != nil {
			return TimeSourceStatus{Source: "chrony", State: "error"}
		}
		return status
	}
	show, showErr := s.command(ctx, "timedatectl", "show")
	timesync, syncErr := s.command(ctx, "timedatectl", "show-timesync", "--all")
	if showErr != nil || syncErr != nil {
		return TimeSourceStatus{Source: "none", State: "unavailable"}
	}
	bound, synced, kernelErr := s.cfg.KernelStatus()
	if kernelErr != nil {
		return TimeSourceStatus{Source: "systemd-timesyncd", State: "error"}
	}
	status, parseErr := ParseTimesyncStatus(string(show), string(timesync), bound, synced)
	if parseErr != nil {
		return TimeSourceStatus{Source: "systemd-timesyncd", State: "error"}
	}
	return status
}

var ntpMessageField = regexp.MustCompile(`(?:^|[ {,])([A-Za-z]+)=([^,} ]+)`)

// ParseTimesyncStatus combines timedatectl show/show-timesync with the kernel's
// maximum error bound. NTPSynchronized alone has neither source identity nor an
// error estimate. No signed offset is claimed: adjtimex maxerror bounds the
// corrected system clock, while show-timesync exposes no current system offset.
func ParseTimesyncStatus(show, timesync string, maxError time.Duration, kernelSynced bool) (TimeSourceStatus, error) {
	s := TimeSourceStatus{Source: "systemd-timesyncd", State: "error"}
	properties := func(text string) map[string]string {
		m := map[string]string{}
		for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
			if key, value, ok := strings.Cut(line, "="); ok {
				m[key] = value
			}
		}
		return m
	}
	p, ts := properties(show), properties(timesync)
	if (p["NTPSynchronized"] != "yes" && p["NTPSynchronized"] != "no") || (p["NTP"] != "yes" && p["NTP"] != "no") || maxError < 0 {
		return s, fmt.Errorf("invalid timedatectl synchronization status or kernel bound")
	}
	identity := ts["ServerAddress"]
	if identity == "" {
		identity = ts["ServerName"]
	}
	if p["NTP"] == "no" || p["NTPSynchronized"] == "no" || !kernelSynced {
		s.State, s.Uncertainty = "unsynchronized", maxError
		if identity != "" {
			s.Source += ":" + identity
		}
		return s, nil
	}
	fields := map[string]string{}
	for _, match := range ntpMessageField.FindAllStringSubmatch(ts["NTPMessage"], -1) {
		fields[match[1]] = match[2]
	}
	stratum, err := strconv.Atoi(fields["Stratum"])
	leap, leapErr := strconv.Atoi(fields["Leap"])
	packets, packetsErr := strconv.ParseUint(fields["PacketCount"], 10, 64)
	if identity == "" || err != nil || stratum < 0 || stratum > 16 || leapErr != nil || leap < 0 || leap > 3 || packetsErr != nil || (fields["Ignored"] != "yes" && fields["Ignored"] != "no") {
		return s, fmt.Errorf("invalid timedatectl timesync source or NTP message")
	}
	s.Source, s.State, s.Uncertainty = "systemd-timesyncd:"+identity, "unsynchronized", maxError
	s.Synchronized = p["NTP"] == "yes" && p["NTPSynchronized"] == "yes" && kernelSynced && leap != 3 && stratum > 0 && stratum < 16 && packets > 0 && fields["Ignored"] == "no"
	if s.Synchronized {
		s.State = "synchronized"
	}
	return s, nil
}

// NewSystemTimeReference constructs the shared reference for a production host.
// It observes the OS immediately, then polls until ctx is cancelled or stop is
// called. stop cancels and joins the poller; defer it before opening stores.
// Injected-clock users can continue using NewTimeReference/UpdateSource directly.
func NewSystemTimeReference(ctx context.Context, cfg TimeReferenceConfig, sourceConfig OSTimeSourceConfig) (*TimeReference, func(), error) {
	probe, err := NewOSTimeSource(sourceConfig)
	if err != nil {
		return nil, nil, err
	}
	ref, err := NewTimeReference(cfg)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	ref.UpdateSource(probe.Poll(ctx)) // parser/probe guarantees nonnegative bounds
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(probe.cfg.PollInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				status := probe.Poll(ctx)
				if ctx.Err() != nil {
					return
				}
				ref.UpdateSource(status)
				timer.Reset(probe.cfg.PollInterval)
			}
		}
	}()
	return ref, func() { cancel(); <-done }, nil
}

// ParseChronyTracking parses the seconds-based CSV from chronyc -c tracking.
// The bound follows chrony's documented clock-error formula: absolute system
// offset + root dispersion + half root delay. Local/orphan mode is not a source.
func ParseChronyTracking(output string) (TimeSourceStatus, error) {
	s := TimeSourceStatus{Source: "chrony", State: "error"}
	r := csv.NewReader(strings.NewReader(strings.TrimSpace(output)))
	f, err := r.Read()
	if err != nil || len(f) != 14 {
		return s, fmt.Errorf("invalid chrony tracking CSV")
	}
	if _, err := r.Read(); err != io.EOF {
		return s, fmt.Errorf("unexpected extra chrony tracking record")
	}
	stratum, err := strconv.Atoi(f[2])
	if err != nil || stratum < 0 || stratum > 16 {
		return s, fmt.Errorf("invalid chrony stratum")
	}
	values := make([]float64, 10)
	for i := range values {
		v, err := strconv.ParseFloat(f[i+3], 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return s, fmt.Errorf("invalid chrony numeric field %d", i+3)
		}
		values[i] = v
	}
	if values[0] < 0 || values[3] < 0 || values[6] < 0 || values[7] < 0 || values[8] < 0 || values[9] < 0 {
		return s, fmt.Errorf("negative chrony time/error field")
	}
	s.Offset, err = secondsDuration(values[1], false)
	if err != nil {
		return s, err
	}
	s.OffsetKnown = true
	s.Uncertainty, err = secondsDuration(math.Abs(values[1])+values[8]+values[7]/2, true)
	if err != nil {
		return s, err
	}
	s.Source = "chrony:" + f[1]
	s.State = "unsynchronized"
	switch f[13] {
	case "Normal", "Insert second", "Delete second":
		s.Synchronized = stratum > 0 && stratum < 16 && values[0] > 0 && f[0] != "00000000" && !strings.EqualFold(f[0], "7F7F0101")
	case "Not synchronised":
	default:
		return s, fmt.Errorf("invalid chrony leap status")
	}
	if s.Synchronized {
		s.State = "synchronized"
	}
	return s, nil
}

func secondsDuration(v float64, roundUp bool) (time.Duration, error) {
	ns := v * float64(time.Second)
	if roundUp {
		ns = math.Ceil(ns)
	} else {
		ns = math.Round(ns)
	}
	if math.IsNaN(ns) || math.IsInf(ns, 0) || ns >= float64(math.MaxInt64) || ns <= float64(math.MinInt64) {
		return 0, fmt.Errorf("time source duration out of range")
	}
	return time.Duration(ns), nil
}
