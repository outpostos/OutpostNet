package state

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// TimeQuality describes the local source's guarantee, independently of a peer reference.
type TimeQuality string

const (
	TimeSynced   TimeQuality = "SYNCED"
	TimeHoldover TimeQuality = "HOLDOVER"
	TimeUnsynced TimeQuality = "UNSYNCED"
)

// TimeSample carries current system time, never the last HLC or a peer-derived time.
type TimeSample struct {
	SentPhysicalMS int64       `json:"sent_physical_ms"`
	TimeQuality    TimeQuality `json:"time_quality"`
}

// TimeSourceStatus is a host observation of its trusted time source (e.g. chrony).
// Uncertainty bounds source error. An unsynchronized observation enters holdover
// only if a previous synchronization is still within the drift budget.
type TimeSourceStatus struct {
	Synchronized bool
	Uncertainty  time.Duration
	Source       string        // daemon and upstream identity, or "none"
	State        string        // synchronized, unsynchronized, unavailable, error
	Offset       time.Duration // signed system-clock offset when OffsetKnown
	OffsetKnown  bool
}

// TimeReferenceConfig configures a process-local reference. Callbacks must be
// concurrency-safe; MonotonicNow must retain Go's monotonic component in production.
type TimeReferenceConfig struct {
	SystemClock  PhysicalClock
	MonotonicNow func() time.Time
	ClockBudget  time.Duration // default 2s between two qualified nodes
	MaxSampleRTT time.Duration // default 2s
	SampleTTL    time.Duration // default 30s; unreachable samples expire
	SourceTTL    time.Duration // default 30s; stale source observations enter holdover
	DriftPPM     float64       // worst-case local oscillator drift, default 50 ppm
}

// TimeStatus is a local observation snapshot for host event/lease producers.
// A peer reference does not promote this node's own time quality.
type TimeStatus struct {
	Quality            TimeQuality
	TimeUnknown        bool
	UsingPeerReference bool
	ClockSuspect       bool
	ReferenceMS        int64
	SystemMS           int64
	AcceptedSamples    uint64
	DiscardedSamples   uint64
	StepRejected       bool
	StepRejections     uint64
	Source             TimeSourceStatus // latest observation, including source loss
	SourceAge          time.Duration    // monotonic elapsed time since that observation
}

type peerTimeSample struct {
	TimeSample
	received time.Time
	rtt      time.Duration
}

// TimeReference supplies Now for host timestamps and delivery.Config.Clock,
// NowMS for state HLCs, and an independent conservative RetentionMS clock.
// It is safe for concurrent use, has no background goroutine, and is not persisted.
type TimeReference struct {
	mu                  sync.Mutex
	cfg                 TimeReferenceConfig
	peers               map[string]peerTimeSample
	source              TimeSourceStatus
	sourceAt            time.Time
	synchronized        bool
	lastSource          TimeSourceStatus
	lastSourceAt        time.Time
	accepted, discarded uint64
	stepWall            int64
	stepAt              time.Time
	stepRejected        bool
	stepRejections      uint64
	lastQuality         TimeQuality
}

// NewTimeReference creates an UNSYNCED reference until source/peer observations arrive.
func NewTimeReference(cfg TimeReferenceConfig) (*TimeReference, error) {
	if cfg.SystemClock == nil {
		cfg.SystemClock = SystemPhysicalClock{}
	}
	if cfg.MonotonicNow == nil {
		cfg.MonotonicNow = time.Now
	}
	if cfg.ClockBudget == 0 {
		cfg.ClockBudget = 2 * time.Second
	}
	if cfg.MaxSampleRTT == 0 {
		cfg.MaxSampleRTT = 2 * time.Second
	}
	if cfg.SampleTTL == 0 {
		cfg.SampleTTL = 30 * time.Second
	}
	if cfg.SourceTTL == 0 {
		cfg.SourceTTL = 30 * time.Second
	}
	if cfg.DriftPPM == 0 {
		cfg.DriftPPM = 50
	}
	if cfg.ClockBudget < 0 || cfg.MaxSampleRTT < 0 || cfg.SampleTTL < 0 || cfg.SourceTTL < 0 || !(cfg.DriftPPM > 0) || cfg.DriftPPM > 1_000_000 {
		return nil, fmt.Errorf("invalid time reference budget, lifetime or drift")
	}
	return &TimeReference{cfg: cfg, peers: make(map[string]peerTimeSample), stepWall: cfg.SystemClock.NowMS(), stepAt: cfg.MonotonicNow(), lastQuality: TimeUnsynced}, nil
}

func (r *TimeReference) qualityLocked(now time.Time) TimeQuality {
	if !r.synchronized {
		return TimeUnsynced
	}
	age := now.Sub(r.sourceAt)
	uncertainty := float64(r.source.Uncertainty) + float64(age)*r.cfg.DriftPPM/1_000_000
	// Each node gets half the pairwise budget, including oscillator drift.
	if age < 0 || uncertainty > float64(r.cfg.ClockBudget)/2 {
		return TimeUnsynced
	}
	if r.source.Synchronized && age <= r.cfg.SourceTTL {
		return TimeSynced
	}
	return TimeHoldover
}

// UpdateSource reports source status; loss never resets the drift estimate.
func (r *TimeReference) UpdateSource(source TimeSourceStatus) error {
	if source.Uncertainty < 0 {
		return fmt.Errorf("time source uncertainty must not be negative")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSource, r.lastSourceAt = source, r.cfg.MonotonicNow()
	if source.Synchronized {
		r.source, r.sourceAt, r.synchronized = source, r.cfg.MonotonicNow(), true
		if r.qualityLocked(r.sourceAt) != TimeUnsynced {
			// A qualified trusted-source observation confirms the corrected clock,
			// including the transition away from a peer reference in UNSYNCED.
			r.confirmSystemClockLocked(r.sourceAt)
		}
	} else {
		r.source.Synchronized = false
	}
	return nil
}

// Sample reads physical time and derived quality for an outgoing handshake.
func (r *TimeReference) Sample() TimeSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return TimeSample{SentPhysicalMS: r.cfg.SystemClock.NowMS(), TimeQuality: r.qualityLocked(r.cfg.MonotonicNow())}
}

// ObservePeer accepts only a response from a successfully authenticated exchange.
// The caller measures the full round trip with a local monotonic clock. Incoming
// request timestamps alone cannot establish that bound and must not call this.
// Invalid/slow exchanges withdraw an older sample for the same peer.
func (r *TimeReference) ObservePeer(peer string, sample TimeSample, rtt time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if peer == "" || sample.SentPhysicalMS < 0 || sample.SentPhysicalMS > math.MaxInt64-r.cfg.SampleTTL.Milliseconds()-r.cfg.MaxSampleRTT.Milliseconds() || !validTimeQuality(sample.TimeQuality) || rtt < 0 || rtt > r.cfg.MaxSampleRTT {
		delete(r.peers, peer)
		r.discarded++
		return false
	}
	r.peers[peer] = peerTimeSample{TimeSample: sample, received: r.cfg.MonotonicNow(), rtt: rtt}
	r.accepted++
	return true
}

func validTimeQuality(q TimeQuality) bool {
	return q == TimeSynced || q == TimeHoldover || q == TimeUnsynced
}

// ForgetPeer withdraws a failed/disconnected peer immediately.
func (r *TimeReference) ForgetPeer(peer string) { r.mu.Lock(); delete(r.peers, peer); r.mu.Unlock() }

func (r *TimeReference) statusLocked() TimeStatus {
	now := r.cfg.MonotonicNow()
	system := r.cfg.SystemClock.NowMS()
	quality := r.qualityLocked(now)
	if r.lastQuality == TimeUnsynced && quality != TimeUnsynced {
		r.confirmSystemClockLocked(now)
	}
	r.lastQuality = quality
	physical := r.stepTimeLocked(system, now)
	s := TimeStatus{Quality: quality, ReferenceMS: physical, SystemMS: system, AcceptedSamples: r.accepted, DiscardedSamples: r.discarded, ClockSuspect: r.stepRejected, StepRejected: r.stepRejected, StepRejections: r.stepRejections}
	s.Source = r.lastSource
	if !r.lastSourceAt.IsZero() {
		s.SourceAge = now.Sub(r.lastSourceAt)
	}
	if s.Quality != TimeUnsynced {
		return s
	}
	var trusted, untrusted []int64
	for peer, sample := range r.peers {
		age := now.Sub(sample.received)
		if age < 0 || age > r.cfg.SampleTTL {
			delete(r.peers, peer)
			continue
		}
		// At response receipt, remote send time lies within [send, send + RTT].
		// Use its midpoint, then extrapolate solely by local monotonic elapsed time.
		value := sample.SentPhysicalMS + (age.Milliseconds() + (sample.rtt / 2).Milliseconds())
		if sample.TimeQuality == TimeSynced || sample.TimeQuality == TimeHoldover {
			trusted = append(trusted, value)
		} else {
			untrusted = append(untrusted, value)
		}
	}
	values := trusted
	if len(values) == 0 {
		values = agreeingTimes(untrusted, r.cfg.ClockBudget.Milliseconds())
	}
	if len(values) == 0 {
		s.TimeUnknown = true
		return s
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	s.ReferenceMS = values[len(values)/2]
	if len(values)%2 == 0 {
		a, b := values[len(values)/2-1], s.ReferenceMS
		s.ReferenceMS = a + (b-a)/2
	}
	s.UsingPeerReference = true
	s.ClockSuspect = s.ClockSuspect || checkFutureTimestamp(Timestamp{WallMS: system}, s.ReferenceMS) != nil || checkFutureTimestamp(Timestamp{WallMS: s.ReferenceMS}, system) != nil
	return s
}

func agreeingTimes(values []int64, budget int64) []int64 {
	if len(values) < 2 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	for start, end := 0, 0; start < len(values); start++ {
		for end < len(values) && values[end]-values[start] <= budget {
			end++
		}
		if end-start >= 2 && end-start > len(values)/2 {
			return values[start:end]
		}
	}
	return nil
}

func (r *TimeReference) TimeStatus() TimeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statusLocked()
}
func (r *TimeReference) NowMS() int64   { return r.TimeStatus().ReferenceMS }
func (r *TimeReference) Now() time.Time { return time.UnixMilli(r.NowMS()) }
func (r *TimeReference) RetentionMS() int64 {
	s := r.TimeStatus()
	if s.SystemMS < s.ReferenceMS {
		return s.SystemMS
	}
	return s.ReferenceMS
}

// ConfirmSystemClock lets an operator explicitly accept a corrected system
// clock. It resets step detection, not the HLC or persisted entries. A qualified
// UpdateSource observation also confirms it. Confirming a still-wrong clock can
// produce future writes and is the caller's responsibility.
func (r *TimeReference) ConfirmSystemClock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.confirmSystemClockLocked(r.cfg.MonotonicNow())
}

func (r *TimeReference) confirmSystemClockLocked(now time.Time) {
	r.stepWall, r.stepAt, r.stepRejected = r.cfg.SystemClock.NowMS(), now, false
	r.lastQuality = r.qualityLocked(now)
}

func (r *TimeReference) stepTimeLocked(system int64, now time.Time) int64 {
	elapsed := now.Sub(r.stepAt).Milliseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	expected := int64(math.MaxInt64)
	if r.stepWall <= math.MaxInt64-elapsed {
		expected = r.stepWall + elapsed
	}
	limit := MaxClockOffsetMS
	if r.stepRejected {
		limit = r.cfg.ClockBudget.Milliseconds()
	}
	if expected <= math.MaxInt64-limit && system > expected+limit {
		if !r.stepRejected {
			r.stepRejections++
			slog.Warn("state: local clock_suspect forward clock step; awaiting clock confirmation")
		}
		r.stepRejected = true
		return expected
	}
	r.stepWall, r.stepAt, r.stepRejected = system, now, false
	return system
}
