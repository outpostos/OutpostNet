// Package delivery provides durable, direct-peer, at-least-once message delivery.
// It is independent of state replication and of any business domain or transport.
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type MessageID struct {
	SourceNodeID      string `json:"source_node_id"`
	SourceIncarnation string `json:"source_incarnation"`
	Seq               uint64 `json:"seq"`
}

type Priority uint8

const (
	P0 Priority = iota
	P1
	P2
	P3
)

type Envelope struct {
	MessageID          MessageID `json:"message_id"`
	DestinationNodeID  string    `json:"destination_node_id"`
	Mailbox            string    `json:"mailbox"`
	Category           string    `json:"category"`
	Priority           Priority  `json:"priority"`
	IdempotencyKey     string    `json:"idempotency_key,omitempty"`
	CorrelationID      string    `json:"correlation_id,omitempty"`
	ExpiryTime         time.Time `json:"expiry_time"`
	Payload            []byte    `json:"payload,omitempty"`
	Checksum           string    `json:"checksum"`
	WantHandoffReceipt bool      `json:"want_handoff_receipt,omitempty"`
}

// Checksum is the hex SHA-256 of the payload. Transport authentication must
// protect the remaining metadata; the checksum is not an authentication token.
func Checksum(payload []byte) string { h := sha256.Sum256(payload); return hex.EncodeToString(h[:]) }

type Status string

const (
	Pending   Status = "PENDING"
	Delivered Status = "DELIVERED"
	HandedOff Status = "HANDED_OFF"
	Expired   Status = "EXPIRED"
	Rejected  Status = "REJECTED"
)

type ResultCode string

const (
	Accepted        ResultCode = "accepted"
	Duplicate       ResultCode = "duplicate"
	RejectedExpired ResultCode = "rejected_expired"
	RejectedInvalid ResultCode = "rejected_invalid"
	Full            ResultCode = "full"
)

type Result struct {
	MessageID MessageID  `json:"message_id"`
	Code      ResultCode `json:"code"`
}

// Receipt records durable consumer handoff, not business execution success.
type Receipt struct {
	MessageID      MessageID `json:"message_id"`
	ReceiverNodeID string    `json:"receiver_node_id"`
}

type Batch struct {
	Envelopes []Envelope `json:"envelopes,omitempty"`
	Receipts  []Receipt  `json:"receipts,omitempty"`
}
type BatchResult struct {
	Envelopes []Result `json:"envelopes,omitempty"`
	Receipts  []Result `json:"receipts,omitempty"`
}

// Transport sends to a direct peer by stable identity. Results must have the
// same length, order and IDs as the request. An error means no acknowledgement
// may be inferred, even when the peer committed before the response was lost.
// Implementations must authenticate peers and bind receipt ReceiverNodeID to
// the authenticated peer. NET-9 supplies only an in-process implementation.
type Transport interface {
	Send(context.Context, string, Batch) (BatchResult, error)
}

var (
	ErrOutboxFull       = errors.New("delivery outbox quota exceeded")
	ErrNotFound         = errors.New("delivery message not found")
	ErrClosed           = errors.New("delivery box closed")
	ErrLocked           = errors.New("delivery directory already open")
	ErrUnwritable       = errors.New("delivery log permanently unwritable")
	ErrInvalidEnvelope  = errors.New("invalid delivery envelope")
	ErrEnvelopeTooLarge = errors.New("delivery envelope too large")
)

// Config applies to one directory, which must be dedicated to either an Inbox
// or an Outbox. Zero limits select the documented defaults.
type Config struct {
	Dir              string
	NodeID           string
	QuotaBytes       int64
	MaxEnvelopeBytes int
	MaxBatchBytes    int
	MaxExpiry        time.Duration
	ClockSkew        time.Duration
	Clock            func() time.Time
	RetryMin         time.Duration
	RetryMax         time.Duration
	Transport        Transport
	// Filesystem fault seams, matching pkg/state's real-file wrapper pattern.
	wrapLog     func(logFile) logFile
	compactHook func(string) error
}

func (c Config) defaults() (Config, error) {
	if strings.TrimSpace(c.Dir) == "" || strings.TrimSpace(c.NodeID) == "" {
		return c, errors.New("delivery directory and node ID required")
	}
	if c.QuotaBytes == 0 {
		c.QuotaBytes = 64 << 20
	}
	if c.MaxEnvelopeBytes == 0 {
		c.MaxEnvelopeBytes = 512 << 10
	}
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = 768 << 10
	}
	if c.MaxExpiry == 0 {
		c.MaxExpiry = 72 * time.Hour
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	if c.RetryMin == 0 {
		c.RetryMin = 100 * time.Millisecond
	}
	if c.RetryMax == 0 {
		c.RetryMax = 30 * time.Second
	}
	if c.QuotaBytes < 0 || c.MaxEnvelopeBytes < 1 || c.MaxBatchBytes < c.MaxEnvelopeBytes+32 || c.MaxExpiry < 0 || c.ClockSkew < 0 || c.RetryMin < 0 || c.RetryMax < c.RetryMin {
		return c, errors.New("invalid delivery limits")
	}
	return c, nil
}

func encodedSize(v any) int     { b, _ := json.Marshal(v); return len(b) }
func clone(e Envelope) Envelope { e.Payload = append([]byte(nil), e.Payload...); return e }
func (e Envelope) valid(c Config, now time.Time) error {
	if e.MessageID.SourceNodeID == "" || e.MessageID.SourceIncarnation == "" || e.MessageID.Seq == 0 || e.DestinationNodeID == "" || e.Mailbox == "" || e.Category == "" || e.Priority > P3 || e.ExpiryTime.IsZero() || e.ExpiryTime.After(now.Add(c.MaxExpiry+c.ClockSkew)) || e.Checksum != Checksum(e.Payload) {
		return ErrInvalidEnvelope
	}
	if encodedSize(e) > c.MaxEnvelopeBytes {
		return fmt.Errorf("%w: limit %d", ErrEnvelopeTooLarge, c.MaxEnvelopeBytes)
	}
	return nil
}
