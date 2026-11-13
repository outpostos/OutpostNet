package delivery

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type logFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	Sync() error
	Truncate(int64) error
}

type entry struct {
	QueuedAt       time.Time `json:"queued_at,omitzero"`
	Envelope       Envelope  `json:"envelope"`
	Status         Status    `json:"status"`
	Acked          bool      `json:"acked,omitempty"`
	ReceiptPending bool      `json:"receipt_pending,omitempty"`
}
type transaction struct {
	Version    int         `json:"version"`
	Sequence   uint64      `json:"sequence"`
	MessageSeq uint64      `json:"message_seq"`
	Snapshot   bool        `json:"snapshot,omitempty"`
	Upserts    []entry     `json:"upserts,omitempty"`
	Deletes    []MessageID `json:"deletes,omitempty"`
}
type manifest struct {
	Version     int    `json:"version"`
	NodeID      string `json:"node_id"`
	Kind        string `json:"kind"`
	Incarnation string `json:"incarnation"`
}

// box serializes commits and publication under mu. No lock is held over a
// transport call. Compaction shares the commit lock, so its snapshot is atomic.
type box struct {
	observed        map[string]*Counters
	mu              sync.Mutex
	flushMu         sync.Mutex
	cfg             Config
	identity        manifest
	file            logFile
	lock            *os.File
	entries         map[MessageID]entry
	leased          map[MessageID]bool
	seq, messageSeq uint64
	offset          int64
	closed          bool
	fatal           error
	changed         chan struct{}
	retries         map[string]retry
	usedBytes       int64
}

func openBox(cfg Config, kind string) (_ *box, err error) {
	cfg, err = cfg.defaults()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}
	b := &box{cfg: cfg, entries: make(map[MessageID]entry), leased: make(map[MessageID]bool), changed: make(chan struct{}), retries: make(map[string]retry)}
	b.lock, err = os.OpenFile(filepath.Join(cfg.Dir, "box.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(b.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = b.lock.Close()
		return nil, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	defer func() {
		if err != nil {
			_ = b.Close()
		}
	}()
	p := filepath.Join(cfg.Dir, "manifest.json")
	data, e := os.ReadFile(p)
	if errors.Is(e, os.ErrNotExist) {
		// Never attach a fresh incarnation to existing log history.
		if _, e = os.Stat(filepath.Join(cfg.Dir, "delivery.log")); !errors.Is(e, os.ErrNotExist) {
			return nil, errors.New("delivery log exists without manifest")
		}
		var random [16]byte
		if _, err = rand.Read(random[:]); err != nil {
			return nil, err
		}
		b.identity = manifest{1, cfg.NodeID, kind, hex.EncodeToString(random[:])}
		data, _ = json.Marshal(b.identity)
		if err = atomicFile(cfg.Dir, "manifest.json", data); err != nil {
			return nil, err
		}
	} else if e != nil {
		return nil, e
	} else {
		if err = json.Unmarshal(data, &b.identity); err != nil {
			return nil, err
		}
		if b.identity.Version != 1 || b.identity.NodeID != cfg.NodeID || b.identity.Kind != kind || b.identity.Incarnation == "" {
			return nil, errors.New("delivery manifest version or identity mismatch")
		}
		if _, e = os.Stat(filepath.Join(cfg.Dir, "delivery.log")); e != nil {
			return nil, fmt.Errorf("delivery manifest without readable log: %w", e)
		}
	}
	if e = os.Remove(filepath.Join(cfg.Dir, "delivery.log.compact")); e != nil && !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(cfg.Dir, "delivery.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if e != nil {
		return nil, e
	}
	b.file = f
	if cfg.wrapLog != nil {
		b.file = cfg.wrapLog(b.file)
	}
	if err = b.recover(); err != nil {
		return nil, err
	}
	if err = syncDir(cfg.Dir); err != nil {
		return nil, err
	}
	return b, nil
}

func syncDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}
func atomicFile(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	n, e := f.Write(data)
	if e == nil && n != len(data) {
		e = io.ErrShortWrite
	}
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, filepath.Join(dir, name)); e != nil {
		return e
	}
	return syncDir(dir)
}

func (b *box) recover() error {
	r := bufio.NewReader(b.file)
	for line := 1; ; line++ {
		data, e := r.ReadBytes('\n')
		if errors.Is(e, io.EOF) {
			if len(data) > 0 {
				if e = b.file.Truncate(b.offset); e != nil {
					return fmt.Errorf("repair delivery tail: %w", e)
				}
				if e = b.file.Sync(); e != nil {
					return e
				}
				slog.Warn("delivery: truncated unconfirmed tail", "bytes", len(data))
			}
			break
		}
		if e != nil {
			return e
		}
		var tx transaction
		if e = json.Unmarshal(data, &tx); e != nil {
			return fmt.Errorf("delivery log line %d: %w", line, e)
		}
		if tx.Version != 1 || tx.Sequence <= b.seq || tx.MessageSeq < b.messageSeq || (tx.Snapshot && line != 1) {
			return fmt.Errorf("invalid delivery log sequence/version at line %d", line)
		}
		if e = b.validateRecovered(tx); e != nil {
			return fmt.Errorf("invalid delivery log line %d: %w", line, e)
		}
		b.apply(tx)
		b.offset += int64(len(data))
	}
	_, e := b.file.Seek(0, io.SeekEnd)
	return e
}
func (b *box) apply(tx transaction) {
	if tx.Snapshot {
		b.entries = make(map[MessageID]entry)
		b.usedBytes = 0
	}
	for _, e := range tx.Upserts {
		if previous, ok := b.entries[e.Envelope.MessageID]; ok {
			b.usedBytes -= entryCharge(previous)
		}
		b.entries[e.Envelope.MessageID] = e
		b.usedBytes += entryCharge(e)
	}
	for _, id := range tx.Deletes {
		if previous, ok := b.entries[id]; ok {
			b.usedBytes -= entryCharge(previous)
		}
		delete(b.entries, id)
		delete(b.leased, id)
	}
	b.seq = tx.Sequence
	b.messageSeq = tx.MessageSeq
}
func (b *box) writable() error {
	if b.closed {
		return ErrClosed
	}
	return b.fatal
}
func (b *box) commit(tx transaction) error {
	if e := b.writable(); e != nil {
		return e
	}
	if b.seq == ^uint64(0) {
		return errors.New("delivery log sequence exhausted")
	}
	tx.Version = 1
	tx.Sequence = b.seq + 1
	if tx.MessageSeq < b.messageSeq {
		tx.MessageSeq = b.messageSeq
	}
	data, e := encodeTransaction(tx)
	if e != nil {
		return e
	}
	n, e := b.file.Write(data)
	if e == nil && n != len(data) {
		e = io.ErrShortWrite
	}
	if e == nil {
		e = b.file.Sync()
	}
	if e != nil {
		rb := b.file.Truncate(b.offset)
		_, seek := b.file.Seek(b.offset, io.SeekStart)
		if rb != nil || seek != nil {
			b.fatal = errors.Join(ErrUnwritable, e, rb, seek)
			return b.fatal
		}
		if se := b.file.Sync(); se != nil {
			slog.Warn("delivery: rollback sync failed", "error", se)
		}
		return e
	}
	b.countExpiry(tx)
	b.apply(tx)
	b.offset += int64(len(data))
	close(b.changed)
	b.changed = make(chan struct{})
	return nil
}

func (b *box) validateRecovered(tx transaction) error {
	seen := make(map[MessageID]bool)
	for _, v := range tx.Upserts {
		e := v.Envelope
		id := e.MessageID
		if id.SourceNodeID == "" || id.SourceIncarnation == "" || id.Seq == 0 || seen[id] || e.DestinationNodeID == "" || e.Mailbox == "" || e.Category == "" || e.Priority > P3 || e.ExpiryTime.IsZero() {
			return errors.New("invalid persisted envelope")
		}
		seen[id] = true
		switch v.Status {
		case Pending, Delivered, HandedOff, Expired, Rejected:
		default:
			return errors.New("invalid persisted status")
		}
		if b.identity.Kind == "outbox" {
			if id.SourceNodeID != b.identity.NodeID || id.SourceIncarnation != b.identity.Incarnation || id.Seq > tx.MessageSeq {
				return errors.New("persisted source identity/counter mismatch")
			}
		} else if e.DestinationNodeID != b.identity.NodeID {
			return errors.New("persisted destination mismatch")
		}
		if v.Status == Pending && !v.Acked && e.Checksum != Checksum(e.Payload) {
			return errors.New("persisted payload checksum mismatch")
		}
	}
	for _, id := range tx.Deletes {
		if id.SourceNodeID == "" || id.SourceIncarnation == "" || id.Seq == 0 || seen[id] {
			return errors.New("invalid persisted removal")
		}
		seen[id] = true
	}
	return nil
}

func encodeTransaction(tx transaction) ([]byte, error) {
	data, err := json.Marshal(tx)
	return append(data, '\n'), err
}

// Reserve the retained metadata even for an empty payload, so Ack/receipt
// bookkeeping can never force a previously accepted message over quota.
func entryCharge(e entry) int64 {
	actual := encodedSize(e)
	e.Envelope.Payload = nil
	e.Status = HandedOff
	e.Acked = true
	e.ReceiptPending = true
	return int64(max(actual, encodedSize(e)))
}
func (b *box) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	close(b.changed)
	var e error
	if b.file != nil {
		e = b.file.Close()
	}
	if b.lock != nil {
		e = errors.Join(e, syscall.Flock(int(b.lock.Fd()), syscall.LOCK_UN), b.lock.Close())
	}
	return e
}
