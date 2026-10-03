package state

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const durableFormatVersion = 1

const (
	durableManifestFile = "manifest.json"
	durableLogFile      = "state.log"
	durableLockFile     = "store.lock"

	durableRecordEntry        = "entry"
	durableRecordPeerProgress = "peer_progress"
)

var (
	// ErrDurableStoreLocked reports that another process or store instance
	// already holds the directory lock.
	ErrDurableStoreLocked = errors.New("state durable store is already open")
	// ErrDurableLogTruncated reports an incomplete (non-newline-terminated) tail
	// record in the durable log. Recovery repairs such a tail by truncating it,
	// because a record without its terminator was never confirmed. This error is
	// returned only when that repair itself fails, in which case opening refuses
	// rather than serving a directory with ambiguous bytes.
	ErrDurableLogTruncated = errors.New("state durable log has an incomplete tail record")
	// ErrDurableStoreUnwritable reports that an append failed and the log could not
	// be rolled back to its pre-append offset. The store is fail-closed: every
	// following mutating call returns this error and no further byte is written.
	ErrDurableStoreUnwritable = errors.New("state durable store is permanently unwritable")
)

// durableLog is the log file surface a DurableStore depends on. It exists so
// tests can inject failures into the real Read/Write/Sync/Truncate path instead
// of short-circuiting before persistence.
type durableLog interface {
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	Sync() error
	Truncate(size int64) error
	Seek(offset int64, whence int) (int64, error)
	Close() error
}

// DurableStoreConfig configures a file-backed state replica.
type DurableStoreConfig struct {
	Dir           string
	NodeID        string
	Policy        MergePolicy
	PhysicalClock PhysicalClock

	// preAppendHook runs before a record is marshalled or written. It models a
	// refusal to persist, not a disk failure: no byte reaches the log and the
	// store stays writable. Tests that must prove write-failure semantics inject
	// failures through wrapLog on the real Write/Sync path instead.
	preAppendHook func(durableRecord) error

	// wrapLog decorates the opened log file. Tests use it to fail real
	// Write/Sync/Truncate calls.
	wrapLog func(durableLog) durableLog
}

// DurableStore is a file-backed Store. Each accepted local or remote state
// change is appended and synced before it becomes visible or observers run.
type DurableStore struct {
	*storeCore

	dir           string
	logFile       durableLog
	lockFile      *os.File
	peerProgress  map[string]uint64
	preAppendHook func(durableRecord) error
	wrapLog       func(durableLog) durableLog

	mu      sync.Mutex
	nextSeq uint64
	// logOffset is the end of the last confirmed record. It is authoritative for
	// appends and rollback because the log is opened with O_APPEND.
	logOffset uint64
	closed    bool
	// permanentErr is sticky. It is set when an append failed and could not be
	// rolled back, which means the log may contain ambiguous bytes.
	permanentErr error
}

// DurableStore implements Store and persists gossip progress.
var (
	_ Store             = (*DurableStore)(nil)
	_ PeerProgressStore = (*DurableStore)(nil)
)

type durableManifest struct {
	FormatVersion int    `json:"format_version"`
	NodeID        string `json:"node_id"`
	PolicyID      string `json:"policy_id"`
}

type durableRecord struct {
	FormatVersion int    `json:"format_version"`
	Sequence      uint64 `json:"sequence"`
	Type          string `json:"type"`
	Entry         *Entry `json:"entry,omitempty"`
	Peer          string `json:"peer,omitempty"`
	Position      uint64 `json:"position,omitempty"`
}

// NewDurableStore opens or creates a durable state replica.
func NewDurableStore(cfg DurableStoreConfig) (*DurableStore, error) {
	if strings.TrimSpace(cfg.Dir) == "" {
		return nil, fmt.Errorf("state durable store dir is required")
	}
	core, err := newStoreCore(cfg.NodeID, cfg.Policy, cfg.PhysicalClock)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("create durable state dir %q: %w", cfg.Dir, err)
	}
	lockFile, err := os.OpenFile(filepath.Join(cfg.Dir, durableLockFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open durable state lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDurableStoreLocked
		}
		return nil, fmt.Errorf("lock durable state dir %q: %w", cfg.Dir, err)
	}

	store := &DurableStore{
		storeCore:     core,
		dir:           cfg.Dir,
		lockFile:      lockFile,
		peerProgress:  make(map[string]uint64),
		preAppendHook: cfg.preAppendHook,
		wrapLog:       cfg.wrapLog,
	}
	if err := store.openManifest(); err != nil {
		_ = store.Close()
		return nil, err
	}
	// recoverLog also repairs an unconfirmed tail record and reports the append
	// offset left behind, so it must run before the log is opened for appending.
	if err := store.recoverLog(); err != nil {
		_ = store.Close()
		return nil, err
	}
	var logFile durableLog
	logFile, err = os.OpenFile(filepath.Join(cfg.Dir, durableLogFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open durable state log: %w", err)
	}
	if cfg.wrapLog != nil {
		logFile = cfg.wrapLog(logFile)
	}
	store.logFile = logFile
	return store, nil
}

// Close releases the durable directory lock and marks the replica not ready.
func (s *DurableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if s.logFile != nil {
		errs = append(errs, s.logFile.Close())
		s.logFile = nil
	}
	if s.lockFile != nil {
		errs = append(errs, syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN))
		errs = append(errs, s.lockFile.Close())
		s.lockFile = nil
	}
	s.closed = true
	s.storeCore.markNotReady()
	return errors.Join(errs...)
}

// Put commits a durable local opaque value.
func (s *DurableStore) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	return s.put(ctx, key, Mutation{Value: value}, s.appendEntry)
}

// Delete commits a durable local tombstone.
func (s *DurableStore) Delete(ctx context.Context, key string) (Entry, error) {
	return s.put(ctx, key, Mutation{Deleted: true}, s.appendEntry)
}

// CompareAndSet atomically commits a durable local mutation if the identity matches.
func (s *DurableStore) CompareAndSet(ctx context.Context, key string, expected EntryIdentity, mutation Mutation) (Entry, bool, error) {
	return s.compareAndSet(ctx, key, expected, mutation, s.appendEntry)
}

// Apply durably merges a remote entry before making it visible.
func (s *DurableStore) Apply(ctx context.Context, incoming Entry) (bool, error) {
	return s.apply(ctx, incoming, s.appendEntry)
}

// PeerProgress returns the last durably saved push position for peer.
func (s *DurableStore) PeerProgress(ctx context.Context, peer string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peerProgress[peer], nil
}

// SavePeerProgress durably records a successfully acknowledged push position.
func (s *DurableStore) SavePeerProgress(ctx context.Context, peer string, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(peer) == "" {
		return fmt.Errorf("state peer progress peer is required")
	}
	return s.appendPeerProgress(peer, position)
}

func (s *DurableStore) openManifest() error {
	path := filepath.Join(s.dir, durableManifestFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		manifest := durableManifest{
			FormatVersion: durableFormatVersion,
			NodeID:        s.nodeID,
			PolicyID:      s.policy.ID(),
		}
		return writeManifest(path, manifest)
	}
	if err != nil {
		return fmt.Errorf("read durable state manifest: %w", err)
	}
	var manifest durableManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("decode durable state manifest: %w", err)
	}
	if manifest.FormatVersion != durableFormatVersion {
		return fmt.Errorf("unsupported durable state format version %d", manifest.FormatVersion)
	}
	if manifest.NodeID != s.nodeID {
		return fmt.Errorf("durable state node id mismatch: got %q want %q", s.nodeID, manifest.NodeID)
	}
	if manifest.PolicyID != s.policy.ID() {
		return fmt.Errorf("durable state policy id mismatch: got %q want %q", s.policy.ID(), manifest.PolicyID)
	}
	return nil
}

func writeManifest(path string, manifest durableManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create durable state manifest temp: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write durable state manifest temp: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync durable state manifest temp: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close durable state manifest temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("install durable state manifest: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// recoverLog replays the durable log and records the append offset it leaves
// behind. Only an unterminated final line is repaired, because a record is
// written by a single Write call that ends in '\n': a missing terminator means
// the record was never confirmed. A corrupt or sequence-regressed record that is
// terminated (necessarily a record from an earlier, confirmed write) still
// refuses the directory.
func (s *DurableStore) recoverLog() error {
	path := filepath.Join(s.dir, durableLogFile)
	opened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open durable state log for recovery: %w", err)
	}
	var handle durableLog = opened
	if s.wrapLog != nil {
		handle = s.wrapLog(handle)
	}
	defer handle.Close()

	reader := bufio.NewReader(handle)
	var offset int64
	for lineNo := 1; ; lineNo++ {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			break
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			if truncErr := s.truncateLogTail(handle, offset); truncErr != nil {
				return errors.Join(fmt.Errorf("%w at line %d", ErrDurableLogTruncated, lineNo), truncErr)
			}
			slog.Warn("state: truncated unconfirmed tail record from durable log",
				"dir", s.dir,
				"line", lineNo,
				"tailBytes", len(line),
				"truncatedTo", offset,
			)
			break
		}
		if err != nil {
			return fmt.Errorf("read durable state log line %d: %w", lineNo, err)
		}
		var record durableRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return fmt.Errorf("decode durable state log line %d: %w", lineNo, err)
		}
		if err := s.replayRecord(record); err != nil {
			return fmt.Errorf("replay durable state log line %d: %w", lineNo, err)
		}
		offset += int64(len(line))
	}
	s.logOffset = uint64(offset)
	return nil
}

// truncateLogTail discards every byte after offset, the end of the last
// confirmed record. A truncate that fails leaves ambiguous bytes behind, so the
// caller refuses to open; a failed sync only means the repair may have to be
// repeated after a crash, and the next recovery repeats it.
func (s *DurableStore) truncateLogTail(handle durableLog, offset int64) error {
	if err := handle.Truncate(offset); err != nil {
		return fmt.Errorf("truncate incomplete durable log tail to %d: %w", offset, err)
	}
	if err := handle.Sync(); err != nil {
		slog.Warn("state: sync of repaired durable log tail failed", "dir", s.dir, "offset", offset, "err", err)
	}
	return nil
}

func (s *DurableStore) replayRecord(record durableRecord) error {
	if record.FormatVersion != durableFormatVersion {
		return fmt.Errorf("unsupported durable record format version %d", record.FormatVersion)
	}
	if record.Sequence == 0 {
		return fmt.Errorf("durable record sequence is required")
	}
	if record.Sequence <= s.nextSeq {
		return fmt.Errorf("durable record sequence regressed: got %d after %d", record.Sequence, s.nextSeq)
	}
	s.nextSeq = record.Sequence
	switch record.Type {
	case durableRecordEntry:
		if record.Entry == nil {
			return fmt.Errorf("durable entry record missing entry")
		}
		if record.Position == 0 {
			return fmt.Errorf("durable entry record missing local position")
		}
		record.Entry.Position = record.Position
		return s.restoreEntry(*record.Entry)
	case durableRecordPeerProgress:
		if strings.TrimSpace(record.Peer) == "" {
			return fmt.Errorf("durable peer progress missing peer")
		}
		if record.Position > s.peerProgress[record.Peer] {
			s.peerProgress[record.Peer] = record.Position
		}
		return nil
	default:
		return fmt.Errorf("unknown durable record type %q", record.Type)
	}
}

func (s *DurableStore) appendEntry(entry Entry) error {
	entry = copyEntry(entry)
	return s.appendRecord(durableRecord{
		Type:     durableRecordEntry,
		Entry:    &entry,
		Position: entry.Position,
	}, func() {
	})
}

func (s *DurableStore) appendPeerProgress(peer string, position uint64) error {
	return s.appendRecord(durableRecord{
		Type:     durableRecordPeerProgress,
		Peer:     peer,
		Position: position,
	}, func() {
		if position > s.peerProgress[peer] {
			s.peerProgress[peer] = position
		}
	})
}

// appendRecord writes one record and syncs it before apply runs. A failed or
// short write, and a failed sync, are rolled back by truncating the log to the
// pre-append offset, so the log never keeps a byte the store did not confirm.
// nextSeq is only advanced by confirmed records: after a successful rollback the
// discarded sequence is reused, and that is safe precisely because no byte of it
// remains in the log. If the rollback itself fails the store becomes permanently
// unwritable instead of risking a second record with an ambiguous prefix.
func (s *DurableStore) appendRecord(record durableRecord, apply func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.permanentErr != nil {
		return s.permanentErr
	}
	if s.closed || s.logFile == nil {
		return fmt.Errorf("durable state store is closed")
	}
	record.FormatVersion = durableFormatVersion
	record.Sequence = s.nextSeq + 1
	if s.preAppendHook != nil {
		if err := s.preAppendHook(record); err != nil {
			return err
		}
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	startOffset := s.logOffset
	written, writeErr := s.logFile.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		return s.failAppend(startOffset, fmt.Errorf("append durable state log (%d of %d bytes): %w", written, len(data), writeErr))
	}
	if err := s.logFile.Sync(); err != nil {
		return s.failAppend(startOffset, fmt.Errorf("sync durable state log: %w", err))
	}
	s.logOffset = startOffset + uint64(len(data))
	s.nextSeq = record.Sequence
	apply()
	return nil
}

// failAppend rolls the log back to offset and returns the original failure. It
// returns ErrDurableStoreUnwritable when the rollback fails, which permanently
// disables writes.
func (s *DurableStore) failAppend(offset uint64, failure error) error {
	if rollbackErr := s.rollbackAppend(offset); rollbackErr != nil {
		s.permanentErr = errors.Join(ErrDurableStoreUnwritable, failure, rollbackErr)
		slog.Error("state: durable append rollback failed; store is now permanently unwritable",
			"dir", s.dir,
			"offset", offset,
			"err", s.permanentErr,
		)
		return s.permanentErr
	}
	return failure
}

// rollbackAppend discards every byte after offset and best-effort syncs the log,
// so a following append cannot leave a torn or duplicated record. It reports an
// error only when the bytes could not be discarded.
func (s *DurableStore) rollbackAppend(offset uint64) error {
	var errs []error
	if err := s.logFile.Truncate(int64(offset)); err != nil {
		errs = append(errs, fmt.Errorf("truncate durable state log to %d: %w", offset, err))
	}
	if _, err := s.logFile.Seek(int64(offset), io.SeekStart); err != nil {
		errs = append(errs, fmt.Errorf("seek durable state log to %d: %w", offset, err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := s.logFile.Sync(); err != nil {
		// Best effort: the bytes are already gone from the file. A failed sync here
		// only means the removal may not survive a crash, and a crash cannot make a
		// following append collide because recovery re-derives nextSeq from the log.
		slog.Warn("state: durable append rollback sync failed", "dir", s.dir, "offset", offset, "err", err)
	}
	return nil
}
