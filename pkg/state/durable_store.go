package state

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// ErrDurableLogTruncated reports a non-newline-terminated tail record.
	ErrDurableLogTruncated = errors.New("state durable log has an incomplete tail record")
)

// DurableStoreConfig configures a file-backed state replica.
type DurableStoreConfig struct {
	Dir           string
	NodeID        string
	Policy        MergePolicy
	PhysicalClock PhysicalClock

	appendHook func(durableRecord) error
}

// DurableStore is a file-backed Store. Each accepted local or remote state
// change is appended and synced before it becomes visible or observers run.
type DurableStore struct {
	*storeCore

	dir          string
	logFile      *os.File
	lockFile     *os.File
	peerProgress map[string]uint64
	appendHook   func(durableRecord) error

	mu      sync.Mutex
	nextSeq uint64
	closed  bool
}

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
		storeCore:    core,
		dir:          cfg.Dir,
		lockFile:     lockFile,
		peerProgress: make(map[string]uint64),
		appendHook:   cfg.appendHook,
	}
	if err := store.openManifest(); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.recoverLog(); err != nil {
		_ = store.Close()
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(cfg.Dir, durableLogFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open durable state log: %w", err)
	}
	store.logFile = logFile
	return store, nil
}

// Close releases the durable directory lock.
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

func (s *DurableStore) recoverLog() error {
	path := filepath.Join(s.dir, durableLogFile)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open durable state log for recovery: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for lineNo := 1; ; lineNo++ {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			break
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return fmt.Errorf("%w at line %d", ErrDurableLogTruncated, lineNo)
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

func (s *DurableStore) appendRecord(record durableRecord, apply func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.logFile == nil {
		return fmt.Errorf("durable state store is closed")
	}
	record.FormatVersion = durableFormatVersion
	record.Sequence = s.nextSeq + 1
	if s.appendHook != nil {
		if err := s.appendHook(record); err != nil {
			return err
		}
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := s.logFile.Write(data); err != nil {
		return fmt.Errorf("append durable state log: %w", err)
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("sync durable state log: %w", err)
	}
	s.nextSeq = record.Sequence
	apply()
	return nil
}
