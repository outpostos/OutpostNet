package state

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ResyncPurgeCounter counts committed removals by original writer and triggering
// peer (with credentials redacted). LocalOrigin distinguishes this node's writes.
type ResyncPurgeCounter struct {
	WriterNodeID string `json:"writer_node_id"`
	Peer         string `json:"peer"`
	LocalOrigin  bool   `json:"local_origin"`
	Entries      uint64 `json:"entries"`
}

// ResyncStatus describes committed resync purges only, not LWW replacements or
// tombstone collection. Durable stores persist it with the purge and compaction.
type ResyncStatus struct {
	PurgedLocalOrigin uint64               `json:"purged_local_origin"`
	PurgedForwarded   uint64               `json:"purged_forwarded"`
	ByWriter          []ResyncPurgeCounter `json:"by_writer,omitempty"`
	// LatestArchivePath is relative to the store data directory, portable and
	// operator-only. Archives are never replayed or replicated.
	LatestArchivePath string `json:"latest_archive_path,omitempty"`
}

func copyResyncStatus(s ResyncStatus) ResyncStatus {
	s.ByWriter = append([]ResyncPurgeCounter(nil), s.ByWriter...)
	return s
}

func (s *storeCore) resyncSummary(entries []Entry, peer, path string) *ResyncStatus {
	status := &ResyncStatus{LatestArchivePath: path}
	for _, e := range entries {
		local := e.Version.WriterNodeID == s.nodeID
		if local {
			status.PurgedLocalOrigin++
		} else {
			status.PurgedForwarded++
		}
		addResyncCounter(status, ResyncPurgeCounter{WriterNodeID: e.Version.WriterNodeID, Peer: peer, LocalOrigin: local, Entries: 1})
	}
	return status
}
func addResyncCounter(s *ResyncStatus, c ResyncPurgeCounter) {
	for i := range s.ByWriter {
		p := &s.ByWriter[i]
		if p.WriterNodeID == c.WriterNodeID && p.Peer == c.Peer && p.LocalOrigin == c.LocalOrigin {
			p.Entries += c.Entries
			return
		}
	}
	s.ByWriter = append(s.ByWriter, c)
	sort.Slice(s.ByWriter, func(i, j int) bool {
		a, b := s.ByWriter[i], s.ByWriter[j]
		if a.WriterNodeID != b.WriterNodeID {
			return a.WriterNodeID < b.WriterNodeID
		}
		return a.Peer < b.Peer
	})
}
func (s *storeCore) recordResyncLocked(delta *ResyncStatus) {
	if delta == nil {
		return
	}
	s.resync.PurgedLocalOrigin += delta.PurgedLocalOrigin
	s.resync.PurgedForwarded += delta.PurgedForwarded
	for _, c := range delta.ByWriter {
		addResyncCounter(&s.resync, c)
	}
	if delta.LatestArchivePath != "" {
		s.resync.LatestArchivePath = delta.LatestArchivePath
	}
}

// writeResyncArchive runs under commitMu before even allocating the purge's
// position. A synced archive and directory entry must precede the purge record.
// In-memory stores have no data directory; ArchiveDir opts into file archives.
func (s *storeCore) writeResyncArchive(entries []Entry, peer string, horizon Timestamp) (string, error) {
	if len(entries) == 0 || s.archiveDir == "" {
		return "", nil
	}
	archive := filepath.Join(s.archiveDir, "archive")
	if err := os.MkdirAll(archive, 0o700); err != nil {
		return "", fmt.Errorf("create resync archive directory: %w", err)
	}
	f, err := os.CreateTemp(archive, "resync-purge-"+time.Now().UTC().Format("20060102T150405.000000000Z")+"-*.jsonl")
	if err != nil {
		return "", fmt.Errorf("create resync purge archive: %w", err)
	}
	path := filepath.Join("archive", filepath.Base(f.Name()))
	var handle durableLog = f
	if s.wrapResyncArchive != nil {
		handle = s.wrapResyncArchive(handle)
	}
	defer handle.Close()
	for _, e := range entries {
		data, err := json.Marshal(struct {
			Entry
			Position uint64    `json:"position"`
			Peer     string    `json:"peer"`
			Horizon  Timestamp `json:"horizon"`
		}{e, e.Position, peer, horizon})
		if err != nil {
			return "", err
		}
		data = append(data, '\n')
		n, err := handle.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return "", fmt.Errorf("write resync archive %q: %w", path, err)
		}
	}
	if err := handle.Sync(); err != nil {
		return "", fmt.Errorf("sync resync archive %q: %w", path, err)
	}
	if err := handle.Close(); err != nil {
		return "", fmt.Errorf("close resync archive %q: %w", path, err)
	}
	syncArchive := s.syncResyncArchiveDir
	if syncArchive == nil {
		syncArchive = syncDir
	}
	if err := syncArchive(archive); err != nil {
		return "", fmt.Errorf("sync resync archive directory: %w", err)
	}
	if err := syncArchive(s.archiveDir); err != nil {
		return "", fmt.Errorf("sync resync archive parent: %w", err)
	}
	return path, nil
}
