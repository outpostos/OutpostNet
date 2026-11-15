package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const clockRebuildPending = "clock-rebuild.pending"

// ErrClockRebuildIncomplete refuses a partially completed offline rebuild. No
// archive is ever replayed automatically, including after an interrupted move.
var ErrClockRebuildIncomplete = errors.New("state clock rebuild is incomplete; stop the host and inspect clock-rebuild.pending and archive/ before operator recovery")

func clockArchiveDir(dir string) (string, error) {
	archive := filepath.Join(dir, "archive")
	if err := os.MkdirAll(archive, 0o700); err != nil {
		return "", fmt.Errorf("create clock archive directory: %w", err)
	}
	return archive, nil
}

// Clock archives are operator-only artifacts. Recovery opens only the live
// manifest/log, and replication reads only the recovered live entries.
func writeClockResetArchive(cfg ClockResetConfig, entries []Entry) (string, error) {
	dir := cfg.Dir
	archive, err := clockArchiveDir(dir)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(archive, "clock-reset-"+time.Now().UTC().Format("20060102T150405.000000000Z")+"-*.jsonl")
	if err != nil {
		return "", fmt.Errorf("create clock reset archive: %w", err)
	}
	path := file.Name()
	var handle durableLog = file
	if cfg.wrapArchive != nil {
		handle = cfg.wrapArchive(handle)
	}
	fail := func(err error) (string, error) {
		_ = handle.Close()
		// Keep even an incomplete archive for operator investigation.
		return "", fmt.Errorf("write clock reset archive %q: %w", path, err)
	}
	for _, entry := range entries {
		// Entry.Position is intentionally excluded from the replication encoding.
		// Include it explicitly in this offline archive without changing the wire.
		data, err := json.Marshal(struct {
			Entry
			Position uint64 `json:"position"`
		}{entry, entry.Position})
		if err != nil {
			return fail(err)
		}
		data = append(data, '\n')
		n, err := handle.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return fail(err)
		}
	}
	if err := handle.Sync(); err != nil {
		return fail(err)
	}
	if err := handle.Close(); err != nil {
		return "", fmt.Errorf("close clock reset archive %q: %w", path, err)
	}
	syncArchiveDir := cfg.archiveSyncDir
	if syncArchiveDir == nil {
		syncArchiveDir = syncDir
	}
	if err := syncArchiveDir(archive); err != nil {
		return "", fmt.Errorf("sync clock reset archive directory: %w", err)
	}
	if err := syncArchiveDir(dir); err != nil {
		return "", fmt.Errorf("sync clock archive parent directory: %w", err)
	}
	return path, nil
}

// archiveClockRebuild moves all previous state files on the same filesystem.
// Keep store.lock in place so another process cannot lock a different inode.
// The synced marker blocks startup across the non-atomic multi-file move until
// both the new snapshot and manifest are installed and synced. On any failure,
// retain the marker and every moved file for explicit operator recovery.
func archiveClockRebuild(dir string) (string, error) {
	archive, err := clockArchiveDir(dir)
	if err != nil {
		return "", err
	}
	path, err := os.MkdirTemp(archive, "rebuild-"+time.Now().UTC().Format("20060102T150405.000000000Z")+"-")
	if err != nil {
		return "", fmt.Errorf("create clock rebuild archive: %w", err)
	}
	if err := syncDir(archive); err != nil {
		return "", fmt.Errorf("sync clock rebuild archive parent: %w", err)
	}
	marker, err := os.OpenFile(filepath.Join(dir, clockRebuildPending), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("create clock rebuild marker: %w", err)
	}
	// A relative path keeps the marker portable with a backed-up data directory.
	_, writeErr := io.WriteString(marker, filepath.Join("archive", filepath.Base(path))+"\n")
	syncErr := marker.Sync()
	closeErr := marker.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return "", fmt.Errorf("persist clock rebuild marker: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return "", fmt.Errorf("sync clock rebuild marker directory: %w", err)
	}
	for _, name := range []string{durableCompactTmpFile, durableManifestFile + ".tmp", durableLogFile, durableManifestFile} {
		if err := os.Rename(filepath.Join(dir, name), filepath.Join(path, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("archive clock rebuild file %q in %q: %w", name, path, err)
		}
	}
	if err := syncDir(path); err != nil {
		return "", fmt.Errorf("sync clock rebuild archive: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return "", fmt.Errorf("sync clock rebuild source: %w", err)
	}
	return path, nil
}

func finishClockRebuild(dir, archive string) error {
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync rebuilt state: %w", err)
	}
	if err := os.Rename(filepath.Join(dir, clockRebuildPending), filepath.Join(archive, clockRebuildPending)); err != nil {
		return fmt.Errorf("complete clock rebuild marker: %w", err)
	}
	if err := syncDir(archive); err != nil {
		return fmt.Errorf("sync completed clock rebuild archive: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync completed clock rebuild directory: %w", err)
	}
	return nil
}
