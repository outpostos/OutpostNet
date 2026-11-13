package delivery

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Compact installs an equivalent snapshot via temp -> fsync -> rename ->
// directory fsync. Failures after rename fail closed until reopen.
func (b *box) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.writable(); err != nil {
		return err
	}
	if b.seq == ^uint64(0) {
		return errors.New("delivery log sequence exhausted")
	}
	var up []entry
	for _, e := range ordered(b.entries, func(entry) bool { return true }) {
		up = append(up, e)
	}
	tx := transaction{Version: 1, Sequence: b.seq + 1, MessageSeq: b.messageSeq, Snapshot: true, Upserts: up}
	data, err := encodeTransaction(tx)
	if err != nil {
		return err
	}
	tmp := filepath.Join(b.cfg.Dir, "delivery.log.compact")
	defer os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	var handle logFile = f
	if b.cfg.wrapLog != nil {
		handle = b.cfg.wrapLog(handle)
	}
	n, err := handle.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = b.compactStep("write-temp")
	}
	if err == nil {
		err = b.compactStep("sync-temp")
	}
	if err == nil {
		err = handle.Sync()
	}
	err = errors.Join(err, handle.Close())
	if err != nil {
		return err
	}
	if err = b.compactStep("rename"); err != nil {
		return err
	}
	path := filepath.Join(b.cfg.Dir, "delivery.log")
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	fail := func(e error) error { b.fatal = errors.Join(ErrUnwritable, e); return b.fatal }
	if err = b.compactStep("sync-dir"); err == nil {
		err = syncDir(b.cfg.Dir)
	}
	if err != nil {
		return fail(err)
	}
	if err = b.compactStep("reopen"); err != nil {
		return fail(err)
	}
	f, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fail(err)
	}
	handle = f
	if b.cfg.wrapLog != nil {
		handle = b.cfg.wrapLog(handle)
	}
	err = b.file.Close()
	b.file = handle
	b.seq = tx.Sequence
	b.offset = int64(len(data))
	return err
}
func (b *box) compactStep(step string) error {
	if b.cfg.compactHook != nil {
		return b.cfg.compactHook(step)
	}
	return nil
}
