package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// This file injects failures into the real durable log, at the Write/Sync/
// Truncate calls the store makes, so that recovery semantics after a failed
// append are proven on disk instead of short-circuited before persistence.

var (
	errInjectedWrite    = errors.New("injected durable log write failure")
	errInjectedSync     = errors.New("injected durable log sync failure")
	errInjectedTruncate = errors.New("injected durable log truncate failure")
)

// logFault is shared state for the wrappers returned by wrap. It is armed
// explicitly by each test so that opening a store never fails by accident.
type logFault struct {
	mu sync.Mutex
	// failWrites fails the next Write calls before any byte reaches the file.
	failWrites int
	// shortWrites writes half of the buffer to the real file and then reports an
	// error, like a disk that ran out of space in the middle of a record.
	shortWrites int
	// failSyncs fails the next Sync calls after the record was fully written.
	failSyncs int
	// failTruncate makes the rollback itself impossible.
	failTruncate bool

	writes    int
	syncs     int
	truncates int
}

func (f *logFault) arm(configure func(*logFault)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	configure(f)
}

func (f *logFault) writeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

func (f *logFault) truncateCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.truncates
}

// wrap decorates a real log handle. Every injected failure happens on the
// genuine file operations of that handle.
func (f *logFault) wrap(handle durableLog) durableLog {
	return &faultLog{durableLog: handle, fault: f}
}

type faultLog struct {
	durableLog
	fault *logFault
}

func (l *faultLog) Write(data []byte) (int, error) {
	l.fault.mu.Lock()
	l.fault.writes++
	failWrites := l.fault.failWrites
	shortWrites := l.fault.shortWrites
	switch {
	case failWrites > 0:
		l.fault.failWrites--
		l.fault.mu.Unlock()
		return 0, errInjectedWrite
	case shortWrites > 0:
		l.fault.shortWrites--
		l.fault.mu.Unlock()
		half := len(data) / 2
		if half == 0 {
			half = 1
		}
		written, err := l.durableLog.Write(data[:half])
		if err != nil {
			return written, err
		}
		return written, io.ErrShortWrite
	default:
		l.fault.mu.Unlock()
		return l.durableLog.Write(data)
	}
}

func (l *faultLog) Sync() error {
	l.fault.mu.Lock()
	l.fault.syncs++
	if l.fault.failSyncs > 0 {
		l.fault.failSyncs--
		l.fault.mu.Unlock()
		return errInjectedSync
	}
	l.fault.mu.Unlock()
	return l.durableLog.Sync()
}

func (l *faultLog) Truncate(size int64) error {
	l.fault.mu.Lock()
	l.fault.truncates++
	fail := l.fault.failTruncate
	l.fault.mu.Unlock()
	if fail {
		return errInjectedTruncate
	}
	return l.durableLog.Truncate(size)
}

func TestDurableStoreRollsBackFailedWriteAndReopens(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fault := &logFault{}
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
		wrapLog:       fault.wrap,
	})
	observed := make(chan Entry, 4)
	stop := store.Observe(func(entry Entry) { observed <- entry })
	defer stop()

	first, err := store.Put(ctx, "opaque/first", []byte("v1"))
	if err != nil {
		t.Fatalf("Put first: %v", err)
	}
	if event := <-observed; event.ChangeID != first.ChangeID {
		t.Fatalf("observer saw %+v want %+v", event, first)
	}

	// The injected failure happens after half of the record reached the file, so
	// it leaves a genuinely torn line behind unless the append is rolled back.
	fault.arm(func(f *logFault) { f.shortWrites = 1 })
	if _, err := store.Put(ctx, "opaque/torn", []byte("torn")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected short write error, got %v", err)
	}
	if _, ok, err := store.Get(ctx, "opaque/torn"); err != nil || ok {
		t.Fatalf("failed commit became visible: ok=%v err=%v", ok, err)
	}
	assertNoEvent(t, observed)

	third, err := store.Put(ctx, "opaque/third", []byte("v3"))
	if err != nil {
		t.Fatalf("Put after a rolled back append: %v", err)
	}
	if third.ChangeID.Counter <= first.ChangeID.Counter {
		t.Fatalf("local change counter was reused or regressed: first=%+v third=%+v", first.ChangeID, third.ChangeID)
	}
	if event := <-observed; event.ChangeID != third.ChangeID {
		t.Fatalf("observer saw %+v want %+v", event, third)
	}

	// Nothing torn and no duplicated sequence survives in the log.
	records := readDurableLogRecords(t, dir)
	assertIncreasingSequences(t, records)

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The directory still opens, which is the point of the rollback: a failed
	// append must not make a durable replica permanently unopenable.
	recovered := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	defer recovered.Close()
	if got := mustGet(t, recovered, "opaque/first"); got.ChangeID != first.ChangeID {
		t.Fatalf("recovered first entry mismatch: got %+v want %+v", got, first)
	}
	if got := mustGet(t, recovered, "opaque/third"); got.ChangeID != third.ChangeID {
		t.Fatalf("recovered third entry mismatch: got %+v want %+v", got, third)
	}
	if _, ok, err := recovered.Get(ctx, "opaque/torn"); err != nil || ok {
		t.Fatalf("rolled back entry was recovered: ok=%v err=%v", ok, err)
	}
	fourth, err := recovered.Put(ctx, "opaque/fourth", []byte("v4"))
	if err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if fourth.ChangeID.Counter <= third.ChangeID.Counter {
		t.Fatalf("recovered store reused a change counter: third=%+v fourth=%+v", third.ChangeID, fourth.ChangeID)
	}
	assertIncreasingSequences(t, readDurableLogRecords(t, dir))
}

func TestDurableStoreRollsBackCompleteButUnsyncedRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fault := &logFault{}
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
		wrapLog:       fault.wrap,
	})
	observed := make(chan Entry, 4)
	stop := store.Observe(func(entry Entry) { observed <- entry })
	defer stop()

	first, err := store.Put(ctx, "opaque/first", []byte("v1"))
	if err != nil {
		t.Fatalf("Put first: %v", err)
	}
	if event := <-observed; event.ChangeID != first.ChangeID {
		t.Fatalf("observer saw %+v want %+v", event, first)
	}

	// The record is written completely and only the sync fails: without a
	// rollback the next successful append would reuse the same sequence and
	// recovery would fail with "sequence regressed" forever.
	fault.arm(func(f *logFault) { f.failSyncs = 1 })
	if _, err := store.Put(ctx, "opaque/unsynced", []byte("unsynced")); !errors.Is(err, errInjectedSync) {
		t.Fatalf("expected sync failure, got %v", err)
	}
	if _, ok, err := store.Get(ctx, "opaque/unsynced"); err != nil || ok {
		t.Fatalf("unsynced commit became visible: ok=%v err=%v", ok, err)
	}
	assertNoEvent(t, observed)

	second, err := store.Put(ctx, "opaque/second", []byte("v2"))
	if err != nil {
		t.Fatalf("Put after a rolled back sync: %v", err)
	}
	if event := <-observed; event.ChangeID != second.ChangeID {
		t.Fatalf("observer saw %+v want %+v", event, second)
	}
	records := readDurableLogRecords(t, dir)
	if len(records) != 2 {
		t.Fatalf("log kept rolled back bytes: %d records %+v", len(records), records)
	}
	assertIncreasingSequences(t, records)

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	recovered := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	defer recovered.Close()
	if got := mustGet(t, recovered, "opaque/second"); got.ChangeID != second.ChangeID {
		t.Fatalf("recovered second entry mismatch: got %+v want %+v", got, second)
	}
	if _, ok, err := recovered.Get(ctx, "opaque/unsynced"); err != nil || ok {
		t.Fatalf("unsynced entry was recovered: ok=%v err=%v", ok, err)
	}
	third, err := recovered.Put(ctx, "opaque/third", []byte("v3"))
	if err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if third.ChangeID.Counter <= second.ChangeID.Counter {
		t.Fatalf("recovered store reused a change counter: second=%+v third=%+v", second.ChangeID, third.ChangeID)
	}
}

func TestDurableStoreFailsClosedWhenRollbackFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fault := &logFault{}
	store := mustDurableStore(t, DurableStoreConfig{
		Dir:           dir,
		NodeID:        "node-a",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
		wrapLog:       fault.wrap,
	})
	observed := make(chan Entry, 4)
	stop := store.Observe(func(entry Entry) { observed <- entry })
	defer stop()

	first, err := store.Put(ctx, "opaque/first", []byte("v1"))
	if err != nil {
		t.Fatalf("Put first: %v", err)
	}
	if event := <-observed; event.ChangeID != first.ChangeID {
		t.Fatalf("observer saw %+v want %+v", event, first)
	}

	// A torn write that cannot be truncated leaves ambiguous bytes in the log, so
	// the store must never write again.
	fault.arm(func(f *logFault) {
		f.shortWrites = 1
		f.failTruncate = true
	})
	if _, err := store.Put(ctx, "opaque/unwritable", []byte("v")); !errors.Is(err, ErrDurableStoreUnwritable) {
		t.Fatalf("expected fail-closed error, got %v", err)
	}
	writesAfterFailure := fault.writeCalls()
	truncatesAfterFailure := fault.truncateCalls()

	remote := newEntryForTest("opaque/remote", []byte("remote"), "node-b", 1, Timestamp{WallMS: 2_000})
	for name, call := range map[string]func() error{
		"Put": func() error {
			_, err := store.Put(ctx, "opaque/after", []byte("v"))
			return err
		},
		"Delete": func() error {
			_, err := store.Delete(ctx, "opaque/first")
			return err
		},
		"CompareAndSet": func() error {
			_, _, err := store.CompareAndSet(ctx, "opaque/first", first.Identity(), Mutation{Value: []byte("v2")})
			return err
		},
		"Apply": func() error {
			_, err := store.Apply(ctx, remote)
			return err
		},
		"SavePeerProgress": func() error {
			return store.SavePeerProgress(ctx, "peer-b", 7)
		},
	} {
		if err := call(); !errors.Is(err, ErrDurableStoreUnwritable) {
			t.Fatalf("%s after a failed rollback = %v, want ErrDurableStoreUnwritable", name, err)
		}
	}
	if got := fault.writeCalls(); got != writesAfterFailure {
		t.Fatalf("store wrote after failing closed: %d writes, want %d", got, writesAfterFailure)
	}
	if got := fault.truncateCalls(); got != truncatesAfterFailure {
		t.Fatalf("store attempted more rollbacks after failing closed: %d, want %d", got, truncatesAfterFailure)
	}
	assertNoEvent(t, observed)

	// Reads keep serving what was already confirmed, and nothing new is visible.
	if got := mustGet(t, store, "opaque/first"); got.ChangeID != first.ChangeID {
		t.Fatalf("fail-closed store lost confirmed state: got %+v want %+v", got, first)
	}
	for _, key := range []string{"opaque/unwritable", "opaque/after", "opaque/remote"} {
		if _, ok, err := store.Get(ctx, key); err != nil || ok {
			t.Fatalf("%s became visible after a failed rollback: ok=%v err=%v", key, ok, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The torn bytes are still on disk, and a later cold start repairs them
	// instead of refusing the directory.
	recovered := mustDurableStore(t, DurableStoreConfig{Dir: dir, NodeID: "node-a"})
	defer recovered.Close()
	if got := mustGet(t, recovered, "opaque/first"); got.ChangeID != first.ChangeID {
		t.Fatalf("recovered first entry mismatch: got %+v want %+v", got, first)
	}
	if _, ok, err := recovered.Get(ctx, "opaque/unwritable"); err != nil || ok {
		t.Fatalf("torn record was recovered: ok=%v err=%v", ok, err)
	}
	second, err := recovered.Put(ctx, "opaque/second", []byte("v2"))
	if err != nil {
		t.Fatalf("repaired store cannot accept writes: %v", err)
	}
	if second.ChangeID.Counter <= first.ChangeID.Counter {
		t.Fatalf("repaired store reused a change counter: first=%+v second=%+v", first.ChangeID, second.ChangeID)
	}
}

func TestDurableHandlePushDoesNotAcknowledgeUnpersistedApply(t *testing.T) {
	ctx := context.Background()
	token := "durable-push-fault-token"
	fault := &logFault{}
	a := newDurableHTTPTestNode(t, "node-a", token, t.TempDir(), &fixedPhysicalClock{now: 1_000}, nil)
	b := newDurableHTTPTestNodeWithConfig(t, "node-b", token, DurableStoreConfig{
		Dir:           t.TempDir(),
		NodeID:        "node-b",
		PhysicalClock: &fixedPhysicalClock{now: 1_000},
		wrapLog:       fault.wrap,
	}, nil)
	defer closeHTTPTestNode(t, a)
	defer closeHTTPTestNode(t, b)
	a.peers.Store([]string{b.server.URL})
	b.peers.Store([]string{a.server.URL})

	entry, err := a.store.Put(ctx, "opaque/push", []byte("v1"))
	if err != nil {
		t.Fatalf("node-a Put: %v", err)
	}

	// A durable apply failure is not a rejection: HandlePush reports the error
	// instead of answering with a fake success.
	fault.arm(func(f *logFault) { f.failWrites = 1 })
	resp, err := b.engine.HandlePush(ctx, PushRequest{
		ProtocolVersion: StateProtocolVersion,
		PolicyID:        NewHLCLWWPolicy().ID(),
		SourceNodeID:    "node-a",
		Entries:         []Entry{entry},
	})
	if err == nil {
		t.Fatalf("HandlePush swallowed a persistence failure: resp=%+v", resp)
	}
	if resp != nil {
		t.Fatalf("HandlePush answered a failed apply with %+v", resp)
	}
	if _, ok, err := b.store.Get(ctx, "opaque/push"); err != nil || ok {
		t.Fatalf("unpersisted apply became visible on node-b: ok=%v err=%v", ok, err)
	}

	// End to end: the receiver fails, so the sender cannot record the push as
	// confirmed and must keep the entry replayable.
	fault.arm(func(f *logFault) { f.failSyncs = 1 })
	if err := a.engine.SyncOnce(ctx); err == nil {
		t.Fatal("expected the push to fail while node-b cannot persist")
	}
	if got, err := a.store.(*DurableStore).PeerProgress(ctx, b.server.URL); err != nil || got != 0 {
		t.Fatalf("failed push advanced the sender cursor: got %d err=%v", got, err)
	}
	if _, ok, err := b.store.Get(ctx, "opaque/push"); err != nil || ok {
		t.Fatalf("node-b stored an entry its log never confirmed: ok=%v err=%v", ok, err)
	}

	clearPeerBackoffForTest(a.engine, b.server.URL)
	if err := a.engine.SyncOnce(ctx); err != nil {
		t.Fatalf("retry SyncOnce: %v", err)
	}
	assertStoredValue(t, b.store, "opaque/push", "v1")
	if got, err := a.store.(*DurableStore).PeerProgress(ctx, b.server.URL); err != nil || got != entry.Position {
		t.Fatalf("sender cursor did not advance after the acknowledged retry: got %d want %d err=%v", got, entry.Position, err)
	}
}

func durableEntryRecordLine(t *testing.T, sequence uint64, key string, counter uint64) []byte {
	t.Helper()
	entry := newEntryForTest(key, []byte("v"), "node-a", counter, Timestamp{WallMS: 1_000})
	return durableRecordLine(t, durableRecord{
		Type:     durableRecordEntry,
		Sequence: sequence,
		Entry:    &entry,
		Position: counter,
	})
}

func durableRecordLine(t *testing.T, record durableRecord) []byte {
	t.Helper()
	record.FormatVersion = durableFormatVersion
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal durable record: %v", err)
	}
	return append(data, '\n')
}

// readDurableLogRecords reads the log as bytes and fails when any byte on disk is
// not part of a complete, decodable, newline-terminated record.
func readDurableLogRecords(t *testing.T, dir string) []durableRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, durableLogFile))
	if err != nil {
		t.Fatalf("read durable log: %v", err)
	}
	if len(data) == 0 {
		return nil
	}
	if data[len(data)-1] != '\n' {
		t.Fatalf("durable log ends with a torn record: %q", data)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	records := make([]durableRecord, 0, len(lines))
	for index, line := range lines {
		var record durableRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("durable log line %d is not a complete record: %v (%q)", index+1, err, line)
		}
		records = append(records, record)
	}
	return records
}

// assertIncreasingSequences proves that no confirmed record on disk reuses or
// regresses a durable sequence number.
func assertIncreasingSequences(t *testing.T, records []durableRecord) {
	t.Helper()
	var previous uint64
	for _, record := range records {
		if record.Sequence == 0 {
			t.Fatalf("durable record has no sequence: %+v", record)
		}
		if record.Sequence <= previous {
			t.Fatalf("durable sequence reused or regressed: got %d after %d", record.Sequence, previous)
		}
		previous = record.Sequence
	}
}
