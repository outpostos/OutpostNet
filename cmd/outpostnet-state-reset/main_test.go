package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestResetCommand(t *testing.T) {
	dir := t.TempDir()
	clock := state.PhysicalClockFunc(func() int64 { return time.Date(2027, 11, 15, 0, 0, 0, 0, time.UTC).UnixMilli() })
	s, err := state.NewDurableStore(state.DurableStoreConfig{Dir: dir, NodeID: "command-node", PhysicalClock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(context.Background(), "future", nil); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	args := []string{"--data-dir", dir, "--reference-time", "2026-11-15T00:00:00Z"}
	if err := run(args, &output); err == nil || output.Len() != 0 {
		t.Fatal("command succeeded against running host")
	}
	s.Close()
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	var result state.ClockResetResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Purged != 1 {
		t.Fatalf("command result: %s %v", output.String(), err)
	}
	if result.Archived != 1 || filepath.Dir(result.ArchivePath) != filepath.Join(dir, "archive") {
		t.Fatalf("command archive result: %+v", result)
	}
	data, err := os.ReadFile(result.ArchivePath)
	if err != nil {
		t.Fatal(err)
	}
	var archived state.Entry
	if err := json.Unmarshal(data, &archived); err != nil || archived.Key != "future" {
		t.Fatalf("command did not retain purged entry: %v", err)
	}
	for _, args := range [][]string{nil, {"--data-dir", dir, "--reference-time", "bad"}, {"--data-dir", dir, "extra"}} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid args succeeded: %v", args)
		}
	}
}
