//go:build linux

package state_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func TestOSTimeSourceDefaultRunner(t *testing.T) {
	data, err := os.ReadFile("testdata/time-source/chrony-node1.csv")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	command := filepath.Join(bin, "chronyc")
	script := "#!/bin/sh\n[ \"$LC_ALL\" = C ] || exit 1\n[ \"$1 $2\" = '-c tracking' ] || exit 2\nprintf '%s\\n' '" + string(data[:len(data)-1]) + "'\n"
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "timedatectl"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	probe, _ := state.NewOSTimeSource(state.OSTimeSourceConfig{CommandTimeout: 200 * time.Millisecond})
	if s := probe.Poll(context.Background()); !s.Synchronized || s.Source != "chrony:202.118.1.130" {
		t.Fatalf("default read-only command: %+v", s)
	}
	// An unresponsive executable must not hold up a timestamp consumer or shutdown.
	if err := os.WriteFile(command, []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if s := probe.Poll(context.Background()); s.Synchronized {
		t.Fatalf("stalled source trusted: %+v", s)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("default command did not time out")
	}
}
