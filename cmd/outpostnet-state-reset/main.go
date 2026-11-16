// outpostnet-state-reset repairs an existing state directory while its host is stopped.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/outpostos/OutpostNet/pkg/state"
)

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("outpostnet-state-reset", flag.ContinueOnError)
	dir := flags.String("data-dir", "", "existing pkg/state directory; stop its host first")
	reference := flags.String("reference-time", "", "confirmed reference time in RFC3339 (default: corrected system clock)")
	rebuild := flags.Bool("rebuild", false, "archive ALL old state files if its GC horizon is in the future; rebuild from healthy peers after restart")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dir == "" || flags.NArg() != 0 {
		return fmt.Errorf("--data-dir is required; positional arguments are not accepted")
	}
	cfg := state.ClockResetConfig{Dir: *dir, Rebuild: *rebuild}
	if *reference != "" {
		timestamp, err := time.Parse(time.RFC3339Nano, *reference)
		if err != nil {
			return fmt.Errorf("invalid --reference-time: %w", err)
		}
		cfg.Reference = state.PhysicalClockFunc(func() int64 { return timestamp.UnixMilli() })
	}
	result, err := state.ResetDurableClock(context.Background(), cfg)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
