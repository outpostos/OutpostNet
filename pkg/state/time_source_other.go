//go:build !linux

package state

import (
	"fmt"
	"time"
)

func kernelTimeStatus() (time.Duration, bool, error) {
	return 0, false, fmt.Errorf("kernel time status unavailable on this platform")
}
