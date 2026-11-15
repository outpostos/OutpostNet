//go:build linux

package state

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

func kernelTimeStatus() (time.Duration, bool, error) {
	var tx unix.Timex // modes=0: read only, unprivileged
	clockState, err := unix.Adjtimex(&tx)
	if err != nil {
		return 0, false, err
	}
	if tx.Maxerror < 0 || tx.Maxerror > (1<<63-1)/int64(time.Microsecond) {
		return 0, false, fmt.Errorf("invalid kernel maximum error")
	}
	return time.Duration(tx.Maxerror) * time.Microsecond, clockState != unix.TIME_ERROR && tx.Status&(unix.STA_UNSYNC|unix.STA_CLOCKERR) == 0, nil
}
