//go:build linux

package runtimecheckpoint

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// Submit only the received range for asynchronous data writeback. This is a
// latency hint, never a durability receipt: CapturePeerCache retains the same
// descriptor and observes errors at final fsync. Unsupported filesystems keep
// the ordinary final-sync path. No background worker outlives cache custody.
func capturePeerWriteback(file *os.File, offset, length int64) error {
	err := unix.SyncFileRange(int(file.Fd()), offset, length, unix.SYNC_FILE_RANGE_WRITE)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
		return nil
	}
	return err
}
