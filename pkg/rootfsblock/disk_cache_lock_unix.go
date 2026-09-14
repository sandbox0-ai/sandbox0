//go:build unix

package rootfsblock

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const diskCacheOpenFlags = unix.O_NOFOLLOW | unix.O_NONBLOCK

func lockDiskReadCache(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

func validateDiskCacheOwner(file *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("disk cache must be owned by the node daemon's effective user")
	}
	return nil
}
