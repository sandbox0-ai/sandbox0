//go:build linux

package rootfsblock

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const liveIndexSeals = unix.F_SEAL_SEAL | unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_WRITE

func newLiveIndexFile(_ string) (*os.File, error) {
	fd, err := unix.MemfdCreate("s0-live-branch-index", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "s0-live-branch-index"), nil
}
func sealLiveIndex(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, liveIndexSeals)
	return err
}
func validateLiveIndexSeal(file *os.File) error {
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	if err != nil || seals&liveIndexSeals != liveIndexSeals {
		return fmt.Errorf("live branch index is not sealed: %w", err)
	}
	return nil
}

func liveJournalIdentity(file *os.File) (uint64, uint64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return 0, 0, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return 0, 0, fmt.Errorf("live journal is not a regular file")
	}
	return uint64(stat.Dev), stat.Ino, nil
}
