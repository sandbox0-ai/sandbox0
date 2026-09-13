//go:build linux

package rootfsblock

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func materializedFileSeekEnabled(file *os.File) (bool, error) {
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return false, fmt.Errorf("inspect materialized image descriptor: %w", err)
	}
	if flags&unix.O_ACCMODE != unix.O_RDONLY {
		return false, fmt.Errorf("materialized image descriptor must be read-only")
	}
	return true, nil
}

func seekMaterializedFile(file *os.File, offset int64, hole bool) (int64, error) {
	whence := unix.SEEK_DATA
	if hole {
		whence = unix.SEEK_HOLE
	}
	next, err := file.Seek(offset, whence)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		return 0, errMaterializedSeekUnsupported
	}
	if !hole && errors.Is(err, unix.ENXIO) {
		return 0, io.EOF
	}
	return next, err
}

func sameMaterializedFile(before, after os.FileInfo) bool {
	a, aOK := before.Sys().(*syscall.Stat_t)
	b, bOK := after.Sys().(*syscall.Stat_t)
	return aOK && bOK && os.SameFile(before, after) && before.Mode() == after.Mode() &&
		before.Size() == after.Size() && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}
