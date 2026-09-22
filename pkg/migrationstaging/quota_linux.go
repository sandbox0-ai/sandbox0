//go:build linux

package migrationstaging

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"
)

// Linux UAPI: fs.h (fsxattr) and dqblk_xfs.h (fs_quota_statv version 1,
// fs_disk_quota version 1). Fixed byte offsets avoid Go/C alignment differences.
// quotactl_fd pins the filesystem through the directory FD (Linux >= 5.14).
func inspect(root string, project uint32) (snapshot, error) {
	var result snapshot
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return result, fmt.Errorf("migration staging path must not contain symlinks: %w", errdefs.ErrFailedPrecondition)
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return result, err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	var fs unix.Statfs_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return result, err
	}
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return result, err
	}
	if stat.Uid != 0 || stat.Mode&0077 != 0 || fs.Type != 0x58465342 {
		return result, fmt.Errorf("migration staging requires a private root-owned XFS directory: %w", errdefs.ErrFailedPrecondition)
	}
	var attr [28]byte
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), 0x801c581f, uintptr(unsafe.Pointer(&attr[0])))
	runtime.KeepAlive(&attr)
	if errno != 0 {
		return result, fmt.Errorf("read migration staging project attributes: %w", errno)
	}
	flags := binary.NativeEndian.Uint32(attr[0:4])
	// Realtime allocation would use a separate quota counter. It is not
	// allowed in this pool, including inheritance by newly-created children.
	if flags&(0x1|0x100) != 0 {
		return result, fmt.Errorf("migration staging cannot use realtime allocation: %w", errdefs.ErrFailedPrecondition)
	}
	var status [160]byte
	status[0] = 1
	if err := quotaFD(fd, 8, project, status[:]); err != nil {
		return result, err
	}
	var quota [112]byte
	if err := quotaFD(fd, 3, project, quota[:]); err != nil {
		return result, err
	}
	if status[0] != 1 || quota[0] != 1 || quota[1] != 2 || binary.NativeEndian.Uint32(quota[4:8]) != project {
		return result, fmt.Errorf("migration staging quota returned another version or project: %w", errdefs.ErrFailedPrecondition)
	}
	if fs.Bsize <= 0 || fs.Bavail > math.MaxUint64/uint64(fs.Bsize) {
		return result, fmt.Errorf("migration staging filesystem capacity is invalid: %w", errdefs.ErrFailedPrecondition)
	}
	return snapshot{device: uint64(stat.Dev), inode: stat.Ino, project: binary.NativeEndian.Uint32(attr[12:16]),
		freeBytes: fs.Bavail * uint64(fs.Bsize),
		inherit:   flags&0x200 != 0, enforced: binary.NativeEndian.Uint16(status[2:4])&0x30 == 0x30,
		hardBlocks: binary.NativeEndian.Uint64(quota[8:16]), hardInodes: binary.NativeEndian.Uint64(quota[24:32]),
		blocks: binary.NativeEndian.Uint64(quota[40:48]), inodes: binary.NativeEndian.Uint64(quota[48:56])}, nil
}

// A previous unbounded deployment may have uncharged children even if an
// administrator set only the parent's project ID. Never silently bless that
// state. Subsequent trusted writers create descendants through PROJINHERIT;
// XFS also enforces project boundaries for link/rename operations.
func verifyExistingTree(root string, limits Limits, device uint64) error {
	var entries uint64
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > limits.Inodes || entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("migration staging tree is unbounded or contains a symlink: %w", errdefs.ErrFailedPrecondition)
		}
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		kind := stat.Mode & unix.S_IFMT
		if uint64(stat.Dev) != device || (kind != unix.S_IFDIR && kind != unix.S_IFREG) {
			return fmt.Errorf("migration staging child is not an ordinary file on its quota filesystem: %w", errdefs.ErrFailedPrecondition)
		}
		var attr [28]byte
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), 0x801c581f, uintptr(unsafe.Pointer(&attr[0])))
		runtime.KeepAlive(&attr)
		if errno != 0 {
			return errno
		}
		flags := binary.NativeEndian.Uint32(attr[:4])
		if binary.NativeEndian.Uint32(attr[12:16]) != limits.ProjectID || flags&(0x1|0x100) != 0 || (kind == unix.S_IFDIR && flags&0x200 == 0) {
			return fmt.Errorf("migration staging has an uncharged or noninheriting child: %w", errdefs.ErrFailedPrecondition)
		}
		return nil
	})
}

func quotaFD(fd int, command, project uint32, output []byte) error {
	cmd := ((uint32('X')<<8 + command) << 8) | 2
	_, _, errno := unix.Syscall6(unix.SYS_QUOTACTL_FD, uintptr(fd), uintptr(cmd), uintptr(project), uintptr(unsafe.Pointer(&output[0])), 0, 0)
	runtime.KeepAlive(output)
	if errno != 0 {
		return fmt.Errorf("read migration staging kernel quota: %w", errno)
	}
	return nil
}
