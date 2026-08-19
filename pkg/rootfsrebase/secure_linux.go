//go:build linux

package rootfsrebase

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const secureResolve = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS

type secureRoot struct {
	path string
	fd   int
	stat unix.Stat_t
}

func openSecureRoot(value string, writable bool) (*secureRoot, error) {
	value = filepath.Clean(strings.TrimSpace(value))
	if !filepath.IsAbs(value) || value == string(filepath.Separator) {
		return nil, fmt.Errorf("RootFS root must be a non-root absolute path")
	}
	info, err := os.Lstat(value)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("RootFS root must be a real directory")
	}
	flags := unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW
	fd, err := unix.Open(value, flags, 0)
	if err != nil {
		return nil, err
	}
	root := &secureRoot{path: value, fd: fd}
	if err := unix.Fstat(fd, &root.stat); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if writable {
		probe, err := root.open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			unix.Close(fd)
			return nil, fmt.Errorf("open writable RootFS target: %w", err)
		}
		unix.Close(probe)
	}
	return root, nil
}

func (r *secureRoot) close() {
	if r != nil && r.fd >= 0 {
		_ = unix.Close(r.fd)
		r.fd = -1
	}
}

func (r *secureRoot) open(relative string, flags int, mode uint32) (int, error) {
	if !validManifestPath(relative) {
		return -1, fmt.Errorf("invalid RootFS path %q", relative)
	}
	how := &unix.OpenHow{
		Flags:   uint64(flags | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:    uint64(mode),
		Resolve: secureResolve,
	}
	fd, err := unix.Openat2(r.fd, relative, how)
	if errors.Is(err, unix.ENOSYS) {
		return -1, fmt.Errorf("openat2 is required for RootFS rebase: %w", err)
	}
	return fd, err
}

func (r *secureRoot) parent(relative string) (int, string, error) {
	if !validManifestPath(relative) || relative == "." {
		return -1, "", fmt.Errorf("invalid non-root RootFS path %q", relative)
	}
	parentPath := path.Dir(relative)
	leaf := path.Base(relative)
	fd, err := r.open(parentPath, unix.O_PATH|unix.O_DIRECTORY, 0)
	if err != nil {
		return -1, "", err
	}
	return fd, leaf, nil
}

func (r *secureRoot) verifyIdentity() error {
	var current unix.Stat_t
	if err := unix.Lstat(r.path, &current); err != nil {
		return err
	}
	if current.Dev != r.stat.Dev || current.Ino != r.stat.Ino || current.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("RootFS root identity changed during rebase")
	}
	return nil
}

func procLeafPath(parentFD int, leaf string) string {
	return fmt.Sprintf("/proc/self/fd/%d/%s", parentFD, leaf)
}
