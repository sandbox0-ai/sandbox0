//go:build linux

package runtimecheckpoint

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Each file is sealed on the exact materializer-owned descriptor after all
// verified writes/clones have joined and fsync has completed, before closing
// that descriptor. Private staging has one exclusive writer. No open/path
// transition is used as evidence that the bytes themselves were checked.
type imageVerificationGuard struct {
	fd    int
	root  os.FileInfo
	files map[string]os.FileInfo
}

func newImageVerificationGuard() *imageVerificationGuard {
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return nil
	}
	return &imageVerificationGuard{fd: fd, files: make(map[string]os.FileInfo)}
}

func sameVerifiedImageFile(a, b os.FileInfo) bool {
	if a == nil || b == nil || !os.SameFile(a, b) || a.Size() != b.Size() || a.Mode() != b.Mode() || !a.ModTime().Equal(b.ModTime()) {
		return false
	}
	first, ok := a.Sys().(*syscall.Stat_t)
	second, other := b.Sys().(*syscall.Stat_t)
	return ok && other && first.Ctim == second.Ctim
}

func (g *imageVerificationGuard) seal(name string, file *os.File) {
	if g == nil || g.fd < 0 {
		return
	}
	before, err := file.Stat()
	if err != nil {
		g.close()
		return
	}
	// Do not watch access/open events: runsc may read the immutable image.
	// MODIFY also catches changes within the same filesystem timestamp tick.
	_, err = unix.InotifyAddWatch(g.fd, fmt.Sprintf("/proc/self/fd/%d", file.Fd()), unix.IN_MODIFY|unix.IN_ATTRIB|unix.IN_MOVE_SELF|unix.IN_DELETE_SELF|unix.IN_UNMOUNT)
	after, statErr := file.Stat()
	if err != nil || statErr != nil || !sameVerifiedImageFile(before, after) {
		g.close()
		return
	}
	g.files[name] = after
}

func (g *imageVerificationGuard) finish(root *os.Root, manifest Manifest) bool {
	if g == nil || g.fd < 0 || len(g.files) != len(manifest.Files) {
		return false
	}
	info, err := root.Stat(".")
	if err != nil {
		return false
	}
	g.root = info
	return g.valid(root, manifest)
}

func (g *imageVerificationGuard) valid(root *os.Root, manifest Manifest) bool {
	if g == nil || g.fd < 0 || len(g.files) != len(manifest.Files) {
		return false
	}
	info, err := root.Stat(".")
	if err != nil || !sameVerifiedImageFile(g.root, info) {
		return false
	}
	for _, file := range manifest.Files {
		current, err := root.Stat(file.Path)
		if err != nil || !sameVerifiedImageFile(g.files[file.Path], current) || current.Size() != file.Size {
			return false
		}
	}
	// Any pending event, including queue overflow/watch removal, invalidates
	// the whole proof. Metadata is checked first and events last so changes
	// during the identity checks cannot be accidentally acknowledged.
	var event [4096]byte
	for {
		n, err := unix.Read(g.fd, event[:])
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return n == -1 && errors.Is(err, unix.EAGAIN)
	}
}

func (g *imageVerificationGuard) close() {
	if g != nil && g.fd >= 0 {
		_ = unix.Close(g.fd)
		g.fd = -1
	}
}
