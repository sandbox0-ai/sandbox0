//go:build linux

package ha

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// ExportFile duplicates the same open file description, preserving the flock
// continuously across transfer. The successor must not act until source commit.
func (l *PrimaryLease) ExportFile() (*os.File, error) {
	if l == nil || l.lockFile == nil {
		return nil, fmt.Errorf("primary lease is absent")
	}
	fd, err := unix.FcntlInt(l.lockFile.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "ctld-inherited-primary"), nil
}

// Relinquish closes only this process's description. LOCK_UN would also
// unlock the inherited description and allow an unrelated standby to promote.
func (l *PrimaryLease) Relinquish() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeErr = l.lockFile.Close()
		l.coordinator.setState(func(state *State) { *state = State{Role: RoleDraining, Epoch: l.Epoch} })
	})
	return l.closeErr
}

func (c *Coordinator) AdoptTransferred(file *os.File, expectedEpoch uint64) (*PrimaryLease, error) {
	if file == nil || expectedEpoch == 0 {
		return nil, fmt.Errorf("invalid inherited primary lease")
	}
	actual, err := file.Stat()
	if err != nil {
		return nil, err
	}
	expected, err := os.Stat(filepath.Join(c.rootDir, "ha", "primary.lock"))
	if err != nil || !os.SameFile(actual, expected) {
		return nil, fmt.Errorf("inherited primary lock has a different inode: %w", err)
	}
	epoch, err := c.currentEpoch()
	if err != nil || epoch != expectedEpoch {
		return nil, fmt.Errorf("inherited primary epoch changed: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	if err := c.recordLockIdentity(file); err != nil {
		return nil, err
	}
	epoch, err = c.advanceEpoch()
	if err != nil {
		return nil, err
	}
	c.setState(func(state *State) { *state = State{Role: RolePrimary, Epoch: epoch, Synchronized: true} })
	return &PrimaryLease{Epoch: epoch, coordinator: c, lockFile: file}, nil
}
