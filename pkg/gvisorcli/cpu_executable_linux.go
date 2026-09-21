//go:build linux && (amd64 || arm64)

package gvisorcli

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

type cpuExecutableMonitor struct {
	mu      sync.Mutex
	file    *os.File
	invalid bool
}

// One nonblocking monitor belongs to the shared warm cache, not each carrier.
// Any subscribed event, lost watch or overflow invalidates the whole snapshot.
// This supplements metadata checks for trusted, locally installed runsc files;
// it is not attestation against a hostile host or mmap/remote filesystem writes.
func watchCPUExecutable(path string, expected os.FileInfo) (*cpuExecutableMonitor, error) {
	executable, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer executable.Close()
	opened, err := executable.Stat()
	if err != nil || !sameCPUExecutable(expected, opened) {
		return nil, errors.Join(fmt.Errorf("runsc changed before installing executable monitor"), err)
	}
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, err
	}
	// Watch the inspected open inode, not a second lookup of a replaceable
	// pathname. The regular path is still rechecked during every validation.
	openedPath := fmt.Sprintf("/proc/self/fd/%d", executable.Fd())
	if _, err := unix.InotifyAddWatch(fd, openedPath, unix.IN_MODIFY|unix.IN_ATTRIB|unix.IN_CLOSE_WRITE|unix.IN_MOVE_SELF|unix.IN_DELETE_SELF); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &cpuExecutableMonitor{file: os.NewFile(uintptr(fd), "runsc-cpu-monitor")}, nil
}

func (m *cpuExecutableMonitor) Unchanged() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.invalid || m.file == nil {
		return fmt.Errorf("CPU launch executable monitor is invalid")
	}
	conn, err := m.file.SyscallConn()
	if err != nil {
		m.invalid = true
		return err
	}
	var readErr error
	n := 0
	err = conn.Control(func(fd uintptr) {
		var event [4096]byte
		n, readErr = unix.Read(int(fd), event[:])
	})
	// A nonblocking empty queue is the only successful observation. No event
	// is filtered away, and a failure can never be cleared by draining again.
	if err == nil && n <= 0 && errors.Is(readErr, unix.EAGAIN) {
		return nil
	}
	m.invalid = true
	return errors.Join(fmt.Errorf("CPU launch executable changed or monitoring failed"), err, readErr)
}

func (m *cpuExecutableMonitor) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.invalid = true
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}
