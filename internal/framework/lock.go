package framework

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const defaultE2ELockTimeout = 10 * time.Minute

// AcquireE2ELock acquires a file lock for a given cluster name.
// It blocks with retries until the lock is acquired or timeout is reached.
func AcquireE2ELock(ctx context.Context, clusterName string, timeout time.Duration) (func(), error) {
	if timeout <= 0 {
		timeout = defaultE2ELockTimeout
	}

	lockPath := filepath.Join(os.TempDir(), fmt.Sprintf("sandbox0-e2e-%s.lock", sanitizeLockName(clusterName)))
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	deadline := time.Now().Add(timeout)
	for {
		if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			release := func() {
				_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
				_ = file.Close()
			}
			return release, nil
		}

		if time.Now().After(deadline) {
			_ = file.Close()
			return nil, fmt.Errorf("timed out waiting for e2e lock %q", lockPath)
		}

		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, fmt.Errorf("context cancelled while waiting for e2e lock: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func sanitizeLockName(name string) string {
	name = strings.ReplaceAll(name, string(os.PathSeparator), "-")
	name = strings.TrimSpace(name)
	if name == "" {
		return "default"
	}
	return name
}
