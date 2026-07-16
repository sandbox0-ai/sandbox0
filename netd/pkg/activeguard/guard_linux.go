//go:build linux

// Package activeguard fences node-local netd runtimes during workload and ctld
// HA transitions.
package activeguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// EnvPath configures the shared node-local lock path.
	EnvPath = "NETD_ACTIVE_LOCK_PATH"
	// EnvInitialDelay gives a migration standby an acquisition head start to
	// the embedded ctld runtime before it begins waiting on the shared lock.
	EnvInitialDelay = "NETD_ACTIVE_LOCK_INITIAL_DELAY"
	// EnvMaxHold makes a migration fallback periodically yield the lock so a
	// recovered embedded ctld runtime can retry without manual intervention.
	EnvMaxHold = "NETD_ACTIVE_LOCK_MAX_HOLD"
	retryDelay = 100 * time.Millisecond
)

// Guard owns the node-local exclusive netd lock.
type Guard struct {
	file      *os.File
	closeOnce sync.Once
	closeErr  error
}

// Acquire waits until ctx is canceled or this process exclusively owns path.
func Acquire(ctx context.Context, path string) (*Guard, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("netd active lock path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create netd active lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open netd active lock: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return nil, err
		}
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &Guard{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("acquire netd active lock: %w", err)
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Close releases the active lock.
func (g *Guard) Close() error {
	if g == nil {
		return nil
	}
	g.closeOnce.Do(func() {
		if g.file == nil {
			return
		}
		if err := unix.Flock(int(g.file.Fd()), unix.LOCK_UN); err != nil {
			g.closeErr = err
		}
		if err := g.file.Close(); err != nil && g.closeErr == nil {
			g.closeErr = err
		}
	})
	return g.closeErr
}
