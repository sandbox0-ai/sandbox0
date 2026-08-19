//go:build linux

package runtimeslot

import (
	"fmt"
	"os"
	"syscall"

	"github.com/containerd/errdefs"
)

func validateSecureNodeSocket(path string, expectedUID uint32) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect node control socket: %w: %w", err, errdefs.ErrUnavailable)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("node control endpoint must be a 0600 Unix socket: %w", errdefs.ErrPermissionDenied)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != expectedUID {
		return fmt.Errorf("node control socket has an unexpected owner: %w", errdefs.ErrPermissionDenied)
	}
	return nil
}
