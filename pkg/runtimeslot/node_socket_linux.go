//go:build linux

package runtimeslot

import (
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"
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

func validateNodeSocketPeer(connection net.Conn, expectedUID uint32) error {
	syscallConnection, ok := connection.(syscall.Conn)
	if !ok {
		return fmt.Errorf("node control connection does not expose peer credentials: %w", errdefs.ErrPermissionDenied)
	}
	raw, err := syscallConnection.SyscallConn()
	if err != nil {
		return fmt.Errorf("open node control connection descriptor: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	var credentials *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		credentials, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return fmt.Errorf("inspect node control peer descriptor: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if credentialErr != nil {
		return fmt.Errorf("read node control peer credentials: %w: %w", credentialErr, errdefs.ErrPermissionDenied)
	}
	if credentials == nil {
		return fmt.Errorf("node control peer returned no credentials: %w", errdefs.ErrPermissionDenied)
	}
	if credentials.Uid != expectedUID {
		return fmt.Errorf("node control peer has unexpected uid %d: %w", credentials.Uid, errdefs.ErrPermissionDenied)
	}
	return nil
}
