package slotnetwork

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
)

// ControlServer owns the elected ctld primary's root-only Unix endpoint.
type ControlServer struct {
	path        string
	listener    net.Listener
	server      *http.Server
	identity    os.FileInfo
	errors      chan error
	shutdownErr error

	once sync.Once
}

// StartControlServer binds the local endpoint before it starts accepting
// policy operations. A reachable predecessor is never unlinked.
func StartControlServer(path string, registry *Registry) (*ControlServer, error) {
	path = strings.TrimSpace(path)
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime slot network control socket must be canonical, absolute, and non-root: %w", errdefs.ErrInvalidArgument)
	}
	if os.Geteuid() != 0 {
		return nil, fmt.Errorf("runtime slot network control server must run as root: %w", errdefs.ErrPermissionDenied)
	}
	handler, err := NewHandler(registry)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return nil, fmt.Errorf("create runtime slot network socket directory: %w", err)
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("inspect runtime slot network socket directory: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 ||
		parentInfo.Mode().Perm()&0o022 != 0 || !pathOwnedByRoot(parentInfo) {
		return nil, fmt.Errorf("runtime slot network socket directory must be root-owned and not writable by group or other: %w", errdefs.ErrPermissionDenied)
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime slot network socket directory: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if resolvedParent != parent {
		return nil, fmt.Errorf("runtime slot network socket directory must not traverse symlinks: %w", errdefs.ErrPermissionDenied)
	}
	if existing, err := os.Lstat(path); err == nil {
		if existing.Mode()&os.ModeSymlink != 0 || existing.Mode()&os.ModeSocket == 0 || !pathOwnedByRoot(existing) {
			return nil, fmt.Errorf("refuse to replace unsafe runtime slot network socket: %w", errdefs.ErrPermissionDenied)
		}
		connection, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			return nil, fmt.Errorf("runtime slot network control socket is already served: %w", errdefs.ErrAlreadyExists)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale runtime slot network socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect runtime slot network socket: %w", err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on runtime slot network socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("secure runtime slot network socket: %w", err)
	}
	identity, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("inspect runtime slot network socket identity: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if identity.Mode().Perm() != 0o600 || identity.Mode()&os.ModeSocket == 0 || !pathOwnedByRoot(identity) {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("runtime slot network socket identity is unsafe: %w", errdefs.ErrPermissionDenied)
	}
	control := &ControlServer{
		path: path, listener: listener, identity: identity, errors: make(chan error, 1),
		server: &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second},
	}
	go func() {
		err := control.server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			control.errors <- fmt.Errorf("runtime slot network control server: %w", err)
		}
	}()
	return control, nil
}

// Errors reports an unexpected listener failure.
func (s *ControlServer) Errors() <-chan error {
	if s == nil {
		return nil
	}
	return s.errors
}

// Shutdown stops serving and removes only the socket incarnation created by
// this server.
func (s *ControlServer) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		s.shutdownErr = s.server.Shutdown(ctx)
		_ = s.listener.Close()
		current, err := os.Lstat(s.path)
		if err == nil && os.SameFile(s.identity, current) {
			if removeErr := os.Remove(s.path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				s.shutdownErr = errors.Join(s.shutdownErr, removeErr)
			}
		}
	})
	return s.shutdownErr
}
