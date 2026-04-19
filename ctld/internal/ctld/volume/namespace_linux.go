//go:build linux

package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

type syscallNamespaceOperator struct{}

func newNamespaceOperator() NamespaceOperator {
	return syscallNamespaceOperator{}
}

func (syscallNamespaceOperator) EnsureMountPoint(ctx context.Context, mountNSPath, mountPoint string) error {
	targetPath, err := namespaceRelativeMountPoint(mountPoint)
	if err != nil {
		return err
	}
	return withMountNamespaceAtSandboxRoot(ctx, mountNSPath, func() error {
		return os.MkdirAll(targetPath, 0o755)
	})
}

func (syscallNamespaceOperator) BindMount(ctx context.Context, mountNSPath, sourcePath, mountPoint string) error {
	targetPath, err := namespaceRelativeMountPoint(mountPoint)
	if err != nil {
		return err
	}
	treeFD, err := unix.OpenTree(unix.AT_FDCWD, sourcePath, unix.OPEN_TREE_CLONE)
	if err != nil {
		return fmt.Errorf("clone mount tree %s: %w", sourcePath, err)
	}
	defer unix.Close(treeFD)

	return withMountNamespaceAtSandboxRoot(ctx, mountNSPath, func() error {
		if err := unix.MoveMount(treeFD, "", unix.AT_FDCWD, targetPath, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
			return fmt.Errorf("move mount %s -> %s: %w", sourcePath, targetPath, err)
		}
		return nil
	})
}

func (syscallNamespaceOperator) Unmount(ctx context.Context, mountNSPath, mountPoint string) error {
	targetPath, err := namespaceRelativeMountPoint(mountPoint)
	if err != nil {
		return err
	}
	return withMountNamespaceAtSandboxRoot(ctx, mountNSPath, func() error {
		if err := unix.Unmount(targetPath, 0); err != nil {
			return fmt.Errorf("unmount %s: %w", targetPath, err)
		}
		return nil
	})
}

func namespaceRelativeMountPoint(mountPoint string) (string, error) {
	cleaned := filepath.Clean(strings.TrimSpace(mountPoint))
	if cleaned == "." || !filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("mount point %q must be an absolute path", mountPoint)
	}
	relative := strings.TrimPrefix(cleaned, string(filepath.Separator))
	if relative == "" {
		return "", fmt.Errorf("mount point %q must not be root", mountPoint)
	}
	return relative, nil
}

func withMountNamespaceAtSandboxRoot(ctx context.Context, mountNSPath string, fn func() error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	currentNS, err := os.Open(filepath.Join(defaultProcRoot, "self/ns/mnt"))
	if err != nil {
		return fmt.Errorf("open current mount namespace: %w", err)
	}
	defer currentNS.Close()

	targetNS, err := os.Open(mountNSPath)
	if err != nil {
		return fmt.Errorf("open target mount namespace %s: %w", mountNSPath, err)
	}
	defer targetNS.Close()

	targetRoot, err := os.Open(targetRootPathForMountNamespace(mountNSPath))
	if err != nil {
		return fmt.Errorf("open target root for mount namespace %s: %w", mountNSPath, err)
	}
	defer targetRoot.Close()

	originalCwd, err := os.Open(".")
	if err != nil {
		return fmt.Errorf("open original cwd: %w", err)
	}
	defer originalCwd.Close()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := unix.Unshare(unix.CLONE_FS); err != nil {
		return fmt.Errorf("unshare filesystem state before setns: %w", err)
	}
	if err := unix.Fchdir(int(targetRoot.Fd())); err != nil {
		return fmt.Errorf("set cwd to target root %s: %w", targetRoot.Name(), err)
	}
	defer func() {
		restoreErr := unix.Fchdir(int(originalCwd.Fd()))
		if err == nil && restoreErr != nil {
			err = fmt.Errorf("restore original cwd: %w", restoreErr)
		}
	}()

	if err := unix.Setns(int(targetNS.Fd()), unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("set mount namespace %s: %w", mountNSPath, err)
	}
	defer func() {
		restoreErr := unix.Setns(int(currentNS.Fd()), unix.CLONE_NEWNS)
		if err == nil && restoreErr != nil {
			err = fmt.Errorf("restore original mount namespace: %w", restoreErr)
		}
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func targetRootPathForMountNamespace(mountNSPath string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(mountNSPath)), "root")
}
