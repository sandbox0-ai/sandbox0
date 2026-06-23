//go:build linux

package rootfs

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

func withMountNamespace(namespacePath string, fn func() error) (err error) {
	namespacePath = strings.TrimSpace(namespacePath)
	if namespacePath == "" {
		return fn()
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	current, err := unix.Open("/proc/self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open current mount namespace: %w", err)
	}
	defer unix.Close(current)

	target, err := unix.Open(namespacePath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open target mount namespace: %w", err)
	}
	defer unix.Close(target)

	if err := unix.Setns(target, unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("enter target mount namespace: %w", err)
	}
	defer func() {
		if restoreErr := unix.Setns(current, unix.CLONE_NEWNS); restoreErr != nil && err == nil {
			err = fmt.Errorf("restore mount namespace: %w", restoreErr)
		}
	}()

	if err := fn(); err != nil {
		return err
	}
	return nil
}

func withMountNamespaceRoot(namespacePath, rootPath string, fn func() error) (err error) {
	namespacePath = strings.TrimSpace(namespacePath)
	rootPath = strings.TrimSpace(rootPath)
	if rootPath == "" {
		return withMountNamespace(namespacePath, fn)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := unix.Unshare(unix.CLONE_FS); err != nil {
		return fmt.Errorf("unshare filesystem state: %w", err)
	}

	currentNS, err := unix.Open("/proc/self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open current mount namespace: %w", err)
	}
	defer unix.Close(currentNS)

	targetNS, err := unix.Open(namespacePath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open target mount namespace: %w", err)
	}
	defer unix.Close(targetNS)

	oldRoot, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open current root: %w", err)
	}
	defer unix.Close(oldRoot)

	oldCWD, err := unix.Open(".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open current working directory: %w", err)
	}
	defer unix.Close(oldCWD)

	defer func() {
		restoreErr := restoreMountNamespaceRoot(currentNS, oldRoot, oldCWD)
		if restoreErr != nil && err == nil {
			err = restoreErr
		}
	}()

	if err := unix.Chroot(rootPath); err != nil {
		return fmt.Errorf("enter rootfs root %s: %w", rootPath, err)
	}
	if err := unix.Chdir("/"); err != nil {
		return fmt.Errorf("enter rootfs working directory: %w", err)
	}
	if err := unix.Setns(targetNS, unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("enter target mount namespace: %w", err)
	}
	if err := fn(); err != nil {
		return err
	}
	return nil
}

func restoreMountNamespaceRoot(currentNS, oldRoot, oldCWD int) error {
	var errs []string
	if err := unix.Fchdir(oldRoot); err != nil {
		errs = append(errs, fmt.Sprintf("restore root cwd: %v", err))
	}
	if err := unix.Setns(currentNS, unix.CLONE_NEWNS); err != nil {
		errs = append(errs, fmt.Sprintf("restore mount namespace: %v", err))
	}
	if err := unix.Chroot("."); err != nil {
		errs = append(errs, fmt.Sprintf("restore root: %v", err))
	}
	if err := unix.Fchdir(oldCWD); err != nil {
		errs = append(errs, fmt.Sprintf("restore cwd: %v", err))
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func mountFuseServerInMountNamespace(fs fuse.RawFileSystem, targetPath, namespacePath, rootPath string, opts *fuse.MountOptions) (*fuse.Server, error) {
	fd, err := mountFuseFDInMountNamespace(targetPath, namespacePath, rootPath, opts)
	if err != nil {
		return nil, err
	}
	server, err := fuse.NewServer(fs, "/dev/fd/"+strconv.Itoa(fd), opts)
	if err != nil {
		_ = unmountPathInMountNamespace(namespacePath, rootPath, targetPath)
		_ = unix.Close(fd)
		return nil, err
	}
	return server, nil
}

func mountFuseFDInMountNamespace(targetPath, namespacePath, rootPath string, opts *fuse.MountOptions) (int, error) {
	fd, err := unix.Open("/dev/fuse", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open fuse device: %w", err)
	}
	if err := withMountNamespaceRoot(namespacePath, rootPath, func() error {
		if err := os.MkdirAll(targetPath, 0o755); err != nil {
			return fmt.Errorf("create fuse mountpoint %s: %w", targetPath, err)
		}
		var st unix.Stat_t
		if err := unix.Stat(targetPath, &st); err != nil {
			return fmt.Errorf("stat fuse mountpoint %s: %w", targetPath, err)
		}
		source := opts.FsName
		if source == "" {
			source = opts.Name
		}
		if source == "" {
			source = "sandbox0-rootfs"
		}
		flags := uintptr(unix.MS_NOSUID | unix.MS_NODEV)
		if opts.DirectMountFlags != 0 {
			flags = opts.DirectMountFlags
		}
		data := fuseMountData(fd, st.Mode&unix.S_IFMT, opts)
		if err := unix.Mount(source, targetPath, "fuse", flags, data); err != nil {
			return fmt.Errorf("mount fuse filesystem at %s: %w", targetPath, err)
		}
		return nil
	}); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func unmountPathInMountNamespace(namespacePath, rootPath, targetPath string) error {
	return withMountNamespaceRoot(namespacePath, rootPath, func() error {
		if err := unix.Unmount(targetPath, 0); err != nil {
			return fmt.Errorf("unmount fuse filesystem at %s: %w", targetPath, err)
		}
		return nil
	})
}

func fuseMountData(fd int, rootMode uint32, opts *fuse.MountOptions) string {
	maxRead := opts.MaxWrite
	if maxRead <= 0 {
		maxRead = 256 * 1024
	}
	parts := []string{
		fmt.Sprintf("fd=%d", fd),
		fmt.Sprintf("rootmode=%o", rootMode),
		fmt.Sprintf("user_id=%d", os.Geteuid()),
		fmt.Sprintf("group_id=%d", os.Getegid()),
		fmt.Sprintf("max_read=%d", maxRead),
	}
	if opts.AllowOther {
		parts = append(parts, "allow_other")
	}
	if opts.FsName != "" {
		parts = append(parts, "fsname="+escapeFuseMountOption(opts.FsName))
	}
	if opts.Name != "" {
		parts = append(parts, "subtype="+escapeFuseMountOption(opts.Name))
	}
	for _, opt := range opts.Options {
		if opt = strings.TrimSpace(opt); opt != "" {
			parts = append(parts, escapeFuseMountOption(opt))
		}
	}
	return strings.Join(parts, ",")
}

func escapeFuseMountOption(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, ",", `\,`)
}
