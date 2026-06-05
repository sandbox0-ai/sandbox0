package process

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const rootFSLauncherArg = "__sandbox0_rootfs_launch"

type rootFSLauncherOptions struct {
	Root    string
	CWD     string
	Command string
	Args    []string
}

// RunLauncherFromArgs executes the internal rootfs launcher mode when args
// start with the private launcher sentinel.
func RunLauncherFromArgs(args []string) (bool, error) {
	if len(args) == 0 || args[0] != rootFSLauncherArg {
		return false, nil
	}
	opts, err := parseRootFSLauncherArgs(args[1:])
	if err != nil {
		return true, err
	}
	return true, runRootFSLauncher(opts)
}

func parseRootFSLauncherArgs(args []string) (rootFSLauncherOptions, error) {
	var opts rootFSLauncherOptions
	for len(args) > 0 {
		switch args[0] {
		case "--root":
			if len(args) < 2 {
				return opts, fmt.Errorf("--root requires a value")
			}
			opts.Root = args[1]
			args = args[2:]
		case "--cwd":
			if len(args) < 2 {
				return opts, fmt.Errorf("--cwd requires a value")
			}
			opts.CWD = args[1]
			args = args[2:]
		case "--":
			args = args[1:]
			if len(args) == 0 {
				return opts, fmt.Errorf("command is required")
			}
			opts.Command = args[0]
			opts.Args = append([]string(nil), args[1:]...)
			args = nil
		default:
			return opts, fmt.Errorf("unknown launcher argument %q", args[0])
		}
	}
	var err error
	opts.Root, err = cleanAbsoluteLauncherPath(opts.Root, "root")
	if err != nil {
		return opts, err
	}
	opts.CWD, err = cleanAbsoluteLauncherPath(opts.CWD, "cwd")
	if err != nil {
		return opts, err
	}
	opts.Command, err = cleanAbsoluteLauncherPath(opts.Command, "command")
	if err != nil {
		return opts, err
	}
	return opts, nil
}

func cleanAbsoluteLauncherPath(value, name string) (string, error) {
	value = filepath.Clean(strings.TrimSpace(value))
	if value == "." || value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	if !filepath.IsAbs(value) {
		return "", fmt.Errorf("%s must be absolute", name)
	}
	return value, nil
}

func runRootFSLauncher(opts rootFSLauncherOptions) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := unix.Unshare(unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("unshare mount namespace: %w", err)
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make mounts private: %w", err)
	}
	if err := ensureMountPoint(opts.Root); err != nil {
		return err
	}
	if err := prepareSandboxRootFSMounts(opts.Root); err != nil {
		return err
	}

	oldRootName := ".sandbox0-old-root-" + strconv.Itoa(unix.Getpid())
	oldRoot := filepath.Join(opts.Root, oldRootName)
	if err := os.RemoveAll(oldRoot); err != nil {
		return fmt.Errorf("remove stale old root directory: %w", err)
	}
	if err := os.Mkdir(oldRoot, 0o700); err != nil {
		return fmt.Errorf("create old root directory: %w", err)
	}
	if err := unix.PivotRoot(opts.Root, oldRoot); err != nil {
		_ = os.Remove(oldRoot)
		return fmt.Errorf("pivot_root into sandbox rootfs: %w", err)
	}

	pivotedOldRoot := filepath.Join(string(filepath.Separator), oldRootName)
	if err := os.Chdir(string(filepath.Separator)); err != nil {
		return fmt.Errorf("chdir after pivot_root: %w", err)
	}
	if err := unix.Unmount(pivotedOldRoot, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("unmount old root: %w", err)
	}
	_ = os.RemoveAll(pivotedOldRoot)

	if err := os.Chdir(opts.CWD); err != nil {
		return fmt.Errorf("chdir to sandbox cwd %s: %w", opts.CWD, err)
	}
	if err := dropLauncherCapabilities(); err != nil {
		return err
	}
	argv := append([]string{opts.Command}, opts.Args...)
	if err := unix.Exec(opts.Command, argv, os.Environ()); err != nil {
		return fmt.Errorf("exec sandbox command %s: %w", opts.Command, err)
	}
	return nil
}

func ensureMountPoint(root string) error {
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("stat sandbox rootfs: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("sandbox rootfs is not a directory: %s", root)
	}
	if err := unix.Mount(root, root, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("bind sandbox rootfs as mount point: %w", err)
	}
	return nil
}

func prepareSandboxRootFSMounts(root string) error {
	procPath, err := ensureSandboxRootDir(root, "proc", 0o555)
	if err != nil {
		return err
	}
	if err := unix.Mount("proc", procPath, "proc", unix.MS_NOSUID|unix.MS_NOEXEC|unix.MS_NODEV, ""); err != nil && !errors.Is(err, unix.EBUSY) {
		return fmt.Errorf("mount proc: %w", err)
	}
	devPath, err := ensureSandboxRootDir(root, "dev", 0o755)
	if err != nil {
		return err
	}
	if err := unix.Mount("/dev", devPath, "", unix.MS_BIND|unix.MS_REC, ""); err != nil && !errors.Is(err, unix.EBUSY) {
		return fmt.Errorf("bind mount dev: %w", err)
	}
	tmpPath, err := ensureSandboxRootDir(root, "tmp", 0o777)
	if err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, 0o777|os.ModeSticky); err != nil {
		return fmt.Errorf("chmod sandbox tmp: %w", err)
	}
	return nil
}

func ensureSandboxRootDir(root, name string, mode os.FileMode) (string, error) {
	target := filepath.Join(root, name)
	info, err := os.Lstat(target)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("sandbox rootfs %s must not be a symlink", name)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("sandbox rootfs %s is not a directory", name)
		}
		return target, nil
	case os.IsNotExist(err):
		if err := os.Mkdir(target, mode); err != nil && !os.IsExist(err) {
			return "", fmt.Errorf("create sandbox rootfs %s: %w", name, err)
		}
		return target, nil
	default:
		return "", fmt.Errorf("stat sandbox rootfs %s: %w", name, err)
	}
}

func dropLauncherCapabilities() error {
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("set no_new_privs: %w", err)
	}
	for _, capID := range []uintptr{unix.CAP_SYS_ADMIN, unix.CAP_SYS_CHROOT} {
		if err := unix.Prctl(unix.PR_CAPBSET_DROP, capID, 0, 0, 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EPERM) {
			return fmt.Errorf("drop capability %d from bounding set: %w", capID, err)
		}
	}
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	data := [2]unix.CapUserData{}
	if err := unix.Capget(&header, &data[0]); err != nil {
		return fmt.Errorf("read process capabilities: %w", err)
	}
	for _, capID := range []uint{unix.CAP_SYS_ADMIN, unix.CAP_SYS_CHROOT} {
		clearCapability(&data, capID)
	}
	if err := unix.Capset(&header, &data[0]); err != nil {
		return fmt.Errorf("drop launcher capabilities: %w", err)
	}
	return nil
}

func clearCapability(data *[2]unix.CapUserData, capID uint) {
	index := capID / 32
	if int(index) >= len(data) {
		return
	}
	mask := uint32(1 << (capID % 32))
	data[index].Effective &^= mask
	data[index].Permitted &^= mask
	data[index].Inheritable &^= mask
}
