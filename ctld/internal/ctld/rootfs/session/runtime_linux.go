//go:build linux

package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moby/sys/mountinfo"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"golang.org/x/sys/unix"
)

var linuxNBDPath = regexp.MustCompile(`^/dev/nbd[0-9]+$`)

const (
	xfsMountFlags = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOATIME
	xfsMountData  = "nouuid,inode64"
)

// LinuxRuntimeConfig configures the host NBD/XFS/Overlay implementation.
type LinuxRuntimeConfig struct {
	DevicePaths     []string
	RequestTimeout  time.Duration
	ReadyTimeout    time.Duration
	MaxRequestBytes uint32
	SysBlockRoot    string
}

// LinuxRuntime owns a bounded pool of pre-created /dev/nbd devices. The node
// bootstrap is responsible for loading the nbd module with enough devices.
type LinuxRuntime struct {
	config       LinuxRuntimeConfig
	sysBlockRoot string
	mu           sync.Mutex
	reserved     map[string]string
}

func NewLinuxRuntime(config LinuxRuntimeConfig) (*LinuxRuntime, error) {
	if len(config.DevicePaths) == 0 {
		return nil, fmt.Errorf("at least one NBD device path is required")
	}
	seen := make(map[string]bool, len(config.DevicePaths))
	for index, path := range config.DevicePaths {
		path = filepath.Clean(strings.TrimSpace(path))
		if !linuxNBDPath.MatchString(path) {
			return nil, fmt.Errorf("NBD device path %q is invalid", path)
		}
		if seen[path] {
			return nil, fmt.Errorf("NBD device path %q is duplicated", path)
		}
		seen[path] = true
		config.DevicePaths[index] = path
	}
	sysBlockRoot := filepath.Clean(strings.TrimSpace(config.SysBlockRoot))
	if sysBlockRoot == "." {
		sysBlockRoot = "/sys/block"
	}
	if !filepath.IsAbs(sysBlockRoot) || sysBlockRoot == "/" {
		return nil, fmt.Errorf("sys block root must be a non-root absolute path")
	}
	config.SysBlockRoot = sysBlockRoot
	return &LinuxRuntime{config: config, sysBlockRoot: sysBlockRoot, reserved: make(map[string]string)}, nil
}

func (r *LinuxRuntime) ReserveDevice(allocationID string) (string, error) {
	if err := validateDeviceAllocationID(allocationID); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for path, owner := range r.reserved {
		if owner == allocationID {
			return path, nil
		}
	}
	for _, path := range r.config.DevicePaths {
		if r.reserved[path] != "" {
			continue
		}
		r.reserved[path] = allocationID
		return path, nil
	}
	return "", fmt.Errorf("no usable NBD device is available")
}

func (r *LinuxRuntime) AdoptDeviceReservation(devicePath, allocationID string) error {
	devicePath = filepath.Clean(strings.TrimSpace(devicePath))
	if err := validateDeviceAllocationID(allocationID); err != nil {
		return err
	}
	if !r.configuredDevice(devicePath) {
		return fmt.Errorf("NBD device %q is outside the configured pool", devicePath)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner := r.reserved[devicePath]; owner != "" {
		if owner == allocationID {
			return nil
		}
		return fmt.Errorf("NBD device %s is already reserved by allocation %s", devicePath, owner)
	}
	for path, owner := range r.reserved {
		if owner == allocationID {
			return fmt.Errorf("NBD allocation %s is already reserved on %s", allocationID, path)
		}
	}
	r.reserved[devicePath] = allocationID
	return nil
}

func (r *LinuxRuntime) ReleaseDeviceReservation(devicePath, allocationID string) {
	devicePath = filepath.Clean(strings.TrimSpace(devicePath))
	r.mu.Lock()
	if r.reserved[devicePath] == allocationID {
		delete(r.reserved, devicePath)
	}
	r.mu.Unlock()
}

func (r *LinuxRuntime) AttachDevice(
	lifetime, readyContext context.Context,
	devicePath, allocationID string,
	backend rootfsblock.WritableBlockDevice,
) (Device, error) {
	devicePath = filepath.Clean(strings.TrimSpace(devicePath))
	r.mu.Lock()
	owner := r.reserved[devicePath]
	r.mu.Unlock()
	if owner != allocationID || owner == "" {
		return nil, fmt.Errorf("NBD device %s is not reserved by allocation %s", devicePath, allocationID)
	}
	return rootfsblock.StartKernelNBD(lifetime, readyContext, backend, rootfsblock.KernelNBDOptions{
		DevicePath: devicePath, RequestTimeout: r.config.RequestTimeout, ReadyTimeout: r.config.ReadyTimeout,
		MaxRequestBytes: r.config.MaxRequestBytes, SysBlockRoot: r.sysBlockRoot,
	})
}

// RecoverOrphanDevice disconnects an exact kernel NBD attachment whose
// userspace owner disappeared across a process restart. The caller must first
// remove every runtime and filesystem reference. The durable allocation stays
// reserved until a later terminal proof is persisted.
func (r *LinuxRuntime) RecoverOrphanDevice(ctx context.Context, devicePath, allocationID string) error {
	devicePath = filepath.Clean(strings.TrimSpace(devicePath))
	if err := validateDeviceAllocationID(allocationID); err != nil {
		return err
	}
	if !r.configuredDevice(devicePath) {
		return fmt.Errorf("NBD device %q is outside the configured pool", devicePath)
	}
	r.mu.Lock()
	owner := r.reserved[devicePath]
	r.mu.Unlock()
	if owner != allocationID {
		return fmt.Errorf("NBD device %s is not reserved by allocation %s", devicePath, allocationID)
	}
	return rootfsblock.RecoverOrphanKernelNBD(ctx, devicePath, r.sysBlockRoot)
}

// InspectCrashFence proves absence only for the exact NBD path and mount roots
// recorded by the session journal. Unknown sysfs state fails closed.
func (r *LinuxRuntime) InspectCrashFence(devicePath, xfsRoot, mergedRoot string) (CrashFenceHostObservation, error) {
	devicePath = filepath.Clean(strings.TrimSpace(devicePath))
	if !linuxNBDPath.MatchString(devicePath) {
		return CrashFenceHostObservation{}, fmt.Errorf("recorded NBD device path %q is invalid", devicePath)
	}
	configured := false
	for _, candidate := range r.config.DevicePaths {
		if candidate == devicePath {
			configured = true
			break
		}
	}
	if !configured {
		return CrashFenceHostObservation{}, fmt.Errorf("recorded NBD device %q is outside the configured pool", devicePath)
	}

	if err := inspectCrashFenceMounts(xfsRoot, mergedRoot); err != nil {
		return CrashFenceHostObservation{}, err
	}
	observation, err := r.inspectCrashFenceDevice(devicePath)
	if err != nil {
		return observation, err
	}
	observation.MergedMountAbsent = true
	observation.XFSMountAbsent = true
	return observation, nil
}

// InspectUnattachedCrashFence closes the AttachDevice-before-journal crash
// window by requiring every configured NBD endpoint and deterministic mount
// path to be idle. It intentionally fails while any other RootFS session is
// using the same pool because an unrecorded device cannot be attributed
// safely.
func (r *LinuxRuntime) InspectUnattachedCrashFence(xfsRoot, mergedRoot string) (CrashFenceHostObservation, error) {
	if err := inspectCrashFenceMounts(xfsRoot, mergedRoot); err != nil {
		return CrashFenceHostObservation{}, err
	}
	for _, devicePath := range r.config.DevicePaths {
		if _, err := r.inspectCrashFenceDevice(devicePath); err != nil {
			return CrashFenceHostObservation{}, fmt.Errorf("inspect unattached NBD pool member %s: %w", devicePath, err)
		}
	}
	return CrashFenceHostObservation{
		NBDPoolAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true,
	}, nil
}

// InspectPreAttachmentCrashFence relies on the current journal ordering: a
// process may call StartKernelNBD only after the exact reserved path is durable.
// A current-schema record without that path therefore only needs its
// deterministic mount roots checked; unrelated pool users are irrelevant.
func (r *LinuxRuntime) InspectPreAttachmentCrashFence(xfsRoot, mergedRoot string) (CrashFenceHostObservation, error) {
	if err := inspectCrashFenceMounts(xfsRoot, mergedRoot); err != nil {
		return CrashFenceHostObservation{}, err
	}
	return CrashFenceHostObservation{
		NBDPoolAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true,
	}, nil
}

func inspectCrashFenceMounts(xfsRoot, mergedRoot string) error {
	paths := map[string]string{
		"merged": filepath.Clean(mergedRoot),
		"xfs":    filepath.Clean(xfsRoot),
		"lower":  filepath.Join(filepath.Clean(xfsRoot), "lower"),
	}
	for name, path := range paths {
		if !filepath.IsAbs(path) || path == "/" {
			return fmt.Errorf("recorded %s mount path is invalid", name)
		}
	}
	entries, err := mountinfo.GetMounts(nil)
	if err != nil {
		return fmt.Errorf("inspect host mounts for crash fence: %w", err)
	}
	for _, entry := range entries {
		for name, path := range paths {
			if filepath.Clean(entry.Mountpoint) == path {
				return fmt.Errorf("recorded %s mount %q is still attached", name, path)
			}
		}
	}
	return nil
}

func (r *LinuxRuntime) inspectCrashFenceDevice(devicePath string) (CrashFenceHostObservation, error) {
	devicePath = filepath.Clean(strings.TrimSpace(devicePath))
	if !linuxNBDPath.MatchString(devicePath) {
		return CrashFenceHostObservation{}, fmt.Errorf("recorded NBD device path %q is invalid", devicePath)
	}
	deviceName := filepath.Base(devicePath)
	pidPath := filepath.Join(r.sysBlockRoot, deviceName, "pid")
	pid := 0
	payload, err := os.ReadFile(pidPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return CrashFenceHostObservation{}, fmt.Errorf("inspect NBD owner %s: %w", pidPath, err)
		}
		sizePath := filepath.Join(r.sysBlockRoot, deviceName, "size")
		sizePayload, sizeErr := os.ReadFile(sizePath)
		if sizeErr != nil {
			return CrashFenceHostObservation{}, fmt.Errorf("inspect disconnected NBD size %s: %w", sizePath, sizeErr)
		}
		sectors, sizeErr := strconv.ParseUint(strings.TrimSpace(string(sizePayload)), 10, 64)
		if sizeErr != nil {
			return CrashFenceHostObservation{}, fmt.Errorf("disconnected NBD size %s is invalid", sizePath)
		}
		if sectors != 0 {
			return CrashFenceHostObservation{}, fmt.Errorf(
				"NBD owner %s is absent while device size remains %d sectors", pidPath, sectors,
			)
		}
	}
	if strings.TrimSpace(string(payload)) != "" {
		pid, err = strconv.Atoi(strings.TrimSpace(string(payload)))
		if err != nil || pid < 0 {
			return CrashFenceHostObservation{}, fmt.Errorf("NBD owner %s is invalid", pidPath)
		}
	}
	holdersPath := filepath.Join(r.sysBlockRoot, deviceName, "holders")
	holderEntries, err := os.ReadDir(holdersPath)
	if err != nil {
		return CrashFenceHostObservation{}, fmt.Errorf("inspect NBD holders %s: %w", holdersPath, err)
	}
	holders := make([]string, 0, len(holderEntries))
	for _, entry := range holderEntries {
		holders = append(holders, entry.Name())
	}
	sort.Strings(holders)
	observation := CrashFenceHostObservation{NBDPID: pid, NBDHolders: holders}
	if pid != 0 || len(holders) != 0 {
		return observation, fmt.Errorf("NBD device %s remains owned by pid=%d holders=%v", devicePath, pid, holders)
	}
	return observation, nil
}

func (r *LinuxRuntime) MountXFS(devicePath, target string) error {
	if err := ensurePrivateDirectory(target); err != nil {
		return err
	}
	if err := unix.Mount(devicePath, target, "xfs", xfsMountFlags, xfsMountData); err != nil {
		return fmt.Errorf("mount %s on %s: %w", devicePath, target, err)
	}
	return nil
}

func (r *LinuxRuntime) MountOverlay(xfsRoot, mergedRoot string) error {
	lower := filepath.Join(xfsRoot, "lower")
	upper := filepath.Join(xfsRoot, "upper")
	work := filepath.Join(xfsRoot, "work")
	for _, path := range []string{lower, upper, work} {
		if err := requireTrustedDirectory(path); err != nil {
			return err
		}
	}
	if err := ensurePrivateDirectory(mergedRoot); err != nil {
		return err
	}
	if err := unix.Mount(lower, lower, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("create read-only lower bind: %w", err)
	}
	if err := unix.Mount("", lower, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV, ""); err != nil {
		_ = unix.Unmount(lower, 0)
		return fmt.Errorf("make lower bind read-only: %w", err)
	}
	options := strings.Join([]string{
		"lowerdir=" + lower, "upperdir=" + upper, "workdir=" + work,
		"index=off", "metacopy=off", "redirect_dir=off", "xino=off",
	}, ",")
	if err := unix.Mount("overlay", mergedRoot, "overlay", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOATIME, options); err != nil {
		_ = unix.Unmount(lower, 0)
		return fmt.Errorf("mount merged OverlayFS: %w", err)
	}
	return nil
}

func (r *LinuxRuntime) UnmountOverlay(mergedRoot string, requireSync bool) error {
	if requireSync {
		if err := syncPath(mergedRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return unmountExact(mergedRoot)
}

func (r *LinuxRuntime) UnmountXFS(xfsRoot string, requireSync bool) error {
	lowerErr := unmountExact(filepath.Join(xfsRoot, "lower"))
	if requireSync {
		if err := syncPath(xfsRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.Join(lowerErr, err)
		}
	}
	return errors.Join(lowerErr, unmountExact(xfsRoot))
}

func (r *LinuxRuntime) configuredDevice(devicePath string) bool {
	if !linuxNBDPath.MatchString(devicePath) {
		return false
	}
	for _, candidate := range r.config.DevicePaths {
		if candidate == devicePath {
			return true
		}
	}
	return false
}

func validateDeviceAllocationID(allocationID string) error {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" || len(allocationID) > 256 {
		return fmt.Errorf("NBD allocation identity is invalid")
	}
	return nil
}

func ensurePrivateDirectory(path string) error {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" {
		return fmt.Errorf("mount target must be a non-root absolute path")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return err
	}
	return requireTrustedDirectory(path)
}

func requireTrustedDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s must be a real directory", path)
	}
	return nil
}

func syncPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := unix.Syncfs(int(file.Fd())); err != nil {
		return fmt.Errorf("syncfs %s: %w", path, err)
	}
	return nil
}

func unmountExact(path string) error {
	err := unix.Unmount(path, 0)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}
