//go:build linux

package nomadruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type cgroupV2RuntimeResources struct {
	root string
}

func newRuntimeResourceCgroup(
	root string,
	capacity protocol.NodeChannelCapacity,
) (runtimeResourceCgroup, error) {
	root = strings.TrimSpace(root)
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime resource cgroup root must be a canonical non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	if err := validateRootOwnedCgroupDirectory(root); err != nil {
		return nil, fmt.Errorf("inspect predelegated runtime resource cgroup root: %w", err)
	}
	controllers, err := readCgroupValue(filepath.Join(root, "cgroup.controllers"))
	if err != nil {
		return nil, fmt.Errorf("read cgroup v2 controllers: %w", err)
	}
	for _, controller := range []string{"cpu", "cpuset", "memory", "pids"} {
		if !containsCgroupToken(controllers, controller) {
			return nil, fmt.Errorf("runtime cgroup root lacks %s controller: %w", controller, errdefs.ErrFailedPrecondition)
		}
	}
	delegated, err := readCgroupValue(filepath.Join(root, "cgroup.subtree_control"))
	if err != nil {
		return nil, fmt.Errorf("read cgroup v2 subtree controls: %w", err)
	}
	for _, controller := range []string{"cpu", "cpuset", "memory", "pids"} {
		if !containsCgroupToken(delegated, controller) {
			return nil, fmt.Errorf("runtime cgroup root has not delegated %s to children: %w", controller, errdefs.ErrFailedPrecondition)
		}
	}
	procs, err := readCgroupValue(filepath.Join(root, "cgroup.procs"))
	if err != nil {
		return nil, fmt.Errorf("read runtime cgroup root processes: %w", err)
	}
	if procs != "" {
		return nil, fmt.Errorf("runtime cgroup root contains processes: %w", errdefs.ErrFailedPrecondition)
	}
	if err := validateRuntimeResourceCgroupCapacity(root, capacity); err != nil {
		return nil, err
	}
	return &cgroupV2RuntimeResources{root: root}, nil
}

func (c *cgroupV2RuntimeResources) Prepare(
	ctx context.Context,
	lease protocol.RuntimeResourceLease,
) error {
	if c == nil || c.root == "" {
		return fmt.Errorf("runtime resource cgroup is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := lease.Validate(); err != nil {
		return fmt.Errorf("runtime resource lease: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := c.path(lease)
	if err != nil {
		return err
	}
	created := false
	if err := os.Mkdir(path, 0o750); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create runtime resource cgroup: %w", err)
		}
		if err := validateRootOwnedCgroupDirectory(path); err != nil {
			return fmt.Errorf("inspect existing runtime resource cgroup: %w", err)
		}
	} else {
		created = true
	}
	values := []struct{ file, value string }{
		{file: "cpuset.mems", value: lease.CPUSetMems},
		{file: "cpuset.cpus", value: lease.CPUSetCPUs},
		{file: "cpu.max", value: strconv.FormatInt(lease.CPUQuotaMicros, 10) + " " + strconv.FormatUint(lease.CPUPeriodMicros, 10)},
		{file: "cpu.weight", value: strconv.FormatUint(lease.CPUWeight, 10)},
		{file: "memory.max", value: strconv.FormatInt(lease.MemoryBytes, 10)},
		{file: "memory.swap.max", value: "0"},
		{file: "pids.max", value: strconv.FormatInt(lease.PIDsLimit, 10)},
	}
	for _, item := range values {
		if err := ctx.Err(); err != nil {
			return rollbackRuntimeResourceCgroup(path, created, err)
		}
		file := filepath.Join(path, item.file)
		if !created {
			actual, err := readCgroupValue(file)
			if err != nil {
				return rollbackRuntimeResourceCgroup(path, created, fmt.Errorf("read existing runtime cgroup %s: %w", item.file, err))
			}
			if actual != item.value {
				return rollbackRuntimeResourceCgroup(path, created, fmt.Errorf("existing runtime cgroup %s changed: %w", item.file, errdefs.ErrFailedPrecondition))
			}
			continue
		}
		if err := os.WriteFile(file, []byte(item.value), 0o600); err != nil {
			return rollbackRuntimeResourceCgroup(path, created, fmt.Errorf("write runtime cgroup %s: %w", item.file, err))
		}
		actual, err := readCgroupValue(file)
		if err != nil || actual != item.value {
			return rollbackRuntimeResourceCgroup(path, created, fmt.Errorf("verify runtime cgroup %s: got %q: %w", item.file, actual, errdefs.ErrFailedPrecondition))
		}
	}
	return nil
}

func rollbackRuntimeResourceCgroup(path string, created bool, cause error) error {
	if !created {
		return cause
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%v; remove incomplete runtime resource cgroup: %w", cause, err)
	}
	return cause
}

func (c *cgroupV2RuntimeResources) RemoveAndConfirm(
	ctx context.Context,
	lease protocol.RuntimeResourceLease,
) (bool, error) {
	if c == nil || c.root == "" {
		return false, fmt.Errorf("runtime resource cgroup is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := lease.Validate(); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := c.path(lease)
	if err != nil {
		return false, err
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	if err := validateRootOwnedCgroupDirectory(path); err != nil {
		return false, fmt.Errorf("inspect runtime resource cgroup: %w", err)
	}
	procs, err := readCgroupValue(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return false, fmt.Errorf("read runtime cgroup processes: %w", err)
	}
	if procs != "" {
		return false, fmt.Errorf("runtime resource cgroup still contains processes: %w", errdefs.ErrFailedPrecondition)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("remove runtime resource cgroup: %w", err)
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, fmt.Errorf("runtime resource cgroup remains present: %w", errdefs.ErrFailedPrecondition)
}

func (c *cgroupV2RuntimeResources) path(lease protocol.RuntimeResourceLease) (string, error) {
	path := filepath.Join(c.root, lease.CgroupName)
	if filepath.Dir(path) != c.root {
		return "", fmt.Errorf("runtime resource cgroup escaped its root: %w", errdefs.ErrPermissionDenied)
	}
	return path, nil
}

func readCgroupValue(path string) (string, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(payload)), nil
}

func containsCgroupToken(value, want string) bool {
	for _, token := range strings.Fields(value) {
		if token == want {
			return true
		}
	}
	return false
}

func validateRootOwnedCgroupDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("cgroup path is not a canonical directory: %w", errdefs.ErrFailedPrecondition)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("cgroup directory must be root-owned and not writable by group or other: %w", errdefs.ErrPermissionDenied)
	}
	return nil
}

func validateRuntimeResourceCgroupCapacity(root string, capacity protocol.NodeChannelCapacity) error {
	if err := capacity.Validate(); err != nil {
		return fmt.Errorf("runtime node capacity: %w", err)
	}
	for name, configured := range map[string]string{
		"cpuset.cpus.effective": capacity.CPUSetCPUs,
		"cpuset.mems.effective": capacity.CPUSetMems,
	} {
		effective, err := readCgroupValue(filepath.Join(root, name))
		if err != nil {
			return fmt.Errorf("read runtime cgroup %s: %w", name, err)
		}
		contains, err := protocol.CPUSetContains(effective, configured)
		if err != nil || !contains {
			return fmt.Errorf("runtime node capacity %s %q is outside effective set %q: %w",
				name, configured, effective, errdefs.ErrFailedPrecondition)
		}
	}
	for name, want := range map[string]string{
		"cpu.max": "max 100000", "memory.max": "max", "memory.swap.max": "max", "pids.max": "max",
	} {
		actual, err := readCgroupValue(filepath.Join(root, name))
		if err != nil {
			return fmt.Errorf("read runtime cgroup %s: %w", name, err)
		}
		if actual != want {
			return fmt.Errorf("runtime resource root %s must be %q, got %q: %w",
				name, want, actual, errdefs.ErrFailedPrecondition)
		}
	}
	var system syscall.Sysinfo_t
	if err := syscall.Sysinfo(&system); err != nil {
		return fmt.Errorf("read host memory capacity: %w", err)
	}
	memoryUnit := uint64(system.Unit)
	if memoryUnit == 0 {
		memoryUnit = 1
	}
	totalMemory := uint64(system.Totalram) * memoryUnit
	if uint64(capacity.MemoryBytes) > totalMemory {
		return fmt.Errorf("runtime node memory capacity exceeds host memory: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}
