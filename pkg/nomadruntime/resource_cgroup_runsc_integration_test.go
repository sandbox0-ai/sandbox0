//go:build linux

package nomadruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

func TestRuntimeResourceCgroupRunscIntegration(t *testing.T) {
	root := strings.TrimSpace(os.Getenv("SANDBOX0_TEST_CGROUP_ROOT"))
	runsc := strings.TrimSpace(os.Getenv("SANDBOX0_TEST_RUNSC"))
	rootfs := strings.TrimSpace(os.Getenv("SANDBOX0_TEST_RUNSC_ROOTFS"))
	if root == "" || runsc == "" || rootfs == "" {
		t.Skip("SANDBOX0_TEST_CGROUP_ROOT, SANDBOX0_TEST_RUNSC, and SANDBOX0_TEST_RUNSC_ROOTFS are required")
	}
	if filepath.Dir(root) != "/sys/fs/cgroup" ||
		root == protocol.RuntimeResourceCgroupRoot ||
		!strings.HasPrefix(filepath.Base(root), "sandbox0-test-") {
		t.Fatal("runsc integration cgroup root must be a dedicated direct child named sandbox0-test-* under /sys/fs/cgroup")
	}
	if info, err := os.Stat(runsc); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("runsc binary = %v, %v", info, err)
	}
	if info, err := os.Stat(rootfs); err != nil || !info.IsDir() {
		t.Fatalf("runsc rootfs = %v, %v", info, err)
	}
	platform := strings.TrimSpace(os.Getenv("SANDBOX0_TEST_RUNSC_PLATFORM"))
	if platform == "" {
		platform = "systrap"
	}
	if platform != "systrap" && platform != "ptrace" {
		t.Fatalf("unsupported runsc integration platform %q", platform)
	}
	startTimeout := 2 * time.Minute
	if value := strings.TrimSpace(os.Getenv("SANDBOX0_TEST_RUNSC_START_TIMEOUT")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 || parsed > 10*time.Minute {
			t.Fatalf("invalid SANDBOX0_TEST_RUNSC_START_TIMEOUT %q", value)
		}
		startTimeout = parsed
	}
	t.Logf("runsc platform %s with startup timeout %s", platform, startTimeout)

	controller, err := newRuntimeResourceCgroup(root, testRuntimeResourceCgroupCapacity())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := protocol.NewRuntimeResourceLease(
		"runsc-integration-operation-1", "claim-1", "slot-1", "cluster-1", "node-1", "node-uid-1", "boot-1",
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_500,
			MemoryBytes: 768 << 20, PIDsLimit: 128,
		},
		"0-1", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, lease.CgroupName)
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("runsc integration lease cgroup must start absent: %v", err)
	}
	if err := controller.Prepare(t.Context(), lease); err != nil {
		t.Fatal(err)
	}

	bundle := t.TempDir()
	stateRoot := filepath.Join(t.TempDir(), "state")
	runLogRoot := t.TempDir()
	if err := os.Mkdir(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	containerID := "sandbox0-resource-cgroup-integration"
	writeRunscIntegrationSpec(t, bundle, rootfs, filepath.Join("/", filepath.Base(root), lease.CgroupName), lease)

	run := func(ctx context.Context, arguments ...string) ([]byte, error) {
		t.Helper()
		base := []string{
			"--root=" + stateRoot,
			"--platform=" + platform,
			"--overlay2=none",
			"--file-access=shared",
			"--network=none",
			"--directfs=true",
		}
		logFile, err := os.CreateTemp(runLogRoot, "runsc-*.log")
		if err != nil {
			return nil, err
		}
		command := exec.CommandContext(ctx, runsc, append(base, arguments...)...)
		// A detached Sentry and Gofer may inherit these descriptors. Regular files
		// let the runsc parent return without waiting for child-owned pipes to close.
		command.Stdout = logFile
		command.Stderr = logFile
		commandErr := command.Run()
		closeErr := logFile.Close()
		output, readErr := os.ReadFile(logFile.Name())
		if commandErr != nil {
			return output, commandErr
		}
		if closeErr != nil {
			return output, closeErr
		}
		return output, readErr
	}
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = run(ctx, "delete", "--force", containerID)
		_ = unix.Unmount(filepath.Join(stateRoot, "null-netns"), unix.MNT_DETACH)
		_, _ = controller.RemoveAndConfirm(ctx, lease)
	}
	t.Cleanup(cleanup)

	startContext, cancelStart := context.WithTimeout(t.Context(), startTimeout)
	output, err := run(startContext, "run", "--detach", "--bundle="+bundle, containerID)
	cancelStart()
	if err != nil {
		t.Fatalf("runsc run: %v\n%s", err, output)
	}
	waitForRuntimeCgroupProcesses(t, path, 30*time.Second)
	if absent, err := controller.RemoveAndConfirm(t.Context(), lease); err == nil || absent || !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("occupied RemoveAndConfirm() = %t, %v", absent, err)
	}
	assertRuntimeCgroupLeaseValues(t, path, lease)

	throttledBefore := readCgroupStat(t, filepath.Join(path, "cpu.stat"), "nr_throttled")
	deadline := time.Now().Add(30 * time.Second)
	for readCgroupStat(t, filepath.Join(path, "cpu.stat"), "nr_throttled") <= throttledBefore {
		if time.Now().After(deadline) {
			t.Fatal("runsc workload was not throttled by the leased CPU quota")
		}
		time.Sleep(250 * time.Millisecond)
	}

	stopContext, cancelStop := context.WithTimeout(t.Context(), time.Minute)
	output, err = run(stopContext, "kill", containerID, "SIGKILL")
	if err == nil {
		output, err = run(stopContext, "delete", "--force", containerID)
	}
	cancelStop()
	if err != nil {
		t.Fatalf("stop runsc container: %v\n%s", err, output)
	}

	deadline = time.Now().Add(30 * time.Second)
	for {
		absent, err := controller.RemoveAndConfirm(t.Context(), lease)
		if err == nil && absent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resource cgroup did not disappear after runsc termination: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func writeRunscIntegrationSpec(
	t *testing.T,
	bundle string,
	rootfs string,
	cgroupPath string,
	lease protocol.RuntimeResourceLease,
) {
	t.Helper()
	period := lease.CPUPeriodMicros
	quota := lease.CPUQuotaMicros
	shares := lease.CPUShares
	memory := lease.MemoryBytes
	pids := lease.PIDsLimit
	spec := specs.Spec{
		Version: specs.Version,
		Process: &specs.Process{
			User: specs.User{UID: 0, GID: 0},
			Args: []string{"/bin/sh", "-c", "for i in 1 2 3 4; do (while :; do :; done) & done; wait"},
			Env:  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			Cwd:  "/", NoNewPrivileges: true,
			Rlimits: []specs.POSIXRlimit{{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024}},
		},
		Root:     &specs.Root{Path: rootfs, Readonly: false},
		Hostname: "sandbox0-resource-test",
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs"},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		},
		Linux: &specs.Linux{
			CgroupsPath: cgroupPath,
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace}, {Type: specs.NetworkNamespace},
				{Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}, {Type: specs.MountNamespace},
			},
			Resources: &specs.LinuxResources{
				Memory: &specs.LinuxMemory{Limit: &memory, Swap: &memory},
				CPU: &specs.LinuxCPU{
					Shares: &shares, Quota: &quota, Period: &period, Cpus: lease.CPUSetCPUs,
				},
				Pids: &specs.LinuxPids{Limit: pids},
			},
		},
	}
	payload, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForRuntimeCgroupProcesses(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		procs, err := readCgroupValue(filepath.Join(path, "cgroup.procs"))
		if err == nil && procs != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runsc did not enter the resource cgroup: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func assertRuntimeCgroupLeaseValues(t *testing.T, path string, lease protocol.RuntimeResourceLease) {
	t.Helper()
	for name, want := range map[string]string{
		"cpuset.mems":     lease.CPUSetMems,
		"cpuset.cpus":     lease.CPUSetCPUs,
		"cpu.max":         fmt.Sprintf("%d %d", lease.CPUQuotaMicros, lease.CPUPeriodMicros),
		"cpu.weight":      strconv.FormatUint(lease.CPUWeight, 10),
		"memory.max":      strconv.FormatInt(lease.MemoryBytes, 10),
		"memory.swap.max": "0",
		"pids.max":        strconv.FormatInt(lease.PIDsLimit, 10),
	} {
		got, err := readCgroupValue(filepath.Join(path, name))
		if err != nil || got != want {
			t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
}

func readCgroupStat(t *testing.T, path string, key string) uint64 {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != key {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	t.Fatalf("cgroup stat %s did not contain %s", path, key)
	return 0
}
