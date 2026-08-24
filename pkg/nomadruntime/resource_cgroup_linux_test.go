//go:build linux

package nomadruntime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestRuntimeResourceCgroupRequiresExactPredelegatedRoot(t *testing.T) {
	root := t.TempDir()
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("cgroup.controllers", "cpu cpuset memory pids\n")
	write("cgroup.subtree_control", "cpu cpuset memory pids\n")
	write("cgroup.procs", "")
	write("cpuset.cpus.effective", "0-3\n")
	write("cpuset.mems.effective", "0\n")
	write("cpu.max", "max 100000\n")
	write("memory.max", "max\n")
	write("memory.swap.max", "max\n")
	write("pids.max", "max\n")

	controller, err := newRuntimeResourceCgroupForOwner(root, testRuntimeResourceCgroupCapacity(), uint32(os.Geteuid()))
	if err != nil || controller == nil {
		t.Fatalf("newRuntimeResourceCgroup() = %T, %v", controller, err)
	}

	write("cgroup.subtree_control", "cpu cpuset memory\n")
	if _, err := newRuntimeResourceCgroupForOwner(root, testRuntimeResourceCgroupCapacity(), uint32(os.Geteuid())); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("missing pids delegation error = %v", err)
	}
	write("cgroup.subtree_control", "cpu cpuset memory pids\n")
	write("cgroup.procs", "123\n")
	if _, err := newRuntimeResourceCgroupForOwner(root, testRuntimeResourceCgroupCapacity(), uint32(os.Geteuid())); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("root process error = %v", err)
	}
	write("cgroup.procs", "")
	outside := testRuntimeResourceCgroupCapacity()
	outside.CPUSetCPUs = "4"
	outside.CPUMillicores = 1_000
	if _, err := newRuntimeResourceCgroupForOwner(root, outside, uint32(os.Geteuid())); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("outside cpuset error = %v", err)
	}
}

func TestRuntimeResourceCgroupPreparesAndVerifiesExactLease(t *testing.T) {
	root := t.TempDir()
	for name, value := range map[string]string{
		"cgroup.controllers":     "cpu cpuset memory pids\n",
		"cgroup.subtree_control": "cpu cpuset memory pids\n",
		"cgroup.procs":           "",
		"cpuset.cpus.effective":  "0-3\n",
		"cpuset.mems.effective":  "0\n",
		"cpu.max":                "max 100000\n",
		"memory.max":             "max\n",
		"memory.swap.max":        "max\n",
		"pids.max":               "max\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	controller, err := newRuntimeResourceCgroupForOwner(root, testRuntimeResourceCgroupCapacity(), uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	lease := testRuntimeResourceCgroupLease(t, "unit-operation-1")
	if err := controller.Prepare(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	if err := controller.Prepare(t.Context(), lease); err != nil {
		t.Fatalf("exact Prepare retry failed: %v", err)
	}
	path := filepath.Join(root, lease.CgroupName)
	for name, want := range map[string]string{
		"cpuset.mems": "0", "cpuset.cpus": "0-1", "cpu.max": "150000 100000",
		"cpu.weight": "59", "memory.max": "805306368", "memory.swap.max": "0", "pids.max": "4096",
	} {
		got, err := readCgroupValue(filepath.Join(path, name))
		if err != nil || got != want {
			t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
	changed, err := protocol.NewRuntimeResourceLease(
		"unit-operation-1", "claim-1", "slot-1", "cluster-1", "node-1", "node-uid-1", "boot-1",
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_000,
			MemoryBytes: 768 << 20, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
		},
		"0-1", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Prepare(t.Context(), changed); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("changed lease retry error = %v", err)
	}
}

func TestRuntimeResourceCgroupV2Integration(t *testing.T) {
	root := strings.TrimSpace(os.Getenv("SANDBOX0_TEST_CGROUP_ROOT"))
	if root == "" {
		t.Skip("SANDBOX0_TEST_CGROUP_ROOT is not configured")
	}
	if root == "/sys/fs/cgroup/sandbox0" || !strings.HasPrefix(filepath.Base(root), "sandbox0-test-") {
		t.Fatal("integration cgroup root must be a dedicated sandbox0-test-* subtree")
	}
	controller, err := newRuntimeResourceCgroup(root, testRuntimeResourceCgroupCapacity())
	if err != nil {
		t.Fatal(err)
	}
	lease := testRuntimeResourceCgroupLease(t, "integration-operation-1")
	path := filepath.Join(root, lease.CgroupName)
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("integration lease cgroup must start absent: %v", err)
	}
	if err := controller.Prepare(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	absent, err := controller.RemoveAndConfirm(t.Context(), lease)
	if err != nil || !absent {
		t.Fatalf("RemoveAndConfirm() = %t, %v", absent, err)
	}
}

func testRuntimeResourceCgroupLease(t *testing.T, operationID string) protocol.RuntimeResourceLease {
	t.Helper()
	lease, err := protocol.NewRuntimeResourceLease(
		operationID, "claim-1", "slot-1", "cluster-1", "node-1", "node-uid-1", "boot-1",
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_500,
			MemoryBytes: 768 << 20, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
		},
		"0-1", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func testRuntimeResourceCgroupCapacity() protocol.NodeChannelCapacity {
	return protocol.NodeChannelCapacity{
		CPUMillicores: 2_000, MemoryBytes: 1 << 30,
		CPUSetCPUs: "0-1", CPUSetMems: "0",
		TTLMilliseconds: protocol.DefaultNodeChannelCapacityTTLMilliseconds,
	}
}
