// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/nomad/plugins/drivers"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestRuntimeLeaseCgroupsPathIsRelativeToUnifiedMount(t *testing.T) {
	name := "s0-" + strings.Repeat("a", 64)
	path, err := runtimeLeaseCgroupsPath("/sys/fs/cgroup/sandbox0", name)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/sandbox0/"+name {
		t.Fatalf("cgroupsPath = %q", path)
	}
	if _, err := runtimeLeaseCgroupsPath("/tmp/sandbox0", "s0-test"); err == nil {
		t.Fatal("resource cgroup root outside the unified mount was accepted")
	}
}

func TestBuildSpecAppliesExactRuntimeResourceLease(t *testing.T) {
	spec := buildSpec(specOptions{
		Command: "/procd", ProcdInternalJWTPublicKeyFile: "/etc/sandbox0/internal-auth/data-public.pem",
		ResolvConfPath: "/alloc/sandbox0/resolv.conf",
		Resources: &driversResources{
			CPUPeriod: 100000, CPUQuota: 150000, CPUShares: 1536,
			CPUSetCpus: "0-1", MemoryLimitBytes: 768 * 1024 * 1024,
			PIDsLimit: 4096, CgroupPath: "/sandbox0/s0-lease",
		},
	})
	if spec.Linux == nil || spec.Linux.Resources == nil || spec.Linux.Resources.CPU == nil ||
		spec.Linux.Resources.CPU.Period == nil || *spec.Linux.Resources.CPU.Period != 100000 ||
		spec.Linux.Resources.CPU.Quota == nil || *spec.Linux.Resources.CPU.Quota != 150000 ||
		spec.Linux.Resources.CPU.Shares == nil || *spec.Linux.Resources.CPU.Shares != 1536 ||
		spec.Linux.Resources.CPU.Cpus != "0-1" {
		t.Fatalf("OCI CPU resources = %+v", spec.Linux)
	}
	if spec.Linux.Resources.Memory == nil || spec.Linux.Resources.Memory.Limit == nil ||
		*spec.Linux.Resources.Memory.Limit != 768*1024*1024 ||
		spec.Linux.Resources.Memory.Swap == nil || *spec.Linux.Resources.Memory.Swap != 768*1024*1024 {
		t.Fatalf("OCI memory resources = %+v", spec.Linux.Resources.Memory)
	}
	if spec.Linux.Resources.Pids == nil || spec.Linux.Resources.Pids.Limit == nil ||
		*spec.Linux.Resources.Pids.Limit != 4096 ||
		spec.Linux.CgroupsPath != "/sandbox0/s0-lease" {
		t.Fatalf("OCI PIDs/cgroup resources = %+v", spec.Linux)
	}
	keyMount := findOCIMount(spec.Mounts, procdInternalJWTPublicKeyDestination)
	if keyMount == nil || keyMount.Source != "/etc/sandbox0/internal-auth/data-public.pem" ||
		!strings.Contains(strings.Join(keyMount.Options, ","), "ro") {
		t.Fatalf("OCI procd internal JWT public-key mount = %+v", spec.Mounts)
	}
	resolvMount := findOCIMount(spec.Mounts, "/etc/resolv.conf")
	if resolvMount == nil || resolvMount.Type != "bind" ||
		resolvMount.Source != "/alloc/sandbox0/resolv.conf" ||
		!containsString(resolvMount.Options, "ro") {
		t.Fatalf("OCI resolver mount = %+v", spec.Mounts)
	}
}

func TestBuildSpecAppliesSecurityClassAndEphemeralMounts(t *testing.T) {
	standard := buildSpec(specOptions{Command: "/procd", SecurityClass: "standard"})
	if containsCapability(standard.Process.Capabilities.Effective, "CAP_SYS_ADMIN") {
		t.Fatal("standard class received CAP_SYS_ADMIN")
	}
	if !containsCapability(standard.Process.Capabilities.Effective, "CAP_CHOWN") ||
		!containsCapability(standard.Process.Capabilities.Effective, "CAP_SETUID") {
		t.Fatalf("standard OCI capabilities = %#v", standard.Process.Capabilities)
	}
	privileged := buildSpec(specOptions{
		Command: "/procd", SecurityClass: "privileged",
		EphemeralMounts: []runtimecontrol.EphemeralMount{
			{MountPath: "/var/lib/docker", SizeBytes: 16 << 30},
			{MountPath: "/dev/shm", SizeBytes: 2 << 30},
		},
	})
	if !containsCapability(privileged.Process.Capabilities.Effective, "CAP_SYS_ADMIN") ||
		!containsCapability(privileged.Process.Capabilities.Bounding, "CAP_NET_ADMIN") {
		t.Fatalf("privileged capabilities = %#v", privileged.Process.Capabilities)
	}
	dockerMount := findOCIMount(privileged.Mounts, "/var/lib/docker")
	if dockerMount == nil || !containsString(dockerMount.Options, "size=17179869184") {
		t.Fatalf("Docker ephemeral mount = %#v", dockerMount)
	}
	shmCount := 0
	for _, mount := range privileged.Mounts {
		if mount.Destination == "/dev/shm" {
			shmCount++
			if !containsString(mount.Options, "size=2147483648") {
				t.Fatalf("shm mount = %#v", mount)
			}
		}
	}
	if shmCount != 1 {
		t.Fatalf("shm mount count = %d", shmCount)
	}
}

func TestWriteClaimBundleGeneratesNomadDNSConfiguration(t *testing.T) {
	bundleDir := t.TempDir()
	handle := newTaskHandle(taskHandleOptions{
		taskConfig: &drivers.TaskConfig{
			ID: "slot-1", NodeID: "node-1", AllocID: "alloc-1",
			DNS: &drivers.DNSConfig{
				Servers:  []string{"192.0.2.53", "198.51.100.53"},
				Searches: []string{"sandbox.internal"},
				Options:  []string{"timeout:2", "attempts:2"},
			},
		},
		driverConfig:       TaskConfig{Command: "/procd"},
		bundleDir:          bundleDir,
		resourceCgroupRoot: "/sys/fs/cgroup/sandbox0",
	})
	lease, err := protocol.NewRuntimeResourceLease(
		"operation-1", "claim-1", "slot-1", "cluster-1", "node-1", "node-1", "boot-1",
		protocol.RuntimeResourceRequest{
			Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 500,
			MemoryBytes: 256 << 20, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
		},
		"0-1", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.writeClaimBundle(nil, lease); err != nil {
		t.Fatal(err)
	}
	resolverPath := filepath.Join(bundleDir, "resolv.conf")
	resolver, err := os.ReadFile(resolverPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"nameserver 192.0.2.53", "nameserver 198.51.100.53",
		"search sandbox.internal", "options timeout:2 attempts:2",
	} {
		if !strings.Contains(string(resolver), expected) {
			t.Fatalf("resolver configuration %q does not contain %q", resolver, expected)
		}
	}
	config, err := os.ReadFile(filepath.Join(bundleDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), `"source": "`+resolverPath+`"`) ||
		!strings.Contains(string(config), `"destination": "/etc/resolv.conf"`) {
		t.Fatalf("OCI bundle does not mount generated resolver configuration: %s", config)
	}
}

func findOCIMount(mounts []specs.Mount, destination string) *specs.Mount {
	for index := range mounts {
		if mounts[index].Destination == destination {
			return &mounts[index]
		}
	}
	return nil
}

func containsCapability(values []string, target string) bool { return containsString(values, target) }

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
