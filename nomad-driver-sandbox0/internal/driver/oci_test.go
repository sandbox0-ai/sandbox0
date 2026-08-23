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
	"strings"
	"testing"
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
		Command: "/procd",
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
}
