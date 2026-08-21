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

import "testing"

func TestBuildSpecAppliesDedicatedCoreLimits(t *testing.T) {
	spec := buildSpec(specOptions{
		Command: "/procd",
		Resources: &driversResources{
			CPUPeriod: 100000, CPUQuota: 100000, CPUShares: 1024,
			CPUSetCpus: "2", MemoryLimitBytes: 1024 * 1024 * 1024,
		},
	})
	if spec.Linux == nil || spec.Linux.Resources == nil || spec.Linux.Resources.CPU == nil ||
		spec.Linux.Resources.CPU.Period == nil || *spec.Linux.Resources.CPU.Period != 100000 ||
		spec.Linux.Resources.CPU.Quota == nil || *spec.Linux.Resources.CPU.Quota != 100000 ||
		spec.Linux.Resources.CPU.Shares == nil || *spec.Linux.Resources.CPU.Shares != 1024 ||
		spec.Linux.Resources.CPU.Cpus != "2" {
		t.Fatalf("OCI CPU resources = %+v", spec.Linux)
	}
	if spec.Linux.Resources.Memory == nil || spec.Linux.Resources.Memory.Limit == nil ||
		*spec.Linux.Resources.Memory.Limit != 1024*1024*1024 {
		t.Fatalf("OCI memory resources = %+v", spec.Linux.Resources.Memory)
	}
}
