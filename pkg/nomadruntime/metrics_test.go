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

package nomadruntime

import (
	"strings"
	"testing"
)

func TestRuntimeMetricTargetValidate(t *testing.T) {
	target := testRuntimeMetricTarget()
	if err := target.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*RuntimeMetricTarget)
		want   string
	}{
		{name: "version", mutate: func(t *RuntimeMetricTarget) { t.Version++ }, want: "unsupported"},
		{name: "identity", mutate: func(t *RuntimeMetricTarget) { t.TeamID = " team" }, want: "team_id"},
		{name: "runtime generation", mutate: func(t *RuntimeMetricTarget) { t.RuntimeGeneration = 0 }, want: "runtime_generation"},
		{name: "cpu", mutate: func(t *RuntimeMetricTarget) { t.CPUMillicpu = 0 }, want: "cpu_millicpu"},
		{name: "memory", mutate: func(t *RuntimeMetricTarget) { t.MemoryMiB = 0 }, want: "memory_mib"},
		{name: "binding", mutate: func(t *RuntimeMetricTarget) { t.BindingDigest = strings.Repeat("a", 63) }, want: "binding_digest"},
		{name: "epoch", mutate: func(t *RuntimeMetricTarget) { t.LaunchAttempt = "launch-b" }, want: "series_epoch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := target
			test.mutate(&invalid)
			if err := invalid.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestRuntimeMetricSeriesEpochIsUnambiguous(t *testing.T) {
	first := RuntimeMetricSeriesEpoch("ab", "c", "launch", "runsc")
	second := RuntimeMetricSeriesEpoch("a", "bc", "launch", "runsc")
	if first == second || !strings.HasPrefix(first, "runsc:") || len(first) != len("runsc:")+64 {
		t.Fatalf("series epochs are not canonical: %q %q", first, second)
	}
}

func testRuntimeMetricTarget() RuntimeMetricTarget {
	target := RuntimeMetricTarget{
		Version: RuntimeMetricTargetVersion, TeamID: "team-a", SandboxID: "sandbox-a",
		RuntimeGeneration: 7, CPUMillicpu: 2000, MemoryMiB: 4096,
		AllocationID: "allocation-a", NodeBootID: "boot-a", LaunchAttempt: "launch-a",
		RunscContainerID: "runsc-a", BindingDigest: strings.Repeat("a", 64),
	}
	target.SeriesEpoch = RuntimeMetricSeriesEpoch(
		target.AllocationID, target.NodeBootID, target.LaunchAttempt, target.RunscContainerID,
	)
	return target
}
