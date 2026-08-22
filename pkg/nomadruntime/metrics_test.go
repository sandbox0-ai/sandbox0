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
	"time"
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

func TestRuntimeMetricSampleValidate(t *testing.T) {
	target := testRuntimeMetricTarget()
	sample := RuntimeMetricSample{
		Version: runtimeMetricSampleVersion, ObservedAt: time.Unix(1, 0),
		Stats: RunscStats{Type: "stats", ID: target.RunscContainerID},
	}
	if err := sample.Validate(target); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	sample.Stats.ID = "another-container"
	if err := sample.Validate(target); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestNormalizeRuntimeMetricTargetsSortsAndRejectsAmbiguity(t *testing.T) {
	first := testRuntimeMetricTarget()
	second := first
	second.AllocationID = "allocation-b"
	second.RunscContainerID = "runsc-b"
	second.BindingDigest = strings.Repeat("b", 64)
	second.SeriesEpoch = RuntimeMetricSeriesEpoch(
		second.AllocationID, second.NodeBootID, second.LaunchAttempt, second.RunscContainerID,
	)
	normalized, err := normalizeRuntimeMetricTargets([]RuntimeMetricTarget{second, first})
	if err != nil || len(normalized) != 2 || normalized[0] != first || normalized[1] != second {
		t.Fatalf("normalizeRuntimeMetricTargets() = %+v, %v", normalized, err)
	}

	duplicateBinding := second
	duplicateBinding.BindingDigest = first.BindingDigest
	if _, err := normalizeRuntimeMetricTargets([]RuntimeMetricTarget{first, duplicateBinding}); err == nil ||
		!strings.Contains(err.Error(), "binding") {
		t.Fatalf("duplicate binding error = %v", err)
	}
	duplicateSeries := first
	duplicateSeries.BindingDigest = second.BindingDigest
	if _, err := normalizeRuntimeMetricTargets([]RuntimeMetricTarget{first, duplicateSeries}); err == nil ||
		!strings.Contains(err.Error(), "metric series") {
		t.Fatalf("duplicate series error = %v", err)
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
