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

package runtimemetrics

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestProjectGVisorRuntimeSamplePreservesSupportedFieldsAndMarksUnsupported(t *testing.T) {
	target := validGVisorMetricTarget()
	stats := validGVisorStats(target.RunscContainerID)
	tracker := &cpuUsageTracker{}
	firstAt := time.Unix(100, 0)
	first, ok := projectGVisorRuntimeSample(target, stats, "region-a", "cluster-a", firstAt, tracker)
	if !ok {
		t.Fatal("first projection failed")
	}
	if first.TeamID != target.TeamID || first.SandboxID != target.SandboxID ||
		first.RuntimeGeneration != target.RuntimeGeneration || first.SeriesEpoch != target.SeriesEpoch ||
		first.SampleID == "" {
		t.Fatalf("sample identity = %+v", first)
	}
	if first.CPU == nil || first.CPU.Usage != nil || first.CPU.TimeSeconds == nil || *first.CPU.TimeSeconds != 1 ||
		first.CPU.LimitCores == nil || *first.CPU.LimitCores != 2 {
		t.Fatalf("first CPU projection = %+v", first.CPU)
	}
	if first.Memory == nil || first.Memory.UsageBytes == nil || *first.Memory.UsageBytes != 512 ||
		first.Memory.LimitBytes == nil || *first.Memory.LimitBytes != 4096<<20 || first.Memory.Utilization != nil {
		t.Fatalf("memory projection = %+v", first.Memory)
	}
	if first.Network == nil || *first.Network.ReceiveBytes != 100 || *first.Network.TransmitBytes != 200 ||
		*first.Network.ReceiveErrors != 3 || *first.Network.TransmitErrors != 4 {
		t.Fatalf("network projection = %+v", first.Network)
	}
	if first.Process == nil || first.Process.Count == nil || *first.Process.Count != 9 {
		t.Fatalf("process projection = %+v", first.Process)
	}
	for _, metric := range []sandboxobservability.RuntimeMetricName{
		sandboxobservability.RuntimeMetricCPUUsage,
		sandboxobservability.RuntimeMetricCPUUtilization,
		sandboxobservability.RuntimeMetricMemoryWorkingSet,
		sandboxobservability.RuntimeMetricMemoryAvailable,
		sandboxobservability.RuntimeMetricMemoryUtilization,
		sandboxobservability.RuntimeMetricRootFSWritableUsage,
		sandboxobservability.RuntimeMetricRootFSWritableInodes,
	} {
		if !sampleMissingMetric(first, metric) {
			t.Fatalf("sample does not mark %q missing: %+v", metric, first.Missing)
		}
	}

	stats.Data.CPU.Usage.Total += 500_000_000
	second, ok := projectGVisorRuntimeSample(target, stats, "region-a", "cluster-a", firstAt.Add(time.Second), tracker)
	if !ok || second.CPU == nil || second.CPU.Usage == nil || *second.CPU.Usage != 0.5 ||
		second.CPU.Utilization == nil || *second.CPU.Utilization != 0.25 {
		t.Fatalf("second CPU projection = %+v, ok=%t", second.CPU, ok)
	}
	if sampleMissingMetric(second, sandboxobservability.RuntimeMetricCPUUsage) ||
		sampleMissingMetric(second, sandboxobservability.RuntimeMetricCPUUtilization) {
		t.Fatalf("derived CPU fields remain missing: %+v", second.Missing)
	}
}

func TestProjectGVisorRuntimeSampleResetsCPUOnSeriesRotation(t *testing.T) {
	target := validGVisorMetricTarget()
	stats := validGVisorStats(target.RunscContainerID)
	tracker := &cpuUsageTracker{}
	at := time.Unix(100, 0)
	_, _ = projectGVisorRuntimeSample(target, stats, "region", "cluster", at, tracker)
	stats.Data.CPU.Usage.Total += uint64(time.Second)
	derived, ok := projectGVisorRuntimeSample(target, stats, "region", "cluster", at.Add(time.Second), tracker)
	if !ok || derived.CPU.Usage == nil {
		t.Fatal("same series did not derive CPU usage")
	}

	rotated := target
	rotated.LaunchAttempt = "launch-b"
	rotated.SeriesEpoch = nomadruntime.RuntimeMetricSeriesEpoch(
		rotated.AllocationID, rotated.NodeBootID, rotated.LaunchAttempt, rotated.RunscContainerID,
	)
	reset, ok := projectGVisorRuntimeSample(rotated, stats, "region", "cluster", at.Add(2*time.Second), tracker)
	if !ok || reset.CPU.Usage != nil {
		t.Fatalf("rotated series inherited CPU baseline: %+v, ok=%t", reset.CPU, ok)
	}
	tracker.prune(map[cpuSeriesKey]struct{}{
		{teamID: rotated.TeamID, sandboxID: rotated.SandboxID, runtimeGeneration: rotated.RuntimeGeneration, seriesEpoch: rotated.SeriesEpoch}: {},
	})
	if len(tracker.baselines) != 1 {
		t.Fatalf("CPU baselines after prune = %d, want 1", len(tracker.baselines))
	}
}

func TestProjectGVisorRuntimeSampleRejectsCounterOverflowAndIdentityMismatch(t *testing.T) {
	target := validGVisorMetricTarget()
	stats := validGVisorStats(target.RunscContainerID)
	tracker := &cpuUsageTracker{}
	stats.Data.NetworkInterfaces = append(stats.Data.NetworkInterfaces,
		&gvisorcli.RunscNetworkInterface{Name: "eth1", RxBytes: math.MaxUint64})
	if _, ok := projectGVisorRuntimeSample(target, stats, "region", "cluster", time.Unix(100, 0), tracker); ok {
		t.Fatal("projection accepted overflowing network counters")
	}
	stats = validGVisorStats(target.RunscContainerID)
	stats.Data.CPU.Usage.Total += uint64(time.Second)
	firstAccepted, ok := projectGVisorRuntimeSample(target, stats, "region", "cluster", time.Unix(101, 0), tracker)
	if !ok || firstAccepted.CPU.Usage != nil {
		t.Fatalf("rejected sample advanced CPU baseline: %+v, ok=%t", firstAccepted.CPU, ok)
	}

	stats = validGVisorStats("another-runsc")
	if _, ok := projectGVisorRuntimeSample(target, stats, "region", "cluster", time.Now(), &cpuUsageTracker{}); ok {
		t.Fatal("projection accepted another runsc identity")
	}
}

func validGVisorMetricTarget() nomadruntime.RuntimeMetricTarget {
	target := nomadruntime.RuntimeMetricTarget{
		Version: nomadruntime.RuntimeMetricTargetVersion,
		TeamID:  "team-a", SandboxID: "sandbox-a", RuntimeGeneration: 7,
		CPUMillicpu: 2000, MemoryMiB: 4096,
		AllocationID: "allocation-a", NodeBootID: "boot-a", LaunchAttempt: "launch-a",
		RunscContainerID: "runsc-a", BindingDigest: strings.Repeat("a", 64),
	}
	target.SeriesEpoch = nomadruntime.RuntimeMetricSeriesEpoch(
		target.AllocationID, target.NodeBootID, target.LaunchAttempt, target.RunscContainerID,
	)
	return target
}

func validGVisorStats(containerID string) gvisorcli.RunscStats {
	return gvisorcli.RunscStats{
		Type: "stats", ID: containerID,
		Data: gvisorcli.RunscStatsData{
			CPU:    gvisorcli.RunscCPU{Usage: gvisorcli.RunscCPUUsage{Total: uint64(time.Second)}},
			Memory: gvisorcli.RunscMemory{Usage: gvisorcli.RunscMemoryEntry{Usage: 512}},
			Pids:   gvisorcli.RunscPids{Current: 9},
			NetworkInterfaces: []*gvisorcli.RunscNetworkInterface{
				{Name: "lo", RxBytes: 1000, TxBytes: 1000},
				{Name: "eth0", RxBytes: 100, TxBytes: 200, RxErrors: 3, TxErrors: 4},
			},
		},
	}
}

func sampleMissingMetric(sample sandboxobservability.RuntimeSample, metric sandboxobservability.RuntimeMetricName) bool {
	for _, missing := range sample.Missing {
		if missing.Metric == metric {
			return true
		}
	}
	return false
}
