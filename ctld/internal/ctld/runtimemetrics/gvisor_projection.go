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
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func projectGVisorRuntimeSample(
	target nomadruntime.RuntimeMetricTarget,
	stats gvisorcli.RunscStats,
	regionID, clusterID string,
	observedAt time.Time,
	tracker *cpuUsageTracker,
) (sandboxobservability.RuntimeSample, bool) {
	if target.Validate() != nil || stats.Validate(target.RunscContainerID) != nil || observedAt.IsZero() || observedAt.UnixNano() <= 0 {
		return sandboxobservability.RuntimeSample{}, false
	}
	observedAt = observedAt.UTC()
	sample := sandboxobservability.RuntimeSample{
		TeamID: target.TeamID, SandboxID: target.SandboxID,
		RegionID: strings.TrimSpace(regionID), ClusterID: strings.TrimSpace(clusterID),
		RuntimeGeneration: target.RuntimeGeneration, SeriesEpoch: target.SeriesEpoch,
		ObservedAt: observedAt,
	}
	projectGVisorCPU(&sample, target, stats.Data.CPU.Usage, observedAt, tracker)
	projectGVisorMemory(&sample, target, stats.Data.Memory)
	if !projectGVisorNetwork(&sample, stats.Data.NetworkInterfaces) {
		return sandboxobservability.RuntimeSample{}, false
	}
	processCount := stats.Data.Pids.Current
	sample.Process = &sandboxobservability.RuntimeProcessValues{Count: &processCount}
	sample.RootFSWritable = &sandboxobservability.RuntimeRootFSWritableValues{}
	appendMissing(&sample, sandboxobservability.RuntimeMetricRootFSWritableUsage, nil,
		sandboxobservability.RuntimeMetricMissingUnsupported, "stock runsc stats do not expose writable rootfs bytes")
	appendMissing(&sample, sandboxobservability.RuntimeMetricRootFSWritableInodes, nil,
		sandboxobservability.RuntimeMetricMissingUnsupported, "stock runsc stats do not expose writable rootfs inodes")
	sample.SampleID = runtimeSampleID(sample)
	return sample, true
}

func projectGVisorCPU(
	sample *sandboxobservability.RuntimeSample,
	target nomadruntime.RuntimeMetricTarget,
	usage gvisorcli.RunscCPUUsage,
	observedAt time.Time,
	tracker *cpuUsageTracker,
) {
	limit := float64(target.CPUMillicpu) / 1000
	totalSeconds := float64(usage.Total) / nanoScale
	values := &sandboxobservability.RuntimeCPUValues{TimeSeconds: &totalSeconds, LimitCores: &limit}
	key := cpuSeriesKey{
		teamID: target.TeamID, sandboxID: target.SandboxID,
		runtimeGeneration: target.RuntimeGeneration, seriesEpoch: target.SeriesEpoch,
	}
	if tracker != nil {
		values.Usage = tracker.observeCumulative(key, observedAt.UnixNano(), usage.Total)
	}
	if values.Usage == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUUsage, nil,
			sandboxobservability.RuntimeMetricMissingUnavailable, "runsc cumulative CPU time needs two samples to derive current usage")
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUUtilization, nil,
			sandboxobservability.RuntimeMetricMissingUnavailable, "runsc current CPU usage cannot be derived yet")
	} else {
		utilization := *values.Usage / limit
		values.Utilization = &utilization
	}
	sample.CPU = values
}

func projectGVisorMemory(
	sample *sandboxobservability.RuntimeSample,
	target nomadruntime.RuntimeMetricTarget,
	memory gvisorcli.RunscMemory,
) {
	limit := uint64(target.MemoryMiB) << 20
	usage := memory.Usage.Usage
	values := &sandboxobservability.RuntimeMemoryValues{UsageBytes: &usage, LimitBytes: &limit}
	appendMissing(sample, sandboxobservability.RuntimeMetricMemoryWorkingSet, nil,
		sandboxobservability.RuntimeMetricMissingUnsupported, "stock runsc stats do not expose memory working set")
	appendMissing(sample, sandboxobservability.RuntimeMetricMemoryAvailable, nil,
		sandboxobservability.RuntimeMetricMissingUnsupported, "stock runsc stats do not expose available memory")
	appendMissing(sample, sandboxobservability.RuntimeMetricMemoryUtilization, nil,
		sandboxobservability.RuntimeMetricMissingUnsupported, "memory utilization requires working set, which stock runsc stats do not expose")
	sample.Memory = values
}

func projectGVisorNetwork(
	sample *sandboxobservability.RuntimeSample,
	interfaces []*gvisorcli.RunscNetworkInterface,
) bool {
	var receiveBytes, transmitBytes, receiveErrors, transmitErrors uint64
	for _, item := range interfaces {
		if item == nil || isLoopbackInterface(item.Name) {
			continue
		}
		if !addUint64(&receiveBytes, item.RxBytes) || !addUint64(&transmitBytes, item.TxBytes) ||
			!addUint64(&receiveErrors, item.RxErrors) || !addUint64(&transmitErrors, item.TxErrors) {
			return false
		}
	}
	sample.Network = &sandboxobservability.RuntimeNetworkValues{
		ReceiveBytes: &receiveBytes, TransmitBytes: &transmitBytes,
		ReceiveErrors: &receiveErrors, TransmitErrors: &transmitErrors,
	}
	return true
}

func addUint64(total *uint64, value uint64) bool {
	if total == nil || math.MaxUint64-*total < value {
		return false
	}
	*total += value
	return true
}
