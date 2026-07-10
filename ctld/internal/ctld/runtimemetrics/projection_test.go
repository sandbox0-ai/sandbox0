package runtimemetrics

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestProjectRuntimeSampleProjectsBulkCRIStats(t *testing.T) {
	cpuLimit := 2.0
	memoryLimit := uint64(4 * 1024)
	observedAt := time.Unix(123, 456).UTC()
	stats := &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{
			Id:       "cri-sandbox-a",
			Metadata: &runtimeapi.PodSandboxMetadata{Uid: "pod-uid-a"},
		},
		Linux: &runtimeapi.LinuxPodSandboxStats{
			Cpu: &runtimeapi.CpuUsage{
				Timestamp:            observedAt.UnixNano() - 10,
				UsageNanoCores:       u64(500_000_000),
				UsageCoreNanoSeconds: u64(12_500_000_000),
			},
			Memory: &runtimeapi.MemoryUsage{
				Timestamp:       observedAt.UnixNano(),
				UsageBytes:      u64(3072),
				WorkingSetBytes: u64(2048),
				AvailableBytes:  u64(2048),
			},
			Network: &runtimeapi.NetworkUsage{
				Timestamp: observedAt.UnixNano() - 5,
				DefaultInterface: &runtimeapi.NetworkInterfaceUsage{
					Name: "eth0", RxBytes: u64(100), TxBytes: u64(200), RxErrors: u64(1), TxErrors: u64(2),
				},
				Interfaces: []*runtimeapi.NetworkInterfaceUsage{
					{Name: "eth1", RxBytes: u64(10), TxBytes: u64(20), RxErrors: u64(3), TxErrors: u64(4)},
					{Name: "lo", RxBytes: u64(1000), TxBytes: u64(1000), RxErrors: u64(1000), TxErrors: u64(1000)},
				},
			},
			Process: &runtimeapi.ProcessUsage{Timestamp: observedAt.UnixNano() - 2, ProcessCount: u64(9)},
			Containers: []*runtimeapi.ContainerStats{
				{
					Attributes:    &runtimeapi.ContainerAttributes{Metadata: &runtimeapi.ContainerMetadata{Name: "sidecar"}},
					WritableLayer: &runtimeapi.FilesystemUsage{UsedBytes: u64(999), InodesUsed: u64(999)},
				},
				{
					Attributes:    &runtimeapi.ContainerAttributes{Metadata: &runtimeapi.ContainerMetadata{Name: sandboxContainerName}},
					WritableLayer: &runtimeapi.FilesystemUsage{Timestamp: observedAt.UnixNano() - 1, UsedBytes: u64(4096), InodesUsed: u64(17)},
				},
			},
		},
	}

	sample, ok := projectRuntimeSample(sandboxIdentity{
		TeamID:            "team-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 7,
		PodUID:            "pod-uid-a",
		CPULimitCores:     &cpuLimit,
		MemoryLimitBytes:  &memoryLimit,
	}, stats, "aws/us-east-1", "cluster-a", observedAt.Add(time.Second), nil)

	require.True(t, ok)
	assert.Equal(t, observedAt, sample.ObservedAt)
	assert.Equal(t, "cri-sandbox-a", sample.SeriesEpoch)
	assert.Equal(t, int64(7), sample.RuntimeGeneration)
	assert.NotEmpty(t, sample.SampleID)
	assert.Empty(t, sample.Missing)
	require.NotNil(t, sample.CPU)
	assert.InDelta(t, 0.5, *sample.CPU.Usage, 0.0001)
	assert.InDelta(t, 0.25, *sample.CPU.Utilization, 0.0001)
	assert.InDelta(t, 12.5, *sample.CPU.TimeSeconds, 0.0001)
	assert.Equal(t, cpuLimit, *sample.CPU.LimitCores)
	require.NotNil(t, sample.Memory)
	assert.Equal(t, uint64(3072), *sample.Memory.UsageBytes)
	assert.Equal(t, uint64(2048), *sample.Memory.WorkingSetBytes)
	assert.InDelta(t, 0.5, *sample.Memory.Utilization, 0.0001)
	require.NotNil(t, sample.Network)
	assert.Equal(t, uint64(110), *sample.Network.ReceiveBytes)
	assert.Equal(t, uint64(220), *sample.Network.TransmitBytes)
	assert.Equal(t, uint64(4), *sample.Network.ReceiveErrors)
	assert.Equal(t, uint64(6), *sample.Network.TransmitErrors)
	assert.Equal(t, uint64(9), *sample.Process.Count)
	assert.Equal(t, uint64(4096), *sample.RootFSWritable.UsageBytes)
	assert.Equal(t, uint64(17), *sample.RootFSWritable.Inodes)
}

func TestProjectRuntimeSampleReportsMissingWithoutFabricatingZeroes(t *testing.T) {
	collectedAt := time.Unix(200, 0).UTC()
	sample, ok := projectRuntimeSample(sandboxIdentity{
		TeamID:            "team-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
		PodUID:            "pod-uid-a",
	}, &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{Metadata: &runtimeapi.PodSandboxMetadata{Uid: "pod-uid-a"}},
		Linux:      &runtimeapi.LinuxPodSandboxStats{},
	}, "region-a", "cluster-a", collectedAt, nil)

	require.True(t, ok)
	assert.Equal(t, collectedAt, sample.ObservedAt)
	assert.Equal(t, "pod-uid-a", sample.SeriesEpoch)
	assert.Nil(t, sample.CPU.Usage)
	assert.Nil(t, sample.CPU.LimitCores)
	assert.Nil(t, sample.Memory.UsageBytes)
	assert.Nil(t, sample.Network.ReceiveBytes)
	assert.Nil(t, sample.Process.Count)
	assert.Nil(t, sample.RootFSWritable.UsageBytes)
	assertMissing(t, sample.Missing, sandboxobservability.RuntimeMetricCPUUsage, nil)
	assertMissing(t, sample.Missing, sandboxobservability.RuntimeMetricCPULimit, nil)
	assertMissing(t, sample.Missing, sandboxobservability.RuntimeMetricNetworkIO, map[string]string{"direction": "receive"})
	assertMissing(t, sample.Missing, sandboxobservability.RuntimeMetricNetworkIO, map[string]string{"direction": "transmit"})
}

func TestProjectRuntimeSampleMarksNonLinuxStatsUnsupported(t *testing.T) {
	sample, ok := projectRuntimeSample(sandboxIdentity{
		TeamID: "team-a", SandboxID: "sandbox-a", RuntimeGeneration: 1, PodUID: "pod-uid-a",
	}, &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{Id: "cri-a", Metadata: &runtimeapi.PodSandboxMetadata{Uid: "pod-uid-a"}},
	}, "region-a", "cluster-a", time.Unix(300, 0).UTC(), nil)

	require.True(t, ok)
	require.NotEmpty(t, sample.Missing)
	for _, missing := range sample.Missing {
		assert.Equal(t, sandboxobservability.RuntimeMetricMissingUnsupported, missing.Reason)
	}
}

func TestProjectRuntimeSamplePrefersNativeCPUUsage(t *testing.T) {
	cpuLimit := 2.0
	derived := 0.2
	sample, ok := projectRuntimeSample(sandboxIdentity{
		TeamID: "team-a", SandboxID: "sandbox-a", RuntimeGeneration: 1, PodUID: "pod-uid-a", CPULimitCores: &cpuLimit,
	}, &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{Id: "cri-a"},
		Linux: &runtimeapi.LinuxPodSandboxStats{Cpu: &runtimeapi.CpuUsage{
			Timestamp:            time.Unix(400, 0).UnixNano(),
			UsageNanoCores:       u64(500_000_000),
			UsageCoreNanoSeconds: u64(10_000_000_000),
		}},
	}, "region-a", "cluster-a", time.Unix(400, 0).UTC(), &derived)

	require.True(t, ok)
	require.NotNil(t, sample.CPU.Usage)
	assert.InDelta(t, 0.5, *sample.CPU.Usage, 0.0001)
	assert.InDelta(t, 0.25, *sample.CPU.Utilization, 0.0001)
	assertNotMissing(t, sample.Missing, sandboxobservability.RuntimeMetricCPUUsage, nil)
	assertNotMissing(t, sample.Missing, sandboxobservability.RuntimeMetricCPUUtilization, nil)
}

func TestProjectRootFSWritableRejectsContainerdSnapshotFailureShape(t *testing.T) {
	sample := sandboxobservability.RuntimeSample{}
	projectRootFSWritable(&sample, []*runtimeapi.ContainerStats{{
		Attributes: &runtimeapi.ContainerAttributes{Metadata: &runtimeapi.ContainerMetadata{Name: sandboxContainerName}},
		WritableLayer: &runtimeapi.FilesystemUsage{
			Timestamp:  0,
			UsedBytes:  u64(0),
			InodesUsed: u64(0),
		},
	}})

	require.NotNil(t, sample.RootFSWritable)
	assert.Nil(t, sample.RootFSWritable.UsageBytes)
	assert.Nil(t, sample.RootFSWritable.Inodes)
	assertMissing(t, sample.Missing, sandboxobservability.RuntimeMetricRootFSWritableUsage, nil)
	assertMissing(t, sample.Missing, sandboxobservability.RuntimeMetricRootFSWritableInodes, nil)
}

func TestProjectRootFSWritablePreservesObservedZeroes(t *testing.T) {
	sample := sandboxobservability.RuntimeSample{}
	projectRootFSWritable(&sample, []*runtimeapi.ContainerStats{{
		Attributes: &runtimeapi.ContainerAttributes{Metadata: &runtimeapi.ContainerMetadata{Name: sandboxContainerName}},
		WritableLayer: &runtimeapi.FilesystemUsage{
			Timestamp:  time.Unix(500, 0).UnixNano(),
			UsedBytes:  u64(0),
			InodesUsed: u64(0),
		},
	}})

	require.NotNil(t, sample.RootFSWritable)
	require.NotNil(t, sample.RootFSWritable.UsageBytes)
	require.NotNil(t, sample.RootFSWritable.Inodes)
	assert.Zero(t, *sample.RootFSWritable.UsageBytes)
	assert.Zero(t, *sample.RootFSWritable.Inodes)
	assertNotMissing(t, sample.Missing, sandboxobservability.RuntimeMetricRootFSWritableUsage, nil)
	assertNotMissing(t, sample.Missing, sandboxobservability.RuntimeMetricRootFSWritableInodes, nil)
}

func TestRuntimeSampleIDIsDeterministic(t *testing.T) {
	sample := sandboxobservability.RuntimeSample{
		TeamID: "team-a", SandboxID: "sandbox-a", RuntimeGeneration: 3, SeriesEpoch: "epoch-a", ObservedAt: time.Unix(100, 1).UTC(),
	}
	assert.Equal(t, runtimeSampleID(sample), runtimeSampleID(sample))
	sample.ObservedAt = sample.ObservedAt.Add(time.Nanosecond)
	assert.NotEqual(t, runtimeSampleID(sandboxobservability.RuntimeSample{
		TeamID: "team-a", SandboxID: "sandbox-a", RuntimeGeneration: 3, SeriesEpoch: "epoch-a", ObservedAt: time.Unix(100, 1).UTC(),
	}), runtimeSampleID(sample))
}

func assertMissing(t *testing.T, missing []sandboxobservability.RuntimeMetricMissing, metric sandboxobservability.RuntimeMetricName, dimensions map[string]string) {
	t.Helper()
	for _, item := range missing {
		if item.Metric == metric && assert.ObjectsAreEqual(dimensions, item.Dimensions) {
			return
		}
	}
	t.Fatalf("missing marker for %s with dimensions %#v not found in %#v", metric, dimensions, missing)
}

func assertNotMissing(t *testing.T, missing []sandboxobservability.RuntimeMetricMissing, metric sandboxobservability.RuntimeMetricName, dimensions map[string]string) {
	t.Helper()
	for _, item := range missing {
		if item.Metric == metric && assert.ObjectsAreEqual(dimensions, item.Dimensions) {
			t.Fatalf("unexpected missing marker for %s with dimensions %#v in %#v", metric, dimensions, missing)
		}
	}
}

func u64(value uint64) *runtimeapi.UInt64Value {
	return &runtimeapi.UInt64Value{Value: value}
}
