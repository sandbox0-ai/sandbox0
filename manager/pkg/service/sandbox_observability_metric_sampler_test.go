package service

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSandboxRuntimeMetricSamplesProjectsAggregateAndContextUsage(t *testing.T) {
	occurredAt := time.Unix(100, 200).UTC()
	samples := BuildSandboxRuntimeMetricSamples(SandboxRuntimeMetricSampleInput{
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		RegionID:   "aws/us-east-1",
		ClusterID:  "cluster-a",
		PodName:    "sandbox-pod",
		Namespace:  "sandbox-ns",
		OccurredAt: occurredAt,
		Usage: &SandboxResourceUsage{
			ContainerMemoryUsage:      1024,
			ContainerMemoryWorkingSet: 768,
			ContainerMemoryLimit:      2048,
			TotalMemoryRSS:            512,
			TotalMemoryVMS:            1536,
			TotalOpenFiles:            9,
			TotalThreadCount:          5,
			TotalIOReadBytes:          100,
			TotalIOWriteBytes:         200,
			ContextCount:              1,
			RunningContextCount:       1,
			PausedContextCount:        0,
			Contexts: []ContextResourceUsage{{
				ContextID: "ctx-1",
				Type:      "cmd",
				Language:  "python",
				Running:   true,
				Usage: ResourceUsage{
					CPUPercent:   12.5,
					MemoryRSS:    256,
					MemoryVMS:    512,
					OpenFiles:    3,
					ThreadCount:  2,
					IOReadBytes:  10,
					IOWriteBytes: 20,
				},
			}},
		},
	})

	require.Len(t, samples, 19)
	containerUsage := findMetricSample(t, samples, "", "container.memory.usage_bytes")
	assert.Equal(t, float64(1024), containerUsage.Value)
	assert.Equal(t, "bytes", containerUsage.Unit)
	assert.Equal(t, "sandbox-pod", containerUsage.Attributes["pod_name"])
	assert.Equal(t, "procd", containerUsage.Attributes["source"])

	contextCPU := findMetricSample(t, samples, "ctx-1", "process.cpu.percent")
	assert.Equal(t, float64(12.5), contextCPU.Value)
	assert.Equal(t, "percent", contextCPU.Unit)
	assert.Equal(t, "cmd", contextCPU.Attributes["context_type"])
	assert.Equal(t, "python", contextCPU.Attributes["language"])
	assert.Equal(t, true, contextCPU.Attributes["running"])
	assert.Contains(t, contextCPU.Cursor, "manager-runtime:sandbox-1:ctx-1:process.cpu.percent:")
}

func findMetricSample(t *testing.T, samples []sandboxobservability.MetricSample, contextID, name string) sandboxobservability.MetricSample {
	t.Helper()
	for _, sample := range samples {
		if sample.ContextID == contextID && sample.Name == name {
			return sample
		}
	}
	t.Fatalf("sample %q for context %q not found", name, contextID)
	return sandboxobservability.MetricSample{}
}
