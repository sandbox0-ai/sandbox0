package http

import (
	"testing"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func TestFunctionUsageMeterActiveWindowsUseRuntimeCapacityOnce(t *testing.T) {
	meter := &functionUsageMeter{regionID: "aws-us-east-1", clusterID: "cluster-a"}
	start := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	end := start.Add(1500 * time.Millisecond)

	windows := meter.activeWindows(&functionActiveWindow{
		start:             start,
		teamID:            "team-1",
		userID:            "user-1",
		functionID:        "fn-1",
		revisionID:        "rev-1",
		runtimeID:         "inst-1",
		sandboxID:         "sb-1",
		resourceMillicpu:  2000,
		resourceMemoryMiB: 1024,
	}, end)

	if len(windows) != 3 {
		t.Fatalf("window count = %d, want 3", len(windows))
	}
	if windows[0].WindowType != meteringpkg.WindowTypeFunctionActiveRuntimeMilliseconds || windows[0].Value != 1500 {
		t.Fatalf("runtime window = %+v", windows[0])
	}
	if windows[1].WindowType != meteringpkg.WindowTypeFunctionActiveMillicpuMilliseconds || windows[1].Value != 3_000_000 {
		t.Fatalf("cpu window = %+v", windows[1])
	}
	if windows[2].WindowType != meteringpkg.WindowTypeFunctionActiveMiBMilliseconds || windows[2].Value != 1_536_000 {
		t.Fatalf("memory window = %+v", windows[2])
	}
}
