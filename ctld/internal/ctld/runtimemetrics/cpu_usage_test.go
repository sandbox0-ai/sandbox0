package runtimemetrics

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestCPUUsageTrackerDerivesCoresFromCumulativeCPUTime(t *testing.T) {
	tracker := &cpuUsageTracker{}
	key := cpuSeriesKey{teamID: "team-a", sandboxID: "sandbox-a", runtimeGeneration: 1, seriesEpoch: "epoch-a"}

	assert.Nil(t, tracker.observe(key, cpuUsage(10_000_000_000, 10_000_000_000)))
	derived := tracker.observe(key, cpuUsage(20_000_000_000, 15_000_000_000))

	require.NotNil(t, derived)
	assert.InDelta(t, 0.5, *derived, 0.0001)
}

func TestCPUUsageTrackerDoesNotCrossResetOrSeriesBoundaries(t *testing.T) {
	tracker := &cpuUsageTracker{}
	key := cpuSeriesKey{teamID: "team-a", sandboxID: "sandbox-a", runtimeGeneration: 1, seriesEpoch: "epoch-a"}

	assert.Nil(t, tracker.observe(key, cpuUsage(20_000_000_000, 20_000_000_000)))
	assert.Nil(t, tracker.observe(key, cpuUsage(10_000_000_000, 10_000_000_000)), "a timestamp rollback must not replace the latest baseline")
	derived := tracker.observe(key, cpuUsage(30_000_000_000, 25_000_000_000))
	require.NotNil(t, derived)
	assert.InDelta(t, 0.5, *derived, 0.0001)

	assert.Nil(t, tracker.observe(key, cpuUsage(40_000_000_000, 2_000_000_000)), "a cumulative reset starts a new baseline")
	derived = tracker.observe(key, cpuUsage(50_000_000_000, 5_000_000_000))
	require.NotNil(t, derived)
	assert.InDelta(t, 0.3, *derived, 0.0001)

	newEpoch := key
	newEpoch.seriesEpoch = "epoch-b"
	assert.Nil(t, tracker.observe(newEpoch, cpuUsage(60_000_000_000, 20_000_000_000)))
	newGeneration := key
	newGeneration.runtimeGeneration++
	assert.Nil(t, tracker.observe(newGeneration, cpuUsage(60_000_000_000, 20_000_000_000)))
}

func TestCPUUsageTrackerPrunesInactiveSeries(t *testing.T) {
	tracker := &cpuUsageTracker{}
	active := cpuSeriesKey{teamID: "team-a", sandboxID: "sandbox-a", runtimeGeneration: 1, seriesEpoch: "epoch-a"}
	stale := cpuSeriesKey{teamID: "team-a", sandboxID: "sandbox-b", runtimeGeneration: 1, seriesEpoch: "epoch-b"}

	tracker.observe(active, cpuUsage(10_000_000_000, 1_000_000_000))
	tracker.observe(stale, cpuUsage(10_000_000_000, 1_000_000_000))
	require.Equal(t, 2, tracker.size())

	tracker.prune(map[cpuSeriesKey]struct{}{active: {}})
	assert.Equal(t, 1, tracker.size())
	assert.Nil(t, tracker.observe(stale, cpuUsage(20_000_000_000, 2_000_000_000)), "a pruned series must start with a fresh baseline")
}

func TestCPUUsageTrackerConcurrentAccess(t *testing.T) {
	tracker := &cpuUsageTracker{}
	active := make(map[cpuSeriesKey]struct{})
	errors := make(chan string, 32)
	var activeMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := cpuSeriesKey{teamID: "team-a", sandboxID: fmt.Sprintf("sandbox-%d", index), runtimeGeneration: 1, seriesEpoch: fmt.Sprintf("epoch-%d", index)}
			tracker.observe(key, cpuUsage(10_000_000_000, 1_000_000_000))
			derived := tracker.observe(key, cpuUsage(20_000_000_000, 2_000_000_000))
			if derived == nil || math.Abs(*derived-0.1) > 0.0001 {
				errors <- fmt.Sprintf("series %d derived %v", index, derived)
			}
			activeMu.Lock()
			active[key] = struct{}{}
			activeMu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}

	tracker.prune(active)
	assert.Equal(t, len(active), tracker.size())
}

func cpuUsage(timestamp, cumulative uint64) *runtimeapi.CpuUsage {
	return &runtimeapi.CpuUsage{
		Timestamp:            int64(timestamp),
		UsageCoreNanoSeconds: u64(cumulative),
	}
}
