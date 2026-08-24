package runtimemetrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCPUUsageTrackerDerivesAndResetsCumulativeUsage(t *testing.T) {
	tracker := &cpuUsageTracker{}
	key := cpuSeriesKey{teamID: "team-a", sandboxID: "sandbox-a", runtimeGeneration: 1, seriesEpoch: "epoch-a"}
	assert.Nil(t, tracker.observeCumulative(key, 1_000_000_000, 2_000_000_000))
	usage := tracker.observeCumulative(key, 2_000_000_000, 2_500_000_000)
	if assert.NotNil(t, usage) {
		assert.InDelta(t, 0.5, *usage, 0.0001)
	}
	assert.Nil(t, tracker.observeCumulative(key, 3_000_000_000, 100))
	usage = tracker.observeCumulative(key, 4_000_000_000, 1_000_000_100)
	if assert.NotNil(t, usage) {
		assert.InDelta(t, 1.0, *usage, 0.0001)
	}
}
