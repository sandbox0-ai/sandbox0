package runtimemetrics

import "sync"

type cpuSeriesKey struct {
	teamID            string
	sandboxID         string
	runtimeGeneration int64
	seriesEpoch       string
}

type cpuUsageBaseline struct {
	timestamp            int64
	usageCoreNanoSeconds uint64
}

// cpuUsageTracker derives instantaneous core usage from cumulative CPU time
// while keeping reset boundaries isolated per runtime series.
type cpuUsageTracker struct {
	mu        sync.Mutex
	baselines map[cpuSeriesKey]cpuUsageBaseline
}

func (t *cpuUsageTracker) observeCumulative(key cpuSeriesKey, timestamp int64, cumulativeCPUTime uint64) *float64 {
	if t == nil || timestamp <= 0 {
		return nil
	}
	current := cpuUsageBaseline{timestamp: timestamp, usageCoreNanoSeconds: cumulativeCPUTime}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.baselines == nil {
		t.baselines = make(map[cpuSeriesKey]cpuUsageBaseline)
	}
	previous, found := t.baselines[key]
	if !found {
		t.baselines[key] = current
		return nil
	}
	if current.timestamp <= previous.timestamp {
		if current.timestamp == previous.timestamp && current.usageCoreNanoSeconds < previous.usageCoreNanoSeconds {
			t.baselines[key] = current
		}
		return nil
	}
	if current.usageCoreNanoSeconds < previous.usageCoreNanoSeconds {
		t.baselines[key] = current
		return nil
	}
	t.baselines[key] = current
	cores := float64(current.usageCoreNanoSeconds-previous.usageCoreNanoSeconds) /
		float64(current.timestamp-previous.timestamp)
	return &cores
}

func (t *cpuUsageTracker) prune(active map[cpuSeriesKey]struct{}) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for key := range t.baselines {
		if _, ok := active[key]; !ok {
			delete(t.baselines, key)
		}
	}
}
