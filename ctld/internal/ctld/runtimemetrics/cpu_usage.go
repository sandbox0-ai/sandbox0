package runtimemetrics

import (
	"strings"
	"sync"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

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

// cpuUsageTracker derives instantaneous core usage from CRI cumulative CPU
// time while keeping reset boundaries isolated per runtime series.
type cpuUsageTracker struct {
	mu        sync.Mutex
	baselines map[cpuSeriesKey]cpuUsageBaseline
}

func cpuSeriesKeyFor(identity sandboxIdentity, attributes *runtimeapi.PodSandboxAttributes) (cpuSeriesKey, bool) {
	key := cpuSeriesKey{
		teamID:            strings.TrimSpace(identity.TeamID),
		sandboxID:         strings.TrimSpace(identity.SandboxID),
		runtimeGeneration: identity.RuntimeGeneration,
		seriesEpoch:       runtimeSeriesEpoch(identity, attributes),
	}
	return key, key.teamID != "" && key.sandboxID != "" && key.seriesEpoch != ""
}

func (t *cpuUsageTracker) observe(key cpuSeriesKey, usage *runtimeapi.CpuUsage) *float64 {
	if t == nil || usage == nil || usage.Timestamp <= 0 || usage.UsageCoreNanoSeconds == nil {
		return nil
	}

	current := cpuUsageBaseline{
		timestamp:            usage.Timestamp,
		usageCoreNanoSeconds: usage.UsageCoreNanoSeconds.Value,
	}
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
	deltaCPU := current.usageCoreNanoSeconds - previous.usageCoreNanoSeconds
	deltaTime := current.timestamp - previous.timestamp
	cores := float64(deltaCPU) / float64(deltaTime)
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

func (t *cpuUsageTracker) size() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.baselines)
}
