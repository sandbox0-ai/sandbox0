package runtimemetrics

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// StatsClient exposes the CRI node-wide pod sandbox stats snapshot.
type StatsClient interface {
	ListPodSandboxStats(context.Context) ([]*runtimeapi.PodSandboxStats, error)
}

// SampleSink is the bounded asynchronous handoff used by the collector.
type SampleSink interface {
	TryEnqueue(sandboxobservability.RuntimeSample) bool
}

// CollectorConfig configures node identity, cadence, and producer dependencies.
type CollectorConfig struct {
	RegionID    string
	ClusterID   string
	NodeName    string
	Interval    time.Duration
	Jitter      time.Duration
	Now         func() time.Time
	Random      func() float64
	Logger      *zap.Logger
	StatsClient StatsClient
	PodLister   corelisters.PodLister
	Sink        SampleSink
}

// Collector maps CRI pod sandbox stats to sandbox runtime samples.
type Collector struct {
	collectMu   sync.Mutex
	regionID    string
	clusterID   string
	nodeName    string
	interval    time.Duration
	jitter      time.Duration
	now         func() time.Time
	random      func() float64
	logger      *zap.Logger
	statsClient StatsClient
	podLister   corelisters.PodLister
	sink        SampleSink
	cpuUsage    *cpuUsageTracker
}

// CollectResult summarizes one bulk collection attempt.
type CollectResult struct {
	StatsReceived int
	Matched       int
	Enqueued      int
	Dropped       int
}

// NewCollector creates a node-local bulk CRI runtime metric collector.
func NewCollector(cfg CollectorConfig) (*Collector, error) {
	if cfg.StatsClient == nil {
		return nil, fmt.Errorf("stats client is nil")
	}
	if cfg.PodLister == nil {
		return nil, fmt.Errorf("pod lister is nil")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("sample sink is nil")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = sandboxobservability.DefaultRuntimeSampleInterval
	}
	if cfg.Jitter < 0 {
		return nil, fmt.Errorf("sample jitter must be non-negative")
	}
	if cfg.Jitter == 0 {
		cfg.Jitter = sandboxobservability.DefaultRuntimeSampleJitter
	}
	if cfg.Jitter >= cfg.Interval {
		cfg.Jitter = cfg.Interval / 2
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Random == nil {
		cfg.Random = rand.Float64
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &Collector{
		regionID:    cfg.RegionID,
		clusterID:   cfg.ClusterID,
		nodeName:    cfg.NodeName,
		interval:    cfg.Interval,
		jitter:      cfg.Jitter,
		now:         cfg.Now,
		random:      cfg.Random,
		logger:      cfg.Logger,
		statsClient: cfg.StatsClient,
		podLister:   cfg.PodLister,
		sink:        cfg.Sink,
		cpuUsage:    &cpuUsageTracker{},
	}, nil
}

// Run collects immediately, then repeats with bounded jitter until cancellation.
func (c *Collector) Run(ctx context.Context) {
	if c == nil {
		return
	}
	for {
		result, err := c.Collect(ctx)
		if err != nil && ctx.Err() == nil {
			c.logger.Warn("Failed to collect sandbox runtime metrics", zap.Error(err))
		} else if result.Dropped > 0 {
			c.logger.Warn("Dropped sandbox runtime metric samples", zap.Int("dropped", result.Dropped))
		}

		timer := time.NewTimer(c.nextDelay())
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// Collect projects one bulk CRI snapshot against the current node pod cache.
func (c *Collector) Collect(ctx context.Context) (CollectResult, error) {
	if c == nil {
		return CollectResult{}, fmt.Errorf("collector is nil")
	}
	c.collectMu.Lock()
	defer c.collectMu.Unlock()
	identities, err := buildIdentityIndex(c.podLister, c.nodeName)
	if err != nil {
		return CollectResult{}, err
	}
	stats, err := c.statsClient.ListPodSandboxStats(ctx)
	if err != nil {
		return CollectResult{}, fmt.Errorf("list CRI pod sandbox stats: %w", err)
	}
	result := CollectResult{StatsReceived: len(stats)}
	collectedAt := c.now().UTC()
	activeCPUSeries := make(map[cpuSeriesKey]struct{})
	if c.cpuUsage == nil {
		c.cpuUsage = &cpuUsageTracker{}
	}
	for _, item := range stats {
		if item == nil {
			continue
		}
		identity, ok := identities.resolve(item.Attributes)
		if !ok {
			continue
		}
		var derivedCPUUsage *float64
		if key, valid := cpuSeriesKeyFor(identity, item.Attributes); valid {
			activeCPUSeries[key] = struct{}{}
			if item.Linux != nil {
				derivedCPUUsage = c.cpuUsage.observe(key, item.Linux.Cpu)
			}
		}
		sample, ok := projectRuntimeSample(identity, item, c.regionID, c.clusterID, collectedAt, derivedCPUUsage)
		if !ok {
			continue
		}
		result.Matched++
		if c.sink.TryEnqueue(sample) {
			result.Enqueued++
		} else {
			result.Dropped++
		}
	}
	c.cpuUsage.prune(activeCPUSeries)
	return result, nil
}

func (c *Collector) nextDelay() time.Duration {
	if c == nil {
		return 0
	}
	if c.jitter <= 0 {
		return c.interval
	}
	random := c.random()
	if random < 0 {
		random = 0
	}
	if random > 1 {
		random = 1
	}
	offset := time.Duration((random*2 - 1) * float64(c.jitter))
	return c.interval + offset
}
