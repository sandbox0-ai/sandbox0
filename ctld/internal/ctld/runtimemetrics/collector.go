package runtimemetrics

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
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

// PerSandboxStatsClient provides the bounded fallback used when a node-wide
// CRI stats request is poisoned by one unresponsive runtime.
type PerSandboxStatsClient interface {
	ListPodSandboxes(context.Context) ([]*runtimeapi.PodSandbox, error)
	PodSandboxStats(context.Context, string) (*runtimeapi.PodSandboxStats, error)
}

// SampleSink is the bounded asynchronous handoff used by the collector.
type SampleSink interface {
	TryEnqueue(sandboxobservability.RuntimeSample) bool
}

// CollectorConfig configures node identity, cadence, and producer dependencies.
type CollectorConfig struct {
	RegionID                 string
	ClusterID                string
	NodeName                 string
	Interval                 time.Duration
	Jitter                   time.Duration
	BulkRequestTimeout       time.Duration
	FallbackMaxConcurrency   int
	FallbackMaxSandboxes     int
	FallbackRequestTimeout   time.Duration
	FallbackCollectionBudget time.Duration
	Now                      func() time.Time
	Random                   func() float64
	Logger                   *zap.Logger
	StatsClient              StatsClient
	PodLister                corelisters.PodLister
	Sink                     SampleSink
}

// Collector maps CRI pod sandbox stats to sandbox runtime samples.
type Collector struct {
	collectMu                sync.Mutex
	regionID                 string
	clusterID                string
	nodeName                 string
	interval                 time.Duration
	jitter                   time.Duration
	bulkRequestTimeout       time.Duration
	fallbackMaxConcurrency   int
	fallbackMaxSandboxes     int
	fallbackRequestTimeout   time.Duration
	fallbackCollectionBudget time.Duration
	fallbackCursor           int
	now                      func() time.Time
	random                   func() float64
	logger                   *zap.Logger
	statsClient              StatsClient
	podLister                corelisters.PodLister
	sink                     SampleSink
	cpuUsage                 *cpuUsageTracker
}

// CollectResult summarizes one bulk collection attempt.
type CollectResult struct {
	StatsReceived int
	Matched       int
	Enqueued      int
	Dropped       int
	Failed        int
	Fallback      bool
}

const (
	defaultBulkRequestTimeout       = 3 * time.Second
	defaultFallbackMaxConcurrency   = 4
	defaultFallbackMaxSandboxes     = 16
	defaultFallbackRequestTimeout   = 2 * time.Second
	defaultFallbackCollectionBudget = 10 * time.Second
)

type fallbackTarget struct {
	id string
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
	if cfg.BulkRequestTimeout <= 0 {
		cfg.BulkRequestTimeout = defaultBulkRequestTimeout
	}
	if cfg.FallbackMaxConcurrency <= 0 {
		cfg.FallbackMaxConcurrency = defaultFallbackMaxConcurrency
	}
	if cfg.FallbackMaxSandboxes <= 0 {
		cfg.FallbackMaxSandboxes = defaultFallbackMaxSandboxes
	}
	if cfg.FallbackRequestTimeout <= 0 {
		cfg.FallbackRequestTimeout = defaultFallbackRequestTimeout
	}
	if cfg.FallbackCollectionBudget <= 0 {
		cfg.FallbackCollectionBudget = defaultFallbackCollectionBudget
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
		regionID:                 cfg.RegionID,
		clusterID:                cfg.ClusterID,
		nodeName:                 cfg.NodeName,
		interval:                 cfg.Interval,
		jitter:                   cfg.Jitter,
		bulkRequestTimeout:       cfg.BulkRequestTimeout,
		fallbackMaxConcurrency:   cfg.FallbackMaxConcurrency,
		fallbackMaxSandboxes:     cfg.FallbackMaxSandboxes,
		fallbackRequestTimeout:   cfg.FallbackRequestTimeout,
		fallbackCollectionBudget: cfg.FallbackCollectionBudget,
		now:                      cfg.Now,
		random:                   cfg.Random,
		logger:                   cfg.Logger,
		statsClient:              cfg.StatsClient,
		podLister:                cfg.PodLister,
		sink:                     cfg.Sink,
		cpuUsage:                 &cpuUsageTracker{},
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
			c.logger.Warn("Failed to collect some sandbox runtime metrics",
				zap.Error(err),
				zap.Bool("fallback", result.Fallback),
				zap.Int("matched", result.Matched),
				zap.Int("enqueued", result.Enqueued),
				zap.Int("failed", result.Failed),
			)
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
	bulkCtx, cancel := context.WithTimeout(ctx, c.bulkRequestTimeout)
	stats, err := c.statsClient.ListPodSandboxStats(bulkCtx)
	cancel()
	if err != nil {
		return c.collectFallback(ctx, identities, fmt.Errorf("list CRI pod sandbox stats: %w", err))
	}
	return c.projectStats(identities, stats, false)
}

func (c *Collector) projectStats(identities identityIndex, stats []*runtimeapi.PodSandboxStats, fallback bool) (CollectResult, error) {
	result := CollectResult{StatsReceived: len(stats)}
	result.Fallback = fallback
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
	if !fallback {
		c.cpuUsage.prune(activeCPUSeries)
	}
	return result, nil
}

func (c *Collector) collectFallback(ctx context.Context, identities identityIndex, bulkErr error) (CollectResult, error) {
	result := CollectResult{Fallback: true}
	client, ok := c.statsClient.(PerSandboxStatsClient)
	if !ok {
		return result, bulkErr
	}
	budgetCtx, cancel := context.WithTimeout(ctx, c.fallbackCollectionBudget)
	defer cancel()
	sandboxes, err := client.ListPodSandboxes(budgetCtx)
	if err != nil {
		return result, errors.Join(bulkErr, fmt.Errorf("list CRI pod sandboxes for fallback: %w", err))
	}
	allTargets := fallbackTargets(identities, sandboxes)
	targets := c.nextFallbackTargets(allTargets)
	result.Matched = len(targets)
	if len(targets) == 0 {
		return result, bulkErr
	}

	type fallbackResult struct {
		stats *runtimeapi.PodSandboxStats
		err   error
	}
	results := make([]fallbackResult, len(targets))
	semaphore := make(chan struct{}, c.fallbackMaxConcurrency)
	var wg sync.WaitGroup
dispatch:
	for i := range targets {
		select {
		case semaphore <- struct{}{}:
		case <-budgetCtx.Done():
			for j := i; j < len(results); j++ {
				results[j].err = budgetCtx.Err()
			}
			break dispatch
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() { <-semaphore }()
			requestCtx, requestCancel := context.WithTimeout(budgetCtx, c.fallbackRequestTimeout)
			results[index].stats, results[index].err = client.PodSandboxStats(requestCtx, targets[index].id)
			requestCancel()
			if results[index].stats == nil && results[index].err == nil {
				results[index].err = fmt.Errorf("empty CRI pod sandbox stats")
			}
		}(i)
	}
	wg.Wait()

	stats := make([]*runtimeapi.PodSandboxStats, 0, len(results))
	var fallbackErr error
	for i := range results {
		if results[i].err != nil {
			result.Failed++
			fallbackErr = errors.Join(fallbackErr, fmt.Errorf("collect CRI pod sandbox %s stats: %w", targets[i].id, results[i].err))
			continue
		}
		stats = append(stats, results[i].stats)
	}
	projected, projectErr := c.projectStats(identities, stats, true)
	result.StatsReceived = projected.StatsReceived
	result.Enqueued = projected.Enqueued
	result.Dropped = projected.Dropped
	return result, errors.Join(bulkErr, fallbackErr, projectErr)
}

func fallbackTargets(identities identityIndex, sandboxes []*runtimeapi.PodSandbox) []fallbackTarget {
	targets := make([]fallbackTarget, 0, len(sandboxes))
	seen := make(map[string]struct{}, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.Id == "" || sandbox.Metadata == nil ||
			sandbox.State != runtimeapi.PodSandboxState_SANDBOX_READY {
			continue
		}
		if _, ok := seen[sandbox.Id]; ok {
			continue
		}
		_, ok := identities.resolve(&runtimeapi.PodSandboxAttributes{Metadata: sandbox.Metadata})
		if !ok {
			continue
		}
		seen[sandbox.Id] = struct{}{}
		targets = append(targets, fallbackTarget{id: sandbox.Id})
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].id < targets[j].id
	})
	return targets
}

func (c *Collector) nextFallbackTargets(targets []fallbackTarget) []fallbackTarget {
	if len(targets) == 0 {
		return nil
	}
	limit := c.fallbackMaxSandboxes
	if limit <= 0 || limit >= len(targets) {
		c.fallbackCursor = 0
		return targets
	}
	start := c.fallbackCursor % len(targets)
	selected := make([]fallbackTarget, 0, limit)
	for i := 0; i < limit; i++ {
		selected = append(selected, targets[(start+i)%len(targets)])
	}
	c.fallbackCursor = (start + limit) % len(targets)
	return selected
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
