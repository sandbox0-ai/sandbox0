package runtimemetrics

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// StatsClient exposes CRI discovery and one-sandbox stats calls.
type StatsClient interface {
	ListPodSandboxes(context.Context) ([]*runtimeapi.PodSandbox, error)
	PodSandboxStats(context.Context, string) (*runtimeapi.PodSandboxStats, error)
}

// SampleSink is the bounded asynchronous handoff used by the collector.
type SampleSink interface {
	TryEnqueue(sandboxobservability.RuntimeSample) bool
}

// CollectorConfig configures node identity, cadence, and producer dependencies.
type CollectorConfig struct {
	RegionID       string
	ClusterID      string
	NodeName       string
	Interval       time.Duration
	Jitter         time.Duration
	MaxConcurrency int
	Now            func() time.Time
	Random         func() float64
	Logger         *zap.Logger
	StatsClient    StatsClient
	PodLister      corelisters.PodLister
	Sink           SampleSink
}

// Collector maps CRI pod sandbox stats to sandbox runtime samples.
type Collector struct {
	collectMu      sync.Mutex
	regionID       string
	clusterID      string
	nodeName       string
	interval       time.Duration
	jitter         time.Duration
	maxConcurrency int
	now            func() time.Time
	random         func() float64
	logger         *zap.Logger
	statsClient    StatsClient
	podLister      corelisters.PodLister
	sink           SampleSink
	cpuUsage       *cpuUsageTracker
}

// CollectResult summarizes one node-local collection attempt.
type CollectResult struct {
	StatsReceived int
	Matched       int
	Enqueued      int
	Dropped       int
	Failed        int
}

type sandboxTarget struct {
	id       string
	identity sandboxIdentity
}

// NewCollector creates a node-local paced CRI runtime metric collector.
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
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = sandboxobservability.DefaultRuntimeSampleMaxConcurrency
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
		regionID:       cfg.RegionID,
		clusterID:      cfg.ClusterID,
		nodeName:       cfg.NodeName,
		interval:       cfg.Interval,
		jitter:         cfg.Jitter,
		maxConcurrency: cfg.MaxConcurrency,
		now:            cfg.Now,
		random:         cfg.Random,
		logger:         cfg.Logger,
		statsClient:    cfg.StatsClient,
		podLister:      cfg.PodLister,
		sink:           cfg.Sink,
		cpuUsage:       &cpuUsageTracker{},
	}, nil
}

// Run continuously spreads per-sandbox CRI calls across each sampling window.
func (c *Collector) Run(ctx context.Context) {
	if c == nil {
		return
	}
	for {
		cycleInterval := c.nextDelay()
		startedAt := time.Now()
		result, err := c.collect(ctx, cycleInterval)
		elapsed := time.Since(startedAt)
		if err != nil && ctx.Err() == nil {
			c.logger.Warn("Failed to collect some sandbox runtime metrics",
				zap.Error(err),
				zap.Int("matched", result.Matched),
				zap.Int("received", result.StatsReceived),
				zap.Int("failed", result.Failed),
			)
		} else if result.Dropped > 0 {
			c.logger.Warn("Dropped sandbox runtime metric samples", zap.Int("dropped", result.Dropped))
		}
		if ctx.Err() != nil {
			return
		}
		if elapsed > cycleInterval {
			c.logger.Warn("Sandbox runtime metric collection exceeded its sampling window",
				zap.Duration("duration", elapsed),
				zap.Duration("sample_interval", cycleInterval),
				zap.Int("matched", result.Matched),
				zap.Int("max_concurrency", c.maxConcurrency),
			)
		} else {
			c.logger.Debug("Collected sandbox runtime metrics",
				zap.Duration("duration", elapsed),
				zap.Int("matched", result.Matched),
				zap.Int("enqueued", result.Enqueued),
				zap.Int("failed", result.Failed),
			)
		}

		if !waitFor(ctx, cycleInterval-elapsed) {
			return
		}
	}
}

// Collect immediately collects all current sandbox samples with bounded concurrency.
func (c *Collector) Collect(ctx context.Context) (CollectResult, error) {
	return c.collect(ctx, 0)
}

func (c *Collector) collect(ctx context.Context, spreadWindow time.Duration) (CollectResult, error) {
	if c == nil {
		return CollectResult{}, fmt.Errorf("collector is nil")
	}
	c.collectMu.Lock()
	defer c.collectMu.Unlock()
	identities, err := buildIdentityIndex(c.podLister, c.nodeName)
	if err != nil {
		return CollectResult{}, err
	}
	sandboxes, err := c.statsClient.ListPodSandboxes(ctx)
	if err != nil {
		return CollectResult{}, fmt.Errorf("list CRI pod sandboxes: %w", err)
	}
	targets := matchedSandboxTargets(identities, sandboxes)
	result := CollectResult{Matched: len(targets)}
	activeCPUSeries := make(map[cpuSeriesKey]struct{})
	if c.cpuUsage == nil {
		c.cpuUsage = &cpuUsageTracker{}
	}
	for _, target := range targets {
		attributes := &runtimeapi.PodSandboxAttributes{Id: target.id}
		if key, valid := cpuSeriesKeyFor(target.identity, attributes); valid {
			activeCPUSeries[key] = struct{}{}
		}
	}

	var resultMu sync.Mutex
	var firstErr error
	semaphore := make(chan struct{}, c.maxConcurrency)
	var wg sync.WaitGroup
	startedAt := time.Now()
dispatch:
	for index, target := range targets {
		if spreadWindow > 0 && !waitUntil(ctx, startedAt.Add(cycleOffset(index, len(targets), spreadWindow))) {
			break
		}
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			break dispatch
		}
		wg.Add(1)
		go func(target sandboxTarget) {
			defer wg.Done()
			defer func() { <-semaphore }()
			statsCtx, cancel := context.WithTimeout(ctx, c.interval)
			stats, statsErr := c.statsClient.PodSandboxStats(statsCtx, target.id)
			cancel()
			if statsErr != nil {
				resultMu.Lock()
				result.Failed++
				if firstErr == nil {
					firstErr = fmt.Errorf("collect CRI pod sandbox %s stats: %w", target.id, statsErr)
				}
				resultMu.Unlock()
				return
			}
			if stats == nil {
				resultMu.Lock()
				result.Failed++
				if firstErr == nil {
					firstErr = fmt.Errorf("collect CRI pod sandbox %s stats: empty response", target.id)
				}
				resultMu.Unlock()
				return
			}

			var derivedCPUUsage *float64
			if key, valid := cpuSeriesKeyFor(target.identity, stats.Attributes); valid && stats.Linux != nil {
				derivedCPUUsage = c.cpuUsage.observe(key, stats.Linux.Cpu)
			}
			sample, ok := projectRuntimeSample(target.identity, stats, c.regionID, c.clusterID, c.now().UTC(), derivedCPUUsage)
			resultMu.Lock()
			defer resultMu.Unlock()
			result.StatsReceived++
			if !ok {
				result.Failed++
				if firstErr == nil {
					firstErr = fmt.Errorf("project CRI pod sandbox %s stats: missing runtime identity", target.id)
				}
				return
			}
			if c.sink.TryEnqueue(sample) {
				result.Enqueued++
			} else {
				result.Dropped++
			}
		}(target)
	}
	wg.Wait()
	c.cpuUsage.prune(activeCPUSeries)
	if firstErr != nil {
		return result, fmt.Errorf("%d sandbox runtime metric collection(s) failed; first error: %w", result.Failed, firstErr)
	}
	return result, ctx.Err()
}

func matchedSandboxTargets(identities identityIndex, sandboxes []*runtimeapi.PodSandbox) []sandboxTarget {
	targets := make([]sandboxTarget, 0, len(sandboxes))
	seen := make(map[string]struct{}, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.Id == "" || sandbox.Metadata == nil {
			continue
		}
		if _, ok := seen[sandbox.Id]; ok {
			continue
		}
		identity, ok := identities.resolve(&runtimeapi.PodSandboxAttributes{Metadata: sandbox.Metadata})
		if !ok {
			continue
		}
		seen[sandbox.Id] = struct{}{}
		targets = append(targets, sandboxTarget{id: sandbox.Id, identity: identity})
	}
	sort.Slice(targets, func(i, j int) bool {
		left, right := targetOrder(targets[i].id), targetOrder(targets[j].id)
		if left == right {
			return targets[i].id < targets[j].id
		}
		return left < right
	})
	return targets
}

func targetOrder(id string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(id))
	return hash.Sum64()
}

func cycleOffset(index, total int, interval time.Duration) time.Duration {
	if index <= 0 || total <= 0 || interval <= 0 {
		return 0
	}
	return time.Duration(index) * interval / time.Duration(total)
}

func waitUntil(ctx context.Context, deadline time.Time) bool {
	return waitFor(ctx, time.Until(deadline))
}

func waitFor(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
