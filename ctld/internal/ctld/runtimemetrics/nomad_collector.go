// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtimemetrics

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

// RuntimeMetricClient exposes the root-only Nomad runtime metric RPCs used by
// the unprivileged collector. The Unix socket is the authorization boundary.
type RuntimeMetricClient interface {
	ListRuntimeMetricTargets(context.Context) ([]nomadruntime.RuntimeMetricTarget, error)
	RuntimeMetricStats(context.Context, nomadruntime.RuntimeMetricTarget) (nomadruntime.RuntimeMetricSample, error)
}

// NomadCollectorConfig configures bounded stock-runsc metric collection.
type NomadCollectorConfig struct {
	RegionID         string
	ClusterID        string
	Interval         time.Duration
	Jitter           time.Duration
	MaxConcurrency   int
	RequestTimeout   time.Duration
	CollectionBudget time.Duration
	Random           func() float64
	Logger           *zap.Logger
	Client           RuntimeMetricClient
	Sink             SampleSink
	Observer         *Observer
}

// NomadCollector collects stock-runsc stats through the root-owned node
// runtime without exposing runtime bindings or privileged paths to ctld main.
type NomadCollector struct {
	collectMu        sync.Mutex
	regionID         string
	clusterID        string
	interval         time.Duration
	jitter           time.Duration
	maxConcurrency   int
	requestTimeout   time.Duration
	collectionBudget time.Duration
	targetCursor     int
	random           func() float64
	logger           *zap.Logger
	client           RuntimeMetricClient
	sink             SampleSink
	observer         *Observer
	cpuUsage         *cpuUsageTracker
}

type nomadCollectionTarget struct {
	target nomadruntime.RuntimeMetricTarget
	sample nomadruntime.RuntimeMetricSample
}

// NewNomadCollector creates a bounded runtime metric collector for Nomad and
// gVisor sandboxes.
func NewNomadCollector(cfg NomadCollectorConfig) (*NomadCollector, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("runtime metric client is nil")
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
		cfg.MaxConcurrency = defaultMaxConcurrency
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.CollectionBudget <= 0 {
		cfg.CollectionBudget = defaultCollectionBudget
	}
	if cfg.Random == nil {
		cfg.Random = rand.Float64
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &NomadCollector{
		regionID: cfg.RegionID, clusterID: cfg.ClusterID,
		interval: cfg.Interval, jitter: cfg.Jitter,
		maxConcurrency: cfg.MaxConcurrency, requestTimeout: cfg.RequestTimeout,
		collectionBudget: cfg.CollectionBudget, random: cfg.Random, logger: cfg.Logger,
		client: cfg.Client, sink: cfg.Sink, observer: cfg.Observer,
		cpuUsage: &cpuUsageTracker{},
	}, nil
}

// Run collects immediately, then repeats with bounded jitter until
// cancellation.
func (c *NomadCollector) Run(ctx context.Context) {
	if c == nil {
		return
	}
	for {
		result, err := c.Collect(ctx)
		if err != nil && ctx.Err() == nil {
			c.logger.Warn("Failed to collect some Nomad runtime metrics",
				zap.Error(err),
				zap.Int("matched", result.Matched),
				zap.Int("enqueued", result.Enqueued),
				zap.Int("failed", result.Failed),
			)
		} else if result.Dropped > 0 {
			c.logger.Warn("Dropped Nomad runtime metric samples", zap.Int("dropped", result.Dropped))
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

// Collect lists currently consumer-bound runtime targets, immediately prunes
// retired CPU series, and samples each active target within fixed bounds.
func (c *NomadCollector) Collect(ctx context.Context) (result CollectResult, err error) {
	if c == nil {
		return CollectResult{}, fmt.Errorf("nomad collector is nil")
	}
	started := time.Now()
	defer func() {
		if c.observer != nil {
			c.observer.ObserveCollection(time.Since(started), result, err)
		}
	}()
	c.collectMu.Lock()
	defer c.collectMu.Unlock()

	budgetCtx, cancel := context.WithTimeout(ctx, c.collectionBudget)
	defer cancel()
	listCtx, listCancel := context.WithTimeout(budgetCtx, c.requestTimeout)
	targets, err := c.client.ListRuntimeMetricTargets(listCtx)
	listCancel()
	if err != nil {
		return result, fmt.Errorf("list Nomad runtime metric targets: %w", err)
	}
	targets, err = nomadruntime.NormalizeRuntimeMetricTargets(targets)
	if err != nil {
		return result, fmt.Errorf("validate Nomad runtime metric targets: %w", err)
	}

	if c.cpuUsage == nil {
		c.cpuUsage = &cpuUsageTracker{}
	}
	activeCPUSeries := make(map[cpuSeriesKey]struct{}, len(targets))
	for _, target := range targets {
		activeCPUSeries[cpuSeriesKeyForRuntimeMetricTarget(target)] = struct{}{}
	}
	c.cpuUsage.prune(activeCPUSeries)
	result.Matched = len(targets)
	if len(targets) == 0 {
		return result, nil
	}

	collectionTargets := make([]nomadCollectionTarget, len(targets))
	for index, target := range targets {
		collectionTargets[index].target = target
	}
	collectionTargets, start := rotateCollectionTargets(collectionTargets, c.targetCursor)
	results, dispatched := runBoundedCollection(
		budgetCtx,
		collectionTargets,
		c.maxConcurrency,
		c.requestTimeout,
		func(requestCtx context.Context, item nomadCollectionTarget) (nomadCollectionTarget, error) {
			sample, sampleErr := c.client.RuntimeMetricStats(requestCtx, item.target)
			if sampleErr == nil {
				sampleErr = sample.Validate(item.target)
			}
			item.sample = sample
			return item, sampleErr
		},
	)
	c.targetCursor = nextCollectionTargetCursor(len(collectionTargets), start, dispatched)

	var collectionErr error
	for index := 0; index < dispatched; index++ {
		if results[index].err != nil {
			result.Failed++
			collectionErr = errors.Join(collectionErr, fmt.Errorf(
				"collect nomad runtime metric binding %s: %w",
				collectionTargets[index].target.BindingDigest,
				results[index].err,
			))
			continue
		}
		result.StatsReceived++
		item := results[index].value
		sample, ok := projectGVisorRuntimeSample(
			item.target,
			item.sample.Stats,
			c.regionID,
			c.clusterID,
			item.sample.ObservedAt,
			c.cpuUsage,
		)
		if !ok {
			result.Failed++
			collectionErr = errors.Join(collectionErr, fmt.Errorf(
				"project nomad runtime metric binding %s",
				item.target.BindingDigest,
			))
			continue
		}
		if c.sink.TryEnqueue(sample) {
			result.Enqueued++
		} else {
			result.Dropped++
		}
	}
	if deferred := len(collectionTargets) - dispatched; deferred > 0 {
		collectionErr = errors.Join(collectionErr, fmt.Errorf(
			"nomad runtime metric collection stopped with %d of %d targets deferred: %w",
			deferred,
			len(collectionTargets),
			budgetCtx.Err(),
		))
	}
	return result, collectionErr
}

func (c *NomadCollector) nextDelay() time.Duration {
	if c == nil {
		return 0
	}
	return boundedJitterDelay(c.interval, c.jitter, c.random)
}

func cpuSeriesKeyForRuntimeMetricTarget(target nomadruntime.RuntimeMetricTarget) cpuSeriesKey {
	return cpuSeriesKey{
		teamID: target.TeamID, sandboxID: target.SandboxID,
		runtimeGeneration: target.RuntimeGeneration, seriesEpoch: target.SeriesEpoch,
	}
}
