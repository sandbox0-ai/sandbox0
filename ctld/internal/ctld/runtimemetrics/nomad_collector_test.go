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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNomadCollectorDerivesCPUUsageFromCumulativeRunscStats(t *testing.T) {
	target := nomadMetricTarget('a')
	client := newFakeRuntimeMetricClient(target)
	client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), uint64(time.Second))
	sink := &recordingSampleSink{}
	collector, err := NewNomadCollector(NomadCollectorConfig{
		RegionID: "region-a", ClusterID: "cluster-a", Client: client, Sink: sink,
	})
	require.NoError(t, err)

	first, err := collector.Collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, CollectResult{StatsReceived: 1, Matched: 1, Enqueued: 1}, first)
	require.Len(t, sink.samples, 1)
	assert.Nil(t, sink.samples[0].CPU.Usage)

	client.setSample(target, nomadMetricSample(target, time.Unix(101, 0), uint64(1500*time.Millisecond)))
	second, err := collector.Collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, CollectResult{StatsReceived: 1, Matched: 1, Enqueued: 1}, second)
	require.Len(t, sink.samples, 2)
	require.NotNil(t, sink.samples[1].CPU.Usage)
	require.NotNil(t, sink.samples[1].CPU.Utilization)
	assert.InDelta(t, 0.5, *sink.samples[1].CPU.Usage, 0.0001)
	assert.InDelta(t, 0.25, *sink.samples[1].CPU.Utilization, 0.0001)
}

func TestNomadCollectorPrunesRemovedCPUBaselineBeforeActiveStatsFailure(t *testing.T) {
	removed := nomadMetricTarget('a')
	active := nomadMetricTarget('b')
	client := newFakeRuntimeMetricClient(removed, active)
	client.samples[removed.BindingDigest] = nomadMetricSample(removed, time.Unix(100, 0), 100)
	client.samples[active.BindingDigest] = nomadMetricSample(active, time.Unix(100, 0), 100)
	collector, err := NewNomadCollector(NomadCollectorConfig{Client: client, Sink: &recordingSampleSink{}})
	require.NoError(t, err)
	_, err = collector.Collect(t.Context())
	require.NoError(t, err)
	require.Len(t, collector.cpuUsage.baselines, 2)

	client.setTargets(active)
	client.setStatsError(active, errors.New("runsc unavailable"))
	result, err := collector.Collect(t.Context())
	require.ErrorContains(t, err, "runsc unavailable")
	assert.Equal(t, CollectResult{Matched: 1, Failed: 1}, result)
	require.Len(t, collector.cpuUsage.baselines, 1)
	_, found := collector.cpuUsage.baselines[cpuSeriesKeyForRuntimeMetricTarget(active)]
	assert.True(t, found, "transient stats failure must retain the active series baseline")
}

func TestNomadCollectorDoesNotPruneCPUBaselineAfterTargetListFailure(t *testing.T) {
	target := nomadMetricTarget('a')
	client := newFakeRuntimeMetricClient(target)
	client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), 100)
	collector, err := NewNomadCollector(NomadCollectorConfig{Client: client, Sink: &recordingSampleSink{}})
	require.NoError(t, err)
	_, err = collector.Collect(t.Context())
	require.NoError(t, err)
	require.Len(t, collector.cpuUsage.baselines, 1)

	client.setListError(errors.New("node runtime unavailable"))
	_, err = collector.Collect(t.Context())
	require.ErrorContains(t, err, "node runtime unavailable")
	assert.Len(t, collector.cpuUsage.baselines, 1)
}

func TestNomadCollectorIsolatesInvalidPerTargetSamples(t *testing.T) {
	invalid := nomadMetricTarget('a')
	valid := nomadMetricTarget('b')
	client := newFakeRuntimeMetricClient(invalid, valid)
	invalidSample := nomadMetricSample(invalid, time.Unix(100, 0), 100)
	invalidSample.Stats.ID = "another-runsc-container"
	client.samples[invalid.BindingDigest] = invalidSample
	client.samples[valid.BindingDigest] = nomadMetricSample(valid, time.Unix(100, 0), 100)
	sink := &recordingSampleSink{}
	collector, err := NewNomadCollector(NomadCollectorConfig{Client: client, Sink: sink})
	require.NoError(t, err)

	result, err := collector.Collect(t.Context())
	require.ErrorContains(t, err, "does not match")
	assert.Equal(t, CollectResult{StatsReceived: 1, Matched: 2, Enqueued: 1, Failed: 1}, result)
	require.Len(t, sink.samples, 1)
	assert.Equal(t, valid.SandboxID, sink.samples[0].SandboxID)
}

func TestNomadCollectorBoundsPerTargetTimeout(t *testing.T) {
	target := nomadMetricTarget('a')
	client := newFakeRuntimeMetricClient(target)
	client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), 100)
	client.block = make(chan struct{})
	collector, err := NewNomadCollector(NomadCollectorConfig{
		Client: client, Sink: &recordingSampleSink{}, RequestTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	result, err := collector.Collect(t.Context())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, CollectResult{Matched: 1, Failed: 1}, result)
}

func TestNomadCollectorBoundsTargetListTimeoutWithoutPruning(t *testing.T) {
	target := nomadMetricTarget('a')
	client := newFakeRuntimeMetricClient(target)
	client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), 100)
	collector, err := NewNomadCollector(NomadCollectorConfig{
		Client: client, Sink: &recordingSampleSink{}, RequestTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	_, err = collector.Collect(t.Context())
	require.NoError(t, err)
	require.Len(t, collector.cpuUsage.baselines, 1)

	client.listBlock = make(chan struct{})
	_, err = collector.Collect(t.Context())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, collector.cpuUsage.baselines, 1)
}

func TestNomadCollectorBoundsStatsConcurrency(t *testing.T) {
	targets := nomadMetricTargets(6)
	client := newFakeRuntimeMetricClient(targets...)
	for _, target := range targets {
		client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), 100)
	}
	client.block = make(chan struct{})
	collector, err := NewNomadCollector(NomadCollectorConfig{
		Client: client, Sink: &recordingSampleSink{}, MaxConcurrency: 2, RequestTimeout: time.Second,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, collectErr := collector.Collect(t.Context())
		done <- collectErr
	}()
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.active == 2
	}, time.Second, time.Millisecond)
	client.mu.Lock()
	assert.LessOrEqual(t, client.maxActive, 2)
	client.mu.Unlock()
	close(client.block)
	require.NoError(t, <-done)
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.LessOrEqual(t, client.maxActive, 2)
}

func TestNomadCollectorRejectsAmbiguousTargetsBeforeStats(t *testing.T) {
	valid := nomadMetricTarget('a')
	duplicate := nomadMetricTarget('b')
	duplicate.BindingDigest = valid.BindingDigest
	invalid := valid
	invalid.RuntimeGeneration = 0
	overLimit := make([]nomadruntime.RuntimeMetricTarget, nomadruntime.RuntimeMetricMaxTargets+1)
	for index := range overLimit {
		overLimit[index] = valid
	}

	for _, test := range []struct {
		name    string
		targets []nomadruntime.RuntimeMetricTarget
		want    string
	}{
		{name: "invalid", targets: []nomadruntime.RuntimeMetricTarget{invalid}, want: "runtime_generation"},
		{name: "duplicate", targets: []nomadruntime.RuntimeMetricTarget{valid, duplicate}, want: "duplicated"},
		{name: "over limit", targets: overLimit, want: "exceeds"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeRuntimeMetricClient(test.targets...)
			collector, err := NewNomadCollector(NomadCollectorConfig{Client: client, Sink: &recordingSampleSink{}})
			require.NoError(t, err)
			_, err = collector.Collect(t.Context())
			require.ErrorContains(t, err, test.want)
			client.mu.Lock()
			defer client.mu.Unlock()
			assert.Empty(t, client.statsCalls)
		})
	}
}

func TestNomadCollectorRotatesAfterCollectionBudgetExhaustion(t *testing.T) {
	targets := nomadMetricTargets(6)
	client := newFakeRuntimeMetricClient(targets...)
	client.block = make(chan struct{})
	collector, err := NewNomadCollector(NomadCollectorConfig{
		Client: client, Sink: &recordingSampleSink{}, MaxConcurrency: 2,
		RequestTimeout: time.Second, CollectionBudget: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	first, firstErr := collector.Collect(t.Context())
	second, secondErr := collector.Collect(t.Context())
	require.ErrorIs(t, firstErr, context.DeadlineExceeded)
	require.ErrorIs(t, secondErr, context.DeadlineExceeded)
	assert.Equal(t, CollectResult{Matched: 6, Failed: 2}, first)
	assert.Equal(t, CollectResult{Matched: 6, Failed: 2}, second)
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.statsCalls, 4)
	assert.ElementsMatch(t, []string{targets[0].BindingDigest, targets[1].BindingDigest}, client.statsCalls[:2])
	assert.ElementsMatch(t, []string{targets[2].BindingDigest, targets[3].BindingDigest}, client.statsCalls[2:])
}

func TestNomadCollectorCountsFullQueueDrops(t *testing.T) {
	target := nomadMetricTarget('a')
	client := newFakeRuntimeMetricClient(target)
	client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), 100)
	sink := &recordingSampleSink{accept: func(sandboxobservability.RuntimeSample) bool { return false }}
	collector, err := NewNomadCollector(NomadCollectorConfig{Client: client, Sink: sink})
	require.NoError(t, err)

	result, err := collector.Collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, CollectResult{StatsReceived: 1, Matched: 1, Dropped: 1}, result)
}

func TestNomadCollectorRunCollectsImmediately(t *testing.T) {
	target := nomadMetricTarget('a')
	client := newFakeRuntimeMetricClient(target)
	client.samples[target.BindingDigest] = nomadMetricSample(target, time.Unix(100, 0), 100)
	client.onList = make(chan struct{}, 1)
	collector, err := NewNomadCollector(NomadCollectorConfig{
		Client: client, Sink: &recordingSampleSink{}, Interval: time.Hour,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		collector.Run(ctx)
		close(done)
	}()
	select {
	case <-client.onList:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("Nomad collector did not collect immediately")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Nomad collector did not stop after cancellation")
	}
}

func TestNomadCollectorUsesSharedDefaultsAndBoundedJitter(t *testing.T) {
	collector, err := NewNomadCollector(NomadCollectorConfig{
		Client: newFakeRuntimeMetricClient(), Sink: &recordingSampleSink{}, Random: func() float64 { return 1 },
	})
	require.NoError(t, err)
	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleInterval, collector.interval)
	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleJitter, collector.jitter)
	assert.Equal(t, defaultMaxConcurrency, collector.maxConcurrency)
	assert.Equal(t, defaultRequestTimeout, collector.requestTimeout)
	assert.Equal(t, defaultCollectionBudget, collector.collectionBudget)
	assert.Equal(t, collector.interval+collector.jitter, collector.nextDelay())
}

type fakeRuntimeMetricClient struct {
	mu          sync.Mutex
	targets     []nomadruntime.RuntimeMetricTarget
	listErr     error
	listBlock   chan struct{}
	onList      chan struct{}
	samples     map[string]nomadruntime.RuntimeMetricSample
	statsErrors map[string]error
	statsCalls  []string
	block       chan struct{}
	active      int
	maxActive   int
}

type recordingSampleSink struct {
	mu      sync.Mutex
	samples []sandboxobservability.RuntimeSample
	accept  func(sandboxobservability.RuntimeSample) bool
}

func (s *recordingSampleSink) TryEnqueue(sample sandboxobservability.RuntimeSample) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accept != nil && !s.accept(sample) {
		return false
	}
	s.samples = append(s.samples, sample)
	return true
}

func newFakeRuntimeMetricClient(targets ...nomadruntime.RuntimeMetricTarget) *fakeRuntimeMetricClient {
	return &fakeRuntimeMetricClient{
		targets:     append([]nomadruntime.RuntimeMetricTarget(nil), targets...),
		samples:     make(map[string]nomadruntime.RuntimeMetricSample),
		statsErrors: make(map[string]error),
	}
}

func (c *fakeRuntimeMetricClient) ListRuntimeMetricTargets(ctx context.Context) ([]nomadruntime.RuntimeMetricTarget, error) {
	c.mu.Lock()
	targets := append([]nomadruntime.RuntimeMetricTarget(nil), c.targets...)
	err := c.listErr
	onList := c.onList
	block := c.listBlock
	c.mu.Unlock()
	if onList != nil {
		select {
		case onList <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	return targets, err
}

func (c *fakeRuntimeMetricClient) RuntimeMetricStats(
	ctx context.Context,
	target nomadruntime.RuntimeMetricTarget,
) (nomadruntime.RuntimeMetricSample, error) {
	c.mu.Lock()
	c.statsCalls = append(c.statsCalls, target.BindingDigest)
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	block := c.block
	sample := c.samples[target.BindingDigest]
	err := c.statsErrors[target.BindingDigest]
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()
	if block != nil {
		select {
		case <-ctx.Done():
			return nomadruntime.RuntimeMetricSample{}, ctx.Err()
		case <-block:
		}
	}
	return sample, err
}

func (c *fakeRuntimeMetricClient) setTargets(targets ...nomadruntime.RuntimeMetricTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets = append([]nomadruntime.RuntimeMetricTarget(nil), targets...)
}

func (c *fakeRuntimeMetricClient) setSample(
	target nomadruntime.RuntimeMetricTarget,
	sample nomadruntime.RuntimeMetricSample,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples[target.BindingDigest] = sample
}

func (c *fakeRuntimeMetricClient) setStatsError(target nomadruntime.RuntimeMetricTarget, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statsErrors[target.BindingDigest] = err
}

func (c *fakeRuntimeMetricClient) setListError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listErr = err
}

func nomadMetricTargets(count int) []nomadruntime.RuntimeMetricTarget {
	targets := make([]nomadruntime.RuntimeMetricTarget, count)
	for index := range targets {
		targets[index] = nomadMetricTarget(rune('a' + index))
	}
	return targets
}

func nomadMetricTarget(suffix rune) nomadruntime.RuntimeMetricTarget {
	target := nomadruntime.RuntimeMetricTarget{
		Version: nomadruntime.RuntimeMetricTargetVersion,
		TeamID:  "team-a", SandboxID: fmt.Sprintf("sandbox-%c", suffix), RuntimeGeneration: 7,
		CPUMillicpu: 2000, MemoryMiB: 4096,
		AllocationID: fmt.Sprintf("allocation-%c", suffix), NodeBootID: "boot-a",
		LaunchAttempt: fmt.Sprintf("launch-%c", suffix), RunscContainerID: fmt.Sprintf("runsc-%c", suffix),
		BindingDigest: strings.Repeat(string(suffix), 64),
	}
	target.SeriesEpoch = nomadruntime.RuntimeMetricSeriesEpoch(
		target.AllocationID, target.NodeBootID, target.LaunchAttempt, target.RunscContainerID,
	)
	return target
}

func nomadMetricSample(
	target nomadruntime.RuntimeMetricTarget,
	observedAt time.Time,
	cumulativeCPUTime uint64,
) nomadruntime.RuntimeMetricSample {
	stats := validGVisorStats(target.RunscContainerID)
	stats.Data.CPU.Usage.Total = cumulativeCPUTime
	return nomadruntime.RuntimeMetricSample{
		Version:    nomadruntime.RuntimeMetricSampleVersion,
		ObservedAt: observedAt,
		Stats:      stats,
	}
}
