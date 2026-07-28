package runtimemetrics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestCollectorUsesOneBulkCallAndEnqueuesMatchedSandboxes(t *testing.T) {
	podA := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "2")
	podB := runtimeMetricPod("ns-a", "pod-b", "pod-uid-b", "node-a", "team-a", "sandbox-b", "3")
	client := &fakeStatsClient{stats: []*runtimeapi.PodSandboxStats{
		minimalPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a"),
		minimalPodStats("cri-b", "ns-a", "pod-b", "pod-uid-b"),
		minimalPodStats("cri-other", "ns-a", "other", "pod-uid-other"),
	}}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		RegionID: "region-a", ClusterID: "cluster-a", NodeName: "node-a",
		StatsClient: client, PodLister: podLister(t, podA, podB), Sink: sink,
		Now: func() time.Time { return time.Unix(100, 0).UTC() }, Random: func() float64 { return 0.5 },
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, CollectResult{StatsReceived: 3, Matched: 2, Enqueued: 2}, result)
	require.Len(t, sink.samples, 2)
	assert.Equal(t, "sandbox-a", sink.samples[0].SandboxID)
	assert.Equal(t, "sandbox-b", sink.samples[1].SandboxID)
}

func TestCollectorDerivesCPUUsageFromLinuxCumulativeStats(t *testing.T) {
	pod := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "2")
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"),
	}
	client := &fakeStatsClient{stats: []*runtimeapi.PodSandboxStats{
		cpuOnlyPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a", 10_000_000_000, 10_000_000_000),
	}}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		RegionID: "region-a", ClusterID: "cluster-a", NodeName: "node-a",
		StatsClient: client, PodLister: podLister(t, pod), Sink: sink,
	})
	require.NoError(t, err)

	_, err = collector.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, sink.samples, 1)
	assert.Nil(t, sink.samples[0].CPU.Usage)
	assertMissing(t, sink.samples[0].Missing, sandboxobservability.RuntimeMetricCPUUsage, nil)

	client.setStats([]*runtimeapi.PodSandboxStats{
		cpuOnlyPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a", 20_000_000_000, 15_000_000_000),
	})
	_, err = collector.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, sink.samples, 2)
	second := sink.samples[1]
	require.NotNil(t, second.CPU.Usage)
	require.NotNil(t, second.CPU.Utilization)
	assert.InDelta(t, 0.5, *second.CPU.Usage, 0.0001)
	assert.InDelta(t, 0.25, *second.CPU.Utilization, 0.0001)
	assertNotMissing(t, second.Missing, sandboxobservability.RuntimeMetricCPUUsage, nil)
	assertNotMissing(t, second.Missing, sandboxobservability.RuntimeMetricCPUUtilization, nil)
}

func TestCollectorReportsCRIErrorWithoutEnqueuing(t *testing.T) {
	client := &fakeStatsClient{err: errors.New("containerd unavailable")}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t, runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")),
		Sink:        sink,
		NodeName:    "node-a",
	})
	require.NoError(t, err)

	_, err = collector.Collect(context.Background())
	require.ErrorContains(t, err, "list CRI pod sandbox stats")
	assert.Empty(t, sink.samples)
}

func TestCollectorFallsBackToIsolatedStatsAfterBulkFailure(t *testing.T) {
	podA := runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")
	podB := runtimeMetricPod("ns-a", "pod-b", "uid-b", "node-a", "team-a", "sandbox-b", "1")
	client := &fakeStatsClient{
		err: errors.New("one runtime poisoned bulk stats"),
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-a", "ns-a", "pod-a", "uid-a"),
			podSandbox("cri-b", "ns-a", "pod-b", "uid-b"),
		},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-b": minimalPodStats("cri-b", "ns-a", "pod-b", "uid-b"),
		},
		statsErrByID: map[string]error{"cri-a": errors.New("runtime unavailable")},
	}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t, podA, podB),
		Sink:        sink,
		NodeName:    "node-a",
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())
	require.ErrorContains(t, err, "one runtime poisoned bulk stats")
	require.ErrorContains(t, err, "runtime unavailable")
	assert.Equal(t, CollectResult{
		StatsReceived: 1,
		Matched:       2,
		Enqueued:      1,
		Failed:        1,
		Fallback:      true,
	}, result)
	require.Len(t, sink.samples, 1)
	assert.Equal(t, "sandbox-b", sink.samples[0].SandboxID)
	assert.ElementsMatch(t, []string{"cri-a", "cri-b"}, client.perSandboxCalls)
}

func TestCollectorFallsBackWhenBulkStatsTimesOut(t *testing.T) {
	pod := runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")
	client := &fakeStatsClient{
		bulkBlock: make(chan struct{}),
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-a", "ns-a", "pod-a", "uid-a"),
		},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-a": minimalPodStats("cri-a", "ns-a", "pod-a", "uid-a"),
		},
	}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient:        client,
		PodLister:          podLister(t, pod),
		Sink:               sink,
		NodeName:           "node-a",
		BulkRequestTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, result.Fallback)
	assert.Equal(t, 1, result.Enqueued)
	require.Len(t, sink.samples, 1)
	assert.Equal(t, "sandbox-a", sink.samples[0].SandboxID)
}

func TestCollectorBoundsFallbackConcurrency(t *testing.T) {
	const sandboxCount = 6
	pods := make([]*corev1.Pod, 0, sandboxCount)
	client := &fakeStatsClient{
		err:       errors.New("bulk unavailable"),
		statsByID: make(map[string]*runtimeapi.PodSandboxStats, sandboxCount),
		block:     make(chan struct{}),
	}
	for i := 0; i < sandboxCount; i++ {
		suffix := string(rune('a' + i))
		name := "pod-" + suffix
		uid := "uid-" + suffix
		id := "cri-" + suffix
		pods = append(pods, runtimeMetricPod("ns-a", name, uid, "node-a", "team-a", "sandbox-"+suffix, "1"))
		client.sandboxes = append(client.sandboxes, podSandbox(id, "ns-a", name, uid))
		client.statsByID[id] = minimalPodStats(id, "ns-a", name, uid)
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient:            client,
		PodLister:              podLister(t, pods...),
		Sink:                   &recordingSampleSink{},
		NodeName:               "node-a",
		FallbackMaxConcurrency: 2,
		FallbackRequestTimeout: time.Second,
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_, _ = collector.Collect(context.Background())
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		client.mu.Lock()
		active := client.active
		maxActive := client.maxActive
		client.mu.Unlock()
		if active == 2 {
			assert.LessOrEqual(t, maxActive, 2)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fallback collector did not reach configured concurrency")
		}
		time.Sleep(time.Millisecond)
	}
	close(client.block)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fallback collection did not complete")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.LessOrEqual(t, client.maxActive, 2)
}

func TestCollectorRotatesBoundedFallbackTargets(t *testing.T) {
	pods := make([]*corev1.Pod, 0, 3)
	client := &fakeStatsClient{
		err:       errors.New("bulk unavailable"),
		statsByID: make(map[string]*runtimeapi.PodSandboxStats, 3),
	}
	for _, suffix := range []string{"a", "b", "c"} {
		name := "pod-" + suffix
		uid := "uid-" + suffix
		id := "cri-" + suffix
		pods = append(pods, runtimeMetricPod("ns-a", name, uid, "node-a", "team-a", "sandbox-"+suffix, "1"))
		client.sandboxes = append(client.sandboxes, podSandbox(id, "ns-a", name, uid))
		client.statsByID[id] = minimalPodStats(id, "ns-a", name, uid)
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient:            client,
		PodLister:              podLister(t, pods...),
		Sink:                   &recordingSampleSink{},
		NodeName:               "node-a",
		FallbackMaxSandboxes:   2,
		FallbackMaxConcurrency: 1,
	})
	require.NoError(t, err)

	_, _ = collector.Collect(context.Background())
	_, _ = collector.Collect(context.Background())

	assert.Equal(t, []string{"cri-a", "cri-b", "cri-c", "cri-a"}, client.perSandboxCalls)
}

func TestCollectorCountsFullQueueDrops(t *testing.T) {
	sink := &recordingSampleSink{accept: func(sandboxobservability.RuntimeSample) bool { return false }}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: &fakeStatsClient{stats: []*runtimeapi.PodSandboxStats{minimalPodStats("cri-a", "ns-a", "pod-a", "uid-a")}},
		PodLister:   podLister(t, runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")),
		Sink:        sink,
		NodeName:    "node-a",
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.Dropped)
	assert.Zero(t, result.Enqueued)
}

func TestCollectorRunCollectsImmediately(t *testing.T) {
	called := make(chan struct{}, 1)
	client := &fakeStatsClient{onCall: func() { called <- struct{}{} }}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t),
		Sink:        &recordingSampleSink{},
		Interval:    time.Hour,
		Random:      func() float64 { return 0.5 },
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		collector.Run(ctx)
		close(done)
	}()
	select {
	case <-called:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("collector did not collect immediately")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("collector did not stop after cancellation")
	}
}

func TestCollectorJitterIsBounded(t *testing.T) {
	collector := &Collector{interval: 15 * time.Second, jitter: 1500 * time.Millisecond}
	collector.random = func() float64 { return 0 }
	assert.Equal(t, 13500*time.Millisecond, collector.nextDelay())
	collector.random = func() float64 { return 1 }
	assert.Equal(t, 16500*time.Millisecond, collector.nextDelay())
	collector.random = func() float64 { return 0.5 }
	assert.Equal(t, 15*time.Second, collector.nextDelay())
}

func TestCollectorUsesSharedRuntimeSampleCadenceDefaults(t *testing.T) {
	collector, err := NewCollector(CollectorConfig{
		StatsClient: &fakeStatsClient{},
		PodLister:   podLister(t),
		Sink:        &recordingSampleSink{},
	})
	require.NoError(t, err)

	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleInterval, collector.interval)
	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleJitter, collector.jitter)
	assert.Equal(t, defaultBulkRequestTimeout, collector.bulkRequestTimeout)
}

func minimalPodStats(epoch, namespace, name, uid string) *runtimeapi.PodSandboxStats {
	return &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{
			Id:       epoch,
			Metadata: &runtimeapi.PodSandboxMetadata{Namespace: namespace, Name: name, Uid: uid},
		},
		Linux: &runtimeapi.LinuxPodSandboxStats{},
	}
}

func podSandbox(id, namespace, name, uid string) *runtimeapi.PodSandbox {
	return &runtimeapi.PodSandbox{
		Id:       id,
		Metadata: &runtimeapi.PodSandboxMetadata{Namespace: namespace, Name: name, Uid: uid},
		State:    runtimeapi.PodSandboxState_SANDBOX_READY,
	}
}

func cpuOnlyPodStats(epoch, namespace, name, uid string, timestamp int64, cumulative uint64) *runtimeapi.PodSandboxStats {
	stats := minimalPodStats(epoch, namespace, name, uid)
	stats.Linux.Cpu = &runtimeapi.CpuUsage{
		Timestamp:            timestamp,
		UsageCoreNanoSeconds: u64(cumulative),
	}
	return stats
}

type fakeStatsClient struct {
	mu              sync.Mutex
	stats           []*runtimeapi.PodSandboxStats
	err             error
	calls           int
	onCall          func()
	sandboxes       []*runtimeapi.PodSandbox
	listErr         error
	statsByID       map[string]*runtimeapi.PodSandboxStats
	statsErrByID    map[string]error
	perSandboxCalls []string
	bulkBlock       chan struct{}
	block           chan struct{}
	active          int
	maxActive       int
}

func (c *fakeStatsClient) ListPodSandboxStats(ctx context.Context) ([]*runtimeapi.PodSandboxStats, error) {
	c.mu.Lock()
	c.calls++
	onCall := c.onCall
	stats := c.stats
	err := c.err
	bulkBlock := c.bulkBlock
	c.mu.Unlock()
	if onCall != nil {
		onCall()
	}
	if bulkBlock != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-bulkBlock:
		}
	}
	return stats, err
}

func (c *fakeStatsClient) ListPodSandboxes(context.Context) ([]*runtimeapi.PodSandbox, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*runtimeapi.PodSandbox(nil), c.sandboxes...), c.listErr
}

func (c *fakeStatsClient) PodSandboxStats(ctx context.Context, id string) (*runtimeapi.PodSandboxStats, error) {
	c.mu.Lock()
	c.perSandboxCalls = append(c.perSandboxCalls, id)
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	block := c.block
	stats := c.statsByID[id]
	err := c.statsErrByID[id]
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	return stats, err
}

func (c *fakeStatsClient) setStats(stats []*runtimeapi.PodSandboxStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = stats
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
