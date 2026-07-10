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

func cpuOnlyPodStats(epoch, namespace, name, uid string, timestamp int64, cumulative uint64) *runtimeapi.PodSandboxStats {
	stats := minimalPodStats(epoch, namespace, name, uid)
	stats.Linux.Cpu = &runtimeapi.CpuUsage{
		Timestamp:            timestamp,
		UsageCoreNanoSeconds: u64(cumulative),
	}
	return stats
}

type fakeStatsClient struct {
	mu     sync.Mutex
	stats  []*runtimeapi.PodSandboxStats
	err    error
	calls  int
	onCall func()
}

func (c *fakeStatsClient) ListPodSandboxStats(context.Context) ([]*runtimeapi.PodSandboxStats, error) {
	c.mu.Lock()
	c.calls++
	onCall := c.onCall
	stats := c.stats
	err := c.err
	c.mu.Unlock()
	if onCall != nil {
		onCall()
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
