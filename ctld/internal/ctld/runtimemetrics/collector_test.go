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

func TestCollectorUsesFilteredStatsCallsAndEnqueuesMatchedSandboxes(t *testing.T) {
	podA := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "2")
	podB := runtimeMetricPod("ns-a", "pod-b", "pod-uid-b", "node-a", "team-a", "sandbox-b", "3")
	client := &fakeStatsClient{
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-a", "ns-a", "pod-a", "pod-uid-a"),
			podSandbox("cri-b", "ns-a", "pod-b", "pod-uid-b"),
			podSandbox("cri-other", "ns-a", "other", "pod-uid-other"),
		},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-a":     minimalPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a"),
			"cri-b":     minimalPodStats("cri-b", "ns-a", "pod-b", "pod-uid-b"),
			"cri-other": minimalPodStats("cri-other", "ns-a", "other", "pod-uid-other"),
		},
	}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		RegionID: "region-a", ClusterID: "cluster-a", NodeName: "node-a",
		StatsClient: client, PodLister: podLister(t, podA, podB), Sink: sink,
		Now: func() time.Time { return time.Unix(100, 0).UTC() }, Random: func() float64 { return 0.5 },
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, client.listCalls)
	assert.ElementsMatch(t, []string{"cri-a", "cri-b"}, client.statsCalls)
	assert.Equal(t, CollectResult{StatsReceived: 2, Matched: 2, Enqueued: 2}, result)
	require.Len(t, sink.samples, 2)
	assert.ElementsMatch(t, []string{"sandbox-a", "sandbox-b"}, []string{sink.samples[0].SandboxID, sink.samples[1].SandboxID})
}

func TestCollectorDerivesCPUUsageFromLinuxCumulativeStats(t *testing.T) {
	pod := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "2")
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"),
	}
	client := &fakeStatsClient{
		sandboxes: []*runtimeapi.PodSandbox{podSandbox("cri-a", "ns-a", "pod-a", "pod-uid-a")},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-a": cpuOnlyPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a", 10_000_000_000, 10_000_000_000),
		},
	}
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

	client.setStats("cri-a", cpuOnlyPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a", 20_000_000_000, 15_000_000_000))
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

func TestCollectorReportsCRIListErrorWithoutEnqueuing(t *testing.T) {
	client := &fakeStatsClient{listErr: errors.New("containerd unavailable")}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t, runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")),
		Sink:        sink,
		NodeName:    "node-a",
	})
	require.NoError(t, err)

	_, err = collector.Collect(context.Background())
	require.ErrorContains(t, err, "list CRI pod sandboxes")
	assert.Empty(t, sink.samples)
}

func TestCollectorContinuesAfterOneSandboxStatsError(t *testing.T) {
	podA := runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")
	podB := runtimeMetricPod("ns-a", "pod-b", "uid-b", "node-a", "team-a", "sandbox-b", "1")
	client := &fakeStatsClient{
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-a", "ns-a", "pod-a", "uid-a"),
			podSandbox("cri-b", "ns-a", "pod-b", "uid-b"),
		},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-b": minimalPodStats("cri-b", "ns-a", "pod-b", "uid-b"),
		},
		statsErrByID: map[string]error{"cri-a": errors.New("stats unavailable")},
	}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client, PodLister: podLister(t, podA, podB), Sink: sink, NodeName: "node-a",
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())
	require.ErrorContains(t, err, "1 sandbox runtime metric collection(s) failed")
	assert.Equal(t, CollectResult{StatsReceived: 1, Matched: 2, Enqueued: 1, Failed: 1}, result)
	require.Len(t, sink.samples, 1)
	assert.Equal(t, "sandbox-b", sink.samples[0].SandboxID)
}

func TestCollectorCountsFullQueueDrops(t *testing.T) {
	sink := &recordingSampleSink{accept: func(sandboxobservability.RuntimeSample) bool { return false }}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: &fakeStatsClient{
			sandboxes: []*runtimeapi.PodSandbox{podSandbox("cri-a", "ns-a", "pod-a", "uid-a")},
			statsByID: map[string]*runtimeapi.PodSandboxStats{"cri-a": minimalPodStats("cri-a", "ns-a", "pod-a", "uid-a")},
		},
		PodLister: podLister(t, runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")),
		Sink:      sink,
		NodeName:  "node-a",
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.Dropped)
	assert.Zero(t, result.Enqueued)
}

func TestCollectorBoundsConcurrentStatsCalls(t *testing.T) {
	const sandboxCount = 6
	release := make(chan struct{})
	started := make(chan string, sandboxCount)
	client := &fakeStatsClient{
		statsByID: make(map[string]*runtimeapi.PodSandboxStats, sandboxCount),
		block:     release,
		onCall:    func(id string) { started <- id },
	}
	pods := make([]*corev1.Pod, 0, sandboxCount)
	for i := 0; i < sandboxCount; i++ {
		id := string(rune('a' + i))
		uid := "uid-" + id
		name := "pod-" + id
		criID := "cri-" + id
		pods = append(pods, runtimeMetricPod("ns-a", name, uid, "node-a", "team-a", "sandbox-"+id, "1"))
		client.sandboxes = append(client.sandboxes, podSandbox(criID, "ns-a", name, uid))
		client.statsByID[criID] = minimalPodStats(criID, "ns-a", name, uid)
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client, PodLister: podLister(t, pods...), Sink: &recordingSampleSink{}, NodeName: "node-a", MaxConcurrency: 2,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, collectErr := collector.Collect(context.Background())
		done <- collectErr
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("collector did not start the expected concurrent stats calls")
		}
	}
	select {
	case id := <-started:
		t.Fatalf("collector exceeded max concurrency with %s", id)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	assert.LessOrEqual(t, client.maxActive, 2)
}

func TestCollectorRunCollectsImmediately(t *testing.T) {
	called := make(chan struct{}, 1)
	pod := runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")
	client := &fakeStatsClient{
		sandboxes: []*runtimeapi.PodSandbox{podSandbox("cri-a", "ns-a", "pod-a", "uid-a")},
		statsByID: map[string]*runtimeapi.PodSandboxStats{"cri-a": minimalPodStats("cri-a", "ns-a", "pod-a", "uid-a")},
		onCall:    func(string) { called <- struct{}{} },
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t, pod),
		Sink:        &recordingSampleSink{},
		NodeName:    "node-a",
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

func TestCycleOffsetSpreadsTargetsAcrossWindow(t *testing.T) {
	assert.Equal(t, time.Duration(0), cycleOffset(0, 4, 100*time.Millisecond))
	assert.Equal(t, 25*time.Millisecond, cycleOffset(1, 4, 100*time.Millisecond))
	assert.Equal(t, 50*time.Millisecond, cycleOffset(2, 4, 100*time.Millisecond))
	assert.Equal(t, 75*time.Millisecond, cycleOffset(3, 4, 100*time.Millisecond))
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
	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleMaxConcurrency, collector.maxConcurrency)
}

func podSandbox(id, namespace, name, uid string) *runtimeapi.PodSandbox {
	return &runtimeapi.PodSandbox{
		Id:       id,
		Metadata: &runtimeapi.PodSandboxMetadata{Namespace: namespace, Name: name, Uid: uid},
		State:    runtimeapi.PodSandboxState_SANDBOX_READY,
	}
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
	mu           sync.Mutex
	sandboxes    []*runtimeapi.PodSandbox
	statsByID    map[string]*runtimeapi.PodSandboxStats
	statsErrByID map[string]error
	listErr      error
	listCalls    int
	statsCalls   []string
	onCall       func(string)
	block        <-chan struct{}
	active       int
	maxActive    int
}

func (c *fakeStatsClient) ListPodSandboxes(context.Context) ([]*runtimeapi.PodSandbox, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listCalls++
	return append([]*runtimeapi.PodSandbox(nil), c.sandboxes...), c.listErr
}

func (c *fakeStatsClient) PodSandboxStats(ctx context.Context, id string) (*runtimeapi.PodSandboxStats, error) {
	c.mu.Lock()
	c.statsCalls = append(c.statsCalls, id)
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	onCall := c.onCall
	block := c.block
	stats := c.statsByID[id]
	err := c.statsErrByID[id]
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()
	if onCall != nil {
		onCall(id)
	}
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	return stats, err
}

func (c *fakeStatsClient) setStats(id string, stats *runtimeapi.PodSandboxStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.statsByID == nil {
		c.statsByID = make(map[string]*runtimeapi.PodSandboxStats)
	}
	c.statsByID[id] = stats
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
