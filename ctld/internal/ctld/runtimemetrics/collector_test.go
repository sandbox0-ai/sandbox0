package runtimemetrics

import (
	"context"
	"errors"
	"fmt"
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

func TestCollectorRequestsStatsOnlyForClaimedSandboxes(t *testing.T) {
	podA := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "2")
	podB := runtimeMetricPod("ns-a", "pod-b", "pod-uid-b", "node-a", "team-a", "sandbox-b", "3")
	client := &fakeStatsClient{
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-a", "ns-a", "pod-a", "pod-uid-a"),
			podSandbox("cri-b", "ns-a", "pod-b", "pod-uid-b"),
		},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-a": minimalPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a"),
			"cri-b": minimalPodStats("cri-b", "ns-a", "pod-b", "pod-uid-b"),
		},
	}
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("cri-unclaimed-%03d", i)
		name := fmt.Sprintf("unclaimed-%03d", i)
		uid := fmt.Sprintf("unclaimed-uid-%03d", i)
		client.sandboxes = append(client.sandboxes, podSandbox(id, "ns-a", name, uid))
		client.statsByID[id] = minimalPodStats(id, "ns-a", name, uid)
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
	assert.ElementsMatch(t, []string{"cri-a", "cri-b"}, client.perSandboxCalls)
	assert.Equal(t, CollectResult{StatsReceived: 2, Matched: 2, Enqueued: 2}, result)
	require.Len(t, sink.samples, 2)
	assert.Equal(t, "sandbox-a", sink.samples[0].SandboxID)
	assert.Equal(t, "sandbox-b", sink.samples[1].SandboxID)
}

func TestCollectorSkipsCRIWhenNoClaimedSandboxes(t *testing.T) {
	client := &fakeStatsClient{
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-unclaimed", "ns-a", "pod-unclaimed", "uid-unclaimed"),
		},
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t),
		Sink:        &recordingSampleSink{},
		NodeName:    "node-a",
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, CollectResult{}, result)
	assert.Zero(t, client.listCalls)
	assert.Empty(t, client.perSandboxCalls)
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

	client.setStatsByID("cri-a", cpuOnlyPodStats("cri-a", "ns-a", "pod-a", "pod-uid-a", 20_000_000_000, 15_000_000_000))
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

func TestCollectorIsolatesPerSandboxStatsFailures(t *testing.T) {
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
	require.ErrorContains(t, err, "runtime unavailable")
	assert.Equal(t, CollectResult{
		StatsReceived: 1,
		Matched:       2,
		Enqueued:      1,
		Failed:        1,
	}, result)
	require.Len(t, sink.samples, 1)
	assert.Equal(t, "sandbox-b", sink.samples[0].SandboxID)
	assert.ElementsMatch(t, []string{"cri-a", "cri-b"}, client.perSandboxCalls)
}

func TestCollectorBoundsPerSandboxStatsTimeout(t *testing.T) {
	pod := runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")
	client := &fakeStatsClient{
		block: make(chan struct{}),
		sandboxes: []*runtimeapi.PodSandbox{
			podSandbox("cri-a", "ns-a", "pod-a", "uid-a"),
		},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-a": minimalPodStats("cri-a", "ns-a", "pod-a", "uid-a"),
		},
	}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient:    client,
		PodLister:      podLister(t, pod),
		Sink:           sink,
		NodeName:       "node-a",
		RequestTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, result.Failed)
	assert.Zero(t, result.Enqueued)
	assert.Empty(t, sink.samples)
}

func TestCollectorBoundsStatsConcurrency(t *testing.T) {
	const sandboxCount = 6
	pods := make([]*corev1.Pod, 0, sandboxCount)
	client := &fakeStatsClient{
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
		StatsClient:    client,
		PodLister:      podLister(t, pods...),
		Sink:           &recordingSampleSink{},
		NodeName:       "node-a",
		MaxConcurrency: 2,
		RequestTimeout: time.Second,
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
			t.Fatal("collector did not reach configured concurrency")
		}
		time.Sleep(time.Millisecond)
	}
	close(client.block)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("collection did not complete")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.LessOrEqual(t, client.maxActive, 2)
}

func TestCollectorCollectsAllClaimedTargetsWithinBudget(t *testing.T) {
	const sandboxCount = 64
	pods := make([]*corev1.Pod, 0, sandboxCount)
	client := &fakeStatsClient{
		statsByID: make(map[string]*runtimeapi.PodSandboxStats, sandboxCount),
	}
	for i := 0; i < sandboxCount; i++ {
		name := fmt.Sprintf("pod-%03d", i)
		uid := fmt.Sprintf("uid-%03d", i)
		id := fmt.Sprintf("cri-%03d", i)
		pods = append(pods, runtimeMetricPod("ns-a", name, uid, "node-a", "team-a", fmt.Sprintf("sandbox-%03d", i), "1"))
		client.sandboxes = append(client.sandboxes, podSandbox(id, "ns-a", name, uid))
		client.statsByID[id] = minimalPodStats(id, "ns-a", name, uid)
	}
	sink := &recordingSampleSink{}
	collector, err := NewCollector(CollectorConfig{
		StatsClient:    client,
		PodLister:      podLister(t, pods...),
		Sink:           sink,
		NodeName:       "node-a",
		MaxConcurrency: 4,
	})
	require.NoError(t, err)

	result, err := collector.Collect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, sandboxCount, result.Matched)
	assert.Equal(t, sandboxCount, result.Enqueued)
	assert.Len(t, client.perSandboxCalls, sandboxCount)
	assert.Len(t, sink.samples, sandboxCount)
}

func TestCollectorRotatesAfterCollectionBudgetExhaustion(t *testing.T) {
	const sandboxCount = 6
	pods := make([]*corev1.Pod, 0, sandboxCount)
	client := &fakeStatsClient{
		statsByID: make(map[string]*runtimeapi.PodSandboxStats, sandboxCount),
		block:     make(chan struct{}),
	}
	for i := 0; i < sandboxCount; i++ {
		name := fmt.Sprintf("pod-%c", 'a'+i)
		uid := fmt.Sprintf("uid-%c", 'a'+i)
		id := fmt.Sprintf("cri-%c", 'a'+i)
		pods = append(pods, runtimeMetricPod("ns-a", name, uid, "node-a", "team-a", fmt.Sprintf("sandbox-%c", 'a'+i), "1"))
		client.sandboxes = append(client.sandboxes, podSandbox(id, "ns-a", name, uid))
		client.statsByID[id] = minimalPodStats(id, "ns-a", name, uid)
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient:      client,
		PodLister:        podLister(t, pods...),
		Sink:             &recordingSampleSink{},
		NodeName:         "node-a",
		MaxConcurrency:   2,
		RequestTimeout:   time.Second,
		CollectionBudget: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	first, firstErr := collector.Collect(context.Background())
	second, secondErr := collector.Collect(context.Background())

	require.ErrorIs(t, firstErr, context.DeadlineExceeded)
	require.ErrorIs(t, secondErr, context.DeadlineExceeded)
	assert.Equal(t, sandboxCount, first.Matched)
	assert.Equal(t, sandboxCount, second.Matched)
	require.Len(t, client.perSandboxCalls, 4)
	assert.ElementsMatch(t, []string{"cri-a", "cri-b"}, client.perSandboxCalls[:2])
	assert.ElementsMatch(t, []string{"cri-c", "cri-d"}, client.perSandboxCalls[2:])
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

func TestCollectorRunCollectsImmediately(t *testing.T) {
	called := make(chan struct{}, 1)
	pod := runtimeMetricPod("ns-a", "pod-a", "uid-a", "node-a", "team-a", "sandbox-a", "1")
	client := &fakeStatsClient{
		onList:    func() { called <- struct{}{} },
		sandboxes: []*runtimeapi.PodSandbox{podSandbox("cri-a", "ns-a", "pod-a", "uid-a")},
		statsByID: map[string]*runtimeapi.PodSandboxStats{
			"cri-a": minimalPodStats("cri-a", "ns-a", "pod-a", "uid-a"),
		},
	}
	collector, err := NewCollector(CollectorConfig{
		StatsClient: client,
		PodLister:   podLister(t, pod),
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
	assert.Equal(t, defaultMaxConcurrency, collector.maxConcurrency)
	assert.Equal(t, defaultRequestTimeout, collector.requestTimeout)
	assert.Equal(t, defaultCollectionBudget, collector.collectionBudget)
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
	listCalls       int
	onList          func()
	sandboxes       []*runtimeapi.PodSandbox
	listErr         error
	statsByID       map[string]*runtimeapi.PodSandboxStats
	statsErrByID    map[string]error
	perSandboxCalls []string
	block           chan struct{}
	active          int
	maxActive       int
}

func (c *fakeStatsClient) ListPodSandboxes(context.Context) ([]*runtimeapi.PodSandbox, error) {
	c.mu.Lock()
	c.listCalls++
	onList := c.onList
	sandboxes := append([]*runtimeapi.PodSandbox(nil), c.sandboxes...)
	err := c.listErr
	c.mu.Unlock()
	if onList != nil {
		onList()
	}
	return sandboxes, err
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

func (c *fakeStatsClient) setStatsByID(id string, stats *runtimeapi.PodSandboxStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
