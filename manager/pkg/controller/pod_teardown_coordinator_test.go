package controller

import (
	"fmt"
	"sync"
	"testing"
	"time"

	config "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestPodTeardownCoordinatorScalesBatchByNodeDistribution(t *testing.T) {
	tests := []struct {
		name      string
		nodeCount int
		want      int
	}{
		{name: "two hundred pods on one node", nodeCount: 1, want: 4},
		{name: "two hundred pods across ten nodes", nodeCount: 10, want: 40},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodes := make([]*corev1.Node, 0, test.nodeCount)
			for i := 0; i < test.nodeCount; i++ {
				nodes = append(nodes, teardownTestNode(fmt.Sprintf("node-%02d", i), corev1.ConditionTrue))
			}
			pods := make([]*corev1.Pod, 0, 200)
			for i := 0; i < 200; i++ {
				pods = append(pods, teardownTestPod(fmt.Sprintf("pod-%03d", i), fmt.Sprintf("node-%02d", i%test.nodeCount), false))
			}
			coordinator, _, _ := newTeardownTestCoordinator(t, pods, nodes, config.PodTeardownConfig{})

			leases, err := coordinator.Acquire(pods, TeardownReasonUnhealthyRepair)
			require.NoError(t, err)
			assert.Len(t, leases, test.want)

			perNode := make(map[string]int)
			for _, lease := range leases {
				perNode[lease.Pod().Spec.NodeName]++
			}
			for nodeName, count := range perNode {
				assert.LessOrEqual(t, count, 4, nodeName)
			}
		})
	}
}

func TestPodTeardownCoordinatorCountsTerminatingPodsAcrossTemplates(t *testing.T) {
	node := teardownTestNode("node-a", corev1.ConditionTrue)
	candidates := make([]*corev1.Pod, 0, 8)
	for i := 0; i < 8; i++ {
		candidates = append(candidates, teardownTestPod(fmt.Sprintf("candidate-%d", i), node.Name, false))
	}
	terminatingA := teardownTestPod("active-other-template", node.Name, true)
	terminatingA.Labels[LabelTemplateID] = "other-template"
	terminatingA.Labels[LabelPoolType] = PoolTypeActive
	terminatingB := teardownTestPod("idle-other-namespace", node.Name, true)
	terminatingB.Namespace = "other-namespace"
	hostNetwork := teardownTestPod("host-network-daemon", node.Name, true)
	hostNetwork.Spec.HostNetwork = true

	allPods := append(append([]*corev1.Pod{}, candidates...), terminatingA, terminatingB, hostNetwork)
	coordinator, _, _ := newTeardownTestCoordinator(t, allPods, []*corev1.Node{node}, config.PodTeardownConfig{})

	leases, err := coordinator.Acquire(candidates, TeardownReasonUnhealthyRepair)
	require.NoError(t, err)
	assert.Len(t, leases, 2, "two existing non-hostNetwork teardowns must consume two of four node slots")
}

func TestPodTeardownCoordinatorUsesNodeHealthLimits(t *testing.T) {
	healthy := teardownTestNode("healthy", corev1.ConditionTrue)
	degraded := teardownTestNode("degraded", corev1.ConditionTrue)
	degraded.Status.Conditions = append(degraded.Status.Conditions, corev1.NodeCondition{
		Type: corev1.NodePIDPressure, Status: corev1.ConditionTrue,
	})
	notReady := teardownTestNode("not-ready", corev1.ConditionFalse)
	unknown := teardownTestNode("unknown", corev1.ConditionUnknown)

	pods := make([]*corev1.Pod, 0, 20)
	for _, nodeName := range []string{healthy.Name, degraded.Name, notReady.Name, unknown.Name, "missing"} {
		for i := 0; i < 4; i++ {
			pods = append(pods, teardownTestPod(fmt.Sprintf("%s-%d", nodeName, i), nodeName, false))
		}
	}
	coordinator, _, _ := newTeardownTestCoordinator(t, pods, []*corev1.Node{healthy, degraded, notReady, unknown}, config.PodTeardownConfig{})

	leases, err := coordinator.Acquire(pods, TeardownReasonUnhealthyRepair)
	require.NoError(t, err)
	counts := make(map[string]int)
	for _, lease := range leases {
		counts[lease.Pod().Spec.NodeName]++
	}
	assert.Equal(t, 4, counts[healthy.Name])
	assert.Equal(t, 1, counts[degraded.Name])
	assert.Zero(t, counts[notReady.Name])
	assert.Zero(t, counts[unknown.Name])
	assert.Zero(t, counts["missing"])
}

func TestPodTeardownCoordinatorWaitsForReplacementReadiness(t *testing.T) {
	nodeA := teardownTestNode("node-a", corev1.ConditionTrue)
	nodeB := teardownTestNode("node-b", corev1.ConditionTrue)
	pods := []*corev1.Pod{
		teardownTestPod("first-a", nodeA.Name, false),
		teardownTestPod("first-b", nodeB.Name, false),
		teardownTestPod("first-c", nodeB.Name, false),
		teardownTestPod("second-a", nodeA.Name, false),
		teardownTestPod("second-b", nodeB.Name, false),
	}
	limits := config.PodTeardownConfig{
		MaxConcurrentPerNode:         4,
		MaxConcurrentPerDegradedNode: 1,
		MaxConcurrentReplacements:    3,
	}
	coordinator, podIndexer, _ := newTeardownTestCoordinator(t, pods, []*corev1.Node{nodeA, nodeB}, limits)

	first, err := coordinator.Acquire(pods[:3], TeardownReasonUnhealthyRepair)
	require.NoError(t, err)
	require.Len(t, first, 3)
	for _, lease := range first {
		lease.Commit()
		require.NoError(t, podIndexer.Delete(lease.Pod()))
	}

	blocked, err := coordinator.Acquire(pods[3:], TeardownReasonStaleRollout)
	require.NoError(t, err)
	assert.Empty(t, blocked, "old Pod disappearance releases node slots but not replacement slots")

	replacement := teardownTestPod("replacement-ready", nodeA.Name, false)
	replacement.Status = readyPodStatus()
	require.NoError(t, podIndexer.Add(replacement))
	oneReleased, err := coordinator.Acquire(pods[3:], TeardownReasonStaleRollout)
	require.NoError(t, err)
	assert.Len(t, oneReleased, 1, "one Ready replacement must release exactly one cluster slot")
}

func TestPodTeardownCoordinatorRecognizesFastReadyReplacementByUID(t *testing.T) {
	node := teardownTestNode("node-a", corev1.ConditionTrue)
	oldReady := teardownTestPod("old-ready", node.Name, false)
	oldReady.Status = readyPodStatus()
	next := teardownTestPod("next", node.Name, false)
	limits := config.PodTeardownConfig{
		MaxConcurrentPerNode:         1,
		MaxConcurrentPerDegradedNode: 1,
		MaxConcurrentReplacements:    1,
	}
	coordinator, podIndexer, _ := newTeardownTestCoordinator(t, []*corev1.Pod{oldReady, next}, []*corev1.Node{node}, limits)

	first, err := coordinator.Acquire([]*corev1.Pod{oldReady}, TeardownReasonStaleRollout)
	require.NoError(t, err)
	require.Len(t, first, 1)
	first[0].Commit()
	require.NoError(t, podIndexer.Delete(oldReady))

	// The old Ready Pod can disappear and its replacement can become Ready
	// before the coordinator observes an intermediate readiness dip. Tracking
	// Ready identities, rather than only the aggregate count, must still release
	// the replacement slot.
	replacement := teardownTestPod("replacement-ready", node.Name, false)
	replacement.Status = readyPodStatus()
	require.NoError(t, podIndexer.Add(replacement))

	second, err := coordinator.Acquire([]*corev1.Pod{next}, TeardownReasonStaleRollout)
	require.NoError(t, err)
	assert.Len(t, second, 1)
}

func TestPodTeardownCoordinatorReusesStalledReplacementForRepair(t *testing.T) {
	node := teardownTestNode("node-a", corev1.ConditionTrue)
	firstPod := teardownTestPod("first", node.Name, false)
	secondPod := teardownTestPod("second", node.Name, false)
	limits := config.PodTeardownConfig{
		MaxConcurrentPerNode:         1,
		MaxConcurrentPerDegradedNode: 1,
		MaxConcurrentReplacements:    1,
	}
	coordinator, podIndexer, _ := newTeardownTestCoordinator(t, []*corev1.Pod{firstPod, secondPod}, []*corev1.Node{node}, limits)

	first, err := coordinator.Acquire([]*corev1.Pod{firstPod}, TeardownReasonUnhealthyRepair)
	require.NoError(t, err)
	require.Len(t, first, 1)
	first[0].Commit()
	require.NoError(t, podIndexer.Delete(firstPod))

	second, err := coordinator.Acquire([]*corev1.Pod{secondPod}, TeardownReasonUnhealthyRepair)
	require.NoError(t, err)
	assert.Len(t, second, 1, "repairing a stalled replacement must reuse its existing replacement slot")
}

func TestPodTeardownCoordinatorReleaseRestoresCapacity(t *testing.T) {
	node := teardownTestNode("node-a", corev1.ConditionTrue)
	firstPod := teardownTestPod("first", node.Name, false)
	secondPod := teardownTestPod("second", node.Name, false)
	limits := config.PodTeardownConfig{
		MaxConcurrentPerNode:         1,
		MaxConcurrentPerDegradedNode: 1,
		MaxConcurrentReplacements:    1,
	}
	coordinator, _, _ := newTeardownTestCoordinator(t, []*corev1.Pod{firstPod, secondPod}, []*corev1.Node{node}, limits)

	first, err := coordinator.Acquire([]*corev1.Pod{firstPod}, TeardownReasonStaleRollout)
	require.NoError(t, err)
	require.Len(t, first, 1)
	first[0].Release()

	second, err := coordinator.Acquire([]*corev1.Pod{secondPod}, TeardownReasonStaleRollout)
	require.NoError(t, err)
	assert.Len(t, second, 1)
}

func TestPodTeardownCoordinatorSerializesConcurrentTemplates(t *testing.T) {
	nodes := []*corev1.Node{
		teardownTestNode("node-a", corev1.ConditionTrue),
		teardownTestNode("node-b", corev1.ConditionTrue),
	}
	pods := make([]*corev1.Pod, 0, 20)
	for i := 0; i < 20; i++ {
		pod := teardownTestPod(fmt.Sprintf("pod-%02d", i), nodes[i%2].Name, false)
		pod.Labels[LabelTemplateID] = fmt.Sprintf("template-%d", i%2)
		pods = append(pods, pod)
	}
	limits := config.PodTeardownConfig{
		MaxConcurrentPerNode:         4,
		MaxConcurrentPerDegradedNode: 1,
		MaxConcurrentReplacements:    5,
	}
	coordinator, _, _ := newTeardownTestCoordinator(t, pods, nodes, limits)

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for shard := 0; shard < 2; shard++ {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidates := make([]*corev1.Pod, 0, 10)
			for i := shard; i < len(pods); i += 2 {
				candidates = append(candidates, pods[i])
			}
			leases, err := coordinator.Acquire(candidates, TeardownReasonUnhealthyRepair)
			require.NoError(t, err)
			results <- len(leases)
		}()
	}
	wg.Wait()
	close(results)
	total := 0
	for count := range results {
		total += count
	}
	assert.Equal(t, 5, total)
}

func TestPodTeardownCoordinatorForceDeleteReusesConfiguredLimits(t *testing.T) {
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	healthy := teardownTestNode("healthy", corev1.ConditionTrue)
	notReady := teardownTestNode("not-ready", corev1.ConditionFalse)
	pods := make([]*corev1.Pod, 0, 20)
	for _, nodeName := range []string{healthy.Name, notReady.Name} {
		for i := 0; i < 10; i++ {
			pod := teardownTestPod(fmt.Sprintf("%s-%d", nodeName, i), nodeName, true)
			deletedAt := metav1.NewTime(now.Add(-staleDeletingPodForceDeleteAfter - time.Minute))
			pod.DeletionTimestamp = &deletedAt
			pods = append(pods, pod)
		}
	}
	coordinator, _, _ := newTeardownTestCoordinator(t, pods, []*corev1.Node{healthy, notReady}, config.PodTeardownConfig{})
	coordinator.now = func() time.Time { return now }

	selected, err := coordinator.SelectForceDeletes(pods)
	require.NoError(t, err)
	counts := make(map[string]int)
	for _, pod := range selected {
		counts[pod.Spec.NodeName]++
	}
	assert.Equal(t, 4, counts[healthy.Name])
	assert.Equal(t, 1, counts[notReady.Name], "lost nodes must progress with the degraded-node limit")

	again, err := coordinator.SelectForceDeletes(pods)
	require.NoError(t, err)
	assert.Empty(t, again, "the force-delete window must not multiply the batch across templates or loops")

	now = now.Add(forceDeleteThrottleWindow + time.Second)
	afterWindow, err := coordinator.SelectForceDeletes(pods)
	require.NoError(t, err)
	assert.Len(t, afterWindow, 5)
}

func TestPodTeardownCoordinatorLimitsUnscheduledCandidatesGlobally(t *testing.T) {
	pods := make([]*corev1.Pod, 0, 10)
	for i := 0; i < 10; i++ {
		pods = append(pods, teardownTestPod(fmt.Sprintf("pending-%d", i), "", false))
	}
	limits := config.PodTeardownConfig{MaxConcurrentPerNode: 4, MaxConcurrentPerDegradedNode: 1, MaxConcurrentReplacements: 3}
	coordinator, _, _ := newTeardownTestCoordinator(t, pods, nil, limits)

	leases, err := coordinator.Acquire(pods, TeardownReasonUnhealthyRepair)
	require.NoError(t, err)
	assert.Len(t, leases, 3)
}

func newTeardownTestCoordinator(
	t *testing.T,
	pods []*corev1.Pod,
	nodes []*corev1.Node,
	limits config.PodTeardownConfig,
) (*PodTeardownCoordinator, cache.Indexer, cache.Indexer) {
	t.Helper()
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, pod := range pods {
		require.NoError(t, podIndexer.Add(pod))
	}
	nodeIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		require.NoError(t, nodeIndexer.Add(node))
	}
	coordinator := NewPodTeardownCoordinator(
		corelisters.NewPodLister(podIndexer),
		corelisters.NewNodeLister(nodeIndexer),
		limits,
		0,
		nil,
		zap.NewNop(),
	)
	return coordinator, podIndexer, nodeIndexer
}

func teardownTestNode(name string, ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: ready,
		}}},
	}
}

func teardownTestPod(name, nodeName string, terminating bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			UID:               types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(time.Now().Add(-config.IdlePodRepairGracePeriod(0) - time.Minute)),
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
		},
		Spec:   corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	if terminating {
		deletedAt := metav1.NewTime(time.Now().Add(-time.Minute))
		pod.DeletionTimestamp = &deletedAt
	}
	return pod
}

func readyPodStatus() corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}},
	}
}
