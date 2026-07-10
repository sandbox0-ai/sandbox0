package runtimemetrics

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestBuildIdentityIndexFiltersAndResolvesActiveLocalSandbox(t *testing.T) {
	eligible := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "7")
	eligible.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1500m"),
		corev1.ResourceMemory: resource.MustParse("2Gi"),
	}
	wrongNode := runtimeMetricPod("ns-a", "pod-b", "pod-uid-b", "node-b", "team-a", "sandbox-b", "1")
	idle := runtimeMetricPod("ns-a", "pod-c", "pod-uid-c", "node-a", "team-a", "sandbox-c", "1")
	idle.Labels[controller.LabelPoolType] = controller.PoolTypeIdle
	invalidGeneration := runtimeMetricPod("ns-a", "pod-d", "pod-uid-d", "node-a", "team-a", "sandbox-d", "invalid")
	terminal := runtimeMetricPod("ns-a", "pod-e", "pod-uid-e", "node-a", "team-a", "sandbox-e", "1")
	terminal.Status.Phase = corev1.PodFailed

	index, err := buildIdentityIndex(podLister(t, eligible, wrongNode, idle, invalidGeneration, terminal), "node-a")
	require.NoError(t, err)
	require.Len(t, index.byUID, 1)

	identity, ok := index.resolve(&runtimeapi.PodSandboxAttributes{Metadata: &runtimeapi.PodSandboxMetadata{
		Uid:       "pod-uid-a",
		Namespace: "ns-a",
		Name:      "pod-a",
	}})
	require.True(t, ok)
	assert.Equal(t, "team-a", identity.TeamID)
	assert.Equal(t, "sandbox-a", identity.SandboxID)
	assert.Equal(t, int64(7), identity.RuntimeGeneration)
	require.NotNil(t, identity.CPULimitCores)
	assert.InDelta(t, 1.5, *identity.CPULimitCores, 0.0001)
	require.NotNil(t, identity.MemoryLimitBytes)
	assert.Equal(t, uint64(2*1024*1024*1024), *identity.MemoryLimitBytes)
}

func TestIdentityIndexFallsBackToNamespaceAndName(t *testing.T) {
	pod := runtimeMetricPod("ns-a", "pod-a", "pod-uid-a", "node-a", "team-a", "sandbox-a", "0")
	index, err := buildIdentityIndex(podLister(t, pod), "node-a")
	require.NoError(t, err)

	identity, ok := index.resolve(&runtimeapi.PodSandboxAttributes{Metadata: &runtimeapi.PodSandboxMetadata{
		Namespace: "ns-a",
		Name:      "pod-a",
	}})
	require.True(t, ok)
	assert.Equal(t, "sandbox-a", identity.SandboxID)
}

func TestIdentityIndexDoesNotMatchStaleUIDByPodName(t *testing.T) {
	pod := runtimeMetricPod("ns-a", "pod-a", "current-uid", "node-a", "team-a", "sandbox-a", "1")
	index, err := buildIdentityIndex(podLister(t, pod), "node-a")
	require.NoError(t, err)

	_, ok := index.resolve(&runtimeapi.PodSandboxAttributes{Metadata: &runtimeapi.PodSandboxMetadata{
		Uid:       "stale-uid",
		Namespace: "ns-a",
		Name:      "pod-a",
	}})
	assert.False(t, ok)
}

func runtimeMetricPod(namespace, name, uid, nodeName, teamID, sandboxID, generation string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(uid),
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: sandboxID,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:            teamID,
				controller.AnnotationRuntimeGeneration: generation,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name: sandboxContainerName,
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func podLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		require.NoError(t, indexer.Add(pod))
	}
	return corelisters.NewPodLister(indexer)
}
