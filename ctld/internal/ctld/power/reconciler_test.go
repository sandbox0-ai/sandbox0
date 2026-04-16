package power

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/cgroup"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPowerReconcilerPausesDesiredPausedPod(t *testing.T) {
	const uid = "pod-uid-1"
	root := t.TempDir()
	cgroupDir := writeReconcilerTestCgroup(t, root, uid, "0", 40*1024*1024)
	pod := newReconcilerTestPod(uid, map[string]string{
		controller.AnnotationConfig:                       `{"ttl":120}`,
		controller.AnnotationExpiresAt:                    time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
		controller.AnnotationPowerStateDesired:            powerStatePaused,
		controller.AnnotationPowerStateDesiredGeneration:  "1",
		controller.AnnotationPowerStateObserved:           powerStateActive,
		controller.AnnotationPowerStateObservedGeneration: "0",
		controller.AnnotationPowerStatePhase:              powerPhasePausing,
	})
	client := fake.NewSimpleClientset(pod)
	reconciler := newReconcilerForTest(client, root)

	require.NoError(t, reconciler.reconcilePod(context.Background(), pod))

	assert.Equal(t, "1", readReconcilerTestFile(t, filepath.Join(cgroupDir, "cgroup.freeze")))
	updated, err := client.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "true", updated.Annotations[controller.AnnotationPaused])
	assert.NotEmpty(t, updated.Annotations[controller.AnnotationPausedAt])
	assert.Empty(t, updated.Annotations[controller.AnnotationExpiresAt])

	state := powerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, powerStatePaused, state.Desired)
	assert.Equal(t, powerStatePaused, state.Observed)
	assert.Equal(t, int64(1), state.ObservedGeneration)
	assert.Equal(t, powerPhaseStable, state.Phase)

	var saved pausedState
	require.NoError(t, json.Unmarshal([]byte(updated.Annotations[controller.AnnotationPausedState]), &saved))
	require.NotNil(t, saved.OriginalTTL)
	assert.Equal(t, int32(120), *saved.OriginalTTL)
	assert.Equal(t, resource.MustParse("128Mi"), saved.Resources["procd"].Requests[corev1.ResourceMemory])

	procd := updated.Spec.Containers[0]
	requestMemory := procd.Resources.Requests[corev1.ResourceMemory]
	limitMemory := procd.Resources.Limits[corev1.ResourceMemory]
	assert.Equal(t, resource.MustParse("10m"), procd.Resources.Requests[corev1.ResourceCPU])
	assert.Equal(t, int64(40*1024*1024), requestMemory.Value())
	assert.Equal(t, int64(80*1024*1024), limitMemory.Value())
}

func TestPowerReconcilerResumesDesiredActivePod(t *testing.T) {
	const uid = "pod-uid-2"
	root := t.TempDir()
	cgroupDir := writeReconcilerTestCgroup(t, root, uid, "1", 20*1024*1024)
	originalTTL := int32(90)
	pausedStateJSON := mustMarshalReconcilerPausedState(t, pausedState{
		Resources: map[string]containerResources{
			"procd": {
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				},
			},
		},
		OriginalTTL: &originalTTL,
	})
	pod := newReconcilerTestPod(uid, map[string]string{
		controller.AnnotationPaused:                       "true",
		controller.AnnotationPausedAt:                     time.Now().UTC().Add(-time.Minute).Format(time.RFC3339),
		controller.AnnotationPausedState:                  pausedStateJSON,
		controller.AnnotationPowerStateDesired:            powerStateActive,
		controller.AnnotationPowerStateDesiredGeneration:  "2",
		controller.AnnotationPowerStateObserved:           powerStatePaused,
		controller.AnnotationPowerStateObservedGeneration: "1",
		controller.AnnotationPowerStatePhase:              powerPhaseResuming,
	})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
	}
	client := fake.NewSimpleClientset(pod)
	reconciler := newReconcilerForTest(client, root)

	require.NoError(t, reconciler.reconcilePod(context.Background(), pod))

	assert.Equal(t, "0", readReconcilerTestFile(t, filepath.Join(cgroupDir, "cgroup.freeze")))
	updated, err := client.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, updated.Annotations[controller.AnnotationPaused])
	assert.Empty(t, updated.Annotations[controller.AnnotationPausedAt])
	assert.Empty(t, updated.Annotations[controller.AnnotationPausedState])
	assert.NotEmpty(t, updated.Annotations[controller.AnnotationExpiresAt])

	state := powerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, powerStateActive, state.Desired)
	assert.Equal(t, powerStateActive, state.Observed)
	assert.Equal(t, int64(2), state.ObservedGeneration)
	assert.Equal(t, powerPhaseStable, state.Phase)

	procd := updated.Spec.Containers[0]
	assert.Equal(t, resource.MustParse("100m"), procd.Resources.Requests[corev1.ResourceCPU])
	assert.Equal(t, resource.MustParse("128Mi"), procd.Resources.Requests[corev1.ResourceMemory])
	assert.Equal(t, resource.MustParse("200m"), procd.Resources.Limits[corev1.ResourceCPU])
	assert.Equal(t, resource.MustParse("256Mi"), procd.Resources.Limits[corev1.ResourceMemory])
}

func TestPowerReconcilerResumesInFlightPauseCancellation(t *testing.T) {
	const uid = "pod-uid-3"
	root := t.TempDir()
	cgroupDir := writeReconcilerTestCgroup(t, root, uid, "1", 20*1024*1024)
	pod := newReconcilerTestPod(uid, map[string]string{
		controller.AnnotationPowerStateDesired:            powerStateActive,
		controller.AnnotationPowerStateDesiredGeneration:  "3",
		controller.AnnotationPowerStateObserved:           powerStateActive,
		controller.AnnotationPowerStateObservedGeneration: "1",
		controller.AnnotationPowerStatePhase:              powerPhaseResuming,
	})
	client := fake.NewSimpleClientset(pod)
	reconciler := newReconcilerForTest(client, root)

	require.NoError(t, reconciler.reconcilePod(context.Background(), pod))

	assert.Equal(t, "0", readReconcilerTestFile(t, filepath.Join(cgroupDir, "cgroup.freeze")))
	updated, err := client.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := powerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, powerStateActive, state.Desired)
	assert.Equal(t, powerStateActive, state.Observed)
	assert.Equal(t, int64(3), state.ObservedGeneration)
	assert.Equal(t, powerPhaseStable, state.Phase)
}

func newReconcilerForTest(client kubernetes.Interface, cgroupRoot string) *PowerReconciler {
	resolver := NewPodResolver(client, "node-a", cgroupRoot)
	controller := NewController(resolver, &cgroup.FS{SettleTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond})
	return NewPowerReconciler(client, nil, resolver, controller, PowerReconcilerConfig{
		PauseMinMemoryRequest:  "10Mi",
		PauseMinMemoryLimit:    "32Mi",
		PauseMemoryBufferRatio: 2,
		PauseMinCPU:            "10m",
	})
}

func newReconcilerTestPod(uid string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			UID:       types.UID(uid),
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
				controller.LabelPoolType:  controller.PoolTypeActive,
			},
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Containers: []corev1.Container{{
				Name: "procd",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func writeReconcilerTestCgroup(t *testing.T, root, uid, frozen string, memoryCurrent int64) string {
	t.Helper()
	dir := filepath.Join(root, "kubepods", "pod"+uid)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.freeze"), []byte(frozen), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte(strconv.FormatInt(memoryCurrent, 10)), 0o644))
	return dir
}

func readReconcilerTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

func mustMarshalReconcilerPausedState(t *testing.T, state pausedState) string {
	t.Helper()
	data, err := json.Marshal(state)
	require.NoError(t, err)
	return string(data)
}
