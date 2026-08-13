package carrierpool

import (
	"testing"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewCarrierPodUsesUniquePullNeverMarkerAndInitGate(t *testing.T) {
	pool, err := New(fake.NewSimpleClientset(), Config{
		Namespace: "sandbox0", ClusterID: "default", MinIdle: 1, MaxIdle: 2,
		CarrierImageRef: "sandbox0ai/infra:carrier-base-v1", WaiterImageRef: "alpine:3.20", Generation: "generation-a",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, pool.config.ReconcileInterval)
	first, err := pool.newCarrierPod(false, nil)
	require.NoError(t, err)
	second, err := pool.newCarrierPod(false, nil)
	require.NoError(t, err)

	firstSlot := first.Annotations[carrier.AnnotationSlot]
	secondSlot := second.Annotations[carrier.AnnotationSlot]
	assert.NotEqual(t, firstSlot, secondSlot)
	firstMarker, err := carrier.MarkerImage(firstSlot)
	require.NoError(t, err)
	assert.Equal(t, firstMarker, first.Spec.Containers[0].Image)
	assert.Equal(t, corev1.PullNever, first.Spec.Containers[0].ImagePullPolicy)
	ephemeralLimit := first.Spec.Containers[0].Resources.Limits[corev1.ResourceEphemeralStorage]
	assert.Equal(t, api.DefaultSandboxEphemeralStorage, ephemeralLimit.String())
	require.Len(t, first.Spec.InitContainers, 1)
	assert.Equal(t, "carrier-wait", first.Spec.InitContainers[0].Name)
	assert.Equal(t, "alpine:3.20", first.Spec.InitContainers[0].Image)
	assert.Equal(t, []string{"/bin/sh", "-ec", "while [ ! -f /var/run/sandbox0/carrier/release ]; do sleep 0.20; done"}, first.Spec.InitContainers[0].Command)
	require.Len(t, first.Spec.InitContainers[0].VolumeMounts, 2)
	assert.Equal(t, carrierBaseVolumeName, first.Spec.InitContainers[0].VolumeMounts[0].Name)
	assert.Equal(t, carrierBaseMountPath, first.Spec.InitContainers[0].VolumeMounts[0].MountPath)
	assert.True(t, first.Spec.InitContainers[0].VolumeMounts[0].ReadOnly)
	baseVolume := first.Spec.Volumes[len(first.Spec.Volumes)-2]
	require.NotNil(t, baseVolume.Image)
	assert.Equal(t, "sandbox0ai/infra:carrier-base-v1", baseVolume.Image.Reference)
	assert.Equal(t, corev1.PullIfNotPresent, baseVolume.Image.PullPolicy)
	assert.Equal(t, carrier.GateVolumeName, first.Spec.Volumes[len(first.Spec.Volumes)-1].Name)
	require.Len(t, first.Spec.TopologySpreadConstraints, 1)
	spread := first.Spec.TopologySpreadConstraints[0]
	assert.Equal(t, corev1.LabelHostname, spread.TopologyKey)
	assert.Equal(t, corev1.ScheduleAnyway, spread.WhenUnsatisfiable)
	assert.Equal(t, map[string]string{
		carrier.LabelPool: "shared", carrier.LabelGeneration: "generation-a",
	}, spread.LabelSelector.MatchLabels)
}

func TestCarrierReadinessRequiresRunningGateAndNeverStartedMain(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{carrier.LabelGeneration: "g"}, Annotations: map[string]string{carrier.AnnotationState: carrier.StateReady}},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Conditions:            []corev1.PodCondition{{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "carrier-wait", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "procd"}},
		},
	}
	assert.True(t, CarrierReady(pod, "g"))
	pod.Status.ContainerStatuses[0].State.Running = &corev1.ContainerStateRunning{}
	assert.False(t, CarrierReady(pod, "g"))
}

func TestCarrierUnusableRejectsExpiredGateAndStartedMain(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
		Labels:            map[string]string{carrier.LabelGeneration: "g"},
	}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	assert.True(t, carrierUnusable(pod, "g", 15*time.Second, time.Now()))
	pod.CreationTimestamp = metav1.Now()
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "procd", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	assert.True(t, carrierUnusable(pod, "g", 15*time.Second, time.Now()))
}

func TestCompatibleRoutesDynamicShapeToColdCarrier(t *testing.T) {
	tpl := &api.SandboxTemplate{Spec: api.SandboxTemplateSpec{MainContainer: api.ContainerSpec{
		Image: "ubuntu:24.04", Resources: api.ResourceQuota{CPU: resource.MustParse("1"), Memory: resource.MustParse("1Gi")},
	}}}
	compatible, reason := Compatible(tpl)
	assert.True(t, compatible, reason)
	tpl.Spec.EnvVars = map[string]string{"TOKEN": "value"}
	compatible, reason = Compatible(tpl)
	assert.False(t, compatible)
	assert.Equal(t, "dynamic_container_shape", reason)
}

func TestCompatibleRoutesNonStandardEphemeralStorageToColdCarrier(t *testing.T) {
	tpl := &api.SandboxTemplate{Spec: api.SandboxTemplateSpec{MainContainer: api.ContainerSpec{
		Image: "alpine:3.20", Resources: api.ResourceQuota{
			CPU: resource.MustParse("150m"), Memory: resource.MustParse("512Mi"), EphemeralStorage: resource.MustParse("2Gi"),
		},
	}}}
	compatible, reason := Compatible(tpl)
	assert.False(t, compatible)
	assert.Equal(t, "immutable_ephemeral_storage_shape", reason)

	tpl.Spec.MainContainer.Resources.EphemeralStorage = resource.MustParse(api.DefaultSandboxEphemeralStorage)
	compatible, reason = Compatible(tpl)
	assert.True(t, compatible, reason)
}
