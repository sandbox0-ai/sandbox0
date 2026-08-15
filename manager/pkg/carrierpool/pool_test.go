package carrierpool

import (
	"context"
	"errors"
	"testing"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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

func TestReconcileDoesNotDeleteConcurrentlyReservedCarrier(t *testing.T) {
	oldest := readyCarrierPod("carrier-oldest", "uid-oldest", "rv-oldest", time.Now().Add(-time.Minute))
	pruned := readyCarrierPod("carrier-pruned", "uid-pruned", "rv-pruned", time.Now())
	client := fake.NewSimpleClientset(oldest, pruned)
	pool, err := New(client, Config{
		Namespace: "sandbox0", ClusterID: "default", MinIdle: 0, MaxIdle: 1,
		CarrierImageRef: "sandbox0ai/infra:carrier-base-v1", Generation: "generation-a",
	}, nil)
	require.NoError(t, err)

	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		require.Equal(t, "carrier-pruned", deleteAction.GetName())
		preconditions := deleteAction.GetDeleteOptions().Preconditions
		require.NotNil(t, preconditions)
		require.NotNil(t, preconditions.UID)
		require.NotNil(t, preconditions.ResourceVersion)
		assert.Equal(t, types.UID("uid-pruned"), *preconditions.UID)
		assert.Equal(t, "rv-pruned", *preconditions.ResourceVersion)

		resource := corev1.SchemeGroupVersion.WithResource("pods")
		object, getErr := client.Tracker().Get(resource, "sandbox0", "carrier-pruned")
		require.NoError(t, getErr)
		reserved := object.(*corev1.Pod).DeepCopy()
		reserved.Annotations[carrier.AnnotationState] = carrier.StateReserved
		reserved.ResourceVersion = "rv-after-reserve"
		require.NoError(t, client.Tracker().Update(resource, reserved, "sandbox0"))
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "pods"}, reserved.Name, errors.New("resource version changed"),
		)
	})

	require.NoError(t, pool.Reconcile(context.Background()))
	current, err := client.CoreV1().Pods("sandbox0").Get(context.Background(), "carrier-pruned", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, carrier.StateReserved, current.Annotations[carrier.AnnotationState])
}

func TestReconcilePrunesUnchangedExcessCarrier(t *testing.T) {
	oldest := readyCarrierPod("carrier-oldest", "uid-oldest", "rv-oldest", time.Now().Add(-time.Minute))
	pruned := readyCarrierPod("carrier-pruned", "uid-pruned", "rv-pruned", time.Now())
	client := fake.NewSimpleClientset(oldest, pruned)
	pool, err := New(client, Config{
		Namespace: "sandbox0", ClusterID: "default", MinIdle: 0, MaxIdle: 1,
		CarrierImageRef: "sandbox0ai/infra:carrier-base-v1", Generation: "generation-a",
	}, nil)
	require.NoError(t, err)

	require.NoError(t, pool.Reconcile(context.Background()))
	_, err = client.CoreV1().Pods("sandbox0").Get(context.Background(), "carrier-pruned", metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "unchanged excess carrier should be pruned: %v", err)
}

func readyCarrierPod(name, uid, resourceVersion string, createdAt time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "sandbox0", UID: types.UID(uid), ResourceVersion: resourceVersion,
			CreationTimestamp: metav1.NewTime(createdAt),
			Labels:            map[string]string{carrier.LabelPool: "shared", carrier.LabelGeneration: "generation-a"},
			Annotations:       map[string]string{carrier.AnnotationState: carrier.StateReady},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Conditions:            []corev1.PodCondition{{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "carrier-wait", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "procd"}},
		},
	}
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
