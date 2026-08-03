package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

const testAutoscalerSafeToEvictAnnotation = "example.com/safe-to-evict"

func TestBuildPodTemplateIncludesTemplateHash(t *testing.T) {
	pm := &PoolManager{autoscalerAnnotationKeys: []string{testAutoscalerSafeToEvictAnnotation}}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels: map[string]string{
				LabelTemplateLogicalID: "logical-a",
			},
		},
	}

	got, err := pm.buildPodTemplate(template, "hash-v1")
	require.NoError(t, err)
	require.NotNil(t, got.Annotations)
	assert.Equal(t, "hash-v1", got.Annotations[AnnotationTemplateSpecHash])
	assert.Equal(t, "true", got.Annotations[testAutoscalerSafeToEvictAnnotation])
	assert.Equal(t, PoolTypeIdle, got.Labels[LabelPoolType])
	assert.Equal(t, "template-a", got.Labels[LabelTemplateID])
	assert.Equal(t, "logical-a", got.Labels[LabelTemplateLogicalID])
}

func TestAutoscalerSafeToEvictAnnotationKeysArePlatformConfigured(t *testing.T) {
	keys, err := NormalizeAutoscalerSafeToEvictAnnotationKeys([]string{
		" goatscaler.io/safe-to-evict ",
		"example.com/safe-to-evict",
		"goatscaler.io/safe-to-evict",
		"",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"example.com/safe-to-evict", "goatscaler.io/safe-to-evict"}, keys)

	annotations := ClaimedSandboxPodAnnotations(map[string]string{
		"goatscaler.io/safe-to-evict": "true",
	}, keys)
	assert.Equal(t, "false", annotations["goatscaler.io/safe-to-evict"])
	assert.Equal(t, "false", annotations["example.com/safe-to-evict"])

	_, err = NormalizeAutoscalerSafeToEvictAnnotationKeys([]string{"not a key"})
	require.Error(t, err)
}

func TestBuildPodTemplateAnnotatesTeamOwnedWarmPool(t *testing.T) {
	pm := &PoolManager{}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-template",
			Namespace: "tpl-team",
			Labels: map[string]string{
				LabelTemplateScope: naming.ScopeTeam,
			},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-1",
				AnnotationTemplateUserID: "user-1",
			},
		},
	}

	got, err := pm.buildPodTemplate(template, "hash-v1")
	require.NoError(t, err)
	assert.Equal(t, "team-1", got.Annotations[AnnotationTeamID])
	assert.Equal(t, "user-1", got.Annotations[AnnotationUserID])
	assert.Equal(t, OwnerKindTeamWarmPool, got.Annotations[AnnotationOwnerKind])
	assert.Equal(t, OwnerKindTeamWarmPool, got.Labels[LabelOwnerKind])
}

func TestBuildPodTemplatePreMountsUserVolumePortalsForIdlePool(t *testing.T) {
	pm := &PoolManager{}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "busybox"},
			VolumeMounts: []v1alpha1.VolumeMountSpec{{
				Name:      "data",
				MountPath: "/workspace/data",
			}},
		},
	}

	got, err := pm.buildPodTemplate(template, "hash-v1")
	require.NoError(t, err)
	assert.NotNil(t, findCSIVolumeByPortal(got.Spec.Volumes, "data"))
	assert.NotNil(t, findCSIVolumeByPortal(got.Spec.Volumes, volumeportal.WebhookStatePortalName))
}

func TestDesiredPoolReplicasUsesMinIdle(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{
				MinIdle: 15,
				MaxIdle: 50,
			},
		},
	}

	assert.Equal(t, int32(15), desiredPoolReplicas(template))
}

func TestReconcileReplicaSetReplicasAppliesFullScaleUp(t *testing.T) {
	ctx := context.Background()
	zero := int32(0)
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-template-a",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
			},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &zero},
	}
	client := fake.NewSimpleClientset(rs)

	pm := &PoolManager{
		k8sClient: client,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	requeueAfter, err := pm.reconcileReplicaSetReplicas(ctx, template, rs, 50)
	require.NoError(t, err)
	assert.Zero(t, requeueAfter)
	stored, err := client.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(50), getInt32Value(stored.Spec.Replicas))

	// Reconcile with the original stale ReplicaSet object. The live read must keep
	// the already-applied target instead of writing from stale state.
	requeueAfter, err = pm.reconcileReplicaSetReplicas(ctx, template, rs, 50)
	require.NoError(t, err)
	assert.Zero(t, requeueAfter)
	stored, err = client.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(50), getInt32Value(stored.Spec.Replicas))
}

func TestUpdateReplicaSetReplicasRetriesConflict(t *testing.T) {
	replicas := int32(3)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-template-a", Namespace: "default"},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
		},
	}
	client := fake.NewSimpleClientset(rs)
	updates := 0
	client.PrependReactor("update", "replicasets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "replicasets"}, rs.Name, errors.New("stale replicaset"))
		}
		return false, nil, nil
	})
	pm := &PoolManager{k8sClient: client, logger: zap.NewNop()}

	updated, err := pm.updateReplicaSetReplicas(context.Background(), rs.Namespace, rs.Name, 15)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 2, updates)
	assert.Equal(t, int32(15), getInt32Value(updated.Spec.Replicas))
}

func findCSIVolumeByPortal(volumes []corev1.Volume, portalName string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].CSI == nil {
			continue
		}
		if volumes[i].CSI.VolumeAttributes[volumeportal.AttributePortalName] == portalName {
			return &volumes[i]
		}
	}
	return nil
}

func TestDrainStaleIdlePodsUsesDeletePreconditions(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}

	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-stale",
			Namespace:       "default",
			UID:             types.UID("uid-stale"),
			ResourceVersion: "11",
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
			},
		},
	}
	freshPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-fresh",
			Namespace:       "default",
			UID:             types.UID("uid-fresh"),
			ResourceVersion: "12",
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "new-hash",
			},
		},
	}

	client := fake.NewSimpleClientset([]runtime.Object{stalePod, freshPod}...)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(stalePod))
	require.NoError(t, podIndexer.Add(freshPod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		delAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		deleteActions++
		opts := delAction.GetDeleteOptions()
		require.NotNil(t, opts.Preconditions)
		require.NotNil(t, opts.Preconditions.UID)
		require.NotNil(t, opts.Preconditions.ResourceVersion)
		assert.Equal(t, types.UID("uid-stale"), *opts.Preconditions.UID)
		assert.Equal(t, "11", *opts.Preconditions.ResourceVersion)
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	rolloutPending, err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.True(t, rolloutPending)
	assert.Equal(t, 1, deleteActions)
}

func TestDrainStaleIdlePodsSkipsAlreadyDeletingPod(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}
	deletedAt := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "idle-stale",
			Namespace:         "default",
			UID:               types.UID("uid-stale"),
			ResourceVersion:   "11",
			DeletionTimestamp: &deletedAt,
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
			},
		},
	}

	client := fake.NewSimpleClientset(stalePod)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(stalePod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		deleteActions++
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	rolloutPending, err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.True(t, rolloutPending)
	assert.Equal(t, 0, deleteActions)
}

func TestDrainStaleIdlePodsSkipsPodDeletingAfterList(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-stale",
			Namespace:       "default",
			UID:             types.UID("uid-stale"),
			ResourceVersion: "11",
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
			},
		},
	}
	deletingPod := stalePod.DeepCopy()
	deletedAt := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	deletingPod.DeletionTimestamp = &deletedAt

	client := fake.NewSimpleClientset(deletingPod)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(stalePod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		deleteActions++
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	rolloutPending, err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.True(t, rolloutPending)
	assert.Equal(t, 0, deleteActions)
}

func TestDrainStaleIdlePodsSkipsClaimedActivePods(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}

	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "active-old",
			Namespace:       "default",
			UID:             types.UID("uid-active"),
			ResourceVersion: "21",
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeActive,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
			},
		},
	}

	client := fake.NewSimpleClientset(activePod)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(activePod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		deleteActions++
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	rolloutPending, err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.False(t, rolloutPending)
	assert.Equal(t, 0, deleteActions)
}

func TestWarmPoolRolloutMaxUnavailable(t *testing.T) {
	tests := []struct {
		name     string
		desired  int32
		expected int32
	}{
		{name: "empty pool", desired: 0, expected: 1},
		{name: "small pool", desired: 3, expected: 1},
		{name: "twenty pod pool", desired: 20, expected: 2},
		{name: "large pool", desired: 180, expected: 10},
		{name: "capped pool", desired: 1000, expected: 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, warmPoolRolloutMaxUnavailable(test.desired))
		})
	}
}

func TestReconcileIdlePoolDisruptionBudgetProtectsReadyPodsAndAllowsUnhealthyEviction(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			UID:       types.UID("template-uid"),
		},
		Spec: v1alpha1.SandboxTemplateSpec{Pool: v1alpha1.PoolStrategy{MinIdle: 20}},
	}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "rs-default-template-a", Namespace: "default"}}
	client := fake.NewSimpleClientset()
	pm := &PoolManager{k8sClient: client, recorder: record.NewFakeRecorder(10), logger: zap.NewNop()}

	require.NoError(t, pm.reconcileIdlePoolDisruptionBudget(context.Background(), template, rs))
	pdb, err := client.PolicyV1().PodDisruptionBudgets("default").Get(context.Background(), rs.Name+"-idle-pdb", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, pdb.Spec.MaxUnavailable)
	assert.Equal(t, intstr.FromInt32(2), *pdb.Spec.MaxUnavailable)
	require.NotNil(t, pdb.Spec.UnhealthyPodEvictionPolicy)
	assert.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy)
	assert.Equal(t, map[string]string{
		LabelTemplateID: "template-a",
		LabelPoolType:   PoolTypeIdle,
	}, pdb.Spec.Selector.MatchLabels)
	require.Len(t, pdb.OwnerReferences, 1)
	assert.Equal(t, template.UID, pdb.OwnerReferences[0].UID)

	template.Spec.Pool.MinIdle = 180
	require.NoError(t, pm.reconcileIdlePoolDisruptionBudget(context.Background(), template, rs))
	pdb, err = client.PolicyV1().PodDisruptionBudgets("default").Get(context.Background(), rs.Name+"-idle-pdb", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, intstr.FromInt32(10), *pdb.Spec.MaxUnavailable)
}

func TestReconcileClaimedPodAutoscalerAnnotationsMigratesExistingPods(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"}}
	active := idlePoolPod("active", types.UID("uid-active"), "hash")
	active.Labels[LabelPoolType] = PoolTypeActive
	reserved := idlePoolPod("reserved", types.UID("uid-reserved"), "hash")
	reserved.Annotations[AnnotationHotClaimReservation] = "reservation"
	idle := idlePoolPod("idle", types.UID("uid-idle"), "hash")
	client := fake.NewSimpleClientset(active, reserved, idle)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, pod := range []*corev1.Pod{active, reserved, idle} {
		require.NoError(t, indexer.Add(pod))
	}
	pm := &PoolManager{
		k8sClient:                client,
		podLister:                corelisters.NewPodLister(indexer),
		recorder:                 record.NewFakeRecorder(10),
		logger:                   zap.NewNop(),
		autoscalerAnnotationKeys: []string{testAutoscalerSafeToEvictAnnotation},
	}

	pending, err := pm.reconcileClaimedPodAutoscalerAnnotations(context.Background(), template)
	require.NoError(t, err)
	assert.False(t, pending)
	for _, name := range []string{"active", "reserved"} {
		pod, err := client.CoreV1().Pods("default").Get(context.Background(), name, metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, "false", pod.Annotations[testAutoscalerSafeToEvictAnnotation])
	}
	pod, err := client.CoreV1().Pods("default").Get(context.Background(), "idle", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, pod.Annotations[testAutoscalerSafeToEvictAnnotation])
}

func TestReconcileClaimedPodAutoscalerAnnotationsUsesBoundedBatches(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"}}
	objects := make([]runtime.Object, 0, claimedPodAnnotationReconcileBatchSize+1)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for i := 0; i < claimedPodAnnotationReconcileBatchSize+1; i++ {
		pod := idlePoolPod(fmt.Sprintf("active-%02d", i), types.UID(fmt.Sprintf("uid-active-%02d", i)), "hash")
		pod.Labels[LabelPoolType] = PoolTypeActive
		objects = append(objects, pod)
		require.NoError(t, indexer.Add(pod))
	}
	client := fake.NewSimpleClientset(objects...)
	updates := 0
	client.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		return false, nil, nil
	})
	pm := &PoolManager{
		k8sClient:                client,
		podLister:                corelisters.NewPodLister(indexer),
		recorder:                 record.NewFakeRecorder(100),
		logger:                   zap.NewNop(),
		autoscalerAnnotationKeys: []string{testAutoscalerSafeToEvictAnnotation},
	}

	pending, err := pm.reconcileClaimedPodAutoscalerAnnotations(context.Background(), template)
	require.NoError(t, err)
	assert.True(t, pending)
	assert.Equal(t, claimedPodAnnotationReconcileBatchSize, updates)
}

func TestDrainStaleIdlePodsLimitsRolloutBatch(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 20},
		},
	}
	objects := make([]runtime.Object, 0, 20)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for i := 0; i < 20; i++ {
		pod := readyIdlePoolPod(
			fmt.Sprintf("idle-stale-%02d", i),
			types.UID(fmt.Sprintf("uid-%02d", i)),
			"old-hash",
		)
		objects = append(objects, pod)
		require.NoError(t, podIndexer.Add(pod))
	}
	client := fake.NewSimpleClientset(objects...)
	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteActions++
		return false, nil, nil
	})
	pm := &PoolManager{
		k8sClient: client,
		podLister: corelisters.NewPodLister(podIndexer),
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	rolloutPending, err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.True(t, rolloutPending)
	assert.Equal(t, 2, deleteActions)
}

func TestDrainStaleIdlePodsWaitsForReplacementReadiness(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 20},
		},
	}
	objects := make([]runtime.Object, 0, 20)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for i := 0; i < 18; i++ {
		pod := readyIdlePoolPod(
			fmt.Sprintf("idle-stale-%02d", i),
			types.UID(fmt.Sprintf("uid-stale-%02d", i)),
			"old-hash",
		)
		objects = append(objects, pod)
		require.NoError(t, podIndexer.Add(pod))
	}
	for i := 0; i < 2; i++ {
		pod := idlePoolPod(
			fmt.Sprintf("idle-replacement-%02d", i),
			types.UID(fmt.Sprintf("uid-replacement-%02d", i)),
			"new-hash",
		)
		objects = append(objects, pod)
		require.NoError(t, podIndexer.Add(pod))
	}
	client := fake.NewSimpleClientset(objects...)
	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteActions++
		return false, nil, nil
	})
	pm := &PoolManager{
		k8sClient: client,
		podLister: corelisters.NewPodLister(podIndexer),
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	rolloutPending, err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.True(t, rolloutPending)
	assert.Equal(t, 0, deleteActions)
}

func idlePoolPod(name string, uid types.UID, templateHash string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "default",
			UID:             uid,
			ResourceVersion: string(uid),
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: templateHash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
}

func readyIdlePoolPod(name string, uid types.UID, templateHash string) *corev1.Pod {
	pod := idlePoolPod(name, uid, templateHash)
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}},
	}
	return pod
}

func TestRepairUnhealthyIdlePodsDeletesStuckCurrentHashIdlePod(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}

	stuckPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "idle-stuck",
			Namespace:         "default",
			UID:               types.UID("uid-stuck"),
			ResourceVersion:   "31",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-unhealthyIdlePodRepairGracePeriod - time.Second)),
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "new-hash",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	readyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "idle-ready",
			Namespace:         "default",
			UID:               types.UID("uid-ready"),
			ResourceVersion:   "32",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-unhealthyIdlePodRepairGracePeriod - time.Second)),
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "new-hash",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	client := fake.NewSimpleClientset(stuckPod, readyPod)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(stuckPod))
	require.NoError(t, podIndexer.Add(readyPod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		delAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		deleteActions++
		opts := delAction.GetDeleteOptions()
		require.NotNil(t, opts.Preconditions)
		require.NotNil(t, opts.Preconditions.UID)
		require.NotNil(t, opts.Preconditions.ResourceVersion)
		assert.Equal(t, types.UID("uid-stuck"), *opts.Preconditions.UID)
		assert.Equal(t, "31", *opts.Preconditions.ResourceVersion)
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	_, err := pm.repairUnhealthyIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.Equal(t, 1, deleteActions)
}

func TestRepairUnhealthyIdlePodsKeepsRecentlyCreatedPod(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}

	recentPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "idle-recent",
			Namespace:         "default",
			UID:               types.UID("uid-recent"),
			ResourceVersion:   "41",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Labels: map[string]string{
				LabelTemplateID: "template-a",
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "new-hash",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	client := fake.NewSimpleClientset(recentPod)
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, podIndexer.Add(recentPod))
	podLister := corelisters.NewPodLister(podIndexer)

	deleteActions := 0
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		deleteActions++
		return false, nil, nil
	})

	pm := &PoolManager{
		k8sClient: client,
		podLister: podLister,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	requeueAfter, err := pm.repairUnhealthyIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.Equal(t, 0, deleteActions)
	assert.Greater(t, requeueAfter, time.Duration(0))
	assert.LessOrEqual(t, requeueAfter, unhealthyIdlePodRepairGracePeriod)
}

func TestReconcileReplicaSetTemplateUpdatesHash(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
	}
	replicas := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-template-a",
			Namespace: "default",
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelTemplateID: "template-a",
						LabelPoolType:   PoolTypeIdle,
					},
					Annotations: map[string]string{
						AnnotationTemplateSpecHash: "old-hash",
					},
				},
				Spec: corev1.PodSpec{},
			},
		},
	}

	client := fake.NewSimpleClientset(rs)
	pm := &PoolManager{
		k8sClient: client,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	updated, err := pm.reconcileReplicaSetTemplate(context.Background(), template, rs, "new-hash")
	require.NoError(t, err)
	require.NotNil(t, updated.Spec.Template.Annotations)
	assert.Equal(t, "new-hash", updated.Spec.Template.Annotations[AnnotationTemplateSpecHash])
}

func TestGetOrCreateReplicaSetAdoptsExistingReplicaSetForRecreatedTemplate(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "internal_jwt_public.key")
	require.NoError(t, os.WriteFile(keyPath, []byte("public-key"), 0o600))
	previousPath := internalauth.DefaultInternalJWTPublicKeyPath
	internalauth.DefaultInternalJWTPublicKeyPath = keyPath
	t.Cleanup(func() {
		internalauth.DefaultInternalJWTPublicKeyPath = previousPath
	})

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			UID:       types.UID("new-template"),
		},
	}
	rsName, err := naming.ReplicasetName(naming.DefaultClusterID, template.Name)
	require.NoError(t, err)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "SandboxTemplate",
					Name:       template.Name,
					UID:        types.UID("old-template"),
					Controller: boolPtr(true),
				},
			},
		},
	}

	client := fake.NewSimpleClientset(rs)
	rsIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, rsIndexer.Add(rs))
	secretIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	pm := &PoolManager{
		k8sClient:        client,
		replicaSetLister: appslisters.NewReplicaSetLister(rsIndexer),
		secretLister:     corelisters.NewSecretLister(secretIndexer),
		recorder:         record.NewFakeRecorder(10),
		logger:           zap.NewNop(),
	}

	got, err := pm.getOrCreateReplicaSet(context.Background(), template)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, types.UID("new-template"), got.OwnerReferences[0].UID)

	stored, err := client.AppsV1().ReplicaSets(template.Namespace).Get(context.Background(), rsName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, stored.OwnerReferences, 1)
	assert.Equal(t, types.UID("new-template"), stored.OwnerReferences[0].UID)
}

func TestTemplateSpecHashIncludesManagerInjectedPlacement(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image: "busybox:latest",
			},
		},
	}

	configA := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
sandbox_pod_placement:
  node_selector:
    sandbox0.ai/node-role: sandbox-a
`)
	t.Setenv("CONFIG_PATH", configA)

	hashA, err := TemplateSpecHash(template)
	require.NoError(t, err)

	configB := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
sandbox_pod_placement:
  node_selector:
    sandbox0.ai/node-role: sandbox-b
`)
	t.Setenv("CONFIG_PATH", configB)

	hashB, err := TemplateSpecHash(template)
	require.NoError(t, err)

	assert.NotEqual(t, hashA, hashB)
}

func TestTemplateSpecHashIncludesPlatformAutoscalerAnnotationKeys(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
		Spec:       v1alpha1.SandboxTemplateSpec{MainContainer: v1alpha1.ContainerSpec{Image: "busybox:latest"}},
	}

	configA := writeManagerConfig(t, `
autoscaler_safe_to_evict_annotation_keys:
  - cluster-autoscaler.kubernetes.io/safe-to-evict
`)
	t.Setenv("CONFIG_PATH", configA)
	hashA, err := TemplateSpecHash(template)
	require.NoError(t, err)

	configB := writeManagerConfig(t, `
autoscaler_safe_to_evict_annotation_keys:
  - goatscaler.io/safe-to-evict
`)
	t.Setenv("CONFIG_PATH", configB)
	hashB, err := TemplateSpecHash(template)
	require.NoError(t, err)
	assert.NotEqual(t, hashA, hashB)
}

func writeManagerConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(contents), 0o600)
	require.NoError(t, err)
	return path
}

func boolPtr(v bool) *bool {
	return &v
}
