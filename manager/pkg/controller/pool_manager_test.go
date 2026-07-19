package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func TestBuildPodTemplateIncludesTemplateHash(t *testing.T) {
	pm := &PoolManager{}
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
	assert.Equal(t, "true", got.Annotations[AnnotationClusterAutoscalerSafeToEvict])
	assert.Equal(t, naming.DefaultClusterID, got.Annotations[AnnotationClusterID])
	assert.Equal(t, PoolTypeIdle, got.Labels[LabelPoolType])
	assert.Equal(t, "template-a", got.Labels[LabelTemplateID])
	assert.Equal(t, "logical-a", got.Labels[LabelTemplateLogicalID])
}

func TestOperatorValidateTeamQuotaReadyFailsClosed(t *testing.T) {
	operator := &Operator{poolManager: &PoolManager{}}
	require.Error(t, operator.ValidateTeamQuotaReady())

	operator.SetTeamQuotaStore(teamquota.NewRepository(nil), func(*corev1.PodSpec) teamquota.Values {
		return nil
	})
	require.Error(t, operator.ValidateTeamQuotaReady())

	operator.SetTeamQuotaRateLimiter(&recordingTeamQuotaRateLimiter{})
	operator.SetClaimStartLimiter(&startlimiter.Limiter{})
	require.NoError(t, operator.ValidateTeamQuotaReady())
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

func TestValidateTeamOwnedTemplateFailsClosedOnInvalidProjection(t *testing.T) {
	teamNamespace, err := naming.TemplateNamespaceForTeam("team-1")
	require.NoError(t, err)
	validName := naming.TemplateNameForCluster(naming.ScopeTeam, "team-1", "logical-a")

	tests := []struct {
		name      string
		template  *v1alpha1.SandboxTemplate
		wantOwner string
		wantError string
	}{
		{
			name: "public template has no team owner",
			template: &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
				Name:   "public-a",
				Labels: map[string]string{LabelTemplateScope: naming.ScopePublic},
			}},
		},
		{
			name: "missing owner annotation",
			template: &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
				Name:      validName,
				Namespace: teamNamespace,
				Labels: map[string]string{
					LabelTemplateScope:     naming.ScopeTeam,
					LabelTemplateLogicalID: "logical-a",
				},
			}},
			wantError: AnnotationTemplateTeamID,
		},
		{
			name: "owner namespace mismatch",
			template: &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
				Name:      validName,
				Namespace: "wrong-namespace",
				Labels: map[string]string{
					LabelTemplateScope:     naming.ScopeTeam,
					LabelTemplateLogicalID: "logical-a",
				},
				Annotations: map[string]string{AnnotationTemplateTeamID: "team-1"},
			}},
			wantError: "does not match owner",
		},
		{
			name: "owner name mismatch",
			template: &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
				Name:      "wrong-name",
				Namespace: teamNamespace,
				Labels: map[string]string{
					LabelTemplateScope:     naming.ScopeTeam,
					LabelTemplateLogicalID: "logical-a",
				},
				Annotations: map[string]string{AnnotationTemplateTeamID: "team-1"},
			}},
			wantError: "does not match owner",
		},
		{
			name: "valid team projection",
			template: &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
				Name:      validName,
				Namespace: teamNamespace,
				Labels: map[string]string{
					LabelTemplateScope:     naming.ScopeTeam,
					LabelTemplateLogicalID: "logical-a",
				},
				Annotations: map[string]string{AnnotationTemplateTeamID: "team-1"},
			}},
			wantOwner: "team-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner, err := ValidateTeamOwnedTemplate(test.template)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				assert.Empty(t, owner)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantOwner, owner)
		})
	}
}

func TestReconcilePoolScalesMalformedTeamTemplateToZero(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-template",
			Namespace: "team-namespace",
			Labels:    map[string]string{LabelTemplateScope: naming.ScopeTeam},
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{MinIdle: 3},
		},
	}
	rsName, err := naming.ReplicasetName(naming.DefaultClusterID, template.Name)
	require.NoError(t, err)
	replicas := int32(3)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: template.Namespace},
		Spec:       appsv1.ReplicaSetSpec{Replicas: &replicas},
	}
	client := fake.NewSimpleClientset(rs)
	pm := &PoolManager{
		k8sClient: client,
		recorder:  record.NewFakeRecorder(10),
		logger:    zap.NewNop(),
	}

	_, err = pm.ReconcilePool(context.Background(), template)
	require.ErrorContains(t, err, AnnotationTemplateTeamID)
	stored, getErr := client.AppsV1().
		ReplicaSets(template.Namespace).
		Get(context.Background(), rsName, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Zero(t, getInt32Value(stored.Spec.Replicas))
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

func TestReconcileReplicaSetReplicasBatchesScaleUpAndRequeues(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	zero := int32(0)
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels: map[string]string{
				LabelTemplateScope: naming.ScopeTeam,
			},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
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
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-a"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionTrue,
		}}},
	}
	client := fake.NewSimpleClientset(node, rs)
	limiter, err := startlimiter.New(ctx, startlimiter.Config{
		ClusterID:      "cluster-a",
		K8sClient:      client,
		PerSandboxNode: 30,
		MaxLimit:       30,
		Redis: rediscache.Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "test",
		},
	})
	require.NoError(t, err)

	rateLimiter := &recordingTeamQuotaRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	quotaStore := &recordingPoolCapacityStore{}
	pm := &PoolManager{
		k8sClient:         client,
		recorder:          record.NewFakeRecorder(10),
		logger:            zap.NewNop(),
		claimStartLimiter: limiter,
		teamQuotaStore:    quotaStore,
		teamQuotaLimiter:  rateLimiter,
		quotaResources:    podSpecQuotaResourcesForTest,
	}

	requeueAfter, err := pm.reconcileReplicaSetReplicas(ctx, template, rs, 50)
	require.NoError(t, err)
	assert.Equal(t, time.Second, requeueAfter)
	stored, err := client.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(30), getInt32Value(stored.Spec.Replicas))
	assert.Equal(t, []int64{30}, rateLimiter.costs)
	require.Len(t, quotaStore.reservations, 1)
	assert.Equal(t, int64(30), quotaStore.reservations[0].Target[teamquota.KeySandboxRuntimeCount])

	// Reconcile with the original stale ReplicaSet object. The live read must keep
	// the first batch at 30 instead of scaling it back down.
	requeueAfter, err = pm.reconcileReplicaSetReplicas(ctx, template, rs, 50)
	require.NoError(t, err)
	assert.Equal(t, time.Second, requeueAfter)
	stored, err = client.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(30), getInt32Value(stored.Spec.Replicas))
	assert.Equal(t, []int64{30}, rateLimiter.costs, "no cluster capacity must consume no team token")
	require.Len(t, quotaStore.reservations, 1)

	for i := 0; i < 30; i++ {
		_, err := client.CoreV1().Pods(template.Namespace).Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("idle-%02d", i),
				Namespace: template.Namespace,
				Labels: map[string]string{
					LabelTemplateID: template.Name,
					LabelPoolType:   PoolTypeIdle,
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	requeueAfter, err = pm.reconcileReplicaSetReplicas(ctx, template, rs, 50)
	require.NoError(t, err)
	assert.Zero(t, requeueAfter)
	stored, err = client.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(50), getInt32Value(stored.Spec.Replicas))
	assert.Equal(t, []int64{30, 20}, rateLimiter.costs)
	require.Len(t, quotaStore.reservations, 2)
	assert.Equal(t, int64(50), quotaStore.reservations[1].Target[teamquota.KeySandboxRuntimeCount])
}

func TestReconcileReplicaSetReplicasAppliesTeamStartRateAdmission(t *testing.T) {
	tests := []struct {
		name           string
		teamOwned      bool
		decision       tokenbucket.Decision
		limiterErr     error
		wantRetryAfter time.Duration
		wantErr        bool
		wantReplicas   int32
		wantCosts      []int64
	}{
		{
			name:         "allowed exact submitted delta",
			teamOwned:    true,
			decision:     tokenbucket.Decision{Allowed: true},
			wantReplicas: 5,
			wantCosts:    []int64{3},
		},
		{
			name:           "denied requeues without updating",
			teamOwned:      true,
			decision:       tokenbucket.Decision{RetryAfter: 2 * time.Second},
			wantRetryAfter: 2 * time.Second,
			wantReplicas:   2,
			wantCosts:      []int64{3},
		},
		{
			name:         "backend error fails closed",
			teamOwned:    true,
			limiterErr:   errors.New("Redis unavailable"),
			wantErr:      true,
			wantReplicas: 2,
			wantCosts:    []int64{3},
		},
		{
			name:         "public warm pool bypasses team limiter",
			decision:     tokenbucket.Decision{Allowed: true},
			wantReplicas: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			current := int32(2)
			template := &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
			}
			if test.teamOwned {
				template.Labels = map[string]string{LabelTemplateScope: naming.ScopeTeam}
				template.Annotations = map[string]string{AnnotationTemplateTeamID: "team-a"}
			}
			rs := &appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{Name: "rs-template-a", Namespace: template.Namespace},
				Spec:       appsv1.ReplicaSetSpec{Replicas: &current},
			}
			client := fake.NewSimpleClientset(rs)
			rateLimiter := &recordingTeamQuotaRateLimiter{
				decision: test.decision,
				err:      test.limiterErr,
			}
			quotaStore := &recordingPoolCapacityStore{}
			pm := &PoolManager{
				k8sClient:        client,
				recorder:         record.NewFakeRecorder(10),
				logger:           zap.NewNop(),
				teamQuotaStore:   quotaStore,
				teamQuotaLimiter: rateLimiter,
				quotaResources:   podSpecQuotaResourcesForTest,
			}

			requeueAfter, err := pm.reconcileReplicaSetReplicas(ctx, template, rs, 5)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.wantRetryAfter, requeueAfter)
			stored, getErr := client.AppsV1().
				ReplicaSets(template.Namespace).
				Get(ctx, rs.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, test.wantReplicas, getInt32Value(stored.Spec.Replicas))
			assert.Equal(t, test.wantCosts, rateLimiter.costs)
			for _, key := range rateLimiter.keys {
				assert.Equal(t, teamquota.KeySandboxStarts, key)
			}
			if test.teamOwned && test.wantReplicas > current {
				if test.wantErr || !test.decision.Allowed {
					assert.Empty(t, quotaStore.reservations)
				} else {
					require.Len(t, quotaStore.reservations, 1)
					assert.Equal(
						t,
						int64(test.wantReplicas),
						quotaStore.reservations[0].Target[teamquota.KeySandboxRuntimeCount],
					)
				}
			}
		})
	}
}

func TestReconcileReplicaSetReplicasScalesOneWhenTeamRateBurstIsOne(t *testing.T) {
	ctx := context.Background()
	current := int32(0)
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels: map[string]string{
				LabelTemplateScope: naming.ScopeTeam,
			},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-template-a", Namespace: template.Namespace},
		Spec:       appsv1.ReplicaSetSpec{Replicas: &current},
	}
	client := fake.NewSimpleClientset(rs)
	rateLimiter := &burstOneTeamQuotaRateLimiter{}
	quotaStore := &recordingPoolCapacityStore{}
	pm := &PoolManager{
		k8sClient:        client,
		recorder:         record.NewFakeRecorder(10),
		logger:           zap.NewNop(),
		teamQuotaStore:   quotaStore,
		teamQuotaLimiter: rateLimiter,
		quotaResources:   podSpecQuotaResourcesForTest,
	}

	requeueAfter, err := pm.reconcileReplicaSetReplicas(ctx, template, rs, 3)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, requeueAfter)
	stored, err := client.AppsV1().
		ReplicaSets(template.Namespace).
		Get(ctx, rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(1), getInt32Value(stored.Spec.Replicas))
	assert.Equal(t, []int64{3, 1, 1}, rateLimiter.costs)
	require.Len(t, quotaStore.reservations, 1)
	assert.Equal(t, int64(1), quotaStore.reservations[0].Target[teamquota.KeySandboxRuntimeCount])
}

func TestReconcileReplicaSetReplicasFailsClosedWhenTeamQuotaIsNotWired(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*PoolManager)
		wantDetail string
	}{
		{
			name:       "capacity missing",
			wantDetail: "capacity accounting is not configured",
		},
		{
			name: "rate limiter missing",
			configure: func(pm *PoolManager) {
				pm.teamQuotaStore = &recordingPoolCapacityStore{}
				pm.quotaResources = podSpecQuotaResourcesForTest
			},
			wantDetail: "rate limiter is not configured",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := int32(0)
			template := &v1alpha1.SandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "template-a",
					Namespace: "default",
					Labels: map[string]string{
						LabelTemplateScope: naming.ScopeTeam,
					},
					Annotations: map[string]string{
						AnnotationTemplateTeamID: "team-a",
					},
				},
			}
			rs := &appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{Name: "rs-template-a", Namespace: "default"},
				Spec:       appsv1.ReplicaSetSpec{Replicas: &current},
			}
			client := fake.NewSimpleClientset(rs)
			pm := &PoolManager{
				k8sClient: client,
				recorder:  record.NewFakeRecorder(10),
				logger:    zap.NewNop(),
			}
			if test.configure != nil {
				test.configure(pm)
			}

			_, err := pm.reconcileReplicaSetReplicas(context.Background(), template, rs, 1)
			require.ErrorContains(t, err, test.wantDetail)
			stored, getErr := client.AppsV1().
				ReplicaSets(rs.Namespace).
				Get(context.Background(), rs.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, int32(0), getInt32Value(stored.Spec.Replicas))
		})
	}
}

func podSpecQuotaResourcesForTest(*corev1.PodSpec) teamquota.Values {
	return teamquota.Values{
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
}

func TestTeamWarmPoolQuotaTargetAddsTerminatingPodsToDesiredCommitment(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels:    map[string]string{LabelTemplateScope: naming.ScopeTeam},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "new-image"},
		},
	}
	deletedAt := metav1.NewTime(time.Now())
	pods := []runtime.Object{
		warmPoolQuotaTestPod(template, "active-old", "old-image", nil),
		warmPoolQuotaTestPod(template, "active-new", "new-image", nil),
		warmPoolQuotaTestPod(template, "terminating-old", "terminating-image", &deletedAt),
	}
	pm := &PoolManager{
		k8sClient: fake.NewSimpleClientset(pods...),
		quotaResources: func(spec *corev1.PodSpec) teamquota.Values {
			memory := int64(0)
			if spec != nil && len(spec.Containers) > 0 {
				switch spec.Containers[0].Image {
				case "old-image":
					memory = 5
				case "new-image":
					memory = 20
				case "terminating-image":
					memory = 7
				}
			}
			return teamquota.Values{teamquota.KeySandboxMemoryBytes: memory}
		},
	}

	target, err := pm.teamWarmPoolQuotaTarget(context.Background(), template, 2)

	require.NoError(t, err)
	assert.Equal(t, int64(3), target[teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, int64(47), target[teamquota.KeySandboxMemoryBytes])
}

func TestTeamWarmPoolQuotaTargetIncludesRuntimeClassOverhead(t *testing.T) {
	const runtimeClassName = "quota-runtime"
	t.Setenv("CONFIG_PATH", writeManagerConfig(t, "sandbox_runtime_class_name: "+runtimeClassName+"\n"))
	runtimeClass := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "sandbox",
		Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("75m"),
			corev1.ResourceMemory: resource.MustParse("96Mi"),
		}},
	}
	template := warmPoolRuntimeClassTestTemplate()
	pm := &PoolManager{
		k8sClient:      fake.NewSimpleClientset(runtimeClass),
		quotaResources: overheadOnlyQuotaResources,
	}

	target, err := pm.teamWarmPoolQuotaTarget(context.Background(), template, 2)

	require.NoError(t, err)
	assert.Equal(t, int64(2), target[teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, int64(150), target[teamquota.KeySandboxCPUMillicores])
	wantMemory := resource.MustParse("192Mi")
	assert.Equal(t, wantMemory.Value(), target[teamquota.KeySandboxMemoryBytes])
	submitted := v1alpha1.BuildIdlePodSpec(template)
	assert.Nil(t, submitted.Overhead, "RuntimeClass overhead must only exist in the quota copy")
}

func TestWarmPoolReplacementQuotaUsesNewRuntimeClassOverhead(t *testing.T) {
	const runtimeClassName = "quota-runtime"
	t.Setenv("CONFIG_PATH", writeManagerConfig(t, "sandbox_runtime_class_name: "+runtimeClassName+"\n"))
	runtimeClass := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "sandbox",
		Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("75m"),
		}},
	}
	template := warmPoolRuntimeClassTestTemplate()
	oldPod := &corev1.Pod{Spec: corev1.PodSpec{
		Overhead: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("10m"),
		},
	}}
	store := &recordingPoolCapacityStore{}
	pm := &PoolManager{
		k8sClient:      fake.NewSimpleClientset(runtimeClass),
		teamQuotaStore: store,
		quotaResources: overheadOnlyQuotaResources,
	}

	reservation, err := pm.reserveTeamWarmPoolReplacementQuota(
		context.Background(),
		template,
		oldPod,
	)

	require.NoError(t, err)
	require.NotNil(t, reservation)
	require.Len(t, store.deltas, 1)
	assert.Equal(t, int64(1), store.deltas[0].Delta[teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, int64(75), store.deltas[0].Delta[teamquota.KeySandboxCPUMillicores])
}

func TestWarmPoolObservedOverheadCommitsExactBeforeScaleAndDelete(t *testing.T) {
	runtimeClassName := "quota-runtime"
	t.Setenv("CONFIG_PATH", writeManagerConfig(t, "sandbox_runtime_class_name: "+runtimeClassName+"\n"))
	template := warmPoolRuntimeClassTestTemplate()
	owner, ok := TeamWarmPoolQuotaOwner(template)
	require.True(t, ok)
	replicas := int32(1)
	rsName, err := naming.ReplicasetName(naming.DefaultClusterID, template.Name)
	require.NoError(t, err)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			UID:       types.UID("rs-uid"),
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &replicas},
	}
	templateHash, err := TemplateSpecHash(template)
	require.NoError(t, err)
	isController := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-observed-overhead",
			Namespace:       template.Namespace,
			UID:             types.UID("pod-uid"),
			ResourceVersion: "7",
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: templateHash,
				AnnotationTeamID:           owner.TeamID,
				AnnotationOwnerKind:        OwnerKindTeamWarmPool,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       rs.Name,
				UID:        rs.UID,
				Controller: &isController,
			}},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClassName,
			Overhead: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("150m"),
			},
		},
	}
	runtimeClass := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "sandbox",
		Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("50m"),
		}},
	}
	operation := teamquota.Operation{ID: "scale-op", Kind: "scale_warm_pool"}
	events := []string{}
	store := &recordingPoolCapacityStore{
		events: &events,
		allocation: &teamquota.RecoveryAllocation{
			Owner:     owner,
			Revision:  4,
			Committed: teamquota.Values{},
			Pending: teamquota.Values{
				teamquota.KeySandboxRuntimeCount:  1,
				teamquota.KeySandboxCPUMillicores: 50,
			},
			Operation: &operation,
		},
	}
	client := fake.NewSimpleClientset(runtimeClass, rs, pod)
	client.PrependReactor("update", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		events = append(events, "scale")
		return false, nil, nil
	})
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		preconditions := deleteAction.GetDeleteOptions().Preconditions
		require.NotNil(t, preconditions)
		require.NotNil(t, preconditions.UID)
		require.NotNil(t, preconditions.ResourceVersion)
		assert.Equal(t, pod.UID, *preconditions.UID)
		assert.Equal(t, pod.ResourceVersion, *preconditions.ResourceVersion)
		events = append(events, "delete")
		return false, nil, nil
	})
	pm := &PoolManager{
		k8sClient:      client,
		teamQuotaStore: store,
		quotaResources: overheadOnlyQuotaResources,
		recorder:       record.NewFakeRecorder(10),
		logger:         zap.NewNop(),
	}

	handled, err := pm.reconcileCommittedTeamWarmPoolQuota(context.Background(), template)

	require.NoError(t, err)
	assert.True(t, handled)
	require.Len(t, store.observedExact, 1)
	assert.Equal(t, int64(1), store.observedExact[0][teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, int64(150), store.observedExact[0][teamquota.KeySandboxCPUMillicores])
	assert.Equal(t, 0, store.abortCalls)
	assertPoolEventOrder(t, events, "commit_observed_exact", "scale", "delete")
	storedRS, err := client.AppsV1().ReplicaSets(rs.Namespace).Get(
		context.Background(),
		rs.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(0), getInt32Value(storedRS.Spec.Replicas))
	_, err = client.CoreV1().Pods(pod.Namespace).Get(
		context.Background(),
		pod.Name,
		metav1.GetOptions{},
	)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestClearTerminalWarmPoolTransferMarkersByState(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		stateFound bool
		wantMarker bool
	}{
		{
			name:       "prepared is retained",
			state:      "prepared",
			stateFound: true,
			wantMarker: true,
		},
		{
			name:       "committed is cleared",
			state:      "committed",
			stateFound: true,
		},
		{
			name:       "aborted is cleared",
			state:      "aborted",
			stateFound: true,
		},
		{
			name: "missing is cleared",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			template := warmPoolRuntimeClassTestTemplate()
			rs := &appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rs-transfer-markers",
					Namespace: template.Namespace,
					Annotations: map[string]string{
						AnnotationTeamQuotaWarmPoolTransfers: `{"transfer-op":3}`,
						"test.sandbox0.ai/preserved":         "true",
					},
				},
			}
			store := &recordingPoolCapacityStore{}
			if test.stateFound {
				store.transferStates = map[string]string{"transfer-op": test.state}
			}
			client := fake.NewSimpleClientset(rs)
			pm := &PoolManager{
				k8sClient:      client,
				teamQuotaStore: store,
				logger:         zap.NewNop(),
			}

			err := pm.clearTerminalWarmPoolTransferMarkers(
				context.Background(),
				template,
				rs.Name,
			)
			require.NoError(t, err)

			stored, err := client.AppsV1().ReplicaSets(rs.Namespace).
				Get(context.Background(), rs.Name, metav1.GetOptions{})
			require.NoError(t, err)
			raw, markerFound := stored.Annotations[AnnotationTeamQuotaWarmPoolTransfers]
			assert.Equal(t, test.wantMarker, markerFound)
			if test.wantMarker {
				assert.JSONEq(t, `{"transfer-op":3}`, raw)
			}
			assert.Equal(t, "true", stored.Annotations["test.sandbox0.ai/preserved"])
			require.Len(t, store.transferStateCalls, 1)
			assert.Equal(t, "team-a", store.transferStateCalls[0].teamID)
			assert.ElementsMatch(t, []string{"transfer-op"}, store.transferStateCalls[0].operationIDs)
		})
	}
}

func TestClearTerminalWarmPoolTransferMarkersFailsClosedOnMalformedAnnotation(t *testing.T) {
	template := warmPoolRuntimeClassTestTemplate()
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-transfer-markers",
			Namespace: template.Namespace,
			Annotations: map[string]string{
				AnnotationTeamQuotaWarmPoolTransfers: `{"transfer-op":`,
			},
		},
	}
	store := &recordingPoolCapacityStore{
		transferStates: map[string]string{"transfer-op": "committed"},
	}
	client := fake.NewSimpleClientset(rs)
	pm := &PoolManager{
		k8sClient:      client,
		teamQuotaStore: store,
		logger:         zap.NewNop(),
	}

	err := pm.clearTerminalWarmPoolTransferMarkers(
		context.Background(),
		template,
		rs.Name,
	)
	require.ErrorContains(t, err, "decode transfer markers")
	assert.Empty(t, store.transferStateCalls)

	stored, getErr := client.AppsV1().ReplicaSets(rs.Namespace).
		Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(
		t,
		rs.Annotations[AnnotationTeamQuotaWarmPoolTransfers],
		stored.Annotations[AnnotationTeamQuotaWarmPoolTransfers],
	)
}

func TestClearTerminalWarmPoolTransferMarkersRejectsUnknownState(t *testing.T) {
	template := warmPoolRuntimeClassTestTemplate()
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-transfer-markers",
			Namespace: template.Namespace,
			Annotations: map[string]string{
				AnnotationTeamQuotaWarmPoolTransfers: `{"transfer-op":3}`,
			},
		},
	}
	store := &recordingPoolCapacityStore{
		transferStates: map[string]string{"transfer-op": "indeterminate"},
	}
	client := fake.NewSimpleClientset(rs)
	pm := &PoolManager{
		k8sClient:      client,
		teamQuotaStore: store,
		logger:         zap.NewNop(),
	}

	err := pm.clearTerminalWarmPoolTransferMarkers(
		context.Background(),
		template,
		rs.Name,
	)
	require.ErrorContains(t, err, `transfer transfer-op has unknown state "indeterminate"`)

	stored, getErr := client.AppsV1().ReplicaSets(rs.Namespace).
		Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.JSONEq(
		t,
		rs.Annotations[AnnotationTeamQuotaWarmPoolTransfers],
		stored.Annotations[AnnotationTeamQuotaWarmPoolTransfers],
	)
}

func TestClearTerminalWarmPoolTransferMarkersConflictRetryPreservesNewPreparedMarker(t *testing.T) {
	template := warmPoolRuntimeClassTestTemplate()
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "rs-transfer-markers",
			Namespace:       template.Namespace,
			ResourceVersion: "1",
			Annotations: map[string]string{
				AnnotationTeamQuotaWarmPoolTransfers: `{"terminal-op":3}`,
			},
		},
	}
	store := &recordingPoolCapacityStore{
		transferStates: map[string]string{
			"terminal-op": "committed",
			"prepared-op": "prepared",
		},
	}
	client := fake.NewSimpleClientset(rs)
	updateCalls := 0
	client.PrependReactor("update", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		updateCalls++
		if updateCalls != 1 {
			return false, nil, nil
		}
		current, err := client.Tracker().Get(
			appsv1.SchemeGroupVersion.WithResource("replicasets"),
			rs.Namespace,
			rs.Name,
		)
		require.NoError(t, err)
		concurrent := current.(*appsv1.ReplicaSet).DeepCopy()
		concurrent.ResourceVersion = "2"
		concurrent.Annotations[AnnotationTeamQuotaWarmPoolTransfers] =
			`{"terminal-op":3,"prepared-op":2}`
		require.NoError(t, client.Tracker().Update(
			appsv1.SchemeGroupVersion.WithResource("replicasets"),
			concurrent,
			concurrent.Namespace,
		))
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "replicasets"},
			rs.Name,
			errors.New("concurrent transfer marker update"),
		)
	})
	pm := &PoolManager{
		k8sClient:      client,
		teamQuotaStore: store,
		logger:         zap.NewNop(),
	}

	err := pm.clearTerminalWarmPoolTransferMarkers(
		context.Background(),
		template,
		rs.Name,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, updateCalls)

	stored, err := client.AppsV1().ReplicaSets(rs.Namespace).
		Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.JSONEq(
		t,
		`{"prepared-op":2}`,
		stored.Annotations[AnnotationTeamQuotaWarmPoolTransfers],
	)
	require.Len(t, store.transferStateCalls, 2)
	assert.ElementsMatch(
		t,
		[]string{"terminal-op"},
		store.transferStateCalls[0].operationIDs,
	)
	assert.ElementsMatch(
		t,
		[]string{"terminal-op", "prepared-op"},
		store.transferStateCalls[1].operationIDs,
	)
}

func warmPoolRuntimeClassTestTemplate() *v1alpha1.SandboxTemplate {
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "runtime-class-template",
			Namespace: "default",
			Labels: map[string]string{
				LabelTemplateScope: naming.ScopeTeam,
			},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "busybox"},
		},
	}
}

func overheadOnlyQuotaResources(spec *corev1.PodSpec) teamquota.Values {
	values := teamquota.Values{
		teamquota.KeySandboxCPUMillicores: 0,
		teamquota.KeySandboxMemoryBytes:   0,
	}
	if spec == nil || spec.Overhead == nil {
		return values
	}
	if quantity, ok := spec.Overhead[corev1.ResourceCPU]; ok {
		values[teamquota.KeySandboxCPUMillicores] = quantity.MilliValue()
	}
	if quantity, ok := spec.Overhead[corev1.ResourceMemory]; ok {
		values[teamquota.KeySandboxMemoryBytes] = quantity.Value()
	}
	return values
}

func assertPoolEventOrder(t *testing.T, events []string, expected ...string) {
	t.Helper()
	next := 0
	for _, event := range events {
		if next < len(expected) && event == expected[next] {
			next++
		}
	}
	if next != len(expected) {
		t.Fatalf("events = %v, want ordered subsequence %v", events, expected)
	}
}

func warmPoolQuotaTestPod(
	template *v1alpha1.SandboxTemplate,
	name,
	image string,
	deletionTimestamp *metav1.Time,
) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         template.Namespace,
			DeletionTimestamp: deletionTimestamp,
			Finalizers:        []string{"test.sandbox0.ai/hold"},
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTeamID:    "team-a",
				AnnotationOwnerKind: OwnerKindTeamWarmPool,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "sandbox",
			Image: image,
		}}},
	}
}

type recordingTeamQuotaRateLimiter struct {
	decision tokenbucket.Decision
	err      error
	costs    []int64
	keys     []teamquota.Key
}

type burstOneTeamQuotaRateLimiter struct {
	costs    []int64
	consumed bool
}

func (l *burstOneTeamQuotaRateLimiter) Take(
	_ context.Context,
	_ string,
	_ teamquota.Key,
	cost int64,
) (tokenbucket.Decision, error) {
	l.costs = append(l.costs, cost)
	if cost > 1 {
		return tokenbucket.Decision{}, tokenbucket.ErrCostExceedsBurst
	}
	if !l.consumed {
		l.consumed = true
		return tokenbucket.Decision{Allowed: true}, nil
	}
	return tokenbucket.Decision{RetryAfter: 2 * time.Second}, nil
}

type recordingPoolCapacityStore struct {
	reservations       []teamquota.ReserveRequest
	deltas             []teamquota.DeltaRequest
	reconciles         []teamquota.Values
	observedExact      []teamquota.Values
	reserveErr         error
	allocation         *teamquota.RecoveryAllocation
	abortCalls         int
	events             *[]string
	transferStates     map[string]string
	transferStateErr   error
	transferStateCalls []recordedTransferStateCall
}

type recordedTransferStateCall struct {
	teamID       string
	operationIDs []string
}

func (s *recordingPoolCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	request.Target = request.Target.Clone()
	s.reservations = append(s.reservations, request)
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		Target:    request.Target.Clone(),
	}, nil
}

func (s *recordingPoolCapacityStore) ReserveDelta(
	_ context.Context,
	request teamquota.DeltaRequest,
) (*teamquota.Reservation, error) {
	request.Delta = request.Delta.Clone()
	s.deltas = append(s.deltas, request)
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		Target:    request.Delta.Clone(),
	}, nil
}

func (*recordingPoolCapacityStore) AttachRuntime(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	panic("unexpected AttachRuntime")
}

func (*recordingPoolCapacityStore) Commit(context.Context, teamquota.OperationRef) error {
	return nil
}

func (s *recordingPoolCapacityStore) CommitObservedExact(
	_ context.Context,
	_ teamquota.OperationRef,
	exact teamquota.Values,
) error {
	s.observedExact = append(s.observedExact, exact.Clone())
	if s.events != nil {
		*s.events = append(*s.events, "commit_observed_exact")
	}
	if s.allocation != nil {
		s.allocation.Committed = exact.Clone()
		s.allocation.Pending = nil
		s.allocation.Operation = nil
		s.allocation.Revision++
	}
	return nil
}

func (s *recordingPoolCapacityStore) Abort(context.Context, teamquota.OperationRef, string) error {
	s.abortCalls++
	if s.events != nil {
		*s.events = append(*s.events, "abort")
	}
	return nil
}

func (*recordingPoolCapacityStore) BeginRelease(
	context.Context,
	teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	panic("unexpected BeginRelease")
}

func (*recordingPoolCapacityStore) ConfirmRelease(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	panic("unexpected ConfirmRelease")
}

func (s *recordingPoolCapacityStore) ReconcileTarget(
	_ context.Context,
	_ teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
) error {
	s.reconciles = append(s.reconciles, target.Clone())
	return nil
}

func (s *recordingPoolCapacityStore) GetRecoveryAllocation(
	_ context.Context,
	owner teamquota.Owner,
) (*teamquota.RecoveryAllocation, error) {
	if s.allocation == nil ||
		s.allocation.Owner.TeamID != owner.TeamID ||
		s.allocation.Owner.Kind != owner.Kind ||
		s.allocation.Owner.ID != owner.ID {
		return nil, nil
	}
	allocation := *s.allocation
	allocation.Committed = allocation.Committed.Clone()
	allocation.Pending = allocation.Pending.Clone()
	if allocation.Operation != nil {
		operation := *allocation.Operation
		allocation.Operation = &operation
	}
	return &allocation, nil
}

func (s *recordingPoolCapacityStore) ReconcileTargetIfRevision(
	_ context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
	expectedRevision int64,
) (bool, error) {
	currentRevision := int64(0)
	if s.allocation != nil {
		currentRevision = s.allocation.Revision
	}
	if currentRevision != expectedRevision {
		return false, nil
	}
	s.reconciles = append(s.reconciles, target.Clone())
	if s.allocation == nil {
		s.allocation = &teamquota.RecoveryAllocation{
			Owner:     owner,
			Revision:  1,
			Committed: target.Clone(),
		}
	} else {
		s.allocation.Revision++
		s.allocation.Committed = target.Clone()
	}
	return true, nil
}

func (s *recordingPoolCapacityStore) TransferStates(
	_ context.Context,
	teamID string,
	operationIDs []string,
) (map[string]string, error) {
	s.transferStateCalls = append(s.transferStateCalls, recordedTransferStateCall{
		teamID:       teamID,
		operationIDs: append([]string(nil), operationIDs...),
	})
	if s.transferStateErr != nil {
		return nil, s.transferStateErr
	}
	states := make(map[string]string, len(operationIDs))
	for _, operationID := range operationIDs {
		if state, ok := s.transferStates[operationID]; ok {
			states[operationID] = state
		}
	}
	return states, nil
}

func (*recordingPoolCapacityStore) PrepareTransfer(
	context.Context,
	teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	panic("unexpected PrepareTransfer")
}

func (*recordingPoolCapacityStore) CommitTransfer(context.Context, teamquota.OperationRef) error {
	panic("unexpected CommitTransfer")
}

func (*recordingPoolCapacityStore) AbortTransfer(
	context.Context,
	teamquota.OperationRef,
	string,
) error {
	panic("unexpected AbortTransfer")
}

func (*recordingPoolCapacityStore) TransferTarget(
	context.Context,
	teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	panic("unexpected TransferTarget")
}

func (l *recordingTeamQuotaRateLimiter) Take(
	_ context.Context,
	_ string,
	key teamquota.Key,
	cost int64,
) (tokenbucket.Decision, error) {
	l.costs = append(l.costs, cost)
	l.keys = append(l.keys, key)
	return l.decision, l.err
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

	err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.Equal(t, 1, deleteActions)
}

func TestDrainStaleIdlePodsReservesOverlapAndChargesTeamStartBeforeReplacement(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels: map[string]string{
				LabelTemplateScope: naming.ScopeTeam,
			},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-stale",
			Namespace:       template.Namespace,
			UID:             types.UID("uid-stale"),
			ResourceVersion: "11",
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
				AnnotationTeamID:           "team-a",
				AnnotationOwnerKind:        OwnerKindTeamWarmPool,
			},
		},
	}
	client := fake.NewSimpleClientset(stalePod)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(stalePod))
	rateLimiter := &recordingTeamQuotaRateLimiter{
		decision: tokenbucket.Decision{Allowed: true},
	}
	quotaStore := &recordingPoolCapacityStore{}
	pm := &PoolManager{
		k8sClient:        client,
		podLister:        corelisters.NewPodLister(indexer),
		teamQuotaStore:   quotaStore,
		teamQuotaLimiter: rateLimiter,
		quotaResources: func(*corev1.PodSpec) teamquota.Values {
			return teamquota.Values{
				teamquota.KeySandboxCPUMillicores:         250,
				teamquota.KeySandboxMemoryBytes:           512,
				teamquota.KeySandboxEphemeralStorageBytes: 1024,
			}
		},
		recorder: record.NewFakeRecorder(10),
		logger:   zap.NewNop(),
	}

	require.NoError(t, pm.drainStaleIdlePods(context.Background(), template, "new-hash"))

	assert.Equal(t, []teamquota.Key{teamquota.KeySandboxStarts}, rateLimiter.keys)
	assert.Equal(t, []int64{1}, rateLimiter.costs)
	require.Len(t, quotaStore.deltas, 1)
	assert.Equal(t, int64(1), quotaStore.deltas[0].Delta[teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, int64(250), quotaStore.deltas[0].Delta[teamquota.KeySandboxCPUMillicores])
	assert.Equal(t, int64(512), quotaStore.deltas[0].Delta[teamquota.KeySandboxMemoryBytes])
	assert.Equal(t, int64(1024), quotaStore.deltas[0].Delta[teamquota.KeySandboxEphemeralStorageBytes])
	_, err := client.CoreV1().Pods(stalePod.Namespace).Get(context.Background(), stalePod.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestDrainStaleIdlePodsKeepsPodWhenReplacementOverlapExceedsTeamQuota(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels:    map[string]string{LabelTemplateScope: naming.ScopeTeam},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-stale",
			Namespace:       template.Namespace,
			UID:             types.UID("uid-stale"),
			ResourceVersion: "11",
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{
				AnnotationTemplateSpecHash: "old-hash",
				AnnotationTeamID:           "team-a",
				AnnotationOwnerKind:        OwnerKindTeamWarmPool,
			},
		},
	}
	client := fake.NewSimpleClientset(stalePod)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(stalePod))
	rateLimiter := &recordingTeamQuotaRateLimiter{decision: tokenbucket.Decision{Allowed: true}}
	quotaStore := &recordingPoolCapacityStore{
		reserveErr: &teamquota.ExceededError{
			TeamID:    "team-a",
			Key:       teamquota.KeySandboxRuntimeCount,
			Limit:     1,
			Committed: 1,
			Requested: 1,
		},
	}
	pm := &PoolManager{
		k8sClient:        client,
		podLister:        corelisters.NewPodLister(indexer),
		teamQuotaStore:   quotaStore,
		teamQuotaLimiter: rateLimiter,
		quotaResources:   podSpecQuotaResourcesForTest,
		recorder:         record.NewFakeRecorder(10),
		logger:           zap.NewNop(),
	}

	err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")

	require.Error(t, err)
	assert.True(t, teamquota.IsExceeded(err))
	require.Len(t, quotaStore.deltas, 1)
	assert.Empty(t, rateLimiter.costs, "a rejected capacity overlap must not consume a start token")
	_, getErr := client.CoreV1().Pods(stalePod.Namespace).Get(context.Background(), stalePod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
}

func TestDrainStaleIdlePodsKeepsPodWhenClusterStartGuardThrottles(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
	}
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "idle-stale",
			Namespace:       template.Namespace,
			UID:             types.UID("uid-stale"),
			ResourceVersion: "11",
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: map[string]string{AnnotationTemplateSpecHash: "old-hash"},
		},
	}
	client := fake.NewSimpleClientset(stalePod)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(stalePod))
	limiter, err := startlimiter.New(context.Background(), startlimiter.Config{
		K8sClient:      client,
		PerSandboxNode: 1,
		MaxLimit:       1,
	})
	require.NoError(t, err)
	pm := &PoolManager{
		k8sClient:         client,
		podLister:         corelisters.NewPodLister(indexer),
		claimStartLimiter: limiter,
		recorder:          record.NewFakeRecorder(10),
		logger:            zap.NewNop(),
	}

	err = pm.drainStaleIdlePods(context.Background(), template, "new-hash")

	require.ErrorIs(t, err, startlimiter.ErrThrottled)
	_, getErr := client.CoreV1().Pods(stalePod.Namespace).Get(context.Background(), stalePod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	retryAfter, limited := warmPoolReplacementRetryAfter(err)
	assert.True(t, limited)
	assert.Equal(t, time.Second, retryAfter)
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

	err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
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

	err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
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

	err := pm.drainStaleIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.Equal(t, 0, deleteActions)
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

	err := pm.repairUnhealthyIdlePods(context.Background(), template, "new-hash")
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

	err := pm.repairUnhealthyIdlePods(context.Background(), template, "new-hash")
	require.NoError(t, err)
	assert.Equal(t, 0, deleteActions)
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

func TestReconcileReplicaSetTemplateReservesNewTeamCommitmentBeforeRollout(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels: map[string]string{
				LabelTemplateScope: naming.ScopeTeam,
			},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "new-image"},
		},
	}
	replicas := int32(2)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-template-a", Namespace: template.Namespace},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationTemplateSpecHash: "old-hash"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sandbox", Image: "old-image"}}},
			},
		},
	}
	oldPods := []runtime.Object{rs}
	for _, name := range []string{"idle-a", "idle-b"} {
		oldPods = append(oldPods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: template.Namespace,
				Labels: map[string]string{
					LabelTemplateID: template.Name,
					LabelPoolType:   PoolTypeIdle,
				},
				Annotations: map[string]string{
					AnnotationTeamID:    "team-a",
					AnnotationOwnerKind: OwnerKindTeamWarmPool,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sandbox", Image: "old-image"}}},
		})
	}
	client := fake.NewSimpleClientset(oldPods...)
	quotaStore := &recordingPoolCapacityStore{}
	pm := &PoolManager{
		k8sClient:      client,
		teamQuotaStore: quotaStore,
		quotaResources: func(spec *corev1.PodSpec) teamquota.Values {
			memory := int64(5)
			if spec != nil && len(spec.Containers) > 0 && spec.Containers[0].Image == "new-image" {
				memory = 20
			}
			return teamquota.Values{teamquota.KeySandboxMemoryBytes: memory}
		},
		recorder: record.NewFakeRecorder(10),
		logger:   zap.NewNop(),
	}

	updated, err := pm.reconcileReplicaSetTemplate(context.Background(), template, rs, "new-hash")

	require.NoError(t, err)
	assert.Equal(t, "new-hash", updated.Spec.Template.Annotations[AnnotationTemplateSpecHash])
	require.Len(t, quotaStore.reservations, 1)
	assert.Equal(t, int64(2), quotaStore.reservations[0].Target[teamquota.KeySandboxRuntimeCount])
	assert.Equal(t, int64(40), quotaStore.reservations[0].Target[teamquota.KeySandboxMemoryBytes])
}

func TestReconcileReplicaSetTemplateDoesNotRollOutWhenTeamQuotaExceeded(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
			Labels:    map[string]string{LabelTemplateScope: naming.ScopeTeam},
			Annotations: map[string]string{
				AnnotationTemplateTeamID: "team-a",
			},
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "new-image"},
		},
	}
	replicas := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-template-a", Namespace: template.Namespace},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationTemplateSpecHash: "old-hash"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sandbox", Image: "old-image"}}},
			},
		},
	}
	client := fake.NewSimpleClientset(rs)
	quotaStore := &recordingPoolCapacityStore{
		reserveErr: &teamquota.ExceededError{
			TeamID:    "team-a",
			Key:       teamquota.KeySandboxMemoryBytes,
			Limit:     10,
			Requested: 20,
		},
	}
	pm := &PoolManager{
		k8sClient:      client,
		teamQuotaStore: quotaStore,
		quotaResources: func(*corev1.PodSpec) teamquota.Values {
			return teamquota.Values{teamquota.KeySandboxMemoryBytes: 20}
		},
		recorder: record.NewFakeRecorder(10),
		logger:   zap.NewNop(),
	}

	_, err := pm.reconcileReplicaSetTemplate(context.Background(), template, rs, "new-hash")

	require.Error(t, err)
	stored, getErr := client.AppsV1().ReplicaSets(rs.Namespace).Get(context.Background(), rs.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, "old-hash", stored.Spec.Template.Annotations[AnnotationTemplateSpecHash])
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
