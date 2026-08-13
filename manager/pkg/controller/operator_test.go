package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	crdfake "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned/fake"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fsrollout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestTemplateS0FSRolloutDisablesLegacyPoolsOnGreenCluster(t *testing.T) {
	configPath := writeManagerConfig(t, `
shared_carrier_pool:
  enabled: false
template_image_fs:
  enabled: true
s0fs_runtime:
  enabled: true
  admission:
    mode: "off"
    reject_legacy_claims: true
`)
	t.Setenv("CONFIG_PATH", configPath)

	mode, admitted, rejectLegacy := templateS0FSRollout(&v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a"},
	})
	assert.Empty(t, mode)
	assert.False(t, admitted)
	assert.True(t, rejectLegacy)
}

func TestTemplateS0FSRolloutAdmitsOnlyReadyCohort(t *testing.T) {
	configPath := writeManagerConfig(t, `
shared_carrier_pool:
  enabled: false
template_image_fs:
  enabled: true
s0fs_runtime:
  enabled: true
  admission:
    mode: "cold"
    team_ids:
      - team-a
    reject_legacy_claims: true
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "cluster-template-a",
			Labels:      map[string]string{"sandbox0.ai/template-scope": naming.ScopeTeam, "sandbox0.ai/template-logical-id": "template-a"},
			Annotations: map[string]string{"sandbox0.ai/template-team-id": "team-a"},
		},
	}
	mode, admitted, rejectLegacy := templateS0FSRollout(template)
	assert.Empty(t, mode)
	assert.False(t, admitted)
	assert.True(t, rejectLegacy)

	template.Status.ImageRevision = &v1alpha1.TemplateImageRevisionStatus{
		State:         v1alpha1.TemplateImageRevisionStateReady,
		ImageFSHeadID: "head-1",
	}
	mode, admitted, rejectLegacy = templateS0FSRollout(template)
	assert.Equal(t, s0fsrollout.AdmissionModeCold, mode)
	assert.True(t, admitted)
	assert.True(t, rejectLegacy)
}

func TestOperatorUpdateTemplateStatusUsesReadyIdlePods(t *testing.T) {
	transitionTime := metav1.NewTime(time.Unix(1_700_000_000, 0))
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "default",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{
				MinIdle: 2,
				MaxIdle: 5,
			},
		},
		Status: v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State: v1alpha1.TemplateCreationStateReady,
				Stage: v1alpha1.TemplateCreationStageReconciling,
			},
			Conditions: []v1alpha1.SandboxTemplateCondition{
				{
					Type:               v1alpha1.SandboxTemplateReady,
					Status:             v1alpha1.ConditionFalse,
					LastTransitionTime: transitionTime,
					Reason:             "InsufficientIdlePods",
					Message:            "Idle pod count (0) is less than minIdle (2)",
				},
				{
					Type:               v1alpha1.SandboxTemplatePoolHealthy,
					Status:             v1alpha1.ConditionTrue,
					LastTransitionTime: transitionTime,
					Reason:             "PoolHealthy",
					Message:            "Pool is healthy",
				},
			},
		},
	}
	informerTemplate := template.DeepCopy()
	crdClient := crdfake.NewSimpleClientset(template.DeepCopy())

	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	reserved := newOperatorTestPod("default", "idle-reserved", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionTrue)
	reserved.Annotations = map[string]string{AnnotationHotClaimReservation: "reservation-token"}
	for _, pod := range []*corev1.Pod{
		newOperatorTestPod("default", "idle-ready", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionTrue),
		newOperatorTestPod("default", "idle-not-ready", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionFalse),
		newOperatorTestPod("default", "idle-pending", "template-a", PoolTypeIdle, corev1.PodPending, corev1.ConditionTrue),
		reserved,
		newOperatorTestPod("default", "active-running", "template-a", PoolTypeActive, corev1.PodRunning, corev1.ConditionFalse),
		newOperatorTestPod("default", "active-pending", "template-a", PoolTypeActive, corev1.PodPending, corev1.ConditionTrue),
	} {
		require.NoError(t, podIndexer.Add(pod))
	}

	publisher := &recordingTemplateStatsPublisher{}
	op := &Operator{
		podLister:      corelisters.NewPodLister(podIndexer),
		crdClient:      crdClient,
		logger:         zap.NewNop(),
		statsPublisher: publisher,
		lastStats:      make(map[string]TemplateCounts),
	}

	err := op.updateTemplateStatus(context.Background(), template)
	require.NoError(t, err)

	assert.Equal(t, informerTemplate, template, "the informer object must not be mutated")

	persisted, err := crdClient.Sandbox0V1alpha1().SandboxTemplates("default").Get(
		context.Background(),
		"template-a",
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(1), persisted.Status.IdleCount)
	assert.Equal(t, int32(2), persisted.Status.ActiveCount)
	assert.Equal(t, template.Status.Creation, persisted.Status.Creation)
	require.Len(t, persisted.Status.Conditions, 2)
	assert.Equal(t, v1alpha1.ConditionFalse, persisted.Status.Conditions[0].Status)
	assert.Equal(t, "InsufficientIdlePods", persisted.Status.Conditions[0].Reason)
	assert.Equal(t, transitionTime, persisted.Status.Conditions[0].LastTransitionTime)
	assert.Equal(t, v1alpha1.ConditionTrue, persisted.Status.Conditions[1].Status)
	assert.Equal(t, "PoolHealthy", persisted.Status.Conditions[1].Reason)
	assert.Equal(t, transitionTime, persisted.Status.Conditions[1].LastTransitionTime)
	assert.False(t, persisted.Status.LastUpdateTime.IsZero())

	updateActions := 0
	for _, action := range crdClient.Actions() {
		if action.GetVerb() == "update" {
			updateActions++
		}
	}
	require.Equal(t, 1, updateActions)

	err = op.updateTemplateStatus(context.Background(), template)
	require.NoError(t, err)
	updateActions = 0
	for _, action := range crdClient.Actions() {
		if action.GetVerb() == "update" {
			updateActions++
		}
	}
	assert.Equal(t, 1, updateActions, "unchanged status must not be written again")

	assert.Equal(t, 1, publisher.calls)
	assert.Equal(t, int32(1), publisher.idleCount)
	assert.Equal(t, int32(2), publisher.activeCount)
	assert.Equal(t, "default/template-a", publisher.statsKey)
	assert.Equal(t, TemplateCounts{IdleCount: 1, ActiveCount: 2}, op.lastStats["default/template-a"])
}

func TestPodUpdateRequiresPoolReconcileFiltersStatusNoise(t *testing.T) {
	base := newOperatorTestPod("default", "idle-a", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionTrue)
	base.Spec.NodeName = "node-a"
	base.Annotations = map[string]string{AnnotationTemplateSpecHash: "hash-a"}

	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
		want   bool
	}{
		{
			name: "pod IP update is ignored",
			mutate: func(pod *corev1.Pod) {
				pod.Status.PodIP = "10.0.0.2"
			},
			want: false,
		},
		{
			name: "ready condition message update is ignored",
			mutate: func(pod *corev1.Pod) {
				pod.Status.Conditions[0].Message = "periodic probe detail"
			},
			want: false,
		},
		{
			name: "readiness transition is relevant",
			mutate: func(pod *corev1.Pod) {
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
			},
			want: true,
		},
		{
			name: "phase transition is relevant",
			mutate: func(pod *corev1.Pod) {
				pod.Status.Phase = corev1.PodFailed
			},
			want: true,
		},
		{
			name: "node assignment is relevant",
			mutate: func(pod *corev1.Pod) {
				pod.Spec.NodeName = "node-b"
			},
			want: true,
		},
		{
			name: "deletion start is relevant",
			mutate: func(pod *corev1.Pod) {
				deletedAt := metav1.Now()
				pod.DeletionTimestamp = &deletedAt
			},
			want: true,
		},
		{
			name: "template hash change is relevant",
			mutate: func(pod *corev1.Pod) {
				pod.Annotations[AnnotationTemplateSpecHash] = "hash-b"
			},
			want: true,
		},
		{
			name: "hot claim reservation is relevant",
			mutate: func(pod *corev1.Pod) {
				pod.Annotations[AnnotationHotClaimReservation] = "reservation"
			},
			want: true,
		},
		{
			name: "template ownership change is relevant",
			mutate: func(pod *corev1.Pod) {
				pod.Labels[LabelTemplateID] = "template-b"
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			updated := base.DeepCopy()
			test.mutate(updated)
			assert.Equal(t, test.want, podUpdateRequiresPoolReconcile(base, updated))
		})
	}
}

type recordingNamespacePolicyReconciler struct {
	calls []string
	err   error
}

func (r *recordingNamespacePolicyReconciler) EnsureBaseline(_ context.Context, namespace string) error {
	r.calls = append(r.calls, namespace)
	return r.err
}

func TestOperatorSyncHandlerPropagatesNamespaceBaselineError(t *testing.T) {
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a", Namespace: "default"},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(template))

	reconciler := &recordingNamespacePolicyReconciler{err: errors.New("boom")}
	op := &Operator{
		templateLister:  TemplateListerImpl{indexer: indexer},
		namespacePolicy: reconciler,
		logger:          zap.NewNop(),
	}

	err := op.syncHandler(context.Background(), "default/template-a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconcile template namespace baseline")
	assert.Equal(t, []string{"default"}, reconciler.calls)
}

type recordingTemplateStatsPublisher struct {
	calls       int
	idleCount   int32
	activeCount int32
	statsKey    string
}

func (p *recordingTemplateStatsPublisher) PublishTemplateStats(_ context.Context, template *v1alpha1.SandboxTemplate, idleCount, activeCount int32) error {
	p.calls++
	p.idleCount = idleCount
	p.activeCount = activeCount
	p.statsKey = template.Namespace + "/" + template.Name
	return nil
}

func newOperatorTestPod(namespace, name, templateID, poolType string, phase corev1.PodPhase, ready corev1.ConditionStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				LabelTemplateID: templateID,
				LabelPoolType:   poolType,
			},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: ready,
				},
			},
		},
	}
}
