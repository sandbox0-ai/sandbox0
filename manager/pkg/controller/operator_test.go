package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestOperatorUpdateTemplateStatusUsesReadyIdlePods(t *testing.T) {
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
	}

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
	probeRunner := &recordingSandboxProbeRunner{}
	op := &Operator{
		podLister:      corelisters.NewPodLister(podIndexer),
		logger:         zap.NewNop(),
		statsPublisher: publisher,
		probeRunner:    probeRunner,
		lastStats:      make(map[string]TemplateCounts),
	}

	err := op.updateTemplateStatus(context.Background(), template)
	require.NoError(t, err)

	assert.Equal(t, int32(1), template.Status.IdleCount)
	assert.Equal(t, int32(2), template.Status.ActiveCount)
	require.Len(t, template.Status.Conditions, 2)
	assert.Equal(t, v1alpha1.ConditionFalse, template.Status.Conditions[0].Status)
	assert.Equal(t, "InsufficientIdlePods", template.Status.Conditions[0].Reason)
	assert.Equal(t, v1alpha1.ConditionTrue, template.Status.Conditions[1].Status)
	assert.Equal(t, "PoolHealthy", template.Status.Conditions[1].Reason)

	assert.Equal(t, 1, publisher.calls)
	assert.Equal(t, int32(1), publisher.idleCount)
	assert.Equal(t, int32(2), publisher.activeCount)
	assert.Equal(t, "default/template-a", publisher.statsKey)
	assert.Equal(t, TemplateCounts{IdleCount: 1, ActiveCount: 2}, op.lastStats["default/template-a"])
	assert.Empty(t, probeRunner.calls)
}

func TestShouldRetrySandboxProbe(t *testing.T) {
	pod := newOperatorTestPod("default", "idle-not-ready", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionTrue)
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:   v1alpha1.SandboxPodReadinessConditionType,
		Status: corev1.ConditionFalse,
		Reason: "InitialDelay",
	})

	assert.True(t, shouldRetrySandboxProbe(pod))

	pod.Status.Conditions = []corev1.PodCondition{
		{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   v1alpha1.SandboxPodStartupConditionType,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   v1alpha1.SandboxPodReadinessConditionType,
			Status: corev1.ConditionTrue,
		},
	}
	assert.False(t, shouldRetrySandboxProbe(pod))
}

func TestPodProbeInputsChangedIgnoresProbeConditionUpdates(t *testing.T) {
	oldPod := newOperatorTestPod("default", "idle", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionTrue)
	oldPod.UID = "pod-uid"
	oldPod.Spec.NodeName = "node-a"
	oldPod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	oldPod.Status.PodIP = "10.0.0.2"
	oldPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "procd",
		Ready: true,
	}}

	conditionUpdate := oldPod.DeepCopy()
	conditionUpdate.Status.Conditions = append(conditionUpdate.Status.Conditions, corev1.PodCondition{
		Type:   v1alpha1.SandboxPodReadinessConditionType,
		Status: corev1.ConditionTrue,
	})
	assert.False(t, podProbeInputsChanged(oldPod, conditionUpdate))

	containerUpdate := oldPod.DeepCopy()
	containerUpdate.Status.ContainerStatuses[0].RestartCount++
	assert.True(t, podProbeInputsChanged(oldPod, containerUpdate))

	phaseUpdate := oldPod.DeepCopy()
	phaseUpdate.Status.Phase = corev1.PodFailed
	assert.True(t, podProbeInputsChanged(oldPod, phaseUpdate))
}

func TestOperatorSyncPodProbeUpdatesConditionsAndSchedulesHealthyProbe(t *testing.T) {
	pod := newOperatorTestPod("default", "idle", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionFalse)
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	require.NoError(t, indexer.Add(pod.DeepCopy()))
	runner := &recordingSandboxProbeRunner{passed: true}
	op := &Operator{
		k8sClient:   fake.NewSimpleClientset(pod.DeepCopy()),
		podLister:   corelisters.NewPodLister(indexer),
		probeRunner: runner,
		logger:      zap.NewNop(),
	}

	requeueAfter, err := op.syncPodProbe(context.Background(), "default/idle")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, requeueAfter, 24*time.Second)
	assert.LessOrEqual(t, requeueAfter, 36*time.Second)
	assert.ElementsMatch(t,
		[]sandboxprobe.Kind{sandboxprobe.KindStartup, sandboxprobe.KindReadiness, sandboxprobe.KindLiveness},
		runner.calls,
	)

	updated, err := op.k8sClient.CoreV1().Pods("default").Get(context.Background(), "idle", metav1.GetOptions{})
	require.NoError(t, err)
	assert.True(t, podConditionTrue(updated.Status.Conditions, v1alpha1.SandboxPodStartupConditionType))
	assert.True(t, podConditionTrue(updated.Status.Conditions, v1alpha1.SandboxPodReadinessConditionType))
	assert.True(t, podConditionTrue(updated.Status.Conditions, v1alpha1.SandboxPodLivenessConditionType))
}

type recordingSandboxProbeRunner struct {
	calls  []sandboxprobe.Kind
	passed bool
}

func (r *recordingSandboxProbeRunner) ProbeSandboxPod(_ context.Context, _ *corev1.Pod, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	r.calls = append(r.calls, kind)
	if r.passed {
		result := sandboxprobe.Passed(kind, "SandboxProbePassed", "sandbox probe passed", nil)
		return &result, nil
	}
	result := sandboxprobe.Failed(kind, "SandboxProbeFailed", "sandbox probe failed", nil)
	return &result, nil
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
