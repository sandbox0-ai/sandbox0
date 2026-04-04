package controller

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	for _, pod := range []*corev1.Pod{
		newOperatorTestPod("default", "idle-ready", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionTrue),
		newOperatorTestPod("default", "idle-not-ready", "template-a", PoolTypeIdle, corev1.PodRunning, corev1.ConditionFalse),
		newOperatorTestPod("default", "idle-pending", "template-a", PoolTypeIdle, corev1.PodPending, corev1.ConditionTrue),
		newOperatorTestPod("default", "active-running", "template-a", PoolTypeActive, corev1.PodRunning, corev1.ConditionFalse),
		newOperatorTestPod("default", "active-pending", "template-a", PoolTypeActive, corev1.PodPending, corev1.ConditionTrue),
	} {
		require.NoError(t, podIndexer.Add(pod))
	}

	publisher := &recordingTemplateStatsPublisher{}
	op := &Operator{
		podLister:      corelisters.NewPodLister(podIndexer),
		logger:         zap.NewNop(),
		statsPublisher: publisher,
		lastStats:      make(map[string]TemplateCounts),
	}

	err := op.updateTemplateStatus(context.Background(), template)
	require.NoError(t, err)

	assert.Equal(t, int32(1), template.Status.IdleCount)
	assert.Equal(t, int32(1), template.Status.ActiveCount)
	require.Len(t, template.Status.Conditions, 2)
	assert.Equal(t, v1alpha1.ConditionFalse, template.Status.Conditions[0].Status)
	assert.Equal(t, "InsufficientIdlePods", template.Status.Conditions[0].Reason)
	assert.Equal(t, v1alpha1.ConditionTrue, template.Status.Conditions[1].Status)
	assert.Equal(t, "PoolHealthy", template.Status.Conditions[1].Reason)

	assert.Equal(t, 1, publisher.calls)
	assert.Equal(t, int32(1), publisher.idleCount)
	assert.Equal(t, int32(1), publisher.activeCount)
	assert.Equal(t, "default/template-a", publisher.statsKey)
	assert.Equal(t, TemplateCounts{IdleCount: 1, ActiveCount: 1}, op.lastStats["default/template-a"])
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
