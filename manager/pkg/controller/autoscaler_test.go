package controller

import (
	"context"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestAutoScalerHooksAreNoOpWhileDisabled(t *testing.T) {
	template := autoscalerTestTemplate("template-a", 15, 50)
	scaler := NewAutoScalerWithConfig(
		fake.NewSimpleClientset(),
		corelisters.NewPodLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		zap.NewNop(),
		AutoScaleConfig{},
	)

	decision, err := scaler.OnColdClaim(context.Background(), template)
	require.NoError(t, err)
	require.NotNil(t, decision)
	assert.False(t, decision.ShouldScale)
	assert.Equal(t, autoscalerDisabledReason, decision.Reason)

	decision, err = scaler.OnHotClaim(context.Background(), template)
	require.NoError(t, err)
	require.NotNil(t, decision)
	assert.False(t, decision.ShouldScale)
	assert.Equal(t, autoscalerDisabledReason, decision.Reason)

	admission, err := scaler.AdmitColdClaim(context.Background(), template)
	require.NoError(t, err)
	require.NotNil(t, admission)
	assert.True(t, admission.Admitted)
	assert.Equal(t, autoscalerDisabledReason, admission.Reason)
	require.NotNil(t, admission.ScaleDecision)
	assert.False(t, admission.ScaleDecision.ShouldScale)

	requeueAfter, err := scaler.ReconcileScaleDown(context.Background(), template, time.Now())
	require.NoError(t, err)
	assert.Zero(t, requeueAfter)

	scaler.CompleteColdClaim(template)
}

func TestToAutoScaleConfigAppliesRuntimeDefaults(t *testing.T) {
	cfg := toAutoScaleConfig(apiconfig.AutoscalerConfig{})
	defaults := DefaultAutoScaleConfig()
	assert.Equal(t, defaults.MinScaleInterval, cfg.MinScaleInterval)
	assert.Equal(t, defaults.MaxScaleStep, cfg.MaxScaleStep)
	assert.Equal(t, defaults.MinIdleBuffer, cfg.MinIdleBuffer)
	assert.Equal(t, defaults.NoTrafficScaleDownAfter, cfg.NoTrafficScaleDownAfter)
}

func TestNewAutoScalerWithConfigAppliesZeroDefaults(t *testing.T) {
	scaler := NewAutoScalerWithConfig(
		fake.NewSimpleClientset(),
		corelisters.NewPodLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		zap.NewNop(),
		AutoScaleConfig{},
	)

	defaults := DefaultAutoScaleConfig()
	assert.Equal(t, defaults.MinScaleInterval, scaler.config.MinScaleInterval)
	assert.Equal(t, defaults.MaxScaleStep, scaler.config.MaxScaleStep)
	assert.Equal(t, defaults.MinIdleBuffer, scaler.config.MinIdleBuffer)
	assert.Equal(t, defaults.NoTrafficScaleDownAfter, scaler.config.NoTrafficScaleDownAfter)
}

func TestNormalizedPoolBounds(t *testing.T) {
	minIdle, maxIdle := normalizedPoolBounds(autoscalerTestTemplate("template-a", 3, 1))
	assert.Equal(t, int32(3), minIdle)
	assert.Equal(t, int32(3), maxIdle)

	minIdle, maxIdle = normalizedPoolBounds(autoscalerTestTemplate("template-a", -1, 5))
	assert.Equal(t, int32(0), minIdle)
	assert.Equal(t, int32(5), maxIdle)
}

func autoscalerTestTemplate(name string, minIdle, maxIdle int32) *v1alpha1.SandboxTemplate {
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			Pool: v1alpha1.PoolStrategy{
				MinIdle: minIdle,
				MaxIdle: maxIdle,
			},
		},
	}
}
