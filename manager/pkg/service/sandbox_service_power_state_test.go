package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSandboxPowerStateFromAnnotationsFallsBackToLegacyPausedAnnotation(t *testing.T) {
	state := sandboxPowerStateFromAnnotations(map[string]string{
		controller.AnnotationPaused: "true",
	})

	assert.Equal(t, SandboxPowerStatePaused, state.Desired)
	assert.Equal(t, SandboxPowerStatePaused, state.Observed)
	assert.Equal(t, SandboxPowerPhaseStable, state.Phase)
	assert.Zero(t, state.DesiredGeneration)
	assert.Zero(t, state.ObservedGeneration)
}

func TestCompletedSandboxPowerStateAssignsGeneration(t *testing.T) {
	state := completedSandboxPowerState(map[string]string{}, SandboxPowerStatePaused)

	assert.Equal(t, SandboxPowerStatePaused, state.Desired)
	assert.Equal(t, SandboxPowerStatePaused, state.Observed)
	assert.Equal(t, int64(1), state.DesiredGeneration)
	assert.Equal(t, int64(1), state.ObservedGeneration)
	assert.Equal(t, SandboxPowerPhaseStable, state.Phase)
}

func TestPodToSandboxIncludesPowerState(t *testing.T) {
	svc := &SandboxService{
		config: SandboxServiceConfig{ProcdPort: 49983},
		logger: zap.NewNop(),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sandbox-1",
			Labels: map[string]string{
				controller.LabelTemplateID: "template-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:                       "team-1",
				controller.AnnotationUserID:                       "user-1",
				controller.AnnotationPaused:                       "true",
				controller.AnnotationPowerStateDesired:            SandboxPowerStateActive,
				controller.AnnotationPowerStateDesiredGeneration:  "4",
				controller.AnnotationPowerStateObserved:           SandboxPowerStatePaused,
				controller.AnnotationPowerStateObservedGeneration: "3",
			},
			CreationTimestamp: metav1.NewTime(time.Unix(1700000000, 0).UTC()),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
		},
	}

	sandbox := svc.podToSandbox(context.Background(), pod, pod.Name)

	assert.Equal(t, SandboxPowerStateActive, sandbox.PowerState.Desired)
	assert.Equal(t, int64(4), sandbox.PowerState.DesiredGeneration)
	assert.Equal(t, SandboxPowerStatePaused, sandbox.PowerState.Observed)
	assert.Equal(t, int64(3), sandbox.PowerState.ObservedGeneration)
	assert.Equal(t, SandboxPowerPhaseResuming, sandbox.PowerState.Phase)
	assert.True(t, sandbox.Paused)
}
