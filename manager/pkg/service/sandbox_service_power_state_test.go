package service

import (
	"context"
	"strconv"
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

func TestCtldPowerStateRequestsPreserveInFlightTransitions(t *testing.T) {
	tests := []struct {
		name      string
		current   SandboxPowerState
		target    string
		wantState SandboxPowerState
	}{
		{
			name: "pause from stable active",
			current: SandboxPowerState{
				Desired:            SandboxPowerStateActive,
				DesiredGeneration:  4,
				Observed:           SandboxPowerStateActive,
				ObservedGeneration: 4,
				Phase:              SandboxPowerPhaseStable,
			},
			target: SandboxPowerStatePaused,
			wantState: SandboxPowerState{
				Desired:            SandboxPowerStatePaused,
				DesiredGeneration:  5,
				Observed:           SandboxPowerStateActive,
				ObservedGeneration: 4,
				Phase:              SandboxPowerPhasePausing,
			},
		},
		{
			name: "resume from stable paused",
			current: SandboxPowerState{
				Desired:            SandboxPowerStatePaused,
				DesiredGeneration:  4,
				Observed:           SandboxPowerStatePaused,
				ObservedGeneration: 4,
				Phase:              SandboxPowerPhaseStable,
			},
			target: SandboxPowerStateActive,
			wantState: SandboxPowerState{
				Desired:            SandboxPowerStateActive,
				DesiredGeneration:  5,
				Observed:           SandboxPowerStatePaused,
				ObservedGeneration: 4,
				Phase:              SandboxPowerPhaseResuming,
			},
		},
		{
			name: "resume cancels in-flight pause without claiming active is already observed",
			current: SandboxPowerState{
				Desired:            SandboxPowerStatePaused,
				DesiredGeneration:  4,
				Observed:           SandboxPowerStateActive,
				ObservedGeneration: 3,
				Phase:              SandboxPowerPhasePausing,
			},
			target: SandboxPowerStateActive,
			wantState: SandboxPowerState{
				Desired:            SandboxPowerStateActive,
				DesiredGeneration:  5,
				Observed:           SandboxPowerStateActive,
				ObservedGeneration: 3,
				Phase:              SandboxPowerPhaseResuming,
			},
		},
		{
			name: "pause cancels in-flight resume without claiming paused is already observed",
			current: SandboxPowerState{
				Desired:            SandboxPowerStateActive,
				DesiredGeneration:  4,
				Observed:           SandboxPowerStatePaused,
				ObservedGeneration: 3,
				Phase:              SandboxPowerPhaseResuming,
			},
			target: SandboxPowerStatePaused,
			wantState: SandboxPowerState{
				Desired:            SandboxPowerStatePaused,
				DesiredGeneration:  5,
				Observed:           SandboxPowerStatePaused,
				ObservedGeneration: 3,
				Phase:              SandboxPowerPhasePausing,
			},
		},
		{
			name: "duplicate in-flight pause is idempotent",
			current: SandboxPowerState{
				Desired:            SandboxPowerStatePaused,
				DesiredGeneration:  4,
				Observed:           SandboxPowerStateActive,
				ObservedGeneration: 3,
				Phase:              SandboxPowerPhasePausing,
			},
			target: SandboxPowerStatePaused,
			wantState: SandboxPowerState{
				Desired:            SandboxPowerStatePaused,
				DesiredGeneration:  4,
				Observed:           SandboxPowerStateActive,
				ObservedGeneration: 3,
				Phase:              SandboxPowerPhasePausing,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			annotations := powerStateAnnotations(tt.current)
			requested := requestedSandboxPowerState(annotations, tt.target)
			got := preserveCtldInFlightPowerTransition(tt.current, requested, tt.target)

			assert.Equal(t, tt.wantState, got)
		})
	}
}

func TestPodToSandboxDoesNotExposeLegacyPowerState(t *testing.T) {
	svc := &SandboxService{
		config: SandboxServiceConfig{ProcdPort: 49983},
		logger: zap.NewNop(),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sandbox-1",
			Labels: map[string]string{
				controller.LabelTemplateID:        "t-team-template-1",
				controller.LabelTemplateLogicalID: "template-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:                       "team-1",
				controller.AnnotationUserID:                       "user-1",
				controller.AnnotationPaused:                       "true",
				controller.AnnotationPowerStateDesired:            SandboxPowerStateActive,
				controller.AnnotationPowerStateDesiredGeneration:  "4",
				controller.AnnotationPowerStateObserved:           SandboxPowerStatePaused,
				controller.AnnotationPowerStateObservedGeneration: "3",
				controller.AnnotationRuntimeGeneration:            "9",
			},
			CreationTimestamp: metav1.NewTime(time.Unix(1700000000, 0).UTC()),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
		},
	}

	sandbox := svc.podToSandbox(context.Background(), pod, pod.Name)

	assert.False(t, sandbox.Paused)
	assert.Equal(t, "template-1", sandbox.TemplateID)
	assert.Equal(t, int64(9), sandbox.RuntimeGeneration)
}

type staticTokenGenerator struct{}

func (staticTokenGenerator) GenerateToken(_, _, _ string) (string, error) {
	return "token", nil
}

func powerStateAnnotations(state SandboxPowerState) map[string]string {
	return map[string]string{
		controller.AnnotationPowerStateDesired:            state.Desired,
		controller.AnnotationPowerStateDesiredGeneration:  strconv.FormatInt(state.DesiredGeneration, 10),
		controller.AnnotationPowerStateObserved:           state.Observed,
		controller.AnnotationPowerStateObservedGeneration: strconv.FormatInt(state.ObservedGeneration, 10),
		controller.AnnotationPowerStatePhase:              state.Phase,
	}
}
