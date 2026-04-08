package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

func TestRequestPauseSandboxRecordsDesiredPausedState(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	k8sClient := fake.NewSimpleClientset(pod)
	svc := &SandboxService{
		k8sClient: k8sClient,
		podLister: newTestPodLister(t, pod),
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	resp, err := svc.RequestPauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerPhasePausing, resp.PowerState.Phase)

	updated, err := k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerStatePaused, state.Desired)
	assert.Equal(t, SandboxPowerPhasePausing, state.Phase)
	assert.Equal(t, int64(1), state.DesiredGeneration)
	assert.Equal(t, SandboxPowerStateActive, state.Observed)
}
