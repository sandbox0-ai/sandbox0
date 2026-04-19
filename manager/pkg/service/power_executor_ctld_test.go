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
	ktesting "k8s.io/client-go/testing"
)

func TestCtldAddressForSandboxUsesNodeInternalIP(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-1", Namespace: "default", Labels: map[string]string{"sandbox0.ai/sandbox-id": "sandbox-1"}},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}},
	}
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod, node),
		config:    SandboxServiceConfig{CtldPort: 8095},
		logger:    zap.NewNop(),
	}

	addr, err := svc.ctldAddressForSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, "http://10.0.0.12:8095", addr)
}

func TestNewSandboxServiceUsesCtldExecutorWhenEnabled(t *testing.T) {
	svc := NewSandboxService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, SandboxServiceConfig{CtldEnabled: true}, zap.NewNop(), nil)
	_, ok := svc.powerExecutor.(*ctldSandboxPowerExecutor)
	assert.True(t, ok)
	assert.Equal(t, 8095, svc.config.CtldPort)
	assert.Equal(t, 15*time.Second, svc.config.CtldClientTimeout)
	require.NotNil(t, svc.ctldClient)
	assert.Equal(t, 15*time.Second, svc.ctldClient.httpClient.Timeout)
}

func TestCtldPowerExecutorRequestsPauseAsDesiredState(t *testing.T) {
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
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095},
		logger:    zap.NewNop(),
		clock:     systemTime{},
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Observed)
	assert.Equal(t, SandboxPowerPhasePausing, resp.PowerState.Phase)

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerStatePaused, state.Desired)
	assert.Equal(t, SandboxPowerStateActive, state.Observed)
	assert.Equal(t, SandboxPowerPhasePausing, state.Phase)
	assert.Empty(t, updated.Annotations[controller.AnnotationPaused])
	assert.Empty(t, updated.Annotations[controller.AnnotationPausedState])
}

func TestCtldPowerExecutorRequestsResumeAsDesiredState(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationPaused:                       "true",
				controller.AnnotationPausedAt:                     time.Now().UTC().Format(time.RFC3339),
				controller.AnnotationPausedState:                  `{"resources":{"procd":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"200m","memory":"256Mi"}}}}`,
				controller.AnnotationPowerStateDesired:            SandboxPowerStatePaused,
				controller.AnnotationPowerStateDesiredGeneration:  "2",
				controller.AnnotationPowerStateObserved:           SandboxPowerStatePaused,
				controller.AnnotationPowerStateObservedGeneration: "2",
				controller.AnnotationPowerStatePhase:              SandboxPowerPhaseStable,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095},
		logger:    zap.NewNop(),
		clock:     systemTime{},
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	resp, err := svc.ResumeSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Resumed)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Observed)
	assert.Equal(t, SandboxPowerPhaseResuming, resp.PowerState.Phase)

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerStateActive, state.Desired)
	assert.Equal(t, SandboxPowerStatePaused, state.Observed)
	assert.Equal(t, SandboxPowerPhaseResuming, state.Phase)
	assert.Equal(t, "true", updated.Annotations[controller.AnnotationPaused])
	assert.NotEmpty(t, updated.Annotations[controller.AnnotationPausedState])
}

func TestTerminateSandboxRequestsAsyncThawBeforeDelete(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationPaused:                       "true",
				controller.AnnotationPowerStateDesired:            SandboxPowerStatePaused,
				controller.AnnotationPowerStateDesiredGeneration:  "2",
				controller.AnnotationPowerStateObserved:           SandboxPowerStatePaused,
				controller.AnnotationPowerStateObservedGeneration: "2",
				controller.AnnotationPowerStatePhase:              SandboxPowerPhaseStable,
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:    corev1.PodConditionType("sandbox0.ai/live"),
			Status:  corev1.ConditionUnknown,
			Reason:  "SandboxPaused",
			Message: "sandbox cgroup is frozen",
		}}},
	}
	k8sClient := fake.NewSimpleClientset(pod)
	svc := &SandboxService{
		k8sClient: k8sClient,
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095},
		logger:    zap.NewNop(),
	}

	err := svc.TerminateSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)

	actions := k8sClient.Actions()
	var firstUpdatedPod *corev1.Pod
	for _, action := range actions {
		updateAction, ok := action.(ktesting.UpdateAction)
		if !ok || action.GetSubresource() != "" {
			continue
		}
		firstUpdatedPod, _ = updateAction.GetObject().(*corev1.Pod)
		break
	}
	require.NotNil(t, firstUpdatedPod)
	state := sandboxPowerStateFromAnnotations(firstUpdatedPod.Annotations)
	assert.Equal(t, SandboxPowerStateActive, state.Desired)
	assert.Equal(t, SandboxPowerStatePaused, state.Observed)
	assert.Equal(t, SandboxPowerPhaseResuming, state.Phase)
	assert.Equal(t, "delete", actions[len(actions)-1].GetVerb())
}
