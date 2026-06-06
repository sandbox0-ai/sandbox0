package service

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
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

func TestPodToSandboxIncludesPowerState(t *testing.T) {
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
	assert.Equal(t, "template-1", sandbox.TemplateID)
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
		config:    SandboxServiceConfig{CtldEnabled: true},
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

func TestRequestPauseSandboxRetriesConflict(t *testing.T) {
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
	updateCalls := 0
	k8sClient.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		updateCalls++
		if updateCalls == 1 {
			return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: "pods"}, "sandbox-1", nil)
		}
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient: k8sClient,
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	resp, err := svc.RequestPauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.GreaterOrEqual(t, updateCalls, 2)

	updated, err := k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerStatePaused, state.Desired)
	assert.Equal(t, SandboxPowerPhasePausing, state.Phase)
}

func TestRequestPauseSandboxByIDRecordsDesiredState(t *testing.T) {
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
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	require.NoError(t, svc.RequestPauseSandboxByID(context.Background(), "sandbox-1"))

	updated, err := k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerStatePaused, state.Desired)
	assert.Equal(t, SandboxPowerStateActive, state.Observed)
	assert.Equal(t, SandboxPowerPhasePausing, state.Phase)
}

func TestRequestSandboxPowerRequiresCtld(t *testing.T) {
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

	_, err := svc.RequestPauseSandbox(context.Background(), "sandbox-1")
	require.ErrorIs(t, err, ErrSandboxPowerRequiresCtld)
	_, err = svc.RequestResumeSandbox(context.Background(), "sandbox-1")
	require.ErrorIs(t, err, ErrSandboxPowerRequiresCtld)

	updated, err := k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, updated.Annotations[controller.AnnotationPowerStateDesired])
}

func TestRequestResumeSandboxKeepsCtldInFlightPausePending(t *testing.T) {
	pod := newPowerStatePod(SandboxPowerStatePaused, SandboxPowerStateActive, SandboxPowerPhasePausing)
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	resp, err := svc.RequestResumeSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Observed)
	assert.Equal(t, SandboxPowerPhaseResuming, resp.PowerState.Phase)
	assert.Equal(t, int64(1), resp.PowerState.ObservedGeneration)

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerPhaseResuming, state.Phase)
}

func TestRequestPauseSandboxKeepsCtldInFlightResumePending(t *testing.T) {
	pod := newPowerStatePod(SandboxPowerStateActive, SandboxPowerStatePaused, SandboxPowerPhaseResuming)
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	resp, err := svc.RequestPauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Observed)
	assert.Equal(t, SandboxPowerPhasePausing, resp.PowerState.Phase)
	assert.Equal(t, int64(1), resp.PowerState.ObservedGeneration)

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	state := sandboxPowerStateFromAnnotations(updated.Annotations)
	assert.Equal(t, SandboxPowerPhasePausing, state.Phase)
}

func TestPauseSandboxAndWaitReturnsAfterObservedPaused(t *testing.T) {
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
	completePowerTransitionOnUpdate(t, k8sClient, SandboxPowerStatePaused)
	svc := &SandboxService{
		k8sClient: k8sClient,
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := svc.PauseSandboxAndWait(ctx, "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStatePaused, resp.PowerState.Observed)
	assert.Equal(t, SandboxPowerPhaseStable, resp.PowerState.Phase)
}

func TestResumeSandboxAndWaitReturnsAfterObservedActive(t *testing.T) {
	pod := newPowerStatePod(SandboxPowerStatePaused, SandboxPowerStatePaused, SandboxPowerPhaseStable)
	pod.Annotations[controller.AnnotationPaused] = "true"
	k8sClient := fake.NewSimpleClientset(pod)
	completePowerTransitionOnUpdate(t, k8sClient, SandboxPowerStateActive)
	svc := &SandboxService{
		k8sClient: k8sClient,
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := svc.ResumeSandboxAndWait(ctx, "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Resumed)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Observed)
	assert.Equal(t, SandboxPowerPhaseStable, resp.PowerState.Phase)
}

func TestPauseSandboxAndWaitReturnsSupersededWhenDesiredChanges(t *testing.T) {
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
	pauseRequested := notifyPowerTransitionRequest(k8sClient, SandboxPowerStatePaused)
	svc := &SandboxService{
		k8sClient: k8sClient,
		podLister: newTestPodLister(t, pod),
		config:    SandboxServiceConfig{CtldEnabled: true},
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		resp *PauseSandboxResponse
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := svc.PauseSandboxAndWait(ctx, "sandbox-1")
		resultCh <- result{resp: resp, err: err}
	}()
	<-pauseRequested

	_, err := svc.RequestResumeSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)

	res := <-resultCh
	require.ErrorIs(t, res.err, ErrSandboxPowerTransitionSuperseded)
	require.NotNil(t, res.resp)
	assert.False(t, res.resp.Paused)
	assert.Equal(t, SandboxPowerStateActive, res.resp.PowerState.Desired)
}

type staticTokenGenerator struct{}

func (staticTokenGenerator) GenerateToken(_, _, _ string) (string, error) {
	return "token", nil
}

func completePowerTransitionOnUpdate(t *testing.T, k8sClient *fake.Clientset, target string) {
	t.Helper()
	k8sClient.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		updateAction, ok := action.(ktesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := updateAction.GetObject().(*corev1.Pod)
		if !ok || pod.Annotations == nil {
			return false, nil, nil
		}
		state := sandboxPowerStateFromAnnotations(pod.Annotations)
		if state.Desired != target || state.Phase == SandboxPowerPhaseStable {
			return false, nil, nil
		}
		completed := completedSandboxPowerState(pod.Annotations, target)
		applySandboxPowerStateAnnotations(pod.Annotations, completed)
		if target == SandboxPowerStatePaused {
			pod.Annotations[controller.AnnotationPaused] = "true"
		} else {
			delete(pod.Annotations, controller.AnnotationPaused)
			delete(pod.Annotations, controller.AnnotationPausedAt)
			delete(pod.Annotations, controller.AnnotationPausedState)
		}
		return false, nil, nil
	})
}

func notifyPowerTransitionRequest(k8sClient *fake.Clientset, target string) <-chan struct{} {
	requested := make(chan struct{})
	k8sClient.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		updateAction, ok := action.(ktesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := updateAction.GetObject().(*corev1.Pod)
		if !ok {
			return false, nil, nil
		}
		state := sandboxPowerStateFromAnnotations(pod.Annotations)
		if state.Desired == target {
			notifyOnce(requested)
		}
		return false, nil, nil
	})
	return requested
}

func notifyOnce(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func newPowerStatePod(desired, observed, phase string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationPowerStateDesired:            desired,
				controller.AnnotationPowerStateDesiredGeneration:  "2",
				controller.AnnotationPowerStateObserved:           observed,
				controller.AnnotationPowerStateObservedGeneration: "1",
				controller.AnnotationPowerStatePhase:              phase,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
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
