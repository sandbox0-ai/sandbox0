package service

import (
	"context"
	"sync"
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

func TestSandboxPowerExecutorOverrideIsUsed(t *testing.T) {
	executor := &recordingPowerExecutor{}
	svc := &SandboxService{logger: zap.NewNop()}
	svc.SetPowerExecutor(executor)

	pauseResp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	resumeResp, err := svc.ResumeSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)

	assert.Equal(t, []string{"sandbox-1"}, executor.pauseCalls)
	assert.Equal(t, []string{"sandbox-1"}, executor.resumeCalls)
	assert.Equal(t, SandboxPowerStatePaused, pauseResp.PowerState.Desired)
	assert.Equal(t, SandboxPowerStateActive, resumeResp.PowerState.Desired)
}

func TestReconcileSandboxPowerStateUsesConfiguredExecutor(t *testing.T) {
	t.Run("pause", func(t *testing.T) {
		pod := newPowerStatePod(SandboxPowerStatePaused, SandboxPowerStateActive, SandboxPowerPhasePausing)
		executor := &recordingPowerExecutor{}
		svc := &SandboxService{
			k8sClient: fake.NewSimpleClientset(pod),
			podLister: newTestPodLister(t, pod),
			logger:    zap.NewNop(),
		}
		svc.SetPowerExecutor(executor)

		svc.reconcileSandboxPowerState("sandbox-1")

		assert.Equal(t, []string{"sandbox-1"}, executor.pauseCalls)
		assert.Empty(t, executor.resumeCalls)
	})

	t.Run("resume", func(t *testing.T) {
		pod := newPowerStatePod(SandboxPowerStateActive, SandboxPowerStatePaused, SandboxPowerPhaseResuming)
		executor := &recordingPowerExecutor{}
		svc := &SandboxService{
			k8sClient: fake.NewSimpleClientset(pod),
			podLister: newTestPodLister(t, pod),
			logger:    zap.NewNop(),
		}
		svc.SetPowerExecutor(executor)

		svc.reconcileSandboxPowerState("sandbox-1")

		assert.Empty(t, executor.pauseCalls)
		assert.Equal(t, []string{"sandbox-1"}, executor.resumeCalls)
	})
}

func TestRequestResumeSandboxDoesNotBlockOnInFlightReconcile(t *testing.T) {
	pod := newPowerStatePod(SandboxPowerStatePaused, SandboxPowerStateActive, SandboxPowerPhasePausing)
	executor := newBlockingPowerExecutor()
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		clock:     systemTime{},
		logger:    zap.NewNop(),
	}
	svc.SetPowerExecutor(executor)

	go svc.reconcileSandboxPowerState("sandbox-1")
	<-executor.pauseStarted

	done := make(chan *ResumeSandboxResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := svc.RequestResumeSandbox(context.Background(), "sandbox-1")
		if err != nil {
			errCh <- err
			return
		}
		done <- resp
	}()

	select {
	case err := <-errCh:
		t.Fatalf("RequestResumeSandbox returned error: %v", err)
	case resp := <-done:
		assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Desired)
		assert.Equal(t, SandboxPowerPhaseStable, resp.PowerState.Phase)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("RequestResumeSandbox blocked on in-flight reconcile")
	}

	close(executor.pauseRelease)
	<-executor.pauseFinished
}

func TestCompletePausedSandboxRejectsStaleGeneration(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationPowerStateDesired:           SandboxPowerStateActive,
				controller.AnnotationPowerStateDesiredGeneration: "2",
				controller.AnnotationPowerStateObserved:          SandboxPowerStateActive,
			},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "procd"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		config: SandboxServiceConfig{
			PauseMinCPU:            "10m",
			PauseMemoryBufferRatio: 1.1,
		},
		clock:  systemTime{},
		logger: zap.NewNop(),
	}

	resp, err := svc.completePausedSandbox(context.Background(), pod, "sandbox-1", &SandboxResourceUsage{ContainerMemoryWorkingSet: 128}, false, expectedSandboxPowerState{Desired: SandboxPowerStatePaused, Generation: 1})
	require.ErrorIs(t, err, errSandboxPowerStateStale)
	assert.False(t, resp.Paused)
	assert.Equal(t, SandboxPowerStateActive, resp.PowerState.Desired)

	updated, getErr := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Empty(t, updated.Annotations[controller.AnnotationPaused])
}

type recordingPowerExecutor struct {
	pauseCalls  []string
	resumeCalls []string
}

type blockingPowerExecutor struct {
	mu            sync.Mutex
	pauseCalls    []string
	resumeCalls   []string
	pauseStarted  chan struct{}
	pauseRelease  chan struct{}
	pauseFinished chan struct{}
}

func newBlockingPowerExecutor() *blockingPowerExecutor {
	return &blockingPowerExecutor{
		pauseStarted:  make(chan struct{}),
		pauseRelease:  make(chan struct{}),
		pauseFinished: make(chan struct{}),
	}
}

func (e *recordingPowerExecutor) Pause(_ context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	e.pauseCalls = append(e.pauseCalls, sandboxID)
	return &PauseSandboxResponse{
		SandboxID: sandboxID,
		Paused:    true,
		PowerState: SandboxPowerState{
			Desired: SandboxPowerStatePaused,
		},
	}, nil
}

func (e *recordingPowerExecutor) Resume(_ context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	e.resumeCalls = append(e.resumeCalls, sandboxID)
	return &ResumeSandboxResponse{
		SandboxID: sandboxID,
		Resumed:   true,
		PowerState: SandboxPowerState{
			Desired: SandboxPowerStateActive,
		},
	}, nil
}

func (e *blockingPowerExecutor) Pause(_ context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	e.mu.Lock()
	e.pauseCalls = append(e.pauseCalls, sandboxID)
	e.mu.Unlock()
	close(e.pauseStarted)
	<-e.pauseRelease
	close(e.pauseFinished)
	return &PauseSandboxResponse{
		SandboxID: sandboxID,
		Paused:    true,
		PowerState: SandboxPowerState{
			Desired: SandboxPowerStatePaused,
		},
	}, nil
}

func (e *blockingPowerExecutor) Resume(_ context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	e.mu.Lock()
	e.resumeCalls = append(e.resumeCalls, sandboxID)
	e.mu.Unlock()
	return &ResumeSandboxResponse{
		SandboxID: sandboxID,
		Resumed:   true,
		PowerState: SandboxPowerState{
			Desired: SandboxPowerStateActive,
		},
	}, nil
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
