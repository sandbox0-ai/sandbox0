package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRecoverTerminatedSandboxRuntimeStartsDurableCrashPause(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodRunning, 137, "OOMKilled")
	store := crashRecoveryTestStore(pod)
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))
	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))

	require.Len(t, store.lifecycleTxns, 1)
	var txn *SandboxLifecycleTxn
	for _, candidate := range store.lifecycleTxns {
		txn = candidate
	}
	require.NotNil(t, txn)
	assert.Equal(t, SandboxLifecycleKindPause, txn.Kind)
	assert.Equal(t, SandboxLifecycleSourceCrash, txn.Source)
	assert.False(t, txn.Cancelable)
	assert.Equal(t, int64(3), txn.FromGeneration)
	assert.Equal(t, pod.Namespace, txn.FromPodNamespace)
	assert.Equal(t, pod.Name, txn.FromPodName)
	assert.Equal(t, []string{"sandbox-1", "sandbox-1"}, enqueuer.calls)
}

func TestSandboxRuntimePodNeedsReplacementWhenProcdTerminatedWhilePodRunning(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodRunning, 137, "OOMKilled")

	assert.True(t, sandboxRuntimePodNeedsReplacement(pod))
}

func TestRecoverTerminatedSandboxRuntimeIgnoresStalePod(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 2, "Error")
	store := crashRecoveryTestStore(pod)
	store.records["sandbox-1"].CurrentPodName = "new-runtime"
	store.records["sandbox-1"].RuntimeGeneration = 4
	enqueuer := &recordingPauseEnqueuer{}
	svc := &SandboxService{
		sandboxStore:  store,
		pauseEnqueuer: enqueuer,
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))

	assert.Empty(t, store.lifecycleTxns)
	assert.Empty(t, enqueuer.calls)
}

func TestRecoverTerminatedSandboxRuntimeWaitsForConflictingLifecycle(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 2, "Error")
	store := crashRecoveryTestStore(pod)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"manual-pause": {
			ID:        "manual-pause",
			SandboxID: "sandbox-1",
			Kind:      SandboxLifecycleKindPause,
			Phase:     SandboxLifecyclePhasePublishing,
			Source:    SandboxLifecycleSourceManual,
		},
	}
	svc := &SandboxService{sandboxStore: store, clock: systemTime{}, logger: zap.NewNop()}

	err := svc.RecoverTerminatedSandboxRuntime(context.Background(), pod)

	require.ErrorIs(t, err, errSandboxCrashRecoveryBlocked)
	assert.Len(t, store.lifecycleTxns, 1)
}

func TestCompleteCrashRecoveryCommitsRootFSBeforeDeletingPod(t *testing.T) {
	var preparedTarget ctldapi.RootFSContainerRef
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/rootfs/snapshots/prepare":
			var req ctldapi.PrepareRootFSSnapshotRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			preparedTarget = req.Target
			require.NoError(t, json.NewEncoder(w).Encode(crashRecoveryPrepareResponse()))
		case "/api/v1/rootfs/snapshots/publish":
			require.NoError(t, json.NewEncoder(w).Encode(crashRecoveryPublishResponse()))
		default:
			t.Fatalf("unexpected ctld path %s", r.URL.Path)
		}
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := crashRecoveryTestPod(corev1.PodFailed, 137, "OOMKilled")
	pod.Status.HostIP = ctldURL.Hostname()
	store := crashRecoveryTestStore(pod)
	client := fake.NewSimpleClientset(pod.DeepCopy())
	deleted := false
	client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		record, err := store.GetSandbox(context.Background(), "sandbox-1")
		require.NoError(t, err)
		require.Equal(t, SandboxStatusPaused, record.Status, "runtime must be fenced before pod deletion")
		require.NotNil(t, store.rootFSStates["sandbox-1"], "rootfs head must commit before pod deletion")
		deleted = true
		return true, nil, nil
	})
	svc := &SandboxService{
		k8sClient:     client,
		podLister:     newTestPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    NewCtldClientWithHTTPClient(ctld.Client()),
		pauseEnqueuer: &recordingPauseEnqueuer{},
		config:        SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))
	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.True(t, deleted)
	assert.Equal(t, "containerd://terminated-container", preparedTarget.ContainerID)
	assert.Equal(t, string(pod.UID), preparedTarget.PodUID)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
	state := store.rootFSStates["sandbox-1"]
	require.NotNil(t, state)
	assert.Equal(t, "sha256:recovered", state.DiffDigest)
	assert.NotEmpty(t, state.LayerID)
}

func TestCompleteCrashRecoveryRetainsPodAndTransactionOnTransientCheckpointFailure(t *testing.T) {
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{Error: "object store temporarily unavailable"})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := crashRecoveryTestPod(corev1.PodFailed, 2, "Error")
	pod.Status.HostIP = ctldURL.Hostname()
	store := crashRecoveryTestStore(pod)
	client := fake.NewSimpleClientset(pod.DeepCopy())
	svc := &SandboxService{
		k8sClient:     client,
		podLister:     newTestPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    NewCtldClientWithHTTPClient(ctld.Client()),
		pauseEnqueuer: &recordingPauseEnqueuer{},
		config:        SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))
	err := svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1")

	require.Error(t, err)
	assert.Equal(t, SandboxStatusRunning, store.records["sandbox-1"].Status)
	require.NotNil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
	for _, action := range client.Actions() {
		assert.False(t, action.GetVerb() == "delete" && action.GetResource().Resource == "pods")
	}
}

func TestCompleteCrashRecoveryFallsBackToLastCommittedHeadWhenSnapshotWasRemoved(t *testing.T) {
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(ctldapi.PrepareRootFSSnapshotResponse{Error: "container snapshot not found"})
	}))
	defer ctld.Close()
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)

	pod := crashRecoveryTestPod(corev1.PodFailed, 137, "OOMKilled")
	pod.Status.ContainerStatuses = nil
	pod.Status.HostIP = ctldURL.Hostname()
	store := crashRecoveryTestStore(pod)
	store.rootFSStates = map[string]*SandboxRootFSState{
		"sandbox-1": {
			SandboxID:         "sandbox-1",
			TeamID:            "team-1",
			LayerID:           "previous-head",
			RuntimeGeneration: 2,
		},
	}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	deleted := false
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		deleted = true
		return true, nil, nil
	})
	svc := &SandboxService{
		k8sClient:     client,
		podLister:     newTestPodLister(t, pod),
		sandboxStore:  store,
		ctldClient:    NewCtldClientWithHTTPClient(ctld.Client()),
		pauseEnqueuer: &recordingPauseEnqueuer{},
		config:        SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}

	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))
	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.True(t, deleted)
	assert.Equal(t, SandboxStatusPaused, store.records["sandbox-1"].Status)
	assert.Equal(t, "previous-head", store.rootFSStates["sandbox-1"].LayerID)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestCrashRecoveryCommitDoesNotResurrectDeletedSandbox(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 137, "OOMKilled")
	store := crashRecoveryTestStore(pod)
	svc := &SandboxService{
		sandboxStore:  store,
		pauseEnqueuer: &recordingPauseEnqueuer{},
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}
	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))
	txn := activeLifecycleTxnForTest(store, "sandbox-1")
	require.NotNil(t, txn)
	require.NoError(t, store.MarkSandboxDeleted(context.Background(), "sandbox-1", time.Now().UTC()))

	committed, err := svc.commitPausingRuntimePaused(context.Background(), "sandbox-1", txn, 3, &SandboxRootFSState{
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
		LayerID:   "uncommitted-crash-head",
	})

	require.NoError(t, err)
	assert.False(t, committed)
	assert.Equal(t, SandboxStatusDeleted, store.records["sandbox-1"].Status)
	assert.Nil(t, store.rootFSStates["sandbox-1"])
}

func TestCompleteCrashRecoveryAbortsWhenRuntimeDeletionAlreadyStarted(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 2, "Error")
	store := crashRecoveryTestStore(pod)
	svc := &SandboxService{
		sandboxStore:  store,
		pauseEnqueuer: &recordingPauseEnqueuer{},
		clock:         systemTime{},
		logger:        zap.NewNop(),
	}
	require.NoError(t, svc.RecoverTerminatedSandboxRuntime(context.Background(), pod))

	deleting := pod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	client := fake.NewSimpleClientset(deleting.DeepCopy())
	svc.k8sClient = client
	svc.podLister = newTestPodLister(t, deleting)

	require.NoError(t, svc.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))

	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
	assert.Equal(t, SandboxStatusRunning, store.records["sandbox-1"].Status)
	for _, action := range client.Actions() {
		assert.False(t, action.GetVerb() == "delete" && action.GetResource().Resource == "pods")
	}
}

func TestCrashRecoveryControllerRecoversTerminalPodOnce(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodSucceeded, 0, "Completed")
	recoverer := &recordingCrashRecoverer{}
	controller := NewSandboxCrashRecoveryController(nil, newTestPodLister(t, pod), recoverer, zap.NewNop())

	handler := controller.ResourceEventHandler()
	handler.AddFunc(pod)
	handler.UpdateFunc(pod.DeepCopy(), pod.DeepCopy())
	require.True(t, controller.processNextWorkItem(context.Background()))

	require.Len(t, recoverer.pods, 1)
	assert.Equal(t, pod.Name, recoverer.pods[0].Name)
}

func TestCrashRecoveryControllerIgnoresStalePodUID(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 2, "Error")
	recoverer := &recordingCrashRecoverer{}
	controller := NewSandboxCrashRecoveryController(nil, newTestPodLister(t, pod), recoverer, zap.NewNop())
	controller.queue.Add(sandboxCrashRecoveryItem{
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		PodUID:    "old-pod-uid",
	})

	require.True(t, controller.processNextWorkItem(context.Background()))

	assert.Empty(t, recoverer.pods)
}

func TestCrashRecoveryControllerRecoversTerminalPodWithoutContainerStatus(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 137, "OOMKilled")
	pod.Status.ContainerStatuses = nil
	recoverer := &recordingCrashRecoverer{}
	controller := NewSandboxCrashRecoveryController(nil, newTestPodLister(t, pod), recoverer, zap.NewNop())

	controller.ResourceEventHandler().AddFunc(pod)
	require.True(t, controller.processNextWorkItem(context.Background()))

	require.Len(t, recoverer.pods, 1)
	assert.Equal(t, pod.Name, recoverer.pods[0].Name)
}

func TestCrashRecoveryControllerRetriesRecovererFailure(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 2, "Error")
	recoverer := &recordingCrashRecoverer{err: errors.New("checkpoint unavailable")}
	controller := NewSandboxCrashRecoveryController(nil, newTestPodLister(t, pod), recoverer, zap.NewNop())
	controller.queue.Add(sandboxCrashRecoveryItem{
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		PodUID:    string(pod.UID),
	})

	require.True(t, controller.processNextWorkItem(context.Background()))

	assert.Equal(t, 1, controller.queue.NumRequeues(sandboxCrashRecoveryItem{
		Namespace: pod.Namespace,
		PodName:   pod.Name,
		PodUID:    string(pod.UID),
	}))
}

func TestSandboxPauseControllerFindsCrashRecoveryAfterManagerRestart(t *testing.T) {
	pod := crashRecoveryTestPod(corev1.PodFailed, 137, "OOMKilled")
	store := crashRecoveryTestStore(pod)
	store.lifecycleTxns = map[string]*SandboxLifecycleTxn{
		"crash-pause": {
			ID:        "crash-pause",
			SandboxID: "sandbox-1",
			Kind:      SandboxLifecycleKindPause,
			Phase:     SandboxLifecyclePhasePublishing,
			Source:    SandboxLifecycleSourceCrash,
		},
	}
	controller := NewSandboxPauseController(&SandboxService{sandboxStore: store}, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.enqueuePausingSandboxes(context.Background())

	require.Equal(t, 1, controller.queue.Len())
	sandboxID, shutdown := controller.queue.Get()
	require.False(t, shutdown)
	controller.queue.Done(sandboxID)
	controller.queue.Forget(sandboxID)
	assert.Equal(t, "sandbox-1", sandboxID)
}

func crashRecoveryTestPod(phase corev1.PodPhase, exitCode int32, reason string) *corev1.Pod {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.UID = types.UID("crashed-pod-uid")
	pod.Status.Phase = phase
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:        "procd",
		ContainerID: "containerd://terminated-container",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:  exitCode,
			Reason:    reason,
			StartedAt: metav1.NewTime(time.Date(2026, time.July, 28, 1, 0, 0, 0, time.UTC)),
			FinishedAt: metav1.NewTime(
				time.Date(2026, time.July, 28, 1, 1, 0, 0, time.UTC),
			),
		}},
	}}
	return pod
}

func crashRecoveryTestStore(pod *corev1.Pod) *memorySandboxStore {
	return &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-1": {
			ID:                  "sandbox-1",
			TeamID:              "team-1",
			UserID:              "user-1",
			Status:              SandboxStatusRunning,
			CurrentPodNamespace: pod.Namespace,
			CurrentPodName:      pod.Name,
			RuntimeGeneration:   runtimeGenerationFromPod(pod),
		},
	}}
}

func crashRecoveryPrepareResponse() ctldapi.PrepareRootFSSnapshotResponse {
	return ctldapi.PrepareRootFSSnapshotResponse{
		Handle: "crash-handle",
		Info: ctldapi.RootFSInfo{
			Runtime:         "runc",
			RuntimeHandler:  "io.containerd.runc.v2",
			Snapshotter:     "overlayfs",
			BaseImageDigest: "sha256:base",
		},
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:recovered",
			Size:      42,
		},
	}
}

func crashRecoveryPublishResponse() ctldapi.PublishRootFSSnapshotResponse {
	prepared := crashRecoveryPrepareResponse()
	prepared.Descriptor.ObjectKey = "sandbox-rootfs/team-1/sandbox-1/3/sha256/recovered.tar"
	return ctldapi.PublishRootFSSnapshotResponse{
		Published:  true,
		Info:       prepared.Info,
		Descriptor: prepared.Descriptor,
	}
}

func activeLifecycleTxnForTest(store *memorySandboxStore, sandboxID string) *SandboxLifecycleTxn {
	txn, _ := store.GetActiveLifecycleTxn(context.Background(), sandboxID)
	return txn
}

type recordingCrashRecoverer struct {
	mu   sync.Mutex
	pods []*corev1.Pod
	err  error
}

func (r *recordingCrashRecoverer) RecoverTerminatedSandboxRuntime(_ context.Context, pod *corev1.Pod) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pods = append(r.pods, pod.DeepCopy())
	return r.err
}
