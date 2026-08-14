package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/fake"
)

type completingPauseEnqueuer struct {
	service *SandboxService
	done    chan error
}

func (e *completingPauseEnqueuer) EnqueueSandboxPause(sandboxID string) {
	go func() {
		e.done <- e.service.CompletePausingSandboxRuntime(context.Background(), sandboxID)
	}()
}

func newPauseAndWaitTestService(t *testing.T, ctld *httptest.Server) (*SandboxService, *memorySandboxStore, *corev1.Pod) {
	t.Helper()
	const sandboxID = "sandbox-1"
	const teamID = "team-1"
	pod := rootFSTestPod("pod-1", sandboxID, teamID)
	markRuntimeIdentityPodReady(t, pod)
	ctldURL, ctldPort := parsedTestServer(t, ctld.URL)
	pod.Status.HostIP = ctldURL.Hostname()
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{
		sandboxID: {
			ID:                  sandboxID,
			TeamID:              teamID,
			RuntimeGeneration:   runtimeGenerationFromPod(pod),
			DesiredState:        sandboxstore.SandboxDesiredStateActive,
			CurrentPodNamespace: pod.Namespace,
			CurrentPodName:      pod.Name,
		},
	}}
	service := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: store,
		ctldClient:   ctldapi.NewClientWithTimeout(time.Second),
		config:       SandboxServiceConfig{CtldEnabled: true, CtldPort: ctldPort},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}
	return service, store, pod
}

func TestPauseSandboxAndWaitDoesNotReturnBeforeCheckpointCommits(t *testing.T) {
	sealStarted := make(chan struct{})
	releaseSeal := make(chan struct{})
	ctld := newRootFSHeadCTLDServer(t, func(ctldapi.SealRootFSHeadRequest) {
		close(sealStarted)
		<-releaseSeal
	})
	defer ctld.Close()

	service, _, pod := newPauseAndWaitTestService(t, ctld)
	enqueuer := &completingPauseEnqueuer{service: service, done: make(chan error, 1)}
	service.pauseEnqueuer = enqueuer
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, service, &procdCalls)()

	type result struct {
		response *PauseSandboxResponse
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		response, err := service.PauseSandboxAndWait(context.Background(), "sandbox-1")
		resultCh <- result{response: response, err: err}
	}()

	select {
	case <-sealStarted:
	case <-time.After(time.Second):
		t.Fatal("checkpoint seal did not start")
	}
	select {
	case got := <-resultCh:
		t.Fatalf("PauseSandboxAndWait returned before checkpoint commit: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseSeal)

	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.response)
		assert.True(t, got.response.Paused)
		assert.Equal(t, managerapi.SandboxStatusPaused, got.response.Status)
	case <-time.After(2 * time.Second):
		t.Fatal("PauseSandboxAndWait did not return after checkpoint commit")
	}
	require.NoError(t, <-enqueuer.done)
}

func TestPauseSandboxAndWaitRejectsAbortedCheckpoint(t *testing.T) {
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "checkpoint failed", http.StatusInternalServerError)
	}))
	defer ctld.Close()

	service, store, pod := newPauseAndWaitTestService(t, ctld)
	enqueuer := &completingPauseEnqueuer{service: service, done: make(chan error, 1)}
	service.pauseEnqueuer = enqueuer
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, service, &procdCalls)()

	response, err := service.PauseSandboxAndWait(context.Background(), "sandbox-1")
	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), "sandbox pause checkpoint failed")
	require.Error(t, <-enqueuer.done)
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
}

func TestPauseSandboxAndWaitPreservesUnavailableCheckpointOutcome(t *testing.T) {
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"object store unavailable"}`))
	}))
	defer ctld.Close()

	service, store, pod := newPauseAndWaitTestService(t, ctld)
	enqueuer := &completingPauseEnqueuer{service: service, done: make(chan error, 1)}
	service.pauseEnqueuer = enqueuer
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, service, &procdCalls)()

	response, err := service.PauseSandboxAndWait(context.Background(), "sandbox-1")
	require.Error(t, err)
	assert.Nil(t, response)
	assert.True(t, ctldapi.IsUnavailableError(err), "error should preserve the ctld 503 outcome: %v", err)
	require.Error(t, <-enqueuer.done)
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
}

func TestPauseSandboxAndWaitUsesRecordedPodWhileInformerCacheWarms(t *testing.T) {
	ctld := newRootFSHeadCTLDServer(t, nil)
	defer ctld.Close()

	service, store, pod := newPauseAndWaitTestService(t, ctld)
	service.podLister = newTestPodLister(t)
	enqueuer := &completingPauseEnqueuer{service: service, done: make(chan error, 1)}
	service.pauseEnqueuer = enqueuer
	var procdCalls []string
	defer attachRootFSTestProcd(t, pod, service, &procdCalls)()
	client := fake.NewSimpleClientset(pod.DeepCopy())
	service.k8sClient = client

	response, err := service.PauseSandboxAndWait(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NoError(t, <-enqueuer.done)
	require.NotNil(t, response)
	assert.True(t, response.Paused)
	assert.Equal(t, managerapi.SandboxStatusPaused, response.Status)
	require.NotNil(t, store.rootFSHeads["sandbox-1"])
	assert.Equal(t, []string{"barrier:true", "pause"}, procdCalls)
	assert.Equal(t, sandboxstore.SandboxDesiredStatePaused, store.records["sandbox-1"].DesiredState)

	getCalls := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "pods" {
			getCalls++
		}
	}
	assert.GreaterOrEqual(t, getCalls, 2, "request and completion should confirm the recorded pod through the API")
}

func TestRequestPauseSandboxRuntimeDoesNotPauseWhenRecordedPodIsStronglyAbsent(t *testing.T) {
	ctld := newRootFSHeadCTLDServer(t, nil)
	defer ctld.Close()

	service, store, _ := newPauseAndWaitTestService(t, ctld)
	service.podLister = newTestPodLister(t)
	service.k8sClient = fake.NewSimpleClientset()

	_, err := service.RequestPauseSandboxRuntime(context.Background(), "sandbox-1")
	require.Error(t, err)
	assert.True(t, k8serrors.IsConflict(err))
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
	assert.Nil(t, activeLifecycleTxnForTest(store, "sandbox-1"))
}

func TestCompleteManualPauseAbortsWhenRecordedPodDisappears(t *testing.T) {
	ctld := newRootFSHeadCTLDServer(t, nil)
	defer ctld.Close()

	service, store, pod := newPauseAndWaitTestService(t, ctld)
	txnID := addRootFSTestPauseTxn(store, pod, sandboxstore.SandboxLifecyclePhasePreparing)
	store.rootFSHeads = map[string]*sandboxstore.SandboxRootFSHead{
		"sandbox-1": rootFSHeadTestFixture(t, "sandbox-1", "team-1", "committed-head", 1),
	}
	service.podLister = newTestPodLister(t)
	service.k8sClient = fake.NewSimpleClientset()

	err := service.CompletePausingSandboxRuntime(context.Background(), "sandbox-1")
	require.ErrorContains(t, err, "disappeared before rootfs checkpoint")
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
	assert.Equal(t, "committed-head", store.rootFSHeads["sandbox-1"].Reference.HeadID)
	assert.Equal(t, sandboxstore.SandboxLifecyclePhaseAborted, store.lifecycleTxns[txnID].Phase)
}
