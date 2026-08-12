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
	assert.Contains(t, err.Error(), "pause did not complete")
	require.Error(t, <-enqueuer.done)
	assert.Equal(t, sandboxstore.SandboxDesiredStateActive, store.records["sandbox-1"].DesiredState)
}
