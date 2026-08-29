package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type staticPauseLifecycleStore struct {
	txns            []*sandboxstore.SandboxLifecycleTxn
	pending         map[string]bool
	claimed         map[string]bool
	claimAttempts   map[string]int
	failedAttempts  map[string]int
	completedClaims map[string]int
	lastRetryDelay  time.Duration
}

func (s staticPauseLifecycleStore) ListActiveLifecycleTxns(context.Context, string, int) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	return s.txns, nil
}

func (s staticPauseLifecycleStore) ListPendingRuntimeRecoverySandboxIDs(context.Context, int) ([]string, error) {
	var sandboxIDs []string
	for sandboxID, pending := range s.pending {
		if pending {
			sandboxIDs = append(sandboxIDs, sandboxID)
		}
	}
	return sandboxIDs, nil
}

func (s staticPauseLifecycleStore) IsRuntimeRecoveryPending(_ context.Context, sandboxID string) (bool, error) {
	return s.pending[sandboxID], nil
}

func (s *staticPauseLifecycleStore) ClaimSandboxRuntimeRecovery(
	_ context.Context,
	sandboxID, workerID string,
	leaseDuration time.Duration,
) (*sandboxstore.SandboxRuntimeRecoveryClaim, error) {
	if !s.pending[sandboxID] || s.claimed[sandboxID] {
		return nil, nil
	}
	if s.claimed == nil {
		s.claimed = make(map[string]bool)
	}
	if s.claimAttempts == nil {
		s.claimAttempts = make(map[string]int)
	}
	s.claimed[sandboxID] = true
	s.claimAttempts[sandboxID]++
	return &sandboxstore.SandboxRuntimeRecoveryClaim{
		SandboxID: sandboxID, LifecycleTxnID: "lifecycle-" + sandboxID,
		WorkerID: workerID, Token: "11111111-1111-4111-8111-111111111111",
		AttemptCount: s.claimAttempts[sandboxID], ClaimedUntil: time.Now().Add(leaseDuration),
	}, nil
}

func (s *staticPauseLifecycleStore) RenewSandboxRuntimeRecoveryClaim(
	context.Context,
	*sandboxstore.SandboxRuntimeRecoveryClaim,
	time.Duration,
) error {
	return nil
}

func (s *staticPauseLifecycleStore) FailSandboxRuntimeRecoveryClaim(
	_ context.Context,
	claim *sandboxstore.SandboxRuntimeRecoveryClaim,
	retryDelay time.Duration,
	_ string,
) error {
	s.claimed[claim.SandboxID] = false
	if s.failedAttempts == nil {
		s.failedAttempts = make(map[string]int)
	}
	s.failedAttempts[claim.SandboxID]++
	s.lastRetryDelay = retryDelay
	return nil
}

func (s *staticPauseLifecycleStore) CompleteSandboxRuntimeRecoveryClaim(
	_ context.Context,
	claim *sandboxstore.SandboxRuntimeRecoveryClaim,
) error {
	s.claimed[claim.SandboxID] = false
	if s.completedClaims == nil {
		s.completedClaims = make(map[string]int)
	}
	s.completedClaims[claim.SandboxID]++
	return nil
}

type recordingPauseReconciler struct {
	completed  []string
	resumed    []string
	onComplete func(string)
	resumeErr  error
}

func (r *recordingPauseReconciler) CompletePausingSandboxRuntime(_ context.Context, sandboxID string) error {
	r.completed = append(r.completed, sandboxID)
	if r.onComplete != nil {
		r.onComplete(sandboxID)
	}
	return nil
}

func (r *recordingPauseReconciler) ResumePausedSandboxRuntime(_ context.Context, sandboxID string) (*managerapi.Sandbox, error) {
	r.resumed = append(r.resumed, sandboxID)
	if r.resumeErr != nil {
		return nil, r.resumeErr
	}
	return &managerapi.Sandbox{ID: sandboxID}, nil
}

func TestSandboxPauseControllerUsesRuntimeReconciler(t *testing.T) {
	store := staticPauseLifecycleStore{
		txns: []*sandboxstore.SandboxLifecycleTxn{
			{SandboxID: "manual", Kind: sandboxstore.SandboxLifecycleKindPause, Source: sandboxstore.SandboxLifecycleSourceManual},
			{SandboxID: "crash", Kind: sandboxstore.SandboxLifecycleKindPause, Source: sandboxstore.SandboxLifecycleSourceCrash},
		},
		pending: map[string]bool{"crash": true},
	}
	backend := &recordingPauseReconciler{}
	controller := NewSandboxPauseController(&store, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.enqueuePausingSandboxes(t.Context())
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.True(t, controller.processNextWorkItem(t.Context()))

	require.ElementsMatch(t, []string{"manual", "crash"}, backend.completed)
	require.Equal(t, []string{"crash"}, backend.resumed)
}

func TestSandboxPauseControllerDropsStaleRecoveryBeforePauseCompletion(t *testing.T) {
	store := staticPauseLifecycleStore{pending: map[string]bool{"stale": false}}
	backend := &recordingPauseReconciler{}
	controller := NewSandboxPauseController(&store, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.EnqueueSandboxRecovery("stale")
	require.True(t, controller.processNextWorkItem(t.Context()))

	require.Empty(t, backend.completed)
	require.Empty(t, backend.resumed)
}

func TestSandboxPauseControllerDropsRecoverySupersededDuringCompletion(t *testing.T) {
	store := staticPauseLifecycleStore{pending: map[string]bool{"superseded": true}}
	backend := &recordingPauseReconciler{onComplete: func(sandboxID string) {
		store.pending[sandboxID] = false
	}}
	controller := NewSandboxPauseController(&store, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.EnqueueSandboxRecovery("superseded")
	require.True(t, controller.processNextWorkItem(t.Context()))

	require.Equal(t, []string{"superseded"}, backend.completed)
	require.Empty(t, backend.resumed)
}

func TestSandboxPauseControllerDurablyDefersFailedRecovery(t *testing.T) {
	store := staticPauseLifecycleStore{pending: map[string]bool{"retry": true}}
	backend := &recordingPauseReconciler{resumeErr: errors.New("no warm slot")}
	controller := NewSandboxPauseController(&store, backend, zap.NewNop())
	controller.retryBase = time.Hour
	controller.retryMax = time.Hour
	t.Cleanup(controller.queue.ShutDown)

	controller.EnqueueSandboxRecovery("retry")
	require.True(t, controller.processNextWorkItem(t.Context()))

	require.Equal(t, []string{"retry"}, backend.resumed)
	require.Equal(t, 1, store.claimAttempts["retry"])
	require.Equal(t, 1, store.failedAttempts["retry"])
	require.Equal(t, time.Hour, store.lastRetryDelay)
	require.Zero(t, controller.queue.NumRequeues(sandboxPauseItem{SandboxID: "retry", Resume: true}),
		"durable recovery failures must not use the process-local hot retry loop")
}

func TestSandboxRuntimeRecoveryBackoffIsBounded(t *testing.T) {
	require.Equal(t, time.Second, sandboxRuntimeRecoveryBackoff(1, time.Second, 30*time.Second))
	require.Equal(t, 2*time.Second, sandboxRuntimeRecoveryBackoff(2, time.Second, 30*time.Second))
	require.Equal(t, 16*time.Second, sandboxRuntimeRecoveryBackoff(5, time.Second, 30*time.Second))
	require.Equal(t, 30*time.Second, sandboxRuntimeRecoveryBackoff(6, time.Second, 30*time.Second))
	require.Equal(t, 30*time.Second, sandboxRuntimeRecoveryBackoff(100, time.Second, 30*time.Second))
}
