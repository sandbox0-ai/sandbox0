package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type staticRootFSLifecycleStore struct {
	forks   []*sandboxstore.SandboxLifecycleTxn
	rebases []*sandboxstore.SandboxLifecycleTxn
	err     error
}

func (s staticRootFSLifecycleStore) ListActiveLifecycleTxns(
	context.Context,
	string,
	int,
) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	return s.forks, s.err
}

func (s staticRootFSLifecycleStore) ListPendingNomadPausedRebases(
	context.Context,
	int,
) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	return s.rebases, s.err
}

type recordingForkReconciler struct {
	completed []string
	err       error
}

func (r *recordingForkReconciler) CompleteSandboxFork(_ context.Context, sandboxID string) error {
	r.completed = append(r.completed, sandboxID)
	return r.err
}

type recordingRebaseReconciler struct {
	completed []string
	err       error
}

func (r *recordingRebaseReconciler) CompleteSandboxRootFSRebase(
	_ context.Context,
	sandboxID string,
) error {
	r.completed = append(r.completed, sandboxID)
	return r.err
}

func TestSandboxRootFSControllerRecoversForksAndRebasesThroughSelectedBackend(t *testing.T) {
	store := staticRootFSLifecycleStore{forks: []*sandboxstore.SandboxLifecycleTxn{
		{SandboxID: "fork-source-a", Kind: sandboxstore.SandboxLifecycleKindFork},
		{SandboxID: "fork-source-b", Kind: sandboxstore.SandboxLifecycleKindFork},
	}, rebases: []*sandboxstore.SandboxLifecycleTxn{
		{SandboxID: "rebase-source", Kind: sandboxstore.SandboxLifecycleKindRebase},
	}}
	fork := &recordingForkReconciler{}
	rebase := &recordingRebaseReconciler{}
	controller := NewSandboxRootFSController(store, fork, rebase, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.enqueuePending(t.Context())
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.ElementsMatch(t, []string{"fork-source-a", "fork-source-b"}, fork.completed)
	require.Equal(t, []string{"rebase-source"}, rebase.completed)
}

func TestSandboxRootFSControllerRateLimitsFailedRecovery(t *testing.T) {
	backend := &recordingForkReconciler{err: errors.New("node channel unavailable")}
	controller := NewSandboxRootFSController(staticRootFSLifecycleStore{}, backend, nil, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)
	controller.EnqueueSandboxFork("fork-source")

	require.True(t, controller.processNextWorkItem(t.Context()))
	require.Equal(t, []string{"fork-source"}, backend.completed)
	require.Equal(t, 1, controller.queue.NumRequeues(sandboxRootFSWorkItem{
		kind: sandboxstore.SandboxLifecycleKindFork, sandboxID: "fork-source",
	}))
}
