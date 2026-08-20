package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type staticForkLifecycleStore struct {
	txns []*sandboxstore.SandboxLifecycleTxn
	err  error
}

func (s staticForkLifecycleStore) ListActiveLifecycleTxns(
	context.Context,
	string,
	int,
) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	return s.txns, s.err
}

type recordingForkReconciler struct {
	completed []string
	err       error
}

func (r *recordingForkReconciler) CompleteSandboxFork(_ context.Context, sandboxID string) error {
	r.completed = append(r.completed, sandboxID)
	return r.err
}

func TestSandboxForkControllerRecoversActiveTransactionsThroughSelectedBackend(t *testing.T) {
	store := staticForkLifecycleStore{txns: []*sandboxstore.SandboxLifecycleTxn{
		{SandboxID: "fork-source-a", Kind: sandboxstore.SandboxLifecycleKindFork},
		{SandboxID: "fork-source-b", Kind: sandboxstore.SandboxLifecycleKindFork},
	}}
	backend := &recordingForkReconciler{}
	controller := NewSandboxForkController(store, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.enqueueActiveForks(t.Context())
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.ElementsMatch(t, []string{"fork-source-a", "fork-source-b"}, backend.completed)
}

func TestSandboxForkControllerRateLimitsFailedRecovery(t *testing.T) {
	backend := &recordingForkReconciler{err: errors.New("node channel unavailable")}
	controller := NewSandboxForkController(staticForkLifecycleStore{}, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)
	controller.EnqueueSandboxFork("fork-source")

	require.True(t, controller.processNextWorkItem(t.Context()))
	require.Equal(t, []string{"fork-source"}, backend.completed)
	require.Equal(t, 1, controller.queue.NumRequeues("fork-source"))
}
