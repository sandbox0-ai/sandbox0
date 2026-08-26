package service

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type staticPauseLifecycleStore struct {
	txns    []*sandboxstore.SandboxLifecycleTxn
	pending map[string]bool
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

type recordingPauseReconciler struct {
	completed  []string
	resumed    []string
	onComplete func(string)
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
	controller := NewSandboxPauseController(store, backend, zap.NewNop())
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
	controller := NewSandboxPauseController(store, backend, zap.NewNop())
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
	controller := NewSandboxPauseController(store, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.EnqueueSandboxRecovery("superseded")
	require.True(t, controller.processNextWorkItem(t.Context()))

	require.Equal(t, []string{"superseded"}, backend.completed)
	require.Empty(t, backend.resumed)
}
