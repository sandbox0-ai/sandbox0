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
	txns []*sandboxstore.SandboxLifecycleTxn
}

func (s staticPauseLifecycleStore) ListActiveLifecycleTxns(context.Context, string, int) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	return s.txns, nil
}

type recordingPauseReconciler struct {
	completed []string
	resumed   []string
}

func (r *recordingPauseReconciler) CompletePausingSandboxRuntime(_ context.Context, sandboxID string) error {
	r.completed = append(r.completed, sandboxID)
	return nil
}

func (r *recordingPauseReconciler) ResumePausedSandboxRuntime(_ context.Context, sandboxID string) (*managerapi.Sandbox, error) {
	r.resumed = append(r.resumed, sandboxID)
	return &managerapi.Sandbox{ID: sandboxID}, nil
}

func TestSandboxPauseControllerUsesSelectedRuntimeBackend(t *testing.T) {
	store := staticPauseLifecycleStore{txns: []*sandboxstore.SandboxLifecycleTxn{
		{SandboxID: "manual", Kind: sandboxstore.SandboxLifecycleKindPause, Source: sandboxstore.SandboxLifecycleSourceManual},
		{SandboxID: "crash", Kind: sandboxstore.SandboxLifecycleKindPause, Source: sandboxstore.SandboxLifecycleSourceCrash},
	}}
	backend := &recordingPauseReconciler{}
	controller := NewSandboxPauseController(store, backend, zap.NewNop())
	t.Cleanup(controller.queue.ShutDown)

	controller.enqueuePausingSandboxes(t.Context())
	require.True(t, controller.processNextWorkItem(t.Context()))
	require.True(t, controller.processNextWorkItem(t.Context()))

	require.ElementsMatch(t, []string{"manual", "crash"}, backend.completed)
	require.Equal(t, []string{"crash"}, backend.resumed)
}
