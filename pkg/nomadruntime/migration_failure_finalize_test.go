package nomadruntime

import (
	"context"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

type failedTargetTerminalAuthority struct {
	rootFSWriterAuthority
	allowed bool
	calls   int
}

func (a *failedTargetTerminalAuthority) VerifyTerminalWriterGrant(context.Context, rootfshandoff.StageRequest) error {
	a.calls++
	if !a.allowed {
		return errdefs.ErrPermissionDenied
	}
	return nil
}

func TestFailedMigrationRootFSFinalizationRequiresTerminalAuthority(t *testing.T) {
	f := newRuntimeTerminalExpiryFixture(t)
	f.fence(t, false)
	authority := &failedTargetTerminalAuthority{}
	runtime := &rootfsRuntime{sessions: f.manager, authority: authority}
	require.ErrorIs(t, runtime.FinalizeFailedMigrationRootFS(t.Context(), f.stage), errdefs.ErrPermissionDenied)
	parent, err := f.manager.Parent(f.stage.Identity)
	require.NoError(t, err)
	require.Equal(t, f.stage.Parent, parent)
	sessions, err := f.manager.RecoverySessions()
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	require.False(t, sessions[0].BranchRemoved)
	authority.allowed = true
	require.NoError(t, runtime.FinalizeFailedMigrationRootFS(t.Context(), f.stage))
	_, err = f.manager.Parent(f.stage.Identity)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.NoError(t, runtime.FinalizeFailedMigrationRootFS(t.Context(), f.stage), "replay after compact journal deletion remains idempotent")
	require.Equal(t, 3, authority.calls)
	require.DirExists(t, f.config.BranchRoot)
	require.DirExists(t, f.config.MountRoot)
}
