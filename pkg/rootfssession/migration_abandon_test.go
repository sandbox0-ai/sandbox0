package session

import (
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

func TestLostMigrationCutRetainsHistoryThroughCrashCleanup(t *testing.T) {
	first, host, stage := newTestManager(t, "lost-migration-cut")
	_, err := first.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	host.failAt = "freeze-xfs"
	_, err = first.CaptureMigrationRootFS(t.Context(), stage, request)
	require.Error(t, err)
	host.failAt = ""
	require.ErrorIs(t, first.AbandonLostMigrationCut(t.Context(), stage, request, "regional-failure"), errdefs.ErrFailedPrecondition,
		"a still-owned cut is not an owner-loss cleanup candidate")
	config := Config{StatePath: first.db.Path(), BranchRoot: first.branchRoot, MountRoot: first.mountRoot,
		Source: first.source, Publisher: first.publisher, Runtime: host}
	require.NoError(t, first.Close())
	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	_, err = second.CaptureMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, ErrMigrationCutOwnerLost)
	recovery, err := second.RecoverySession(stage.Parent)
	require.NoError(t, err)
	require.True(t, recovery.MigrationCutOwnerLost)
	changed := request
	changed.CaptureRequestDigest = strings.Repeat("ef", 32)
	require.Error(t, second.AbandonLostMigrationCut(t.Context(), stage, changed, "regional-failure"))
	require.NoError(t, second.AbandonLostMigrationCut(t.Context(), stage, request, "regional-failure"))
	require.NoError(t, second.AbandonLostMigrationCut(t.Context(), stage, request, "regional-failure"))
	require.ErrorIs(t, second.AbandonLostMigrationCut(t.Context(), stage, request, "different-failure"), errdefs.ErrAlreadyExists)
	stored, err := second.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stateFailed, stored.State)
	require.Nil(t, stored.Migration)
	require.Equal(t, request, stored.AbandonedMigration.Request)
	require.FileExists(t, stored.BranchPath, "failure authorization does not discard the dirty tail")
	require.NoError(t, second.ReconcileFreezes(t.Context()))
	require.Zero(t, host.count("thaw-xfs"), "background recovery cannot thaw before runtime fencing")
	_, err = second.Ensure(t.Context(), stage)
	require.Error(t, err, "abandonment cannot recreate the source writer")
	require.Error(t, second.BeginRetire(stage.Parent, stage.Identity, "ordinary-pause"))
	downgraded := stored
	downgraded.Version = 11
	require.Error(t, validateAbandonedMigrationCut(downgraded))
	require.NoError(t, second.Close())
	third, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = third.Close() })
	require.NoError(t, third.AbandonLostMigrationCut(t.Context(), stage, request, "regional-failure"))
	// The caller has fenced the guest before entering ordinary physical release.
	require.NoError(t, third.Release(t.Context(), stage.Identity))
	_, err = third.CrashFenceExternal(stage, "wrong-operation")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	_, err = third.CrashFence(stage, "regional-failure")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	_, err = third.CrashFenceExternal(stage, "regional-failure")
	require.NoError(t, err)
	stored, err = third.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, request, stored.AbandonedMigration.Request)
	require.Empty(t, stored.SealedDescriptor)
	require.FileExists(t, stored.BranchPath)
	// Reclamation is a separate call after regional terminal acknowledgement.
	require.NoError(t, third.ReclaimTerminalArtifacts(stage.Parent, stage.Identity))
	require.NoError(t, third.ForgetVerifiedTerminal(stage.Parent, stage.Identity))
	_, err = third.load(stage.Parent)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestSyncedMigrationCutCannotBecomeLostCutAbandonment(t *testing.T) {
	first, host, stage := newTestManager(t, "synced-migration-cut")
	_, err := first.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	cut, err := first.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	config := Config{StatePath: first.db.Path(), BranchRoot: first.branchRoot, MountRoot: first.mountRoot,
		Source: first.source, Publisher: first.publisher, Runtime: host}
	require.NoError(t, first.Close())
	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	require.ErrorIs(t, second.AbandonLostMigrationCut(t.Context(), stage, request, "regional-failure"), errdefs.ErrFailedPrecondition)
	actual, err := second.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, cut, actual)
}
