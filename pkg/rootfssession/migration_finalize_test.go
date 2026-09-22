package session

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func migrationFinalizeFixture(t *testing.T) (*Manager, *fakeHostRuntime, rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSFinalizeRequest) {
	t.Helper()
	m, runtime, stage := newTestManager(t, "migration-finalize")
	_, err := m.Ensure(t.Context(), stage)
	require.NoError(t, err)
	_, err = m.live[stage.Parent].branch.WriteAt(bytes.Repeat([]byte{0x7a}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	cut, err := m.CaptureMigrationRootFS(t.Context(), stage, migrationCutRequest(t, stage))
	require.NoError(t, err)
	detached, err := m.DetachMigrationRootFS(t.Context(), stage, rootfshandoff.MigrationRootFSDetachRequest{OperationID: cut.Request.OperationID, CutDigest: cut.Digest, AuthorizationDigest: strings.Repeat("a", 64)})
	require.NoError(t, err)
	request := rootfshandoff.MigrationRootFSFinalizeRequest{OperationID: cut.Request.OperationID, DetachProofDigest: detached.Digest, AuthorizationDigest: strings.Repeat("b", 64)}
	require.NoError(t, request.ValidateFor(stage, detached))
	return m, runtime, stage, request
}

func TestMigrationRootFSFinalizeRemovesOnlyDetachedArtifactsAndRetainsExactProof(t *testing.T) {
	m, runtime, stage, request := migrationFinalizeFixture(t)
	before, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.FileExists(t, before.BranchPath)
	paths := sessionPaths(m.branchRoot, m.mountRoot, stage.Parent)
	unrelated := filepath.Join(m.mountRoot, "unrelated")
	require.NoError(t, os.WriteFile(unrelated, []byte("retain"), 0600))
	hostCalls := runtime.callsSnapshot()
	proof, err := m.FinalizeMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(stage, *before.Migration.DetachProof, request))
	require.NoFileExists(t, before.BranchPath)
	require.NoDirExists(t, filepath.Dir(paths.xfs))
	require.FileExists(t, unrelated)
	require.Equal(t, hostCalls, runtime.callsSnapshot(), "finalization must not attach, freeze, publish or detach another runtime")
	require.Zero(t, m.nodeDirty.Usage().UsedBytes)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, before.Migration.Result, stored.Migration.Result)
	require.Equal(t, before.Migration.DetachProof, stored.Migration.DetachProof)
	require.Nil(t, stored.CrashFence)
	require.Empty(t, stored.RetireOperationID)
	again, err := m.FinalizeMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	changed := request
	changed.AuthorizationDigest = strings.Repeat("c", 64)
	_, err = m.FinalizeMigrationRootFS(t.Context(), stage, changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	require.ErrorIs(t, m.ReclaimTerminalArtifacts(stage.Parent, stage.Identity), errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, m.ForgetVerifiedTerminal(stage.Parent, stage.Identity), errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, m.ForgetFinalizedMigrationRootFS(stage, changed), errdefs.ErrFailedPrecondition)
	require.NoError(t, m.ForgetFinalizedMigrationRootFS(stage, request))
	require.NoError(t, m.ForgetFinalizedMigrationRootFS(stage, request), "exact forgotten cleanup is idempotent")
	_, err = m.RecoverySession(stage.Parent)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestMigrationRootFSFinalizeRestartsAcrossDeletionBoundaries(t *testing.T) {
	for _, boundary := range []string{"intent", "wal-removed", "artifacts-removed", "proof"} {
		t.Run(boundary, func(t *testing.T) {
			m, runtime, stage, request := migrationFinalizeFixture(t)
			stored, err := m.load(stage.Parent)
			require.NoError(t, err)
			stored.Migration.FinalizeRequest = &request
			require.NoError(t, m.save(stored))
			switch boundary {
			case "wal-removed":
				require.NoError(t, os.Remove(stored.BranchPath))
			case "artifacts-removed":
				require.NoError(t, m.reclaimTerminalArtifactsLocked(&stored))
			case "proof":
				_, err = m.FinalizeMigrationRootFS(t.Context(), stage, request)
				require.NoError(t, err)
			}
			config := Config{StatePath: m.db.Path(), BranchRoot: m.branchRoot, MountRoot: m.mountRoot, Source: m.source, Publisher: m.publisher, Runtime: runtime}
			require.NoError(t, m.Close())
			reopened, err := New(config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			proof, err := reopened.FinalizeMigrationRootFS(t.Context(), stage, request)
			require.NoError(t, err)
			require.NoError(t, proof.ValidateFor(stage, *stored.Migration.DetachProof, request))
			require.NoError(t, reopened.ForgetFinalizedMigrationRootFS(stage, request))
		})
	}
}

func TestMigrationRootFSFinalizeRejectsWrongFenceAndRetainedOwnership(t *testing.T) {
	for _, failure := range []string{"writer", "detach", "operation", "reserved-device", "live-owner", "wrong-path", "no-detach-proof"} {
		t.Run(failure, func(t *testing.T) {
			m, _, stage, request := migrationFinalizeFixture(t)
			stored, err := m.load(stage.Parent)
			require.NoError(t, err)
			branchPath := stored.BranchPath
			switch failure {
			case "writer":
				stage.Identity.WriterEpoch++
			case "detach":
				request.DetachProofDigest = strings.Repeat("d", 64)
			case "operation":
				request.OperationID = "another-operation"
			case "reserved-device":
				stored.DeviceReservationReleased = false
				require.NoError(t, m.save(stored))
			case "live-owner":
				m.live[stage.Parent] = nil
				defer delete(m.live, stage.Parent)
			case "wrong-path":
				stored.BranchPath = filepath.Join(t.TempDir(), "unrelated.wal")
				require.NoError(t, m.save(stored))
			case "no-detach-proof":
				stored.Migration.DetachProof = nil
				require.NoError(t, m.save(stored))
			}
			_, err = m.FinalizeMigrationRootFS(t.Context(), stage, request)
			require.Error(t, err)
			require.FileExists(t, branchPath)
		})
	}
}

func TestMigrationRootFSFinalizeRequiresNewJournalEnvelope(t *testing.T) {
	m, _, stage, request := migrationFinalizeFixture(t)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	stored.Migration.FinalizeRequest = &request
	stored.Version = 9
	require.ErrorIs(t, validateMigrationCutRecord(stored), errdefs.ErrFailedPrecondition)
	stored.Version = sessionSchemaVersion
	require.NoError(t, validateMigrationCutRecord(stored))
	stored.BranchRemoved = true
	stored.Migration.FinalizeRequest = nil
	require.ErrorIs(t, validateMigrationCutRecord(stored), errdefs.ErrFailedPrecondition)
}
