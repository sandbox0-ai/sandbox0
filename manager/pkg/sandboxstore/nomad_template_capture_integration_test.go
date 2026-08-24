package sandboxstore

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func TestNomadRunningTemplateCapturePublishesExactSnapshotIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "template-capture")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	request := &NomadTemplateCaptureRequest{
		OperationID: "template-capture-operation", SourceSandboxID: source.ID,
		TeamID: source.TeamID, SnapshotID: "template-build-capture-operation",
	}
	candidate, err := fixture.store.RequestNomadRunningTemplateCapture(fixture.ctx, request)
	require.NoError(t, err)
	require.False(t, candidate.Completed)
	require.Equal(t, fixture.slotID, candidate.Slot.ID)
	require.Equal(t, fixture.issue.GrantID, candidate.SourceWriterGrantID)
	require.Equal(t, fixture.initial.ID, candidate.SourceGenerationID)
	require.Equal(t,
		NomadTemplateCaptureFilesystemID(request.OperationID, request.SnapshotID),
		candidate.TargetFilesystemID,
	)
	require.Equal(t,
		NomadTemplateCaptureGenerationID(request.OperationID, request.SnapshotID),
		candidate.TargetGenerationID,
	)

	retriedCandidate, err := fixture.store.RequestNomadRunningTemplateCapture(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, candidate.TargetGenerationID, retriedCandidate.TargetGenerationID)
	require.Equal(t, candidate.BindingDigest, retriedCandidate.BindingDigest)
	changedRequest := *request
	changedRequest.SnapshotID += "-changed"
	_, err = fixture.store.RequestNomadRunningTemplateCapture(fixture.ctx, &changedRequest)
	require.ErrorIs(t, err, ErrNomadTemplateCaptureConflict)

	publication := nomadTemplateCaptureCheckpointRequest(t, fixture, source, candidate)
	target, err := fixture.store.ForkRunningRootFSFilesystem(fixture.ctx, publication)
	require.NoError(t, err)
	require.Equal(t, candidate.TargetFilesystemID, target.ID)
	require.Equal(t, candidate.TargetGenerationID, target.HeadGenerationID)
	require.Equal(t, fixture.filesystem.ID, target.SourceFilesystemID)

	// A callback response lost after commit is an exact idempotent retry even
	// after the source allocation identity advances.
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes
		SET runtime_generation = runtime_generation + 1,
			runtime_id = 'replacement-allocation'
		WHERE sandbox_id = $1
	`, source.ID)
	require.NoError(t, err)
	retriedTarget, err := fixture.store.ForkRunningRootFSFilesystem(fixture.ctx, publication)
	require.NoError(t, err)
	require.Equal(t, target.ID, retriedTarget.ID)

	completed, err := fixture.store.RequestNomadRunningTemplateCapture(fixture.ctx, request)
	require.NoError(t, err)
	require.True(t, completed.Completed)
	require.NotNil(t, completed.Snapshot)
	require.Equal(t, request.SnapshotID, completed.Snapshot.ID)
	require.Equal(t, candidate.TargetFilesystemID, completed.Snapshot.FilesystemID)
	require.Equal(t, candidate.TargetGenerationID, completed.Snapshot.HeadGenerationID)
	require.Equal(t, source.ID, completed.Snapshot.SourceSandboxID)

	loadedSource, err := fixture.store.GetRootFSFilesystem(fixture.ctx, source.ID)
	require.NoError(t, err)
	require.Equal(t, fixture.initial.ID, loadedSource.HeadGenerationID)
	grant, err := fixture.store.GetRootFSWriterGrant(fixture.ctx, candidate.SourceWriterGrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State)
	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, source.ID)
	require.NoError(t, err)
	require.Nil(t, active)

	derivedID := "sandbox-template-derived"
	derived := rootFSTestSandboxRecord(derivedID, source.TeamID)
	derived.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, fixture.store.UpsertSandbox(fixture.ctx, derived))
	restored, err := fixture.store.RestoreRootFSFromSnapshot(fixture.ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: derivedID, SnapshotID: request.SnapshotID, TeamID: request.TeamID,
	})
	require.NoError(t, err)
	require.Equal(t, candidate.TargetFilesystemID, restored.SourceFilesystemID)

	require.NoError(t, fixture.store.DeleteTemplateBuildRootFSCapture(
		fixture.ctx, request.SnapshotID, request.TeamID,
	))
	_, err = fixture.store.GetRootFSSnapshot(fixture.ctx, request.SnapshotID, request.TeamID)
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	deleted, err := fixture.store.DeleteReleasedNomadTemplateCaptures(fixture.ctx, request.TeamID, 10)
	require.NoError(t, err)
	require.Zero(t, deleted, "a derived sandbox must retain the capture generation")
	require.NoError(t, fixture.store.MarkSandboxDeleted(fixture.ctx, derivedID, time.Now().UTC()))

	// Generic filesystem GC must leave capture-owned metadata to the exact
	// capture collector even after the final derived child disappears.
	deleted, err = fixture.store.DeleteUnreferencedRootFSFilesystems(fixture.ctx, request.TeamID, 10)
	require.NoError(t, err)
	require.Zero(t, deleted)
	deleted, err = fixture.store.DeleteReleasedNomadTemplateCaptures(fixture.ctx, request.TeamID, 10)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	var auditRows int
	require.NoError(t, fixture.store.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*) FROM manager.rootfs_running_template_captures WHERE operation_id = $1
	`, request.OperationID).Scan(&auditRows))
	require.Zero(t, auditRows)
}

func TestNomadRunningTemplateCaptureCancellationWinsPublicationRaceIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "template-capture-cancel")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	request := &NomadTemplateCaptureRequest{
		OperationID: "template-capture-cancel-operation", SourceSandboxID: source.ID,
		TeamID: source.TeamID, SnapshotID: "template-build-capture-cancel-operation",
	}
	candidate, err := fixture.store.RequestNomadRunningTemplateCapture(fixture.ctx, request)
	require.NoError(t, err)
	publication := nomadTemplateCaptureCheckpointRequest(t, fixture, source, candidate)

	require.NoError(t, fixture.store.DeleteTemplateBuildRootFSCapture(
		fixture.ctx, request.SnapshotID, request.TeamID,
	))
	_, err = fixture.store.ForkRunningRootFSFilesystem(fixture.ctx, publication)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound)
	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, source.ID)
	require.NoError(t, err)
	require.Nil(t, active)
	filesystem, err := fixture.store.GetRootFSFilesystem(fixture.ctx, candidate.TargetFilesystemID)
	require.NoError(t, err)
	require.Nil(t, filesystem)
}

func nomadTemplateCaptureCheckpointRequest(
	t *testing.T,
	fixture *nomadPauseStoreFixture,
	source *SandboxRecord,
	candidate *NomadTemplateCaptureCandidate,
) *ForkRunningRootFSFilesystemRequest {
	t.Helper()
	baseDescriptor, err := rootfsblock.DecodeDescriptor(fixture.initial.Descriptor)
	require.NoError(t, err)
	checkpointDescriptor, checkpointPayload, err := rootfsblock.BuildCompositeGeneration(
		baseDescriptor,
		[]rootfsblock.BlockUpdate{{Block: 7, Data: bytes.Repeat([]byte{0x51}, rootfsblock.LogicalBlockSize)}},
	)
	require.NoError(t, err)
	checkpoint := &RootFSGeneration{
		ID: candidate.TargetGenerationID, FilesystemID: candidate.TargetFilesystemID,
		ParentGenerationID: fixture.initial.ID,
		SourceOCIDigest:    fixture.initial.SourceOCIDigest,
		BaseArtifactDigest: fixture.initial.BaseArtifactDigest,
		BaseBlockRoot:      fixture.initial.BaseBlockRoot,
		CurrentBlockHead:   checkpointDescriptor.MappingRoot.RootDigest,
		WriterEpoch:        candidate.SourceWriterEpoch,
		FormatGeneration:   fixture.initial.FormatGeneration,
		DurabilityState:    RootFSGenerationStateCompositeDurable,
		LocatorVersion:     fixture.initial.LocatorVersion + 1,
		Descriptor:         checkpointPayload,
	}
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version:     rootfshandoff.RunningForkCheckpointVersion,
		OperationID: candidate.OperationID, SourceSandboxID: source.ID,
		SourceFilesystemID:         candidate.SourceFilesystemID,
		TargetSandboxID:            candidate.TargetFilesystemID,
		SourceWriterGrantID:        candidate.SourceWriterGrantID,
		SourceWriterEpoch:          candidate.SourceWriterEpoch,
		BindingVersion:             candidate.BindingVersion,
		BindingDigest:              hex.EncodeToString(candidate.BindingDigest),
		ExpectedSourceGenerationID: candidate.SourceGenerationID,
		CheckpointGenerationID:     candidate.TargetGenerationID,
		CheckpointSequence:         1,
		CheckpointDescriptorDigest: digest.FromBytes(checkpointPayload).String(),
	}
	proofDigest, err := proof.Digest()
	require.NoError(t, err)
	return &ForkRunningRootFSFilesystemRequest{
		OperationID: candidate.OperationID, SourceSandboxID: source.ID,
		TargetSandboxID: candidate.TargetFilesystemID, TargetTeamID: source.TeamID,
		SourceGrantID:     candidate.SourceWriterGrantID,
		SourceWriterEpoch: candidate.SourceWriterEpoch,
		BindingVersion:    candidate.BindingVersion, BindingDigest: candidate.BindingDigest,
		CheckpointProof: proof, CheckpointProofDigest: proofDigest[:],
		ExpectedSourceGenerationID: candidate.SourceGenerationID,
		Generation:                 checkpoint,
	}
}
