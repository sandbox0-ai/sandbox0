package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func captureRunningRootFSTemplate(
	ctx context.Context,
	tx pgx.Tx,
	sourceSandbox *SandboxRecord,
	intent *nomadTemplateCaptureIntent,
	req *ForkRunningRootFSFilesystemRequest,
) (*RootFSFilesystem, error) {
	if intent == nil || req == nil || sourceSandbox == nil ||
		intent.OperationID != req.OperationID || intent.SourceSandboxID != req.SourceSandboxID ||
		intent.SourceFilesystemID != req.CheckpointProof.SourceFilesystemID ||
		intent.SourceGrantID != req.SourceGrantID || intent.SourceWriterEpoch != req.SourceWriterEpoch ||
		intent.SourceGenerationID != req.ExpectedSourceGenerationID ||
		intent.TargetFilesystemID != req.TargetSandboxID ||
		intent.CheckpointGeneration != req.Generation.ID || req.Generation.FilesystemID != intent.TargetFilesystemID ||
		intent.BindingVersion != req.BindingVersion || !bytes.Equal(intent.BindingDigest, req.BindingDigest) {
		return nil, fmt.Errorf("%w: running template checkpoint identity changed", ErrRootFSFilesystemConflict)
	}
	if intent.State == nomadTemplateCaptureStatePublished {
		return loadRunningRootFSTemplateCaptureRetry(ctx, tx, sourceSandbox, intent, req)
	}
	if intent.State != nomadTemplateCaptureStatePending || sourceSandbox.TeamID != intent.TeamID {
		return nil, fmt.Errorf("%w: template capture intent is not pending", ErrRootFSFilesystemConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
	`, intent.OperationID, sourceSandbox.ID))
	if err != nil {
		return nil, fmt.Errorf("lock running template capture lifecycle: %w", err)
	}
	if !nomadTemplateCaptureLifecycleMatches(lifecycle, sourceSandbox, intent, false) {
		return nil, fmt.Errorf("%w: running template capture lifecycle changed", ErrRootFSFilesystemConflict)
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, sourceSandbox)
	if err != nil {
		return nil, err
	}
	if writer.filesystem.ID != intent.SourceFilesystemID || writer.generation.ID != intent.SourceGenerationID ||
		writer.grant.ID != intent.SourceGrantID || writer.grant.WriterEpoch != intent.SourceWriterEpoch ||
		writer.grant.BindingVersion != intent.BindingVersion ||
		!bytes.Equal(writer.grant.BindingDigest, intent.BindingDigest) {
		return nil, fmt.Errorf("%w: source writer changed before template publication", ErrRootFSWriterGrantConflict)
	}
	checkpoint := req.Generation
	if req.CheckpointProof.SourceFilesystemID != writer.filesystem.ID ||
		checkpoint.SourceOCIDigest != writer.generation.SourceOCIDigest ||
		checkpoint.BaseArtifactDigest != writer.generation.BaseArtifactDigest ||
		checkpoint.BaseBlockRoot != writer.generation.BaseBlockRoot ||
		checkpoint.FormatGeneration != writer.generation.FormatGeneration {
		return nil, fmt.Errorf("%w: template checkpoint changed immutable source lineage", ErrRootFSGenerationConflict)
	}
	sourceDescriptor, err := rootfsblock.DecodeDescriptor(writer.generation.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("decode template capture source generation: %w", err)
	}
	checkpointDescriptor, err := rootfsblock.DecodeDescriptor(checkpoint.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("decode template capture checkpoint generation: %w", err)
	}
	if checkpointDescriptor.LogicalSizeBytes != sourceDescriptor.LogicalSizeBytes ||
		checkpointDescriptor.BlockSizeBytes != sourceDescriptor.BlockSizeBytes {
		return nil, fmt.Errorf("%w: template checkpoint changed logical device geometry", ErrRootFSGenerationConflict)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id, source_filesystem_id,
			head_generation_id, writer_epoch, base_artifact_digest, format_generation,
			created_at, updated_at
		) VALUES ($1, $2, $3, NULL, $4, $5, $6, NOW(), NOW())
		ON CONFLICT (filesystem_id) DO NOTHING
	`, intent.TargetFilesystemID, intent.TeamID, writer.filesystem.ID, intent.SourceWriterEpoch,
		writer.filesystem.BaseArtifactDigest, writer.filesystem.FormatGeneration)
	if err != nil {
		return nil, fmt.Errorf("create template capture filesystem: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: template capture filesystem already exists", ErrRootFSFilesystemConflict)
	}
	if err := insertPreparedRootFSGeneration(ctx, tx, checkpoint); err != nil {
		return nil, err
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_generation_id = $2, updated_at = NOW()
		WHERE filesystem_id = $1 AND head_generation_id IS NULL AND writer_epoch = $3
	`, intent.TargetFilesystemID, intent.CheckpointGeneration, intent.SourceWriterEpoch)
	if err != nil {
		return nil, fmt.Errorf("install template capture checkpoint: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: template capture filesystem changed", ErrRootFSFilesystemConflict)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_snapshots (
			snapshot_id, filesystem_id, team_id, source_sandbox_id,
			head_generation_id, name, description, created_at, expires_at
		) VALUES ($1, $2, $3, $4, $5,
			'Template RootFS capture',
			'Internal immutable live-writer checkpoint retained by a template.',
			NOW(), NULL)
	`, intent.SnapshotID, intent.TargetFilesystemID, intent.TeamID,
		intent.SourceSandboxID, intent.CheckpointGeneration); err != nil {
		return nil, fmt.Errorf("create running template snapshot: %w", err)
	}
	proofDigest := append([]byte(nil), req.CheckpointProofDigest...)
	tag, err = tx.Exec(ctx, `
		UPDATE manager.rootfs_running_template_captures
		SET state = $2,
			checkpoint_sequence = $3,
			checkpoint_descriptor_digest = $4,
			checkpoint_proof_digest = $5,
			published_at = NOW(),
			updated_at = NOW()
		WHERE operation_id = $1 AND state = $6
	`, intent.OperationID, nomadTemplateCaptureStatePublished,
		int64(req.CheckpointProof.CheckpointSequence), req.CheckpointProof.CheckpointDescriptorDigest,
		proofDigest, nomadTemplateCaptureStatePending)
	if err != nil {
		return nil, fmt.Errorf("publish running template capture intent: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: template capture intent changed", ErrRootFSFilesystemConflict)
	}
	if err := (sandboxStoreTx{tx: tx}).CommitLifecycleTxn(
		ctx, intent.OperationID, intent.CheckpointGeneration,
	); err != nil {
		return nil, fmt.Errorf("commit running template capture lifecycle: %w", err)
	}
	return getRootFSFilesystemByID(ctx, tx, intent.TargetFilesystemID)
}

func loadRunningRootFSTemplateCaptureRetry(
	ctx context.Context,
	tx pgx.Tx,
	source *SandboxRecord,
	intent *nomadTemplateCaptureIntent,
	req *ForkRunningRootFSFilesystemRequest,
) (*RootFSFilesystem, error) {
	if intent.CheckpointSequence == nil || intent.DescriptorDigest == nil ||
		*intent.CheckpointSequence != int64(req.CheckpointProof.CheckpointSequence) ||
		*intent.DescriptorDigest != req.CheckpointProof.CheckpointDescriptorDigest ||
		!bytes.Equal(intent.ProofDigest, req.CheckpointProofDigest) {
		return nil, fmt.Errorf("%w: published template checkpoint proof changed", ErrRootFSFilesystemConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR SHARE
	`, intent.OperationID, source.ID))
	if err != nil {
		return nil, fmt.Errorf("load published template capture lifecycle: %w", err)
	}
	if !nomadTemplateCaptureLifecycleMatches(lifecycle, source, intent, true) {
		return nil, fmt.Errorf("%w: published template capture lifecycle changed", ErrRootFSFilesystemConflict)
	}
	var snapshotFilesystemID, snapshotGenerationID string
	if err := tx.QueryRow(ctx, `
		SELECT filesystem_id, head_generation_id
		FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1 AND team_id = $2
	`, intent.SnapshotID, intent.TeamID).Scan(&snapshotFilesystemID, &snapshotGenerationID); err != nil {
		return nil, fmt.Errorf("load published template snapshot retry: %w", err)
	}
	if snapshotFilesystemID != intent.TargetFilesystemID || snapshotGenerationID != intent.CheckpointGeneration {
		return nil, fmt.Errorf("%w: published template snapshot changed", ErrRootFSFilesystemConflict)
	}
	storedGeneration, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1
	`, intent.CheckpointGeneration))
	if err != nil {
		return nil, fmt.Errorf("load published template generation retry: %w", err)
	}
	if !runningForkGenerationMatches(storedGeneration, req.Generation) ||
		digest.FromBytes(storedGeneration.Descriptor).String() != req.CheckpointProof.CheckpointDescriptorDigest {
		return nil, fmt.Errorf("%w: published template generation changed", ErrRootFSGenerationConflict)
	}
	return getRootFSFilesystemByID(ctx, tx, intent.TargetFilesystemID)
}

func getRootFSFilesystemByID(ctx context.Context, tx pgx.Tx, filesystemID string) (*RootFSFilesystem, error) {
	filesystem, err := scanRootFSFilesystem(tx.QueryRow(ctx, `
		SELECT filesystem.filesystem_id, filesystem.team_id,
			filesystem.source_filesystem_id, filesystem.writer_epoch,
			filesystem.head_generation_id, filesystem.base_artifact_digest,
			filesystem.format_generation, artifact.source_oci_ref,
			generation.source_oci_digest, filesystem.created_at, filesystem.updated_at
		FROM manager.rootfs_filesystems filesystem
		LEFT JOIN manager.rootfs_generations generation
			ON generation.generation_id = filesystem.head_generation_id
		JOIN manager.rootfs_base_artifacts artifact
			ON artifact.artifact_digest = filesystem.base_artifact_digest
		WHERE filesystem.filesystem_id = $1
	`, filesystemID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSFilesystemNotFound, filesystemID)
	}
	return filesystem, err
}
