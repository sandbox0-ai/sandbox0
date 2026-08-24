package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// ForkRunningRootFSFilesystemRequest installs a node checkpoint as a new
// target filesystem while leaving the source writer grant and head untouched.
type ForkRunningRootFSFilesystemRequest struct {
	OperationID                string
	SourceSandboxID            string
	TargetSandboxID            string
	TargetTeamID               string
	SourceGrantID              string
	SourceWriterEpoch          int64
	BindingVersion             int
	BindingDigest              []byte
	CheckpointProof            rootfshandoff.RunningForkCheckpointProof
	CheckpointProofDigest      []byte
	ExpectedSourceGenerationID string
	Generation                 *RootFSGeneration
}

// ForkRunningRootFSFilesystem atomically binds an immutable live checkpoint
// to a paused target. The source's consumed writer remains renewable and its
// durable head is deliberately not advanced.
func (s *PGSandboxStore) ForkRunningRootFSFilesystem(
	ctx context.Context,
	req *ForkRunningRootFSFilesystemRequest,
) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := validateForkRunningRootFSFilesystemRequest(req)
	if err != nil {
		return nil, err
	}
	var target *RootFSFilesystem
	err = s.WithSandboxLock(ctx, normalized.SourceSandboxID, func(
		lockCtx context.Context,
		locked SandboxStoreTx,
		sourceSandbox *SandboxRecord,
	) error {
		txStore, ok := locked.(sandboxStoreTx)
		if !ok {
			return fmt.Errorf("running rootfs fork requires a PostgreSQL transaction")
		}
		var forkErr error
		target, forkErr = forkRunningRootFSFilesystem(lockCtx, txStore.tx, sourceSandbox, normalized)
		return forkErr
	})
	return target, err
}

func validateForkRunningRootFSFilesystemRequest(
	req *ForkRunningRootFSFilesystemRequest,
) (*ForkRunningRootFSFilesystemRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("running rootfs fork request is required")
	}
	normalized := *req
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.SourceSandboxID = strings.TrimSpace(req.SourceSandboxID)
	normalized.TargetSandboxID = strings.TrimSpace(req.TargetSandboxID)
	normalized.TargetTeamID = strings.TrimSpace(req.TargetTeamID)
	normalized.SourceGrantID = strings.TrimSpace(req.SourceGrantID)
	normalized.ExpectedSourceGenerationID = strings.TrimSpace(req.ExpectedSourceGenerationID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	normalized.CheckpointProofDigest = append([]byte(nil), req.CheckpointProofDigest...)
	for name, value := range map[string]string{
		"operation_id": normalized.OperationID, "source_sandbox_id": normalized.SourceSandboxID,
		"target_sandbox_id": normalized.TargetSandboxID, "source_grant_id": normalized.SourceGrantID,
		"expected_source_generation_id": normalized.ExpectedSourceGenerationID,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
	}
	if normalized.SourceSandboxID == normalized.TargetSandboxID {
		return nil, fmt.Errorf("source and target sandboxes must differ")
	}
	if normalized.SourceWriterEpoch <= 0 {
		return nil, fmt.Errorf("source_writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("checkpoint_proof_digest", normalized.CheckpointProofDigest); err != nil {
		return nil, err
	}
	generation, err := normalizeDurableRootFSGeneration(req.Generation, normalized.ExpectedSourceGenerationID)
	if err != nil {
		return nil, err
	}
	if generation.FilesystemID != normalized.TargetSandboxID || generation.WriterEpoch != normalized.SourceWriterEpoch {
		return nil, fmt.Errorf("checkpoint generation must belong to the target at the source writer epoch")
	}
	proof := normalized.CheckpointProof
	proofDigest, err := proof.Digest()
	if err != nil {
		return nil, fmt.Errorf("checkpoint proof: %w", err)
	}
	descriptorDigest := digest.FromBytes(generation.Descriptor).String()
	if proof.OperationID != normalized.OperationID || proof.SourceSandboxID != normalized.SourceSandboxID ||
		proof.TargetSandboxID != normalized.TargetSandboxID || proof.SourceWriterGrantID != normalized.SourceGrantID ||
		proof.SourceWriterEpoch != normalized.SourceWriterEpoch || proof.BindingVersion != normalized.BindingVersion ||
		proof.BindingDigest != fmt.Sprintf("%x", normalized.BindingDigest) ||
		proof.ExpectedSourceGenerationID != normalized.ExpectedSourceGenerationID ||
		proof.CheckpointGenerationID != generation.ID || proof.CheckpointDescriptorDigest != descriptorDigest ||
		!bytes.Equal(proofDigest[:], normalized.CheckpointProofDigest) {
		return nil, fmt.Errorf("checkpoint proof does not match the running fork request")
	}
	normalized.Generation = generation
	return &normalized, nil
}

func forkRunningRootFSFilesystem(
	ctx context.Context,
	tx pgx.Tx,
	sourceSandbox *SandboxRecord,
	req *ForkRunningRootFSFilesystemRequest,
) (*RootFSFilesystem, error) {
	intent, err := getNomadTemplateCaptureIntentForUpdate(ctx, tx, req.OperationID)
	if err != nil {
		return nil, err
	}
	if intent != nil {
		return captureRunningRootFSTemplate(ctx, tx, sourceSandbox, intent, req)
	}
	if retry, err := loadRunningRootFSForkRetry(ctx, tx, req); err != nil || retry != nil {
		return retry, err
	}
	if sourceSandbox == nil || sourceSandbox.ID != req.SourceSandboxID || !sourceSandbox.DeletedAt.IsZero() ||
		sourceSandbox.DesiredState != SandboxDesiredStateActive {
		return nil, fmt.Errorf("%w: source sandbox is not active", ErrRootFSFilesystemConflict)
	}
	targetSandbox, err := scanSandboxRecord(tx.QueryRow(ctx,
		sandboxRecordSelectSQL()+` WHERE sandbox_id = $1 FOR UPDATE`, req.TargetSandboxID))
	if errors.Is(err, pgx.ErrNoRows) || targetSandbox == nil {
		return nil, fmt.Errorf("%w: target sandbox %s", ErrRootFSFilesystemNotFound, req.TargetSandboxID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock running fork target sandbox: %w", err)
	}
	teamID := req.TargetTeamID
	if teamID == "" {
		teamID = sourceSandbox.TeamID
	}
	if sourceSandbox.TeamID == "" || targetSandbox.TeamID != sourceSandbox.TeamID || targetSandbox.TeamID != teamID ||
		targetSandbox.DesiredState != SandboxDesiredStatePaused || targetSandbox.RuntimeGeneration != 0 ||
		targetSandbox.RuntimeID != "" || targetSandbox.RuntimeNamespace != "" ||
		!targetSandbox.DeletedAt.IsZero() {
		return nil, fmt.Errorf("%w: target sandbox is not a paused team-owned destination", ErrRootFSFilesystemConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
	`, req.OperationID, req.SourceSandboxID))
	if err != nil {
		return nil, fmt.Errorf("lock running-fork lifecycle: %w", err)
	}
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindFork ||
		lifecycle.Phase != SandboxLifecyclePhasePublishing || lifecycle.Source != SandboxLifecycleSourceManual ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		lifecycle.FromGeneration != sourceSandbox.RuntimeGeneration ||
		lifecycle.ToGeneration != sourceSandbox.RuntimeGeneration ||
		lifecycle.FromRuntimeNamespace != sourceSandbox.RuntimeNamespace ||
		lifecycle.FromRuntimeID != sourceSandbox.RuntimeID ||
		lifecycle.ToRuntimeNamespace != "" || lifecycle.ToRuntimeID != "" ||
		lifecycle.TargetSandboxID != req.TargetSandboxID ||
		lifecycle.TargetGenerationID != req.Generation.ID ||
		lifecycle.ExpectedGenerationID != req.ExpectedSourceGenerationID || lifecycle.PreparedGenerationID != "" {
		return nil, fmt.Errorf("%w: source running-fork lifecycle changed", ErrRootFSFilesystemConflict)
	}
	targetRecordDigest, err := NomadSandboxForkTargetRecordDigest(targetSandbox)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(lifecycle.TargetRecordDigest, targetRecordDigest) {
		return nil, fmt.Errorf("%w: running-fork target identity changed", ErrRootFSFilesystemConflict)
	}
	targetLifecycle, err := getActiveLifecycleTxn(ctx, tx, req.TargetSandboxID)
	if err != nil {
		return nil, fmt.Errorf("lock running-fork target lifecycle: %w", err)
	}
	if targetLifecycle != nil {
		return nil, fmt.Errorf("%w: target sandbox has an active lifecycle operation", ErrRootFSFilesystemConflict)
	}
	targetClaim, err := lockSandboxRuntimeClaim(ctx, tx, req.TargetSandboxID)
	if err != nil {
		return nil, err
	}
	if targetClaim.OperationID != NomadSandboxForkClaimOperationID(req.OperationID, req.TargetSandboxID) ||
		targetClaim.Phase != SandboxRuntimeClaimPhaseReady || !targetClaim.CompletedAt.IsZero() ||
		!targetClaim.LeaseExpiresAt.IsZero() {
		return nil, fmt.Errorf("%w: target logical claim changed", ErrRootFSFilesystemConflict)
	}
	var targetBound bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.sandbox_rootfs_bindings WHERE sandbox_id = $1)
	`, req.TargetSandboxID).Scan(&targetBound); err != nil {
		return nil, fmt.Errorf("check running fork target binding: %w", err)
	}
	if targetBound {
		return nil, fmt.Errorf("%w: target sandbox already has a rootfs", ErrRootFSFilesystemConflict)
	}

	source, sourceGeneration, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, req.SourceSandboxID)
	if err != nil {
		return nil, err
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, req.SourceGrantID)
	if err != nil {
		return nil, err
	}
	if source.TeamID != teamID ||
		source.HeadGenerationID != req.ExpectedSourceGenerationID || source.WriterEpoch != req.SourceWriterEpoch ||
		sourceGeneration.ID != req.ExpectedSourceGenerationID {
		return nil, fmt.Errorf("%w: source head or writer epoch changed", ErrRootFSFilesystemConflict)
	}
	if grant.FilesystemID != source.ID || grant.SandboxID != req.SourceSandboxID ||
		grant.InitialGenerationID != req.ExpectedSourceGenerationID || grant.WriterEpoch != req.SourceWriterEpoch ||
		grant.BindingVersion != req.BindingVersion || !bytes.Equal(grant.BindingDigest, req.BindingDigest) {
		return nil, fmt.Errorf("%w: source writer binding changed", ErrRootFSWriterGrantConflict)
	}
	if grant.State != RootFSWriterGrantStateConsumed {
		return nil, rootFSWriterGrantStateError(grant)
	}
	if !grant.LeaseExpiresAt.After(grant.databaseNow) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterLeaseExpired, grant.ID)
	}
	checkpoint := req.Generation
	if req.CheckpointProof.SourceFilesystemID != source.ID ||
		checkpoint.SourceOCIDigest != sourceGeneration.SourceOCIDigest ||
		checkpoint.BaseArtifactDigest != sourceGeneration.BaseArtifactDigest ||
		checkpoint.BaseBlockRoot != sourceGeneration.BaseBlockRoot ||
		checkpoint.FormatGeneration != sourceGeneration.FormatGeneration {
		return nil, fmt.Errorf("%w: checkpoint changed immutable source lineage", ErrRootFSGenerationConflict)
	}
	sourceDescriptor, err := rootfsblock.DecodeDescriptor(sourceGeneration.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("decode running fork source generation: %w", err)
	}
	checkpointDescriptor, err := rootfsblock.DecodeDescriptor(checkpoint.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("decode running fork checkpoint generation: %w", err)
	}
	if checkpointDescriptor.LogicalSizeBytes != sourceDescriptor.LogicalSizeBytes ||
		checkpointDescriptor.BlockSizeBytes != sourceDescriptor.BlockSizeBytes {
		return nil, fmt.Errorf("%w: checkpoint changed logical device geometry", ErrRootFSGenerationConflict)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id, source_filesystem_id,
			head_generation_id, writer_epoch, base_artifact_digest, format_generation,
			created_at, updated_at
		) VALUES ($1, $2, $3, NULL, $4, $5, $6, NOW(), NOW())
		ON CONFLICT (filesystem_id) DO NOTHING
	`, req.TargetSandboxID, teamID, source.ID, req.SourceWriterEpoch,
		source.BaseArtifactDigest, source.FormatGeneration)
	if err != nil {
		return nil, fmt.Errorf("create running fork filesystem: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: target filesystem already exists", ErrRootFSFilesystemConflict)
	}
	if err := insertPreparedRootFSGeneration(ctx, tx, checkpoint); err != nil {
		return nil, err
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_generation_id = $2, updated_at = NOW()
		WHERE filesystem_id = $1 AND head_generation_id IS NULL AND writer_epoch = $3
	`, req.TargetSandboxID, checkpoint.ID, req.SourceWriterEpoch)
	if err != nil {
		return nil, fmt.Errorf("install running fork checkpoint: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: running fork checkpoint target changed", ErrRootFSFilesystemConflict)
	}
	tag, err = tx.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_bindings (
			sandbox_id, filesystem_id, team_id, created_at, updated_at
		) VALUES ($1, $1, $2, NOW(), NOW())
		ON CONFLICT (sandbox_id) DO NOTHING
	`, req.TargetSandboxID, teamID)
	if err != nil {
		return nil, fmt.Errorf("bind running fork target: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: running fork target binding changed", ErrRootFSFilesystemConflict)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO manager.rootfs_running_forks (
			operation_id, source_sandbox_id, source_filesystem_id, source_grant_id,
			source_writer_epoch, source_generation_id, target_sandbox_id,
			target_filesystem_id, checkpoint_generation_id, binding_version,
			binding_digest, checkpoint_sequence, checkpoint_descriptor_digest,
			checkpoint_proof_digest, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, $11, $12, $13, NOW())
	`, req.OperationID, req.SourceSandboxID, source.ID, req.SourceGrantID,
		req.SourceWriterEpoch, req.ExpectedSourceGenerationID, req.TargetSandboxID,
		checkpoint.ID, req.BindingVersion, req.BindingDigest, req.CheckpointProof.CheckpointSequence,
		req.CheckpointProof.CheckpointDescriptorDigest, req.CheckpointProofDigest)
	if err != nil {
		return nil, fmt.Errorf("record running rootfs fork: %w", err)
	}
	if err := (sandboxStoreTx{tx: tx}).CommitLifecycleTxn(ctx, req.OperationID, checkpoint.ID); err != nil {
		return nil, fmt.Errorf("commit running-fork lifecycle: %w", err)
	}
	return getRootFSFilesystemByID(ctx, tx, req.TargetSandboxID)
}

func loadRunningRootFSForkRetry(
	ctx context.Context,
	tx pgx.Tx,
	req *ForkRunningRootFSFilesystemRequest,
) (*RootFSFilesystem, error) {
	var sourceSandboxID, sourceFilesystemID, sourceGrantID, sourceGenerationID string
	var targetSandboxID, targetFilesystemID, checkpointGenerationID string
	var sourceWriterEpoch int64
	var bindingVersion int
	var checkpointSequence int64
	var checkpointDescriptorDigest string
	var bindingDigest, checkpointProofDigest []byte
	err := tx.QueryRow(ctx, `
		SELECT source_sandbox_id, source_filesystem_id, source_grant_id,
			source_writer_epoch, source_generation_id, target_sandbox_id,
			target_filesystem_id, checkpoint_generation_id, binding_version,
			binding_digest, checkpoint_sequence, checkpoint_descriptor_digest,
			checkpoint_proof_digest
		FROM manager.rootfs_running_forks WHERE operation_id = $1
	`, req.OperationID).Scan(
		&sourceSandboxID, &sourceFilesystemID, &sourceGrantID, &sourceWriterEpoch,
		&sourceGenerationID, &targetSandboxID, &targetFilesystemID,
		&checkpointGenerationID, &bindingVersion, &bindingDigest, &checkpointSequence,
		&checkpointDescriptorDigest, &checkpointProofDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load running rootfs fork retry: %w", err)
	}
	if sourceSandboxID != req.SourceSandboxID || sourceGrantID != req.SourceGrantID ||
		req.CheckpointProof.SourceFilesystemID != sourceFilesystemID ||
		sourceWriterEpoch != req.SourceWriterEpoch || sourceGenerationID != req.ExpectedSourceGenerationID ||
		targetSandboxID != req.TargetSandboxID || targetFilesystemID != req.Generation.FilesystemID ||
		checkpointGenerationID != req.Generation.ID || bindingVersion != req.BindingVersion ||
		checkpointSequence != int64(req.CheckpointProof.CheckpointSequence) ||
		checkpointDescriptorDigest != req.CheckpointProof.CheckpointDescriptorDigest ||
		!bytes.Equal(bindingDigest, req.BindingDigest) || !bytes.Equal(checkpointProofDigest, req.CheckpointProofDigest) {
		return nil, fmt.Errorf("%w: running fork operation fields changed", ErrRootFSFilesystemConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR SHARE
	`, req.OperationID, req.SourceSandboxID))
	if err != nil {
		return nil, fmt.Errorf("load running-fork lifecycle retry: %w", err)
	}
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindFork ||
		lifecycle.Phase != SandboxLifecyclePhaseCommitted || lifecycle.Source != SandboxLifecycleSourceManual ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		lifecycle.ExpectedGenerationID != req.ExpectedSourceGenerationID ||
		lifecycle.PreparedGenerationID != checkpointGenerationID ||
		lifecycle.ToRuntimeNamespace != "" || lifecycle.ToRuntimeID != "" ||
		lifecycle.TargetSandboxID != req.TargetSandboxID ||
		lifecycle.TargetGenerationID != checkpointGenerationID {
		return nil, fmt.Errorf("%w: running fork lifecycle changed", ErrRootFSFilesystemConflict)
	}
	targetSandbox, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1 FOR SHARE
	`, req.TargetSandboxID))
	if err != nil {
		return nil, fmt.Errorf("load running-fork target retry: %w", err)
	}
	if targetSandbox == nil ||
		targetSandbox.DesiredState != SandboxDesiredStatePaused || targetSandbox.RuntimeGeneration != 0 ||
		targetSandbox.RuntimeID != "" || targetSandbox.RuntimeNamespace != "" ||
		!targetSandbox.DeletedAt.IsZero() {
		return nil, fmt.Errorf("%w: running fork target sandbox changed", ErrRootFSFilesystemConflict)
	}
	targetRecordDigest, err := NomadSandboxForkTargetRecordDigest(targetSandbox)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(lifecycle.TargetRecordDigest, targetRecordDigest) {
		return nil, fmt.Errorf("%w: running fork target identity changed", ErrRootFSFilesystemConflict)
	}
	targetClaim, err := lockSandboxRuntimeClaim(ctx, tx, req.TargetSandboxID)
	if err != nil {
		return nil, err
	}
	if targetClaim.OperationID != NomadSandboxForkClaimOperationID(req.OperationID, req.TargetSandboxID) ||
		targetClaim.Phase != SandboxRuntimeClaimPhaseReady || !targetClaim.CompletedAt.IsZero() {
		return nil, fmt.Errorf("%w: running fork target claim changed", ErrRootFSFilesystemConflict)
	}
	storedGeneration, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1
	`, checkpointGenerationID))
	if err != nil {
		return nil, fmt.Errorf("load running fork checkpoint retry: %w", err)
	}
	if !runningForkGenerationMatches(storedGeneration, req.Generation) {
		return nil, fmt.Errorf("%w: running fork checkpoint fields changed", ErrRootFSGenerationConflict)
	}
	target, err := getRootFSFilesystemByID(ctx, tx, targetFilesystemID)
	if err != nil {
		return nil, fmt.Errorf("load running fork target retry: %w", err)
	}
	if target.SourceFilesystemID != sourceFilesystemID || target.WriterEpoch < sourceWriterEpoch ||
		(req.TargetTeamID != "" && target.TeamID != req.TargetTeamID) {
		return nil, fmt.Errorf("%w: running fork target changed", ErrRootFSFilesystemConflict)
	}
	return target, nil
}

func runningForkGenerationMatches(stored, requested *RootFSGeneration) bool {
	return stored != nil && requested != nil && stored.ID == requested.ID &&
		stored.FilesystemID == requested.FilesystemID && stored.ParentGenerationID == requested.ParentGenerationID &&
		stored.SourceOCIDigest == requested.SourceOCIDigest && stored.BaseArtifactDigest == requested.BaseArtifactDigest &&
		stored.BaseBlockRoot == requested.BaseBlockRoot && stored.WriterEpoch == requested.WriterEpoch &&
		stored.FormatGeneration == requested.FormatGeneration && stored.LocatorVersion >= requested.LocatorVersion
}
