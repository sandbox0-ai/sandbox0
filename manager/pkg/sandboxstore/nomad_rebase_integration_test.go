package sandboxstore

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

type nomadPausedRebaseStoreFixture struct {
	ctx            context.Context
	pool           *pgxpool.Pool
	store          *PGSandboxStore
	record         *SandboxRecord
	filesystem     *RootFSFilesystem
	source         *RootFSGeneration
	sourceArtifact *RootFSBaseArtifact
	targetArtifact *RootFSBaseArtifact
}

func TestRequestNomadPausedRebaseRejectsChangedRetryIdentityIntegration(t *testing.T) {
	fixture := newNomadPausedRebaseStoreFixture(t, "changed-retry")
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	request := fixture.request("rebase-changed-retry", deadline)
	first, err := fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.NoError(t, err)

	changedDeadline := *request
	changedDeadline.RollbackExpiresAt = deadline.Add(time.Minute)
	_, err = fixture.store.RequestNomadPausedRebase(fixture.ctx, &changedDeadline)
	require.ErrorIs(t, err, ErrNomadSandboxRebaseConflict)

	thirdRequest := readyRootFSBaseArtifactTestRequest()
	thirdRequest.ArtifactDigest = digest.FromString("rebase-third-artifact").String()
	thirdRequest.SourceOCIDigest = digest.FromString("rebase-third-oci").String()
	thirdRequest.SourceOCIRef = "registry.example/sandbox:third@" + thirdRequest.SourceOCIDigest
	thirdRequest.BaseBlockRoot = digest.FromString("rebase-third-root").String()
	thirdRequest.Descriptor = encodeTestRootFSDescriptor(t, "rebase-third", thirdRequest.BaseBlockRoot)
	third, err := fixture.store.PutReadyRootFSBaseArtifact(fixture.ctx, thirdRequest)
	require.NoError(t, err)
	changedTarget := *request
	changedTarget.TargetBaseArtifactDigest = third.ArtifactDigest
	_, err = fixture.store.RequestNomadPausedRebase(fixture.ctx, &changedTarget)
	require.ErrorIs(t, err, ErrNomadSandboxRebaseConflict)

	retried, err := fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, first.TargetGenerationID, retried.TargetGenerationID)
	require.Equal(t, first.TargetWriterEpoch, retried.TargetWriterEpoch)
	assertNomadPausedRebaseLifecycle(t, fixture, request.OperationID, SandboxLifecyclePhasePreparing)
}

func TestRequestNomadPausedRebaseRejectsMutatedSourceIntegration(t *testing.T) {
	fixture := newNomadPausedRebaseStoreFixture(t, "mutated-source")
	request := fixture.request(
		"rebase-mutated-source", time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
	)
	_, err := fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.rootfs_filesystems
		SET base_artifact_digest = $2
		WHERE filesystem_id = $1
	`, fixture.filesystem.ID, fixture.targetArtifact.ArtifactDigest)
	require.NoError(t, err)

	_, err = fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.ErrorIs(t, err, ErrNomadSandboxRebaseNotReady)
	assertNomadPausedRebaseLifecycle(t, fixture, request.OperationID, SandboxLifecyclePhasePreparing)
}

func TestRequestNomadPausedRebaseRejectsExpiredHardTTLIntegration(t *testing.T) {
	fixture := newNomadPausedRebaseStoreFixture(t, "hard-ttl")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes SET hard_expires_at = NOW() - INTERVAL '1 second'
		WHERE sandbox_id = $1
	`, fixture.record.ID)
	require.NoError(t, err)

	request := fixture.request(
		"rebase-expired-hard-ttl", time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
	)
	_, err = fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.ErrorIs(t, err, ErrNomadSandboxRebaseNotReady)
	active, getErr := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.record.ID)
	require.NoError(t, getErr)
	require.Nil(t, active)
}

func TestPublishPausedRootFSRebaseRequiresPreoperationIntegration(t *testing.T) {
	fixture := newNomadPausedRebaseStoreFixture(t, "missing-preoperation")
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	operationID := "rebase-missing-preoperation"
	target := fixture.outputGeneration(t, NomadPausedRebaseGenerationID(
		operationID, fixture.record.ID, fixture.source.ID, fixture.targetArtifact.ArtifactDigest,
	), fixture.filesystem.WriterEpoch+1, "missing-preoperation")
	health := sha256.Sum256([]byte("missing-preoperation-health"))

	_, err := fixture.store.PublishPausedRootFSRebase(fixture.ctx, &PublishPausedRootFSRebaseRequest{
		SandboxID: fixture.record.ID, TeamID: fixture.record.TeamID, OperationID: operationID,
		ExpectedSourceGenerationID: fixture.source.ID,
		ExpectedBaseArtifactDigest: fixture.sourceArtifact.ArtifactDigest,
		Generation:                 target, HealthCheckDigest: health[:], RollbackExpiresAt: deadline,
	})
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
	stored, getErr := fixture.store.GetRootFSFilesystem(fixture.ctx, fixture.record.ID)
	require.NoError(t, getErr)
	require.Equal(t, fixture.source.ID, stored.HeadGenerationID)
}

func TestPublishPausedRootFSRebaseRejectsMutatedTargetGenerationIntegration(t *testing.T) {
	fixture := newNomadPausedRebaseStoreFixture(t, "mutated-target")
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	request := fixture.request("rebase-mutated-target", deadline)
	candidate, err := fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.NoError(t, err)
	target := fixture.outputGeneration(
		t, candidate.TargetGenerationID, candidate.TargetWriterEpoch, "expected-target",
	)
	mutated := *target
	mutated.CurrentBlockHead = digest.FromString("mutated-target-head").String()
	mutated.Descriptor = encodeTestRootFSDescriptor(t, "mutated-target", mutated.CurrentBlockHead)
	_, err = fixture.pool.Exec(fixture.ctx, `
		INSERT INTO manager.rootfs_generations (
			generation_id, filesystem_id, parent_generation_id, source_oci_digest,
			base_artifact_digest, base_block_root, current_block_head, writer_epoch,
			format_generation, durability_state, locator_version, descriptor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
	`, mutated.ID, mutated.FilesystemID, mutated.ParentGenerationID, mutated.SourceOCIDigest,
		mutated.BaseArtifactDigest, mutated.BaseBlockRoot, mutated.CurrentBlockHead,
		mutated.WriterEpoch, mutated.FormatGeneration, mutated.DurabilityState,
		mutated.LocatorVersion, mutated.Descriptor)
	require.NoError(t, err)
	health := sha256.Sum256([]byte("mutated-target-health"))

	_, err = fixture.store.PublishPausedRootFSRebase(fixture.ctx, &PublishPausedRootFSRebaseRequest{
		SandboxID: fixture.record.ID, TeamID: fixture.record.TeamID, OperationID: request.OperationID,
		ExpectedSourceGenerationID: fixture.source.ID,
		ExpectedBaseArtifactDigest: fixture.sourceArtifact.ArtifactDigest,
		Generation:                 target, HealthCheckDigest: health[:], RollbackExpiresAt: deadline,
	})
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
	stored, getErr := fixture.store.GetRootFSFilesystem(fixture.ctx, fixture.record.ID)
	require.NoError(t, getErr)
	require.Equal(t, fixture.source.ID, stored.HeadGenerationID)
	assertNomadPausedRebaseLifecycle(t, fixture, request.OperationID, SandboxLifecyclePhasePreparing)
}

func TestNomadPausedRebaseCleanupAbortsPreoperationIntegration(t *testing.T) {
	fixture := newNomadPausedRebaseStoreFixture(t, "cleanup")
	request := fixture.request(
		"rebase-cleanup", time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
	)
	_, err := fixture.store.RequestNomadPausedRebase(fixture.ctx, request)
	require.NoError(t, err)

	_, err = fixture.store.RequestSandboxRuntimeClaimCleanup(
		fixture.ctx, fixture.record.ID, "delete while rebase is preparing",
	)
	require.NoError(t, err)
	assertNomadPausedRebaseLifecycle(t, fixture, request.OperationID, SandboxLifecyclePhaseAborted)
	stored, getErr := fixture.store.GetRootFSFilesystem(fixture.ctx, fixture.record.ID)
	require.NoError(t, getErr)
	require.Equal(t, fixture.source.ID, stored.HeadGenerationID)
}

func newNomadPausedRebaseStoreFixture(t *testing.T, suffix string) *nomadPausedRebaseStoreFixture {
	t.Helper()
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-rebase-"+suffix, "team-rebase")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	record.DesiredState = SandboxDesiredStatePaused
	record.ClusterID = "cluster-rebase"
	require.NoError(t, store.UpsertSandbox(ctx, record))
	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandbox_runtime_claims (
			sandbox_id, operation_id, phase, lease_expires_at
		) VALUES ($1, $2, $3, NULL)
	`, record.ID, "claim-"+suffix, SandboxRuntimeClaimPhaseReady)
	require.NoError(t, err)

	sourceRequest := readyRootFSBaseArtifactTestRequest()
	sourceArtifact, err := store.PutReadyRootFSBaseArtifact(ctx, sourceRequest)
	require.NoError(t, err)
	filesystem, source, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: record.ID, TeamID: record.TeamID, SourceOCIRef: sourceArtifact.SourceOCIRef,
		SourceOCIDigest:    sourceArtifact.SourceOCIDigest,
		BaseArtifactDigest: sourceArtifact.ArtifactDigest,
	})
	require.NoError(t, err)
	targetRequest := readyRootFSBaseArtifactTestRequest()
	targetRequest.ArtifactDigest = digest.FromString("rebase-target-artifact-" + suffix).String()
	targetRequest.SourceOCIDigest = digest.FromString("rebase-target-oci-" + suffix).String()
	targetRequest.SourceOCIRef = "registry.example/sandbox:target@" + targetRequest.SourceOCIDigest
	targetRequest.BaseBlockRoot = digest.FromString("rebase-target-root-" + suffix).String()
	targetRequest.Descriptor = encodeTestRootFSDescriptor(
		t, "rebase-target-base-"+suffix, targetRequest.BaseBlockRoot,
	)
	targetArtifact, err := store.PutReadyRootFSBaseArtifact(ctx, targetRequest)
	require.NoError(t, err)
	return &nomadPausedRebaseStoreFixture{
		ctx: ctx, pool: pool, store: store, record: record, filesystem: filesystem,
		source: source, sourceArtifact: sourceArtifact, targetArtifact: targetArtifact,
	}
}

func (f *nomadPausedRebaseStoreFixture) request(
	operationID string,
	rollbackExpiresAt time.Time,
) *NomadPausedRebaseRequest {
	return &NomadPausedRebaseRequest{
		OperationID: operationID, SandboxID: f.record.ID, ExpectedTeamID: f.record.TeamID,
		TargetBaseArtifactDigest: f.targetArtifact.ArtifactDigest,
		RollbackExpiresAt:        rollbackExpiresAt,
	}
}

func (f *nomadPausedRebaseStoreFixture) outputGeneration(
	t *testing.T,
	generationID string,
	writerEpoch int64,
	suffix string,
) *RootFSGeneration {
	t.Helper()
	head := digest.FromString("rebase-output-head-" + suffix).String()
	return &RootFSGeneration{
		ID: generationID, FilesystemID: f.filesystem.ID, ParentGenerationID: f.source.ID,
		SourceOCIDigest:    f.targetArtifact.SourceOCIDigest,
		BaseArtifactDigest: f.targetArtifact.ArtifactDigest,
		BaseBlockRoot:      f.targetArtifact.BaseBlockRoot, CurrentBlockHead: head,
		WriterEpoch: writerEpoch, FormatGeneration: f.targetArtifact.FormatGeneration,
		DurabilityState: RootFSGenerationStateS3Materialized,
		LocatorVersion:  f.source.LocatorVersion + 1,
		Descriptor:      encodeTestRootFSDescriptor(t, "rebase-output-"+suffix, head),
	}
}

func assertNomadPausedRebaseLifecycle(
	t *testing.T,
	fixture *nomadPausedRebaseStoreFixture,
	operationID, wantPhase string,
) {
	t.Helper()
	var phase, sourceBase, targetBase, targetGeneration, expectedHead string
	var rollbackExpiresAt time.Time
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT phase, source_base_artifact_digest, target_base_artifact_digest,
			target_generation_id, expected_head_layer_id, rollback_expires_at
		FROM manager.sandbox_lifecycle_txns
		WHERE txn_id = $1
	`, operationID).Scan(
		&phase, &sourceBase, &targetBase, &targetGeneration, &expectedHead, &rollbackExpiresAt,
	))
	require.Equal(t, wantPhase, phase)
	require.Equal(t, fixture.sourceArtifact.ArtifactDigest, sourceBase)
	require.Equal(t, fixture.targetArtifact.ArtifactDigest, targetBase)
	require.Equal(t, fixture.source.ID, expectedHead)
	require.Equal(t, NomadPausedRebaseGenerationID(
		operationID, fixture.record.ID, fixture.source.ID, fixture.targetArtifact.ArtifactDigest,
	), targetGeneration)
	require.False(t, rollbackExpiresAt.IsZero())
}
