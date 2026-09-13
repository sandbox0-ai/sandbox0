package sandboxstore

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

// The fixture contains a real encoded empty block map, not an OCI or startup
// workload. This test proves transactional provenance/selection and mixed-worker
// fencing, including the case where both policies have identical descriptors.
func TestRootFSImportMappingPublicationSelectionAndOldWorkerFenceIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	legacyBegin, legacyResult, legacyRef := mappingImportFixture(t, "mapping-same-inputs", "")
	_, legacyLease := prepareGeometryImport(t, store, legacyBegin, legacyRef)
	legacy, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: legacyLease, Result: legacyResult})
	require.NoError(t, err)
	begin, result, ref := mappingImportFixture(t, "mapping-same-inputs", rootfsblock.ContiguousMappingV1)
	require.NotEqual(t, legacyBegin.OperationID, begin.OperationID)
	require.Equal(t, legacyResult.DescriptorDigest, result.DescriptorDigest)
	op, lease := prepareGeometryImport(t, store, begin, ref)
	reloaded, err := store.GetRootFSImportOperation(ctx, op.ID)
	require.NoError(t, err)
	require.Equal(t, begin.Spec, reloaded.Spec)
	_, err = store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: legacyResult})
	require.ErrorIs(t, err, ErrRootFSImportConflict)
	// Simulate a pre-policy worker's SQL, bypassing the new Go check entirely.
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET state='ready', lease_owner=NULL,lease_token=NULL,
		lease_expires_at=NULL,result_artifact_digest=$2,ready_at=NOW() WHERE operation_id=$1`, op.ID, legacy.ArtifactDigest)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
	reloaded, err = store.GetRootFSImportOperation(ctx, op.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSImportStateBuilding, reloaded.State)
	require.Equal(t, lease.Token, reloaded.LeaseToken)
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET mapping_group_policy='' WHERE operation_id=$1`, op.ID)
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
	artifact, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	require.NotEqual(t, legacy.ArtifactDigest, artifact.ArtifactDigest)
	require.Equal(t, rootfsblock.ContiguousMappingV1, artifact.ImportMappingGroupPolicy)
	proof, err := rootfsimporter.DecodeReadyArtifactAttestation(artifact.Attestation)
	require.NoError(t, err)
	require.Equal(t, 3, proof.Version)
	require.Empty(t, proof.DataLayoutFallback)
	retry, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, retry.ArtifactDigest)
	_, err = store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	policy := ReadyRootFSArtifactRequirements{FormatGeneration: 2, LogicalSizeBytes: begin.Spec.LogicalSizeBytes,
		ProcdProtocol: begin.Spec.ProcdProtocol, ProcdDigest: begin.Spec.ProcdDigest, SourceOCIRef: begin.Spec.SourceOCIRef, ImportDataRangeBytes: 64 << 10}
	selected, err := store.GetReadyRootFSBaseArtifact(ctx, result.SourceOCIDigest.String(), artifact.Platform, policy)
	require.NoError(t, err)
	require.Equal(t, legacy.ArtifactDigest, selected.ArtifactDigest, "new policy must not displace legacy source selection")
	policy.ImportMappingGroupPolicy = rootfsblock.ContiguousMappingV1
	selected, err = store.GetReadyRootFSBaseArtifact(ctx, result.SourceOCIDigest.String(), artifact.Platform, policy)
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
	_, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, artifact.ArtifactDigest, artifact.Platform, policy)
	require.ErrorContains(t, err, "source selection filters")
	policy.ImportMappingGroupPolicy, policy.ImportDataRangeBytes, policy.SourceOCIRef = "", 0, ""
	for _, item := range []*RootFSBaseArtifact{legacy, artifact} {
		selected, err := store.GetReadyRootFSBaseArtifactByDigest(ctx, item.ArtifactDigest, item.Platform, policy)
		require.NoError(t, err)
		require.Equal(t, item.ArtifactDigest, selected.ArtifactDigest)
	}
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_base_artifacts SET mapping_group_policy='' WHERE artifact_digest=$1`, artifact.ArtifactDigest)
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_base_artifacts SET attestation=$2 WHERE artifact_digest=$1`, artifact.ArtifactDigest, legacy.Attestation)
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
}
