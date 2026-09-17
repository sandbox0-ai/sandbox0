package sandboxstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestReadyRootFSArtifactGeometrySelectionIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	legacyRequest := readyRootFSBaseArtifactTestRequest()
	legacy, err := store.PutReadyRootFSBaseArtifact(ctx, legacyRequest)
	require.NoError(t, err)
	require.Nil(t, legacy.ImportDataRangeBytes)
	requirements := ReadyRootFSArtifactRequirements{
		FormatGeneration: legacy.FormatGeneration, LogicalSizeBytes: legacy.LogicalSizeBytes,
		ProcdProtocol: legacy.ProcdProtocol, ProcdDigest: legacy.ProcdDigest,
	}
	selectSource := func(policy ReadyRootFSArtifactRequirements) (*RootFSBaseArtifact, error) {
		return store.GetReadyRootFSBaseArtifact(ctx, legacy.SourceOCIDigest, legacy.Platform, policy)
	}
	selected, err := selectSource(requirements)
	require.NoError(t, err)
	require.Equal(t, legacy.ArtifactDigest, selected.ArtifactDigest)
	strict := requirements
	strict.ImportDataRangeBytes = 16 << 10
	strict.SourceOCIRef = legacy.SourceOCIRef
	_, err = selectSource(strict)
	require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound, "unknown is not an inferred geometry")

	one := publishGeometrySelectionFixture(t, store, legacy, "one", 16<<10)
	eight := publishGeometrySelectionFixture(t, store, legacy, "eight", 64<<10)
	alias := *legacy
	alias.SourceOCIRef = "mirror.example/same@" + legacy.SourceOCIDigest
	aliased := publishGeometrySelectionFixture(t, store, &alias, "alias", 16<<10)
	for index, artifact := range []*RootFSBaseArtifact{one, eight, aliased, legacy} {
		_, err := pool.Exec(ctx, `UPDATE manager.rootfs_base_artifacts SET created_at = $2 WHERE artifact_digest = $1`,
			artifact.ArtifactDigest, time.Date(2026, 1, 1, 0, 0, index, 0, time.UTC))
		require.NoError(t, err)
	}
	selected, err = selectSource(requirements)
	require.NoError(t, err)
	require.Equal(t, legacy.ArtifactDigest, selected.ArtifactDigest, "legacy zero policy keeps latest-compatible behavior")
	selected, err = selectSource(strict)
	require.NoError(t, err)
	require.Equal(t, one.ArtifactDigest, selected.ArtifactDigest, "newer 64 KiB, alias, and unknown artifacts must not displace the exact 16 KiB source")
	strict.SourceOCIRef = ""
	selected, err = selectSource(strict)
	require.NoError(t, err)
	require.Equal(t, aliased.ArtifactDigest, selected.ArtifactDigest)
	strict.SourceOCIRef = alias.SourceOCIRef
	selected, err = selectSource(strict)
	require.NoError(t, err)
	require.Equal(t, aliased.ArtifactDigest, selected.ArtifactDigest)
	strict.SourceOCIRef = legacy.SourceOCIRef
	strict.ImportDataRangeBytes = 64 << 10
	selected, err = selectSource(strict)
	require.NoError(t, err)
	require.Equal(t, eight.ArtifactDigest, selected.ArtifactDigest)
	strict.ImportDataRangeBytes = 32 << 10
	_, err = selectSource(strict)
	require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
	strict.SourceOCIRef = "registry.example/other@" + digest.FromString("other").String()
	_, err = selectSource(strict)
	require.ErrorContains(t, err, "source_oci_digest")

	for _, artifact := range []*RootFSBaseArtifact{legacy, one, eight, aliased} {
		selected, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, artifact.ArtifactDigest, artifact.Platform, requirements)
		require.NoError(t, err)
		require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
	}
	for _, policy := range []ReadyRootFSArtifactRequirements{
		{ImportDataRangeBytes: 16 << 10}, {SourceOCIRef: legacy.SourceOCIRef},
	} {
		_, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, legacy.ArtifactDigest, legacy.Platform, policy)
		require.ErrorContains(t, err, "source selection filters are not allowed")
	}
}

func publishGeometrySelectionFixture(t *testing.T, store *PGSandboxStore, source *RootFSBaseArtifact, suffix string, size int) *RootFSBaseArtifact {
	t.Helper()
	begin, result, reference := rootFSImportTestFixture(t, "geometry-"+suffix)
	begin.Spec.SourceOCIRef = source.SourceOCIRef
	begin.Spec.Platform = rootfsimporter.ReadyArtifactPlatform(source.Platform)
	begin.Spec.ProcdProtocol, begin.Spec.ProcdDigest = source.ProcdProtocol, source.ProcdDigest
	begin.Spec.FormatGeneration = source.FormatGeneration
	begin.Spec.LogicalSizeBytes = source.LogicalSizeBytes
	begin.Spec.BlockOptions.DataRangeBytes = size
	result.SourceOCIRef, result.SourceOCIDigest = source.SourceOCIRef, digest.Digest(source.SourceOCIDigest)
	result.ProcdDigest = digest.Digest(source.ProcdDigest)
	// The empty mapping fixture must cover the selected source's logical size.
	page, err := rootfsblock.EncodeMappingPage(rootfsblock.MappingPage{StartBlock: 0, BlockCount: uint64(source.LogicalSizeBytes / rootfsblock.LogicalBlockSize)})
	require.NoError(t, err)
	reference.Checksum, reference.Size = digest.FromBytes(page).String(), int64(len(page))
	reference.Key = begin.Spec.BlockOptions.ObjectPrefix + "/maps/sha256/" + digest.FromBytes(page).Encoded()
	result.LogicalSizeBytes = source.LogicalSizeBytes
	result.Descriptor.LogicalSizeBytes = source.LogicalSizeBytes
	result.BaseBlockRoot = digest.FromBytes(page)
	result.Descriptor.MappingRoot.RootDigest = result.BaseBlockRoot.String()
	result.Descriptor.MappingRoot.Object = rootfsblock.ObjectRange{Key: reference.Key, Length: reference.Size, Checksum: reference.Checksum}
	result.DescriptorBytes, err = rootfsblock.EncodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	result.DescriptorDigest = digest.FromBytes(result.DescriptorBytes)
	result.References, result.Bytes = []rootfsblock.ObjectReference{reference}, reference.Size
	_, lease := prepareGeometryImport(t, store, begin, reference)
	artifact, err := store.PublishReadyRootFSImport(t.Context(), &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	require.NotNil(t, artifact.ImportDataRangeBytes)
	require.Equal(t, size, *artifact.ImportDataRangeBytes)
	return artifact
}

func prepareGeometryImport(t *testing.T, store *PGSandboxStore, begin *BeginRootFSImportRequest, reference rootfsblock.ObjectReference) (*RootFSImportOperation, RootFSImportLease) {
	t.Helper()
	_, err := store.BeginRootFSImport(t.Context(), begin)
	require.NoError(t, err)
	operation, err := store.LeaseNextRootFSImport(t.Context(), "geometry-test-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, operation)
	require.Equal(t, begin.OperationID, operation.ID)
	lease, err := operation.Lease()
	require.NoError(t, err)
	require.NoError(t, store.PrepareRootFSImportObject(t.Context(), lease, reference))
	require.NoError(t, store.MarkRootFSImportObjectPublished(t.Context(), lease, reference))
	return operation, lease
}

func TestRootFSImportGeometryPublicationRetryAndGCIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, result, reference := rootFSImportTestFixture(t, "geometry-retry")
	begin.Spec.BlockOptions.DataRangeBytes = 16 << 10
	operation, lease := prepareGeometryImport(t, store, begin, reference)
	publication := &PublishReadyRootFSImportRequest{Lease: lease, Result: result}
	artifact, err := store.PublishReadyRootFSImport(ctx, publication)
	require.NoError(t, err)
	require.Equal(t, 16<<10, *artifact.ImportDataRangeBytes)
	replay, err := store.PublishReadyRootFSImport(ctx, publication)
	require.NoError(t, err)
	require.Equal(t, artifact, replay)

	// Even the same attestation cannot acquire a conflicting import geometry.
	other := *begin
	other.OperationID += "-conflict"
	other.Spec.BlockOptions.DataRangeBytes = 64 << 10
	_, otherLease := prepareGeometryImport(t, store, &other, reference)
	_, err = store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: otherLease, Result: result})
	require.ErrorIs(t, err, ErrRootFSBaseArtifactConflict)
	require.NoError(t, store.AbandonRootFSImport(ctx, otherLease, "conflicting geometry fixture"))

	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET block_data_range_bytes = $2 WHERE operation_id = $1`, operation.ID, 64<<10)
	require.NoError(t, err)
	_, err = store.PublishReadyRootFSImport(ctx, publication)
	require.ErrorIs(t, err, ErrRootFSImportConflict, "a changed durable spec cannot silently pass a publication retry")
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET block_data_range_bytes = $2, updated_at = NOW() - INTERVAL '1 hour' WHERE operation_id = $1`, operation.ID, 16<<10)
	require.NoError(t, err)
	garbage, err := store.ReconcileRootFSImportGarbage(ctx, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, 1, garbage.PurgedReady)
	_, err = store.GetRootFSImportOperation(ctx, operation.ID)
	require.ErrorIs(t, err, ErrRootFSImportNotFound)
	selected, err := store.GetReadyRootFSBaseArtifact(ctx, artifact.SourceOCIDigest, artifact.Platform, ReadyRootFSArtifactRequirements{
		FormatGeneration: artifact.FormatGeneration, LogicalSizeBytes: artifact.LogicalSizeBytes,
		ProcdProtocol: artifact.ProcdProtocol, ProcdDigest: artifact.ProcdDigest,
		ImportDataRangeBytes: 16 << 10, SourceOCIRef: artifact.SourceOCIRef,
	})
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
	require.Equal(t, 16<<10, *selected.ImportDataRangeBytes)
	assertGeometryImmutable(t, pool, artifact.ArtifactDigest)
}

func assertGeometryImmutable(t *testing.T, pool *pgxpool.Pool, artifactDigest string) {
	t.Helper()
	for _, value := range []any{nil, 0, 4097, 64 << 10} {
		_, err := pool.Exec(t.Context(), `UPDATE manager.rootfs_base_artifacts SET import_data_range_bytes = $2 WHERE artifact_digest = $1`, artifactDigest, value)
		require.ErrorContains(t, err, "immutable", fmt.Sprintf("value %v", value))
	}
	_, err := pool.Exec(t.Context(), `UPDATE manager.rootfs_base_artifacts SET import_data_range_bytes = import_data_range_bytes WHERE artifact_digest = $1`, artifactDigest)
	require.NoError(t, err, "an unchanged value remains idempotent")
}

func TestRootFSImportGeometryRollingPublisherRecoveryIntegration(t *testing.T) {
	for _, action := range []string{"begin-ready-retry", "publication-ready-retry", "ready-operation-gc"} {
		t.Run(action, func(t *testing.T) {
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)
			begin, result, lease, artifactDigest := insertRollingGeometryFixture(t, pool, store, action)
			switch action {
			case "begin-ready-retry":
				operation, err := store.BeginRootFSImport(t.Context(), begin)
				require.NoError(t, err)
				require.Equal(t, RootFSImportStateReady, operation.State)
			case "publication-ready-retry":
				artifact, err := store.PublishReadyRootFSImport(t.Context(), &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
				require.NoError(t, err)
				require.Equal(t, artifactDigest, artifact.ArtifactDigest)
			case "ready-operation-gc":
				_, err := pool.Exec(t.Context(), `UPDATE manager.rootfs_import_operations SET updated_at = NOW() - INTERVAL '2 days' WHERE operation_id = $1`, begin.OperationID)
				require.NoError(t, err)
				garbage, err := store.ReconcileRootFSImportGarbage(t.Context(), 24*time.Hour, 1)
				require.NoError(t, err)
				require.Equal(t, 1, garbage.PurgedReady)
			}
			platform := RootFSArtifactPlatform{OS: result.Platform.OS, Architecture: result.Platform.Architecture, Variant: result.Platform.Variant}
			selected, err := store.GetReadyRootFSBaseArtifact(t.Context(), result.SourceOCIDigest.String(), platform, ReadyRootFSArtifactRequirements{
				FormatGeneration: begin.Spec.FormatGeneration, LogicalSizeBytes: begin.Spec.LogicalSizeBytes,
				ProcdProtocol: begin.Spec.ProcdProtocol, ProcdDigest: begin.Spec.ProcdDigest,
				ImportDataRangeBytes: 16 << 10, SourceOCIRef: begin.Spec.SourceOCIRef,
			})
			require.NoError(t, err, "an old publisher after migration must not strand strict discovery on the same ready operation")
			require.Equal(t, artifactDigest, selected.ArtifactDigest)
			require.Equal(t, 16<<10, *selected.ImportDataRangeBytes)
			assertGeometryImmutable(t, pool, artifactDigest)
		})
	}
}

func insertRollingGeometryFixture(t *testing.T, pool *pgxpool.Pool, store *PGSandboxStore, suffix string) (*BeginRootFSImportRequest, rootfsimporter.BuildResult, RootFSImportLease, string) {
	t.Helper()
	begin, result, reference := rootFSImportTestFixture(t, "rolling-"+suffix)
	begin.Spec.BlockOptions.DataRangeBytes = 16 << 10
	_, lease := prepareGeometryImport(t, store, begin, reference)
	artifactDigest := insertPreGeometryArtifact(t, pool, begin, result)
	_, err := pool.Exec(t.Context(), `INSERT INTO manager.rootfs_base_artifact_objects (artifact_digest, object_key) VALUES ($1, $2)`, artifactDigest, reference.Key)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_import_operations
		SET state = 'ready', result_artifact_digest = $2, ready_at = NOW(),
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
		WHERE operation_id = $1`, begin.OperationID, artifactDigest)
	require.NoError(t, err)
	var geometry *int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT import_data_range_bytes FROM manager.rootfs_base_artifacts WHERE artifact_digest = $1`, artifactDigest).Scan(&geometry))
	require.Nil(t, geometry)
	return begin, result, lease, artifactDigest
}

func TestRootFSImportGeometryUnknownRetryFailsClosedIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, result, lease, artifactDigest := insertRollingGeometryFixture(t, pool, store, "ambiguous")
	other := *begin
	other.OperationID += "-different"
	other.Spec.BlockOptions.DataRangeBytes = 64 << 10
	insertPreGeometryReadyOperation(t, store, pool, &other, artifactDigest)
	_, err := store.BeginRootFSImport(t.Context(), begin)
	require.ErrorIs(t, err, ErrRootFSImportConflict)
	_, err = store.PublishReadyRootFSImport(t.Context(), &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.ErrorIs(t, err, ErrRootFSImportConflict)
	// Partial garbage batches must not erase one side of conflicting provenance
	// and allow a later pass to promote the survivor as unanimous history.
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_import_operations SET updated_at = NOW() - INTERVAL '2 days'`)
	require.NoError(t, err)
	for range 2 {
		garbage, err := store.ReconcileRootFSImportGarbage(t.Context(), 24*time.Hour, 1)
		require.NoError(t, err)
		require.Zero(t, garbage.PurgedReady)
	}
	// Retained ambiguity must not occupy every bounded GC admission slot.
	validBegin, _, _, validDigest := insertRollingGeometryFixture(t, pool, store, "gc-after-ambiguity")
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_import_operations SET updated_at = NOW() - INTERVAL '1 day' - INTERVAL '1 hour' WHERE operation_id = $1`, validBegin.OperationID)
	require.NoError(t, err)
	garbage, err := store.ReconcileRootFSImportGarbage(t.Context(), 24*time.Hour, 1)
	require.NoError(t, err)
	require.Equal(t, 1, garbage.PurgedReady)
	var validGeometry *int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT import_data_range_bytes FROM manager.rootfs_base_artifacts WHERE artifact_digest = $1`, validDigest).Scan(&validGeometry))
	require.NotNil(t, validGeometry)
	require.Equal(t, 16<<10, *validGeometry)
	var geometry *int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT import_data_range_bytes FROM manager.rootfs_base_artifacts WHERE artifact_digest = $1`, artifactDigest).Scan(&geometry))
	require.Nil(t, geometry)
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_base_artifacts SET import_data_range_bytes = $2 WHERE artifact_digest = $1`, artifactDigest, 16<<10)
	require.ErrorContains(t, err, "provenance")
	_, err = store.PublishReadyRootFSImport(t.Context(), &PublishReadyRootFSImportRequest{
		Lease: RootFSImportLease{OperationID: begin.OperationID, WorkerID: "legacy", Token: strings.Repeat("a", 64)}, Result: result,
	})
	require.ErrorIs(t, err, ErrRootFSImportConflict)
}

func TestRootFSImportGeometryPromotionChecksImmutableInputsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	_, _, _, artifactDigest := insertRollingGeometryFixture(t, pool, store, "immutable-inputs")
	_, err := pool.Exec(t.Context(), `UPDATE manager.rootfs_base_artifacts
		SET import_data_range_bytes = 16384, source_oci_ref = 'registry.example/changed@' || source_oci_digest
		WHERE artifact_digest = $1`, artifactDigest)
	require.ErrorContains(t, err, "provenance")
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_base_artifacts SET import_data_range_bytes = 65536 WHERE artifact_digest = $1`, artifactDigest)
	require.ErrorContains(t, err, "provenance", "a valid range with the wrong provenance cannot be promoted")
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_base_artifacts SET import_data_range_bytes = 16384 WHERE artifact_digest = $1`, artifactDigest)
	require.NoError(t, err, "matching unanimous provenance permits only NULL to known")
	assertGeometryImmutable(t, pool, artifactDigest)
}
