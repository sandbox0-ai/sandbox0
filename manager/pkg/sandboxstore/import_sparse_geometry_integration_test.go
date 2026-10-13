package sandboxstore

import (
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func sparseImportFixture(t *testing.T, suffix, layout, mapping string) (*BeginRootFSImportRequest, rootfsimporter.BuildResult, rootfsblock.ObjectReference) {
	t.Helper()
	begin, result, ref := layoutImportFixture(t, suffix, layout)
	begin.Spec.InitialFilesystemSizeBytes = begin.Spec.LogicalSizeBytes
	begin.Spec.LogicalSizeBytes = 64 << 40
	begin.Spec.BlockOptions.MappingGroupPolicy = mapping
	var err error
	begin.OperationID, begin.Spec, err = rootfsimporter.DeterministicOperation(begin.Spec)
	require.NoError(t, err)
	page, err := rootfsblock.EncodeMappingPage(rootfsblock.MappingPage{
		Version: 2, BlockCount: uint64(begin.Spec.LogicalSizeBytes / rootfsblock.LogicalBlockSize),
	})
	require.NoError(t, err)
	ref.Key = begin.Spec.BlockOptions.ObjectPrefix + "/maps/sha256/" + digest.FromBytes(page).Encoded()
	ref.Size, ref.Checksum = int64(len(page)), digest.FromBytes(page).String()
	result.InitialFilesystemSizeBytes, result.LogicalSizeBytes = begin.Spec.InitialFilesystemSizeBytes, begin.Spec.LogicalSizeBytes
	result.MappingGroupPolicy = mapping
	result.Descriptor.LogicalSizeBytes = result.LogicalSizeBytes
	result.Descriptor.MappingRoot = rootfsblock.MappingRootLocator{Version: 2, RootDigest: ref.Checksum,
		Object: rootfsblock.ObjectRange{Key: ref.Key, Length: ref.Size, Checksum: ref.Checksum}}
	result.DescriptorBytes, err = rootfsblock.EncodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	result.DescriptorDigest, result.BaseBlockRoot = digest.FromBytes(result.DescriptorBytes), digest.Digest(ref.Checksum)
	result.Objects, result.Bytes, result.References = 1, ref.Size, []rootfsblock.ObjectReference{ref}
	require.NoError(t, result.Validate())
	return begin, result, ref
}

func TestSparseImportPublishesExactGeometryAndRetainsDigestLookup(t *testing.T) {
	for _, policies := range []struct{ layout, mapping string }{
		{}, {layout: rootfsimporter.XFSFileRangesV1}, {mapping: rootfsblock.ContiguousMappingV1},
		{rootfsimporter.XFSFileRangesV1, rootfsblock.ContiguousMappingV1},
	} {
		t.Run(policies.layout+"/"+policies.mapping, func(t *testing.T) {
			ctx := t.Context()
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)
			begin, built, ref := sparseImportFixture(t, "sparse-cas", policies.layout, policies.mapping)
			operation, err := store.BeginRootFSImport(ctx, begin)
			require.NoError(t, err)
			require.Equal(t, begin.Spec, operation.Spec)
			operation, err = store.LeaseNextRootFSImport(ctx, "sparse-worker", time.Minute)
			require.NoError(t, err)
			lease, err := operation.Lease()
			require.NoError(t, err)
			require.NoError(t, store.PrepareRootFSImportObject(ctx, lease, ref))
			require.NoError(t, store.MarkRootFSImportObjectPublished(ctx, lease, ref))
			request := &PublishReadyRootFSImportRequest{Lease: lease, Result: built}
			artifact, err := store.PublishReadyRootFSImport(ctx, request)
			require.NoError(t, err)
			require.Equal(t, built.InitialFilesystemSizeBytes, artifact.InitialFilesystemSizeBytes)
			require.Equal(t, built.LogicalSizeBytes, artifact.LogicalSizeBytes)
			proof, err := rootfsimporter.DecodeReadyArtifactAttestation(artifact.Attestation)
			require.NoError(t, err)
			require.Equal(t, rootfsimporter.ReadyArtifactSparseAttestationVersion, proof.Version)
			replayed, err := store.PublishReadyRootFSImport(ctx, request)
			require.NoError(t, err)
			require.Equal(t, artifact.ArtifactDigest, replayed.ArtifactDigest)
			requirements := ReadyRootFSArtifactRequirements{
				FormatGeneration: 2, LogicalSizeBytes: built.LogicalSizeBytes, InitialFilesystemSizeBytes: built.InitialFilesystemSizeBytes,
				ProcdProtocol: begin.Spec.ProcdProtocol, ProcdDigest: begin.Spec.ProcdDigest,
				ImportDataLayoutPolicy: policies.layout, ImportMappingGroupPolicy: policies.mapping,
			}
			if policies.layout != "" {
				requirements.ImportDataRangeBytes = begin.Spec.BlockOptions.DataRangeBytes
			}
			selected, err := store.GetReadyRootFSBaseArtifact(ctx, built.SourceOCIDigest.String(), artifact.Platform, requirements)
			require.NoError(t, err)
			require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
			requirements.InitialFilesystemSizeBytes *= 2
			_, err = store.GetReadyRootFSBaseArtifact(ctx, built.SourceOCIDigest.String(), artifact.Platform, requirements)
			require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
			// Captured templates keep their public initial capacity and exact
			// digest; changing regional import policy must not invalidate them.
			requirements.LogicalSizeBytes, requirements.InitialFilesystemSizeBytes = built.InitialFilesystemSizeBytes, 0
			requirements.ImportDataLayoutPolicy, requirements.ImportMappingGroupPolicy, requirements.ImportDataRangeBytes = "", "", 0
			selected, err = store.GetReadyRootFSBaseArtifactByDigest(ctx, artifact.ArtifactDigest, artifact.Platform, requirements)
			require.NoError(t, err)
			require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
			_, err = pool.Exec(ctx, `UPDATE manager.rootfs_base_artifacts SET initial_filesystem_size_bytes = initial_filesystem_size_bytes * 2 WHERE artifact_digest = $1`, artifact.ArtifactDigest)
			require.ErrorContains(t, err, "geometry is immutable")
			_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET initial_filesystem_size_bytes = 0 WHERE operation_id = $1`, begin.OperationID)
			require.ErrorContains(t, err, "geometry is immutable")
			changed := built
			changed.InitialFilesystemSizeBytes = 0
			_, err = store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: changed})
			require.ErrorIs(t, err, ErrRootFSImportConflict)
			// Bypass Go validation as an old publisher could. The regional
			// transaction still rejects an artifact with another initial size.
			other := *begin
			other.Spec.InitialFilesystemSizeBytes *= 2
			other.OperationID, other.Spec, err = rootfsimporter.DeterministicOperation(other.Spec)
			require.NoError(t, err)
			_, err = store.BeginRootFSImport(ctx, &other)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET state = 'ready', result_artifact_digest = $2, ready_at = NOW() WHERE operation_id = $1`, other.OperationID, artifact.ArtifactDigest)
			require.ErrorContains(t, err, "does not match import geometry")
		})
	}
}

func TestSparseGeometryMigrationRollsBackOnlyWithoutSparseRows(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPoolAt(t, 120)
	down := func() error {
		return migrate.Down(ctx, pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{}))
	}
	require.NoError(t, down())
	var columns int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema = 'manager' AND column_name = 'initial_filesystem_size_bytes'`).Scan(&columns))
	require.Zero(t, columns)
	applySandboxStoreMigrationsThrough(t, pool, 120)
	store := NewPGSandboxStore(pool)
	begin, _, _ := sparseImportFixture(t, "sparse-rollback", "", "")
	_, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	require.ErrorContains(t, down(), "Cannot remove committed RootFS sparse geometry")
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema = 'manager' AND column_name = 'initial_filesystem_size_bytes'`).Scan(&columns))
	require.Equal(t, 2, columns)
}
