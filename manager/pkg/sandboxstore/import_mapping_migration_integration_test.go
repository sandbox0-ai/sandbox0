package sandboxstore

import (
	"testing"

	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestRootFSImportMappingMigrationRollbackAndCombinedLayoutIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPoolAt(t, 55)
	down := func() error {
		return migrate.Down(ctx, pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{}))
	}
	store := NewPGSandboxStore(pool)
	legacyBegin, legacyResult, legacyRef := layoutImportFixture(t, "mapping-upgrade-legacy", rootfsimporter.XFSFileRangesV1)
	_, legacyLease := prepareGeometryImport(t, store, legacyBegin, legacyRef)
	legacy, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: legacyLease, Result: legacyResult})
	require.NoError(t, err)
	require.NoError(t, down(), "layout-only rows must not prevent removing mapping policy")
	var columns int
	query := `SELECT count(*) FROM information_schema.columns WHERE table_schema='manager' AND column_name='mapping_group_policy'`
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Zero(t, columns)
	applySandboxStoreMigrationsThrough(t, pool, 55)
	reloaded, err := store.GetRootFSImportOperation(ctx, legacyBegin.OperationID)
	require.NoError(t, err)
	require.Equal(t, legacyBegin.Spec, reloaded.Spec)
	require.Equal(t, legacy.ArtifactDigest, reloaded.ArtifactDigest)
	begin, result, ref := mappingImportFixture(t, "mapping-layout-combined", rootfsblock.ContiguousMappingV1)
	begin.Spec.DataLayoutPolicy = rootfsimporter.XFSFileRangesV1
	begin.OperationID, begin.Spec, err = rootfsimporter.DeterministicOperation(begin.Spec)
	require.NoError(t, err)
	result.DataLayoutPolicy, result.DataLayoutRangeBytes = rootfsimporter.XFSFileRangesV1, 64<<10
	_, lease := prepareGeometryImport(t, store, begin, ref)
	require.ErrorContains(t, down(), "Cannot remove RootFS mapping provenance")
	artifact, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	proof, err := rootfsimporter.DecodeReadyArtifactAttestation(artifact.Attestation)
	require.NoError(t, err)
	require.Equal(t, 3, proof.Version)
	require.Equal(t, begin.Spec.DataLayoutPolicy, proof.DataLayoutPolicy)
	require.Equal(t, begin.Spec.BlockOptions.MappingGroupPolicy, proof.MappingGroupPolicy)
	require.ErrorContains(t, down(), "Cannot remove RootFS mapping provenance")
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Equal(t, 2, columns)
}
