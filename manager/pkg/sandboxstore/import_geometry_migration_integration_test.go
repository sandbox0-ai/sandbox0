package sandboxstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestRootFSImportGeometryMigrationProvenanceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationDatabase(t)
	prepareSandboxStoreCredentialSchema(t, pool)
	applySandboxStoreBeforeImportGeometry(t, pool)
	store := NewPGSandboxStore(pool)

	type fixture struct {
		digest string
		want   *int
	}
	var fixtures []fixture
	for _, tc := range []struct {
		name   string
		sizes  []int
		change func(*BeginRootFSImportRequest)
		want   int
	}{
		{name: "missing"},
		{name: "one", sizes: []int{1 << 20}, want: 1 << 20},
		{name: "eight", sizes: []int{8 << 20}, want: 8 << 20},
		{name: "unanimous", sizes: []int{1 << 20, 1 << 20}, want: 1 << 20},
		{name: "maximum-page-entries", sizes: []int{1 << 20}, want: 1 << 20, change: func(req *BeginRootFSImportRequest) {
			req.Spec.BlockOptions.PageEntries = 65536
		}},
		{name: "ambiguous", sizes: []int{1 << 20, 8 << 20}},
		{name: "source-alias", sizes: []int{1 << 20}, change: func(req *BeginRootFSImportRequest) {
			req.Spec.SourceOCIRef = strings.Replace(req.Spec.SourceOCIRef, "registry.example/", "mirror.example/", 1)
		}},
		{name: "platform", sizes: []int{1 << 20}, change: func(req *BeginRootFSImportRequest) {
			req.Spec.Platform.Architecture = "arm64"
		}},
		{name: "procd", sizes: []int{1 << 20}, change: func(req *BeginRootFSImportRequest) {
			req.Spec.ProcdProtocol = "different.procd"
		}},
		{name: "size", sizes: []int{1 << 20}, change: func(req *BeginRootFSImportRequest) {
			req.Spec.LogicalSizeBytes += 4096
		}},
		{name: "mixed-valid-and-invalid", sizes: []int{1 << 20, 1 << 20}, change: func(req *BeginRootFSImportRequest) {
			if strings.HasSuffix(req.OperationID, "-1") {
				req.Spec.FormatGeneration++
			}
		}},
	} {
		begin, result, _ := rootFSImportTestFixture(t, "migration-"+tc.name)
		artifactDigest := insertPreGeometryArtifact(t, pool, begin, result)
		for index, size := range tc.sizes {
			operation := *begin
			operation.OperationID = fmt.Sprintf("%s-%d", begin.OperationID, index)
			operation.Spec.BlockOptions.DataRangeBytes = size
			if tc.change != nil {
				tc.change(&operation)
			}
			insertPreGeometryReadyOperation(t, store, pool, &operation, artifactDigest)
		}
		item := fixture{digest: artifactDigest}
		if tc.want != 0 {
			want := tc.want
			item.want = &want
		}
		fixtures = append(fixtures, item)
	}
	// SQL previously bounded these options only loosely. A surviving ready row
	// with malformed geometry must not become trusted merely because it exists.
	for _, invalid := range []struct {
		column string
		value  int
	}{
		{"block_data_range_bytes", 1}, {"block_data_range_bytes", 16 << 20},
		{"block_pack_bytes", 1}, {"block_pack_bytes", 128 << 20},
		{"block_page_entries", 1}, {"block_page_entries", 65537},
	} {
		begin, result, _ := rootFSImportTestFixture(t, fmt.Sprintf("migration-invalid-%s-%d", invalid.column, invalid.value))
		artifactDigest := insertPreGeometryArtifact(t, pool, begin, result)
		insertPreGeometryReadyOperation(t, store, pool, begin, artifactDigest)
		_, err := pool.Exec(ctx, "UPDATE manager.rootfs_import_operations SET "+invalid.column+" = $2 WHERE operation_id = $1", begin.OperationID, invalid.value)
		require.NoError(t, err)
		fixtures = append(fixtures, fixture{digest: artifactDigest})
	}

	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	for _, item := range fixtures {
		var got *int
		require.NoError(t, pool.QueryRow(ctx, `SELECT import_data_range_bytes FROM manager.rootfs_base_artifacts WHERE artifact_digest = $1`, item.digest).Scan(&got))
		require.Equal(t, item.want, got, item.digest)
		if got == nil {
			_, err := pool.Exec(ctx, `UPDATE manager.rootfs_base_artifacts SET import_data_range_bytes = 1048576 WHERE artifact_digest = $1`, item.digest)
			require.ErrorContains(t, err, "provenance", "unproven history must remain unknown")
		}
	}
	// All surviving ready operation rows may now be collected without changing
	// the artifact's provenance or rewriting its descriptor/attestation.
	_, err := pool.Exec(ctx, `UPDATE manager.rootfs_import_operations SET updated_at = NOW() - INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = store.ReconcileRootFSImportGarbage(ctx, 24*time.Hour, 1000)
	require.NoError(t, err)
	for _, item := range fixtures {
		var got *int
		require.NoError(t, pool.QueryRow(ctx, `SELECT import_data_range_bytes FROM manager.rootfs_base_artifacts WHERE artifact_digest = $1`, item.digest).Scan(&got))
		require.Equal(t, item.want, got)
	}
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}), "rerunning migrations must preserve provenance")
}

func applySandboxStoreBeforeImportGeometry(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	files, err := storemigrations.FS.ReadDir(".")
	require.NoError(t, err)
	prior := fstest.MapFS{}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") || file.Name() >= "00053" {
			continue
		}
		payload, err := storemigrations.FS.ReadFile(file.Name())
		require.NoError(t, err)
		prior[file.Name()] = &fstest.MapFile{Data: payload, Mode: 0o444}
	}
	require.NoError(t, migrate.Up(t.Context(), pool, ".", migrate.WithBaseFS(prior),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}), migrate.WithSchema(sandboxStoreSchemaName)))
}

func insertPreGeometryArtifact(t *testing.T, pool *pgxpool.Pool, begin *BeginRootFSImportRequest, result rootfsimporter.BuildResult) string {
	t.Helper()
	attestation, payload, artifactDigest, err := result.Attest(begin.Spec.FormatGeneration, begin.Spec.ProcdProtocol)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `
		INSERT INTO manager.rootfs_base_artifacts (
			artifact_digest, source_oci_ref, source_oci_digest, manifest_digest, config_digest,
			base_block_root, format_generation, oci_os, oci_architecture, oci_variant,
			procd_protocol, procd_digest, logical_size_bytes, descriptor_digest,
			state, descriptor, attestation
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'ready', $15, $16)
	`, artifactDigest.String(), attestation.SourceOCIRef, attestation.SourceOCIDigest,
		attestation.ManifestDigest, attestation.ConfigDigest, attestation.BaseBlockRoot,
		attestation.FormatGeneration, attestation.Platform.OS, attestation.Platform.Architecture,
		attestation.Platform.Variant, attestation.ProcdProtocol, attestation.ProcdDigest,
		attestation.LogicalSizeBytes, attestation.DescriptorDigest, result.DescriptorBytes, payload)
	require.NoError(t, err)
	return artifactDigest.String()
}

func insertPreGeometryReadyOperation(t *testing.T, _ *PGSandboxStore, pool *pgxpool.Pool, begin *BeginRootFSImportRequest, artifactDigest string) {
	t.Helper()
	// This is a pre-upgrade row: current store SQL may reference columns which
	// intentionally do not exist until later migrations have run.
	normalized, sourceDigest, err := normalizeBeginRootFSImport(begin)
	require.NoError(t, err)
	spec := normalized.Spec
	_, err = pool.Exec(t.Context(), `
		INSERT INTO manager.rootfs_import_operations (operation_id, source_oci_ref, source_oci_digest,
			oci_os, oci_architecture, oci_variant, format_generation, procd_protocol, procd_digest,
			logical_size_bytes, block_data_range_bytes, block_pack_bytes, block_page_entries, object_prefix,
			state, result_artifact_digest, ready_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'ready',$15,NOW())
	`, normalized.OperationID, spec.SourceOCIRef, sourceDigest.String(), spec.Platform.OS, spec.Platform.Architecture,
		spec.Platform.Variant, spec.FormatGeneration, spec.ProcdProtocol, spec.ProcdDigest, spec.LogicalSizeBytes,
		spec.BlockOptions.DataRangeBytes, spec.BlockOptions.PackBytes, spec.BlockOptions.PageEntries, spec.BlockOptions.ObjectPrefix, artifactDigest)
	require.NoError(t, err)
}
