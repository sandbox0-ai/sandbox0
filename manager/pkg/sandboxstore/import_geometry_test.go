package sandboxstore

import (
	"fmt"
	"testing"

	"github.com/opencontainers/go-digest"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestReadyRootFSArtifactRequirementsImportPolicy(t *testing.T) {
	base, err := validateReadyRootFSBaseArtifact(readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	requirements := ReadyRootFSArtifactRequirements{
		FormatGeneration: base.FormatGeneration, LogicalSizeBytes: base.LogicalSizeBytes,
		ProcdProtocol: base.ProcdProtocol, ProcdDigest: base.ProcdDigest,
	}
	for _, size := range []int{0, rootfsblock.LogicalBlockSize, 1 << 20, 8 << 20} {
		t.Run(fmt.Sprintf("valid-%d", size), func(t *testing.T) {
			policy := requirements
			policy.ImportDataRangeBytes = size
			policy.SourceOCIRef = base.SourceOCIRef
			require.NoError(t, policy.validateSourceLookup(base.SourceOCIDigest))
		})
	}
	for _, size := range []int{-1, 1, 4097, 3 << 20, (8 << 20) + 4096} {
		t.Run(fmt.Sprintf("invalid-%d", size), func(t *testing.T) {
			policy := requirements
			policy.ImportDataRangeBytes = size
			require.ErrorContains(t, policy.Validate(), "import_data_range_bytes")
		})
	}
	for _, ref := range []string{"ubuntu:latest", " " + base.SourceOCIRef, base.SourceOCIRef + " "} {
		policy := requirements
		policy.SourceOCIRef = ref
		require.ErrorContains(t, policy.Validate(), "source_oci_ref")
	}
	requirements.SourceOCIRef = base.SourceOCIRef
	require.ErrorContains(t, requirements.validateSourceLookup(digest.FromString("other-source").String()), "source_oci_digest")
	requirements.SourceOCIRef = "mirror.example/same@" + base.SourceOCIDigest
	require.NoError(t, requirements.validateSourceLookup(base.SourceOCIDigest), "an exact alias is valid only for its own source lookup")
}

func TestRootFSImportPublicationRequiresCanonicalGeometry(t *testing.T) {
	begin, result, _ := rootFSImportTestFixture(t, "geometry-validation")
	spec, err := rootfsimporter.NormalizeOperationSpec(begin.Spec)
	require.NoError(t, err)
	operation := &RootFSImportOperation{Spec: spec, SourceOCIDigest: result.SourceOCIDigest.String()}
	_, originalAttestation, originalDigest, err := validateRootFSImportResult(operation, result)
	require.NoError(t, err)
	operation.Spec.BlockOptions.DataRangeBytes = 1 << 20
	_, attestation, artifactDigest, err := validateRootFSImportResult(operation, result)
	require.NoError(t, err)
	require.Equal(t, originalAttestation, attestation, "import provenance does not alter canonical attestation bytes")
	require.Equal(t, originalDigest, artifactDigest)
	for _, size := range []int{0, -1, 1, 3 << 20, 16 << 20} {
		operation.Spec.BlockOptions.DataRangeBytes = size
		_, _, _, err := validateRootFSImportResult(operation, result)
		require.ErrorIs(t, err, ErrRootFSImportConflict)
	}
}

func TestRootFSImportGeometryMatchDoesNotGuessUnknown(t *testing.T) {
	size := 1 << 20
	operation := &RootFSImportOperation{Spec: rootfsimporter.OperationSpec{
		BlockOptions: rootfsblock.BuildOptions{DataRangeBytes: size},
	}}
	artifact := &RootFSBaseArtifact{}
	require.False(t, rootFSBaseArtifactMatchesImportGeometry(artifact, operation))
	artifact.ImportDataRangeBytes = &size
	require.True(t, rootFSBaseArtifactMatchesImportGeometry(artifact, operation))
	operation.Spec.BlockOptions.DataRangeBytes = 8 << 20
	require.False(t, rootFSBaseArtifactMatchesImportGeometry(artifact, operation))
}

func TestRootFSImportGeometryMigrationBoundsMatchBuilder(t *testing.T) {
	payload, err := storemigrations.FS.ReadFile("00053_rootfs_import_geometry.sql")
	require.NoError(t, err)
	require.Contains(t, string(payload), fmt.Sprintf("operation.block_page_entries BETWEEN 2 AND %d", rootfsblock.MaxMappingPageEntries))
	require.Contains(t, string(payload), fmt.Sprintf("operation.block_data_range_bytes <= %d", rootfsblock.MaxDataRangeBytes))
	require.Contains(t, string(payload), fmt.Sprintf("operation.block_pack_bytes <= %d", rootfsblock.DefaultPackBytes))
}
