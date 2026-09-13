package sandboxstore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

// This test exercises PostgreSQL authority and real compressed block publication,
// not an OCI import, privileged filesystem mount, or startup latency boundary.
func TestCompressedImportDurablePublicationAndSelectionIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, result, _ := rootFSImportTestFixture(t, "compressed-durable")
	begin.Spec.FormatGeneration = rootfsblock.CompressedFormatVersion
	begin.Spec.BlockOptions = rootfsblock.BuildOptions{}
	operationID, normalized, err := rootfsimporter.DeterministicOperation(begin.Spec)
	require.NoError(t, err)
	begin.OperationID = operationID
	operation, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	require.Equal(t, normalized, operation.Spec)
	reloaded, err := store.GetRootFSImportOperation(ctx, operationID)
	require.NoError(t, err)
	require.Equal(t, normalized, reloaded.Spec, "persisted generation restores the builder format")
	require.Equal(t, 2, reloaded.Spec.BlockOptions.FormatVersion)
	require.Equal(t, 64<<10, reloaded.Spec.BlockOptions.DataRangeBytes)
	leased, err := store.LeaseNextRootFSImport(ctx, "compressed-worker", time.Minute)
	require.NoError(t, err)
	lease, err := leased.Lease()
	require.NoError(t, err)
	journal, err := NewRootFSImportPublicationJournal(store, lease)
	require.NoError(t, err)
	objects := objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore)
	image, err := os.Create(filepath.Join(t.TempDir(), "logical-image"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = image.Close() })
	require.NoError(t, image.Truncate(normalized.LogicalSizeBytes))
	expected := bytes.Repeat([]byte("compressed persisted data"), 4096)
	_, err = image.WriteAt(expected, 0)
	require.NoError(t, err)
	built, err := rootfsblock.BuildMaterializedGeneration(ctx, image, normalized.LogicalSizeBytes,
		rootfsimporter.JournaledPublisher{OperationID: operationID, Journal: journal,
			Publisher: rootfsblock.ObjectStorePublisher{Store: objects}}, reloaded.Spec.BlockOptions)
	require.NoError(t, err)
	result.Descriptor, result.DescriptorBytes = built.Descriptor, built.Payload
	result.DescriptorDigest = digest.FromBytes(built.Payload)
	result.BaseBlockRoot = digest.Digest(built.Descriptor.MappingRoot.RootDigest)
	result.Objects, result.Bytes, result.References = built.Objects, built.Bytes, built.References
	artifact, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	require.Equal(t, 2, artifact.FormatGeneration)
	require.NotNil(t, artifact.ImportDataRangeBytes)
	require.Equal(t, 64<<10, *artifact.ImportDataRangeBytes)
	replayed, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, replayed.ArtifactDigest)
	policy := ReadyRootFSArtifactRequirements{
		FormatGeneration: 2, LogicalSizeBytes: normalized.LogicalSizeBytes,
		ProcdProtocol: normalized.ProcdProtocol, ProcdDigest: normalized.ProcdDigest,
		SourceOCIRef: normalized.SourceOCIRef, ImportDataRangeBytes: 64 << 10,
	}
	selected, err := store.GetReadyRootFSBaseArtifact(ctx, result.SourceOCIDigest.String(), artifact.Platform, policy)
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
	policy.FormatGeneration = 1
	_, err = store.GetReadyRootFSBaseArtifact(ctx, result.SourceOCIDigest.String(), artifact.Platform, policy)
	require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("compressed-sandbox", "team-1")))
	_, generation, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: "compressed-sandbox", TeamID: "team-1", SourceOCIRef: normalized.SourceOCIRef,
		SourceOCIDigest: result.SourceOCIDigest.String(), BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	require.Equal(t, 2, generation.FormatGeneration)
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	require.NoError(t, err)
	require.Equal(t, 2, descriptor.Version)
	reader, err := rootfsblock.NewReader(objects, descriptor, 0)
	require.NoError(t, err)
	actual := make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
