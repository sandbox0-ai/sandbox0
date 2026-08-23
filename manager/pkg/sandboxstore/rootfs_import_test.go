package sandboxstore

import (
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

func TestNormalizeBeginRootFSImportUsesOneCanonicalContract(t *testing.T) {
	req, _, _ := rootFSImportTestFixture(t, "normalize")
	normalized, sourceDigest, err := normalizeBeginRootFSImport(req)
	require.NoError(t, err)
	require.Equal(t, normalized.Spec.SourceOCIRef, req.Spec.SourceOCIRef)
	require.Equal(t, digest.FromString("import-source-normalize"), sourceDigest)
	require.Equal(t, rootfsblock.DefaultPackBytes, normalized.Spec.BlockOptions.PackBytes)

	changed := *req
	changed.OperationID = " invalid"
	_, _, err = normalizeBeginRootFSImport(&changed)
	require.ErrorContains(t, err, "operation ID")
}

func TestNormalizeRootFSImportLeaseAndStates(t *testing.T) {
	token := strings.Repeat("a", 64)
	lease, err := normalizeRootFSImportLease(RootFSImportLease{
		OperationID: "rootfs-import-unit", WorkerID: "manager-a", Token: token,
	})
	require.NoError(t, err)
	require.Equal(t, token, lease.Token)

	_, err = normalizeRootFSImportLease(RootFSImportLease{
		OperationID: "rootfs-import-unit", WorkerID: "manager-a", Token: strings.ToUpper(token),
	})
	require.Error(t, err)
	states, err := normalizeRootFSImportStates([]string{RootFSImportStateReady, RootFSImportStatePending})
	require.NoError(t, err)
	require.Equal(t, []string{RootFSImportStatePending, RootFSImportStateReady}, states)
	_, err = normalizeRootFSImportStates([]string{RootFSImportStateReady, RootFSImportStateReady})
	require.ErrorContains(t, err, "duplicate")
	require.Error(t, validateRootFSImportLeaseTTL(MinRootFSImportLeaseTTL-time.Millisecond))
}

func rootFSImportTestFixture(
	t *testing.T,
	suffix string,
) (*BeginRootFSImportRequest, rootfsimporter.BuildResult, rootfsblock.ObjectReference) {
	t.Helper()
	sourceDigest := digest.FromString("import-source-" + suffix)
	procdDigest := digest.FromString("import-procd-" + suffix)
	prefix := "rootfs/import/" + suffix
	spec := rootfsimporter.OperationSpec{
		SourceOCIRef:     "registry.example/sandbox@" + sourceDigest.String(),
		Platform:         rootfsimporter.ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 3, ProcdProtocol: "sandbox0.procd.v3",
		ProcdDigest: procdDigest.String(), LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
		BlockOptions: rootfsblock.BuildOptions{ObjectPrefix: prefix},
	}
	normalized, err := rootfsimporter.NormalizeOperationSpec(spec)
	require.NoError(t, err)
	pageBytes, err := rootfsblock.EncodeMappingPage(rootfsblock.MappingPage{
		StartBlock: 0, BlockCount: uint64(normalized.LogicalSizeBytes / rootfsblock.LogicalBlockSize),
	})
	require.NoError(t, err)
	pageDigest := digest.FromBytes(pageBytes)
	reference := rootfsblock.ObjectReference{
		Key: prefix + "/maps/sha256/" + pageDigest.Encoded(), Kind: rootfsblock.ObjectKindMappingPage,
		Size: int64(len(pageBytes)), Checksum: pageDigest.String(),
	}
	descriptor := rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: normalized.LogicalSizeBytes,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: pageDigest.String(),
			Object: rootfsblock.ObjectRange{
				Key: reference.Key, Length: reference.Size, Checksum: reference.Checksum,
			},
		},
	}
	descriptorBytes, err := rootfsblock.EncodeDescriptor(descriptor)
	require.NoError(t, err)
	result := rootfsimporter.BuildResult{
		SourceOCIRef: normalized.SourceOCIRef, SourceOCIDigest: sourceDigest,
		ManifestDigest: digest.FromString("import-manifest-" + suffix),
		ConfigDigest:   digest.FromString("import-config-" + suffix),
		Platform:       ocispec.Platform{OS: "linux", Architecture: "amd64"},
		LayerDigests:   []digest.Digest{digest.FromString("import-layer-" + suffix)},
		DiffIDs:        []digest.Digest{digest.FromString("import-diff-" + suffix)},
		ProcdDigest:    procdDigest, UnpackedBytes: 1, Files: 1,
		LogicalSizeBytes: normalized.LogicalSizeBytes,
		DescriptorDigest: digest.FromBytes(descriptorBytes), BaseBlockRoot: pageDigest,
		Descriptor: descriptor, DescriptorBytes: descriptorBytes,
		Objects: 1, Bytes: reference.Size, References: []rootfsblock.ObjectReference{reference},
	}
	require.NoError(t, result.Validate())
	return &BeginRootFSImportRequest{OperationID: "rootfs-import-" + suffix, Spec: spec}, result, reference
}
