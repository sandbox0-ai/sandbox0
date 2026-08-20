package sandboxstore

import (
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestValidateReadyRootFSBaseArtifactRequiresImmutableDigests(t *testing.T) {
	req := readyRootFSBaseArtifactTestRequest()
	normalized, err := validateReadyRootFSBaseArtifact(req)
	require.NoError(t, err)
	require.Equal(t, req.ArtifactDigest, normalized.ArtifactDigest)
	require.Equal(t, req.SourceOCIDigest, normalized.SourceOCIDigest)
	require.NotSame(t, &req.Descriptor[0], &normalized.Descriptor[0])

	req.SourceOCIDigest = "ubuntu:latest"
	_, err = validateReadyRootFSBaseArtifact(req)
	require.ErrorContains(t, err, "source_oci_digest")

	req = readyRootFSBaseArtifactTestRequest()
	req.Platform.Architecture = "AMD64"
	_, err = validateReadyRootFSBaseArtifact(req)
	require.ErrorContains(t, err, "architecture")
}

func TestInitialRootFSGenerationIDSeparatesFilesystems(t *testing.T) {
	artifact := "sha256:" + strings.Repeat("a", 64)
	first := initialRootFSGenerationID("filesystem-a", artifact, 1)
	second := initialRootFSGenerationID("filesystem-b", artifact, 1)
	require.NotEqual(t, first, second)
	require.Equal(t, first, initialRootFSGenerationID("filesystem-a", artifact, 1))
}

func readyRootFSBaseArtifactTestRequest() *PutReadyRootFSBaseArtifactRequest {
	rootDigest := digest.FromString("base-block-root").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: rootDigest,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/base/map.page", Offset: 0, Length: 4096,
				Checksum: digest.FromString("base-map-page").String(),
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return &PutReadyRootFSBaseArtifactRequest{
		ArtifactDigest:   "sha256:" + strings.Repeat("a", 64),
		SourceOCIRef:     "registry.example/sandbox@sha256:" + strings.Repeat("b", 64),
		SourceOCIDigest:  "sha256:" + strings.Repeat("b", 64),
		BaseBlockRoot:    rootDigest,
		FormatGeneration: 1,
		Platform:         RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		Descriptor:       descriptor,
	}
}
