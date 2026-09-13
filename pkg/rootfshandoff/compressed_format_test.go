package rootfshandoff

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestGenerationDescriptorRequiresCompressedFormatBinding(t *testing.T) {
	checksum := digest.FromString("mapping-root").String()
	block := rootfsblock.Descriptor{Version: 1, LogicalSizeBytes: 4096, BlockSizeBytes: 4096,
		MappingRoot: rootfsblock.MappingRootLocator{Version: 1, RootDigest: checksum,
			Object: rootfsblock.ObjectRange{Key: "maps/root", Length: 1, Checksum: checksum}}}
	payload, err := rootfsblock.EncodeDescriptor(block)
	require.NoError(t, err)
	// This test checks admission metadata, not physical page availability.
	generation := GenerationDescriptor{Version: GenerationDescriptorVersion, GenerationID: "generation", FilesystemID: "filesystem",
		SourceOCIDigest: checksum, BaseArtifactDigest: checksum, BaseBlockRoot: checksum, CurrentBlockHead: checksum,
		FormatGeneration: 1, DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: 1, Descriptor: payload}
	require.NoError(t, generation.Validate())
	generation.FormatGeneration = 2
	require.ErrorContains(t, generation.Validate(), "format generation")
	block.Version, block.MappingRoot.Version = 2, 2
	generation.Descriptor, err = rootfsblock.EncodeDescriptor(block)
	require.NoError(t, err)
	require.NoError(t, generation.Validate())
	generation.FormatGeneration = 1
	require.ErrorContains(t, generation.Validate(), "format generation")
}
