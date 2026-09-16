package rootfsimporter

import (
	"bytes"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestCompressedImportRequiresMatchingDurableFormatBinding(t *testing.T) {
	input := OperationSpec{
		SourceOCIRef:     "registry.example/sandbox@" + digest.FromString("compressed-source").String(),
		Platform:         ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 2, ProcdProtocol: "sandbox0.procd.v1",
		ProcdDigest: digest.FromString("procd").String(), LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
		BlockOptions: rootfsblock.BuildOptions{FormatVersion: rootfsblock.CompressedFormatVersion},
	}
	normalized, err := NormalizeOperationSpec(input)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.CompressedDataRangeBytes, normalized.BlockOptions.DataRangeBytes)
	for _, generation := range []int{1, 3} {
		bad := input
		bad.FormatGeneration = generation
		_, err := NormalizeOperationSpec(bad)
		require.ErrorContains(t, err, "format generation")
	}
	input.BlockOptions.FormatVersion = 0
	implicitID, implicit, err := DeterministicOperation(input)
	require.NoError(t, err)
	require.Equal(t, normalized, implicit)
	explicitID, _, err := DeterministicOperation(normalized)
	require.NoError(t, err)
	require.Equal(t, explicitID, implicitID)
	input.BlockOptions.FormatVersion = 1
	_, err = NormalizeOperationSpec(input)
	require.ErrorContains(t, err, "unsupported build format version 1")
}

func TestCompressedArtifactAttestationCannotUseLegacyGeneration(t *testing.T) {
	result := readyAttestationTestBuildResult(t)
	objects := &attestationObjectPublisher{objects: make(map[string][]byte)}
	logical := bytes.Repeat([]byte{0x5a}, int(result.LogicalSizeBytes))
	built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), bytes.NewReader(logical), int64(len(logical)), objects,
		rootfsblock.BuildOptions{FormatVersion: rootfsblock.CompressedFormatVersion})
	require.NoError(t, err)
	result.Descriptor, result.DescriptorBytes = built.Descriptor, built.Payload
	result.DescriptorDigest, result.BaseBlockRoot = digest.FromBytes(built.Payload), digest.Digest(built.Descriptor.MappingRoot.RootDigest)
	result.Objects, result.Bytes, result.References = built.Objects, built.Bytes, built.References
	_, _, _, err = result.Attest(1, "procd-http-v1")
	require.ErrorContains(t, err, "format generation")
	attestation, _, _, err := result.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.Equal(t, 2, attestation.FormatGeneration)
}
