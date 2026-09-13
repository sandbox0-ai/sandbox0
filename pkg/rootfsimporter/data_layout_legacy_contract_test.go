package rootfsimporter

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// Keep this pre-layout wire shape independent of ReadyArtifactAttestation:
// adding optional fields must not rename or reorder legacy identity inputs.
func TestDataLayoutPreservesExactLegacyAttestationBytesAndIdentity(t *testing.T) {
	for _, generation := range []int{1, 2} {
		result := readyAttestationTestBuildResult(t)
		if generation == 2 {
			result = layoutAttestationResult(t)
		}
		_, actual, identity, err := result.Attest(generation, "procd-http-v1")
		require.NoError(t, err)
		legacy := struct {
			Version          int                   `json:"version"`
			StorageFormat    string                `json:"storage_format"`
			FormatGeneration int                   `json:"format_generation"`
			SourceOCIRef     string                `json:"source_oci_ref"`
			SourceOCIDigest  string                `json:"source_oci_digest"`
			ManifestDigest   string                `json:"manifest_digest"`
			ConfigDigest     string                `json:"config_digest"`
			Platform         ReadyArtifactPlatform `json:"platform"`
			ProcdProtocol    string                `json:"procd_protocol"`
			ProcdDigest      string                `json:"procd_digest"`
			LogicalSizeBytes int64                 `json:"logical_size_bytes"`
			DescriptorDigest string                `json:"descriptor_digest"`
			BaseBlockRoot    string                `json:"base_block_root"`
		}{
			1, "block-cow-v1", generation, result.SourceOCIRef, result.SourceOCIDigest.String(),
			result.ManifestDigest.String(), result.ConfigDigest.String(),
			ReadyArtifactPlatform{OS: result.Platform.OS, Architecture: result.Platform.Architecture, Variant: result.Platform.Variant},
			"procd-http-v1", result.ProcdDigest.String(), result.LogicalSizeBytes,
			result.DescriptorDigest.String(), result.BaseBlockRoot.String(),
		}
		want, err := json.Marshal(legacy)
		require.NoError(t, err)
		require.Equal(t, want, actual)
		require.Equal(t, digest.FromBytes(want), identity)
	}
}
