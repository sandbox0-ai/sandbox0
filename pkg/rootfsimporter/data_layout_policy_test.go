package rootfsimporter

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestDataLayoutPolicyBindsOperationWithoutChangingLegacyIdentity(t *testing.T) {
	input := OperationSpec{SourceOCIRef: "registry.example/sandbox@" + digest.FromString("layout-source").String(),
		Platform: ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"}, FormatGeneration: 2,
		ProcdProtocol: "procd-http-v1", ProcdDigest: digest.FromString("procd").String(), LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes}
	id, spec, err := DeterministicOperation(input)
	require.NoError(t, err)
	// Freeze the pre-policy JSON shape/order independently from operationIdentity.
	legacy := struct {
		SourceOCIRef     string                   `json:"source_oci_ref"`
		Platform         ReadyArtifactPlatform    `json:"platform"`
		FormatGeneration int                      `json:"format_generation"`
		ProcdProtocol    string                   `json:"procd_protocol"`
		ProcdDigest      string                   `json:"procd_digest"`
		LogicalSizeBytes int64                    `json:"logical_size_bytes"`
		BlockOptions     rootfsblock.BuildOptions `json:"block_options"`
	}{spec.SourceOCIRef, spec.Platform, spec.FormatGeneration, spec.ProcdProtocol, spec.ProcdDigest, spec.LogicalSizeBytes, spec.BlockOptions}
	body, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.Equal(t, "template-import:"+digest.FromBytes(body).Encoded(), id)
	input.DataLayoutPolicy = XFSFileRangesV1
	other, normalized, err := DeterministicOperation(input)
	require.NoError(t, err)
	require.NotEqual(t, id, other)
	require.Equal(t, spec.BlockOptions, normalized.BlockOptions)
	repeated, again, err := DeterministicOperation(normalized)
	require.NoError(t, err)
	require.Equal(t, other, repeated)
	require.Equal(t, normalized, again)
	for _, policy := range []string{"unknown", " " + XFSFileRangesV1, XFSFileRangesV1 + " "} {
		input.DataLayoutPolicy = policy
		_, _, err := DeterministicOperation(input)
		require.Error(t, err)
	}
	require.Error(t, ValidateDataLayoutPolicy(XFSFileRangesV1, 1, rootfsblock.BuildOptions{}))
	require.Error(t, ValidateDataLayoutPolicy(XFSFileRangesV1, 2, rootfsblock.BuildOptions{DataRangeBytes: 16 << 10}))
}

func layoutAttestationResult(t *testing.T) BuildResult {
	t.Helper()
	result := readyAttestationTestBuildResult(t)
	data := bytes.Repeat([]byte{0x31}, 128<<10)
	built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), bytes.NewReader(data), int64(len(data)),
		&attestationObjectPublisher{objects: map[string][]byte{}}, rootfsblock.BuildOptions{FormatVersion: 2})
	require.NoError(t, err)
	result.Descriptor, result.DescriptorBytes = built.Descriptor, built.Payload
	result.DescriptorDigest, result.BaseBlockRoot = digest.FromBytes(built.Payload), digest.Digest(built.Descriptor.MappingRoot.RootDigest)
	result.LogicalSizeBytes = int64(len(data))
	result.References, result.Bytes, result.Objects = built.References, built.Bytes, built.Objects
	return result
}

func TestDataLayoutAttestationBindsRequestedPolicyAndActualFallback(t *testing.T) {
	result := layoutAttestationResult(t)
	legacy, legacyBytes, legacyID, err := result.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.Equal(t, 1, legacy.Version)
	require.NotContains(t, string(legacyBytes), "data_layout_")
	decoded, err := DecodeReadyArtifactAttestation(legacyBytes)
	require.NoError(t, err)
	require.Equal(t, legacy, decoded)
	result.DataLayoutPolicy, result.DataLayoutRangeBytes = XFSFileRangesV1, 64<<10
	preferred, body, preferredID, err := result.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.Equal(t, 2, preferred.Version)
	require.NotEqual(t, legacyID, preferredID)
	decoded, err = DecodeReadyArtifactAttestation(body)
	require.NoError(t, err)
	require.Equal(t, preferred, decoded)
	result.DataLayoutFallback = rootfsartifact.XFSDataRangeBudgetFallback
	fallback, body, fallbackID, err := result.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, preferredID, fallbackID)
	require.Equal(t, preferred.DescriptorDigest, fallback.DescriptorDigest, "policy evidence is independent of descriptor identity")
	decoded, err = DecodeReadyArtifactAttestation(body)
	require.NoError(t, err)
	require.Equal(t, fallback, decoded)
	for name, mutate := range map[string]func(*ReadyArtifactAttestation){
		"downgrade":        func(a *ReadyArtifactAttestation) { a.Version = 1 },
		"missing-policy":   func(a *ReadyArtifactAttestation) { a.DataLayoutPolicy = "" },
		"missing-geometry": func(a *ReadyArtifactAttestation) { a.DataLayoutRangeBytes = 0 },
		"wrong-geometry":   func(a *ReadyArtifactAttestation) { a.DataLayoutRangeBytes = 16 << 10 },
		"wrong-format":     func(a *ReadyArtifactAttestation) { a.FormatGeneration = 1 },
		"unknown-fallback": func(a *ReadyArtifactAttestation) { a.DataLayoutFallback = "invalid-metadata" },
	} {
		t.Run(name, func(t *testing.T) {
			a := fallback
			mutate(&a)
			b, err := json.Marshal(a)
			require.NoError(t, err)
			_, err = DecodeReadyArtifactAttestation(b)
			require.Error(t, err)
		})
	}
}
