package rootfsimporter

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestMappingGroupPolicyBindsOperationAndPreservesEmptyIdentity(t *testing.T) {
	input := OperationSpec{SourceOCIRef: "registry.example/sandbox@" + digest.FromString("mapping-source").String(),
		Platform: ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"}, FormatGeneration: 2,
		ProcdProtocol: "procd-http-v1", ProcdDigest: digest.FromString("procd").String(), LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes}
	legacyID, normalized, err := DeterministicOperation(input)
	require.NoError(t, err)
	options, err := json.Marshal(normalized.BlockOptions)
	require.NoError(t, err)
	require.NotContains(t, string(options), "MappingGroupPolicy")
	input.BlockOptions.MappingGroupPolicy = rootfsblock.ContiguousMappingV1
	id, normalized, err := DeterministicOperation(input)
	require.NoError(t, err)
	require.NotEqual(t, legacyID, id)
	repeated, again, err := DeterministicOperation(normalized)
	require.NoError(t, err)
	require.Equal(t, id, repeated)
	require.Equal(t, normalized, again)
	for _, policy := range []string{"unknown", " " + rootfsblock.ContiguousMappingV1, rootfsblock.ContiguousMappingV1 + " "} {
		input.BlockOptions.MappingGroupPolicy = policy
		_, _, err := DeterministicOperation(input)
		require.Error(t, err)
	}
	for _, format := range []int{0, 1, 3} {
		require.Error(t, ValidateMappingGroupPolicy(rootfsblock.ContiguousMappingV1, format))
	}
}

func TestMappingGroupAttestationBindsBothPoliciesWithoutRewritingLegacy(t *testing.T) {
	for _, layout := range []string{"", XFSFileRangesV1} {
		t.Run(layout, func(t *testing.T) {
			result := layoutAttestationResult(t)
			result.DataLayoutPolicy = layout
			if layout != "" {
				result.DataLayoutRangeBytes = 64 << 10
			}
			legacy, legacyBody, legacyID, err := result.Attest(2, "procd-http-v1")
			require.NoError(t, err)
			require.NotContains(t, string(legacyBody), "mapping_group_policy")
			result.MappingGroupPolicy = rootfsblock.ContiguousMappingV1
			proof, body, id, err := result.Attest(2, "procd-http-v1")
			require.NoError(t, err)
			require.Equal(t, 3, proof.Version)
			require.Equal(t, legacy.DescriptorDigest, proof.DescriptorDigest)
			require.Equal(t, layout, proof.DataLayoutPolicy)
			require.NotEqual(t, legacyID, id)
			decoded, err := DecodeReadyArtifactAttestation(body)
			require.NoError(t, err)
			require.Equal(t, proof, decoded)
			for name, mutate := range map[string]func(*ReadyArtifactAttestation){
				"v1-downgrade":   func(a *ReadyArtifactAttestation) { a.Version = 1 },
				"v2-downgrade":   func(a *ReadyArtifactAttestation) { a.Version = 2 },
				"missing-policy": func(a *ReadyArtifactAttestation) { a.MappingGroupPolicy = "" },
				"unknown-policy": func(a *ReadyArtifactAttestation) { a.MappingGroupPolicy = "unknown" },
				"wrong-format":   func(a *ReadyArtifactAttestation) { a.FormatGeneration = 1 },
			} {
				t.Run(name, func(t *testing.T) {
					changed := proof
					mutate(&changed)
					payload, err := json.Marshal(changed)
					require.NoError(t, err)
					_, err = DecodeReadyArtifactAttestation(payload)
					require.Error(t, err)
				})
			}
			result.MappingGroupPolicy = ""
			again, body, id, err := result.Attest(2, "procd-http-v1")
			require.NoError(t, err)
			require.Equal(t, legacy, again)
			require.Equal(t, legacyBody, body)
			require.Equal(t, legacyID, id)
		})
	}
}
