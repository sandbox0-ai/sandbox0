//go:build linux

package rootfsimporter

import (
	"context"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestBlockBuilderMappingPolicyIsPublishedInProof(t *testing.T) {
	f := newOCIBlockBuildFixture(t)
	f.request.BlockOptions = rootfsblock.BuildOptions{FormatVersion: 2, MappingGroupPolicy: rootfsblock.ContiguousMappingV1}
	built, err := f.builder().Build(t.Context(), f.request)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.ContiguousMappingV1, built.MappingGroupPolicy)
	proof, _, _, err := built.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.Equal(t, 3, proof.Version)
	require.Equal(t, built.MappingGroupPolicy, proof.MappingGroupPolicy)
}

func TestBlockBuilderRejectsUnsupportedMappingPolicyBeforeUnpacking(t *testing.T) {
	for _, policy := range []string{"unknown", rootfsblock.ContiguousMappingV1} {
		f := newOCIBlockBuildFixture(t)
		f.request.BlockOptions.MappingGroupPolicy = policy
		if policy == rootfsblock.ContiguousMappingV1 {
			f.request.BlockOptions.FormatVersion = 1
		}
		_, err := f.builder().Build(t.Context(), f.request)
		require.Error(t, err)
		require.Zero(t, f.unpacker.calls)
		require.Empty(t, f.publisher.objects)
	}
	f := newOCIBlockBuildFixture(t)
	f.request.BlockOptions = rootfsblock.BuildOptions{FormatVersion: 2, MappingGroupPolicy: rootfsblock.ContiguousMappingV1}
	_, err := f.builder().BuildMaterializedGeneration(t.Context(), MaterializedGenerationBuildRequest{
		BuildRequest: f.request, MutationDigest: digest.FromString("mutation"),
		Mutator: RootMutatorFunc(func(context.Context, string) error { t.Fatal("must not mutate"); return nil }),
	})
	require.ErrorContains(t, err, "base image imports only")
	require.Zero(t, f.unpacker.calls)
	require.Empty(t, f.publisher.objects)
}
