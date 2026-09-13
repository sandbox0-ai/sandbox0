package nomadclaim

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestClaimMappingPolicyIsExactAndKeepsGenericCarrierIdentity(t *testing.T) {
	for _, configured := range []string{"", rootfsblock.ContiguousMappingV1} {
		for _, available := range []string{"", rootfsblock.ContiguousMappingV1} {
			t.Run(configured+"-"+available, func(t *testing.T) {
				f := newClaimServiceFixture(t)
				f.config.RootFSFormatGeneration = 2
				f.config.RootFSImportMappingGroupPolicy = configured
				f.store.artifact.FormatGeneration = 2
				f.store.artifact.ImportMappingGroupPolicy = available
				var err error
				f.service, err = New(f.config)
				require.NoError(t, err)
				response, err := f.service.ClaimSandbox(t.Context(), &service.ClaimRequest{TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "layout-claim"})
				require.Len(t, f.store.artifactRequirements, 1)
				require.Equal(t, configured, f.store.artifactRequirements[0].ImportMappingGroupPolicy)
				require.Empty(t, f.store.digestArtifactRequirements)
				if configured != available {
					require.ErrorIs(t, err, service.ErrDataPlaneNotReady)
					require.Nil(t, response)
					require.Zero(t, f.store.writeCount)
					require.Empty(t, f.planner.requests)
				} else {
					require.NoError(t, err)
					require.NotEmpty(t, response.SandboxID)
					require.Len(t, f.planner.requests, 1)
					require.Equal(t, f.runtimeClass.CompatibilityDigest, f.planner.requests[0].CompatibilityDigest)
				}
			})
		}
	}
}

func TestClaimMappingPolicyDoesNotChangeCommittedSourceRequirements(t *testing.T) {
	f := newClaimServiceFixture(t)
	f.config.RootFSFormatGeneration = 2
	f.config.RootFSImportMappingGroupPolicy = rootfsblock.ContiguousMappingV1
	var err error
	f.service, err = New(f.config)
	require.NoError(t, err)
	spec := f.config.Templates.(*fakeTemplateStore).template.Spec
	requirements, err := f.service.rootFSArtifactRequirements(spec, 1)
	require.NoError(t, err)
	require.Empty(t, requirements.ImportMappingGroupPolicy)
	require.Empty(t, requirements.SourceOCIRef)
	require.Zero(t, requirements.ImportDataRangeBytes)
	f.config.RootFSImportMappingGroupPolicy = "unknown"
	_, err = New(f.config)
	require.Error(t, err)
}
