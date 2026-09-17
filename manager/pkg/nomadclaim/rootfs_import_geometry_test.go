package nomadclaim

import (
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func newClaimServiceFixtureWithImportGeometry(t *testing.T, dataRange int) claimServiceFixture {
	t.Helper()
	fixture := newClaimServiceFixture(t)
	fixture.config.RootFSImportDataRangeBytes = dataRange
	var err error
	fixture.service, err = New(fixture.config)
	require.NoError(t, err)
	legacyRange := 64 << 10
	fixture.store.artifact.ImportDataRangeBytes = &legacyRange
	return fixture
}

func TestClaimImageImportGeometryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		configured, readyRange          int
		otherReference, wantUnavailable bool
	}{
		{name: "default_accepts_unknown_provenance"},
		{name: "default_accepts_64k", readyRange: 64 << 10},
		{name: "default_accepts_16k", readyRange: 16 << 10},
		{name: "strict_16k_rejects_64k", configured: 16 << 10, readyRange: 64 << 10, wantUnavailable: true},
		{name: "strict_16k_accepts_16k", configured: 16 << 10, readyRange: 16 << 10},
		{name: "strict_16k_rejects_unknown_provenance", configured: 16 << 10, wantUnavailable: true},
		{name: "strict_64k_rejects_16k", configured: 64 << 10, readyRange: 16 << 10, wantUnavailable: true},
		{name: "strict_exact_reference", configured: 16 << 10, readyRange: 16 << 10, otherReference: true, wantUnavailable: true},
		{name: "default_exact_reference", readyRange: 64 << 10, otherReference: true, wantUnavailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newClaimServiceFixtureWithImportGeometry(t, tc.configured)
			if tc.readyRange == 0 {
				fixture.store.artifact.ImportDataRangeBytes = nil
			} else {
				dataRange := tc.readyRange
				fixture.store.artifact.ImportDataRangeBytes = &dataRange
			}
			image := fixture.config.Templates.(*fakeTemplateStore).template.Spec.MainContainer.Image
			if tc.otherReference {
				fixture.store.artifact.SourceOCIRef = "mirror.example/other@" + fixture.store.artifact.SourceOCIDigest
			}
			response, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
				TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "geometry-claim",
			})
			require.Len(t, fixture.store.artifactRequirements, 1)
			lookup := fixture.store.artifactRequirements[0]
			require.Equal(t, tc.configured, lookup.ImportDataRangeBytes)
			require.Equal(t, image, lookup.SourceOCIRef)
			require.Empty(t, fixture.store.digestArtifactRequirements, "an image lookup must not fall back to a digest lookup")
			if tc.wantUnavailable {
				require.ErrorIs(t, err, service.ErrDataPlaneNotReady)
				require.Nil(t, response)
				require.Zero(t, fixture.store.writeCount)
				require.Empty(t, fixture.planner.requests)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, response.SandboxID)
				require.Len(t, fixture.planner.requests, 1)
				require.Equal(t, fixture.runtimeClass.CompatibilityDigest, fixture.planner.requests[0].CompatibilityDigest,
					"import selection policy must not change runtime class identity")
			}
		})
	}
}

func TestClaimImportGeometryRejectsInvalidBuildOptions(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	for _, dataRange := range []int{-1, (16 << 10) + 1, 3 << 20, rootfsblock.MaxDataRangeBytes + rootfsblock.LogicalBlockSize} {
		t.Run(fmt.Sprint(dataRange), func(t *testing.T) {
			cfg := fixture.config
			cfg.RootFSImportDataRangeBytes = dataRange
			claimer, err := New(cfg)
			require.ErrorContains(t, err, "RootFS import data range")
			require.Nil(t, claimer)
			require.Empty(t, fixture.store.artifactRequirements)
			require.Zero(t, fixture.store.writeCount)
		})
	}
}

func TestClaimImportGeometryDoesNotChangeSnapshotRequirements(t *testing.T) {
	fixture := newClaimServiceFixtureWithImportGeometry(t, 16<<10)
	spec := fixture.config.Templates.(*fakeTemplateStore).template.Spec
	requirements, err := fixture.service.rootFSArtifactRequirements(spec, fixture.store.artifact.FormatGeneration)
	require.NoError(t, err)
	require.Zero(t, requirements.ImportDataRangeBytes)
	require.Empty(t, requirements.SourceOCIRef)
}

func TestClaimImportGeometryPreservesExistingPausedRuntime(t *testing.T) {
	fixture := newClaimServiceFixtureWithImportGeometry(t, 0)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.config.RootFSImportDataRangeBytes = 16 << 10
	var err error
	fixture.service, err = New(fixture.config)
	require.NoError(t, err)
	fixture.store.artifactRequirements = nil
	fixture.store.digestArtifactRequirements = nil

	resumed, err := fixture.service.ResumePausedSandboxRuntime(t.Context(), sandboxID)
	require.NoError(t, err)
	require.Equal(t, sandboxID, resumed.ID)
	require.Empty(t, fixture.store.artifactRequirements, "resume must not select a replacement image artifact")
	for _, requirements := range fixture.store.digestArtifactRequirements {
		require.Zero(t, requirements.ImportDataRangeBytes)
		require.Empty(t, requirements.SourceOCIRef)
	}
	require.Len(t, fixture.planner.requests, 1)
	require.Equal(t, fixture.runtimeClass.CompatibilityDigest, fixture.planner.requests[0].CompatibilityDigest)
}
