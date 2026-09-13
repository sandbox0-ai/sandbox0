package nomadclaim

import (
	"fmt"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/stretchr/testify/require"
)

func setClaimImportFormat(t *testing.T, fixture *claimServiceFixture, format int) {
	t.Helper()
	fixture.config.RootFSFormatGeneration = format
	fixture.config.RootFSImportDataRangeBytes = 0
	var err error
	fixture.service, err = New(fixture.config)
	require.NoError(t, err)
}

func TestClaimImageImportFormatPolicy(t *testing.T) {
	for _, policy := range []int{0, 1, 2} {
		for _, available := range []int{1, 2} {
			t.Run(fmt.Sprintf("policy-%d-ready-%d", policy, available), func(t *testing.T) {
				fixture := newClaimServiceFixture(t)
				setClaimImportFormat(t, &fixture, policy)
				fixture.store.artifact.FormatGeneration = available
				response, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
					TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "format-claim",
				})
				want := max(policy, 1)
				require.Len(t, fixture.store.artifactRequirements, 1)
				require.Equal(t, want, fixture.store.artifactRequirements[0].FormatGeneration)
				require.Empty(t, fixture.store.digestArtifactRequirements, "image claims must not fall back to old formats")
				if available != want {
					require.ErrorIs(t, err, service.ErrDataPlaneNotReady)
					require.Nil(t, response)
					require.Zero(t, fixture.store.writeCount)
					require.Empty(t, fixture.planner.requests)
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, response.SandboxID)
				require.Len(t, fixture.planner.requests, 1)
				require.Equal(t, fixture.runtimeClass.CompatibilityDigest, fixture.planner.requests[0].CompatibilityDigest,
					"formats supported by the same runtime use the same generic carrier catalog")
			})
		}
	}
}

func TestClaimImportFormatRejectsInvalidPolicy(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	for _, test := range []struct{ format, dataRange int }{
		{-1, 0}, {3, 0}, {10005, 0}, {2, 1 << 20}, {2, -1}, {2, rootfsblock.CompressedDataRangeBytes + 4096},
	} {
		t.Run(fmt.Sprint(test), func(t *testing.T) {
			cfg := fixture.config
			cfg.RootFSFormatGeneration, cfg.RootFSImportDataRangeBytes = test.format, test.dataRange
			claimer, err := New(cfg)
			require.Error(t, err)
			require.Nil(t, claimer)
			require.Zero(t, fixture.store.writeCount)
			require.Empty(t, fixture.store.artifactRequirements)
		})
	}
}

func committedFormatFixture(t *testing.T, format int) claimServiceFixture {
	t.Helper()
	fixture := newClaimServiceFixture(t)
	fixture.store.artifact.FormatGeneration = format
	fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
		ID: "snapshot-format", TeamID: "team-1", SourceSandboxID: "source-sandbox",
		FilesystemID: "source-filesystem", HeadGenerationID: "source-generation",
		SourceOCIDigest: fixture.store.artifact.SourceOCIDigest, BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		FormatGeneration: format,
	}
	fixture.store.generation = &sandboxstore.RootFSGeneration{
		ID: fixture.store.snapshot.HeadGenerationID, FilesystemID: fixture.store.snapshot.FilesystemID,
		SourceOCIDigest: fixture.store.artifact.SourceOCIDigest, BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		FormatGeneration: format, DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable,
	}
	return fixture
}

func TestClaimImportFormatPreservesCommittedSources(t *testing.T) {
	for _, persisted := range []int{1, 2} {
		for _, source := range []string{"snapshot", "captured-template", "capture-metadata"} {
			t.Run(fmt.Sprintf("format-%d-%s", persisted, source), func(t *testing.T) {
				fixture := committedFormatFixture(t, persisted)
				setClaimImportFormat(t, &fixture, 3-persisted)
				tpl := fixture.config.Templates.(*fakeTemplateStore).template
				if source == "capture-metadata" {
					metadata, err := fixture.service.nomadTemplateCaptureMetadata(t.Context(), fixture.store,
						"source-sandbox", "team-1", fixture.store.snapshot, tpl.Spec)
					require.NoError(t, err)
					require.Equal(t, persisted, metadata.FormatGeneration)
					require.Equal(t, fixture.store.generation.ID, metadata.HeadGenerationID)
				} else {
					req := &service.ClaimRequest{TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "committed-format"}
					if source == "snapshot" {
						req.SnapshotID = fixture.store.snapshot.ID
					} else {
						tpl.RootFS = &templatepkg.RootFSTemplateSource{
							StorageFormat: templatepkg.RootFSTemplateStorageFormatBlockCOWV1,
							SnapshotID:    fixture.store.snapshot.ID, GenerationID: fixture.store.generation.ID,
							SourceOCIDigest: fixture.store.artifact.SourceOCIDigest, BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
							FormatGeneration: persisted, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
						}
					}
					response, err := fixture.service.ClaimSandbox(t.Context(), req)
					require.NoError(t, err)
					require.NotEmpty(t, response.SandboxID)
					require.Len(t, fixture.store.restoreCalls, 1)
					require.Equal(t, fixture.store.snapshot.ID, fixture.store.restoreCalls[0].SnapshotID)
					require.Empty(t, fixture.store.ensureCalls)
					require.Len(t, fixture.planner.requests, 1)
					require.Equal(t, fixture.runtimeClass.CompatibilityDigest, fixture.planner.requests[0].CompatibilityDigest)
				}
				require.Empty(t, fixture.store.artifactRequirements)
				require.Len(t, fixture.store.digestArtifactRequirements, 1)
				lookup := fixture.store.digestArtifactRequirements[0]
				require.Equal(t, persisted, lookup.FormatGeneration)
				require.Zero(t, lookup.ImportDataRangeBytes)
				require.Empty(t, lookup.SourceOCIRef)
			})
		}
	}
}

func TestClaimImportFormatPreservesResumeAcrossPolicyChanges(t *testing.T) {
	for _, persisted := range []int{1, 2} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			fixture := newClaimServiceFixture(t)
			setClaimImportFormat(t, &fixture, persisted)
			fixture.store.artifact.FormatGeneration = persisted
			sandboxID := preparePausedNomadResume(t, fixture)
			expectedGeneration := fixture.store.resumeCandidate.SourceGenerationID
			setClaimImportFormat(t, &fixture, 3-persisted)
			fixture.store.artifactRequirements = nil
			fixture.store.digestArtifactRequirements = nil
			resumed, err := fixture.service.ResumePausedSandboxRuntime(t.Context(), sandboxID)
			require.NoError(t, err)
			require.Equal(t, sandboxID, resumed.ID)
			require.Empty(t, fixture.store.artifactRequirements)
			require.Empty(t, fixture.store.digestArtifactRequirements)
			require.Len(t, fixture.planner.requests, 1)
			require.Equal(t, expectedGeneration, fixture.store.resumeCandidate.SourceGenerationID)
			require.Equal(t, fixture.runtimeClass.CompatibilityDigest, fixture.planner.requests[0].CompatibilityDigest)
		})
	}
}

func TestClaimImportFormatPolicyCannotOverrideSnapshotAttestation(t *testing.T) {
	for _, target := range []string{"snapshot", "generation", "artifact"} {
		t.Run(target, func(t *testing.T) {
			fixture := committedFormatFixture(t, 1)
			setClaimImportFormat(t, &fixture, 2)
			switch target {
			case "snapshot":
				fixture.store.snapshot.FormatGeneration = 2
			case "generation":
				fixture.store.generation.FormatGeneration = 2
			case "artifact":
				fixture.store.artifact.FormatGeneration = 2
			}
			_, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
				TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "changed-attestation",
				SnapshotID: fixture.store.snapshot.ID,
			})
			require.Error(t, err)
			require.Zero(t, fixture.store.writeCount)
			require.Empty(t, fixture.store.restoreCalls)
			require.Empty(t, fixture.planner.requests)
		})
	}
}
