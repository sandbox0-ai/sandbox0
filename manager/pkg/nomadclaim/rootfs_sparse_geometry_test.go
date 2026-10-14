package nomadclaim

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/stretchr/testify/require"
)

func TestSparseImageClaimSelectsBothCapacities(t *testing.T) {
	for _, initial := range []int64{0, 8 << 30, 16 << 30} {
		f := newClaimServiceFixture(t)
		f.config.RootFSImportSparseDeviceSizeBytes = 64 << 40
		var err error
		f.service, err = New(f.config)
		require.NoError(t, err)
		f.store.artifact.LogicalSizeBytes = 64 << 40
		f.store.artifact.InitialFilesystemSizeBytes = initial
		result, err := f.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
			TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "sparse-claim",
		})
		require.Len(t, f.store.artifactRequirements, 1)
		r := f.store.artifactRequirements[0]
		require.Equal(t, int64(64<<40), r.LogicalSizeBytes)
		require.Equal(t, int64(8<<30), r.InitialFilesystemSizeBytes)
		if initial == 8<<30 {
			require.NoError(t, err)
			require.NotNil(t, result)
		} else {
			require.ErrorIs(t, err, service.ErrDataPlaneNotReady)
			require.Empty(t, f.planner.requests)
		}
	}
}

func TestSparseImportPolicyPreservesPausedRuntime(t *testing.T) {
	f := newClaimServiceFixture(t)
	id := preparePausedNomadResume(t, f)
	f.config.RootFSImportSparseDeviceSizeBytes = 64 << 40
	var err error
	f.service, err = New(f.config)
	require.NoError(t, err)
	f.store.artifactRequirements = nil
	f.store.digestArtifactRequirements = nil
	resumed, err := f.service.ResumePausedSandboxRuntime(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, id, resumed.ID)
	require.Empty(t, f.store.artifactRequirements)
	for _, r := range f.store.digestArtifactRequirements {
		require.Equal(t, int64(8<<30), r.LogicalSizeBytes)
		require.Zero(t, r.InitialFilesystemSizeBytes)
	}
}
