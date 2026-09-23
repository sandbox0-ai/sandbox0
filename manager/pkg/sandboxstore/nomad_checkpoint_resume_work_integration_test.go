package sandboxstore

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNomadCheckpointResumeWorkScansOnlyExactAcceptedLiveMemoryOperationsIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "memory-resume-work")
	for _, id := range []string{"work-memory-child", "work-cold-child"} {
		_, err := f.store.ForkNomadPausedSandbox(f.ctx, memoryForkRequest(t, f, f.sandboxID, id, id+"-fork"))
		require.NoError(t, err)
	}
	work, err := f.store.ListNomadCheckpointResumes(f.ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, work, "retained memory is not an accepted resume")
	var expected []NomadCheckpointResumeWork
	for _, id := range []string{f.sandboxID, "work-memory-child", "work-cold-child"} {
		candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: id, ExpectedTeamID: "team-slot", Memory: id != "work-cold-child"})
		require.NoError(t, err)
		if id != "work-cold-child" {
			expected = append(expected, NomadCheckpointResumeWork{OperationID: candidate.OperationID, SandboxID: id})
		}
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].OperationID < expected[j].OperationID })
	work, err = NewPGSandboxStore(f.pool).ListNomadCheckpointResumes(f.ctx, "", 1)
	require.NoError(t, err)
	require.Equal(t, expected[:1], work)
	next, err := f.store.ListNomadCheckpointResumes(f.ctx, work[0].OperationID, 1)
	require.NoError(t, err)
	require.Equal(t, expected[1:], next)
	end, err := f.store.ListNomadCheckpointResumes(f.ctx, next[0].OperationID, 1)
	require.NoError(t, err)
	require.Empty(t, end)
	aborted, err := f.store.AbortNomadSandboxResume(f.ctx, expected[0].SandboxID, expected[0].OperationID, "test capacity unavailable")
	require.NoError(t, err)
	require.True(t, aborted)
	work, err = f.store.ListNomadCheckpointResumes(f.ctx, "", 10)
	require.NoError(t, err)
	require.Equal(t, expected[1:], work)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE sandbox_id=$1`, expected[1].SandboxID)
	require.NoError(t, err)
	work, err = f.store.ListNomadCheckpointResumes(f.ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, work)
	_, err = f.store.ListNomadCheckpointResumes(f.ctx, "", MaxRuntimeSlotReconcileLimit+1)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
}
