package sandboxstore

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/stretchr/testify/require"
)

func setPausedQuota(t *testing.T, store *PGSandboxStore, team string, limit int64) {
	t.Helper()
	require.NoError(t, quota.NewRepository(store.pool).PutPolicy(t.Context(), &quota.Policy{
		TeamID: team, Dimension: quota.DimensionPausedSandboxes, LimitValue: limit,
	}))
}

func TestPausedQuotaBlocksNewClaimsButHonorsRetriesAndOverridesIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	ctx := t.Context()
	require.NoError(t, quota.NewRepository(store.pool).SyncDefaultPolicies(ctx, "test", []quota.DefaultLimit{
		{Dimension: quota.DimensionPausedSandboxes, LimitValue: 1},
	}))
	record := rootFSTestSandboxRecord("admitted", "team-1")
	request := &ReserveSandboxClaimRequest{Record: record, OperationID: "admitted-op", LeaseTTL: time.Minute}
	_, err := store.ReserveSandboxClaim(ctx, request)
	require.NoError(t, err)
	paused := rootFSTestSandboxRecord("retained", "team-1")
	paused.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, paused))
	current, err := store.CountPausedSandboxes(ctx, "team-1")
	require.NoError(t, err)
	require.Equal(t, int64(1), current)
	_, err = store.ReserveSandboxClaim(ctx, request)
	require.NoError(t, err, "an accepted claim retry is not a new identity")
	newRequest := &ReserveSandboxClaimRequest{Record: rootFSTestSandboxRecord("new", "team-1"), OperationID: "new-op", LeaseTTL: time.Minute}
	_, err = store.ReserveSandboxClaim(ctx, newRequest)
	require.ErrorIs(t, err, ErrPausedSandboxQuotaExceeded)
	absent, err := store.GetSandbox(ctx, "new")
	require.NoError(t, err)
	require.Nil(t, absent, "rejection creates no sandbox or claim")
	setPausedQuota(t, store, "team-1", 2)
	_, err = store.ReserveSandboxClaim(ctx, newRequest)
	require.NoError(t, err, "free team override takes precedence immediately")
	other := &ReserveSandboxClaimRequest{Record: rootFSTestSandboxRecord("other", "team-2"), OperationID: "other-op", LeaseTTL: time.Minute}
	_, err = store.ReserveSandboxClaim(ctx, other)
	require.NoError(t, err, "quota is scoped to a team")
}

func TestPausedQuotaSerializesConcurrentForksAndReleasesOnDeletionIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "paused-quota-concurrency")
	pause, err := f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
	require.NoError(t, err)
	f.publishPlannedPause(t, pause.OperationID)
	source, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	setPausedQuota(t, f.store, source.TeamID, 5)
	const attempts = 16
	requests := make([]*NomadSandboxForkRequest, attempts)
	errs := make([]error, attempts)
	var wg sync.WaitGroup
	for i := range attempts {
		requests[i] = &NomadSandboxForkRequest{OperationID: fmt.Sprintf("quota-fork-%d", i), SourceSandboxID: source.ID,
			ExpectedTeamID: source.TeamID, Target: nomadRunningForkTargetRecord(source, fmt.Sprintf("quota-child-%d", i))}
		wg.Go(func() { _, errs[i] = NewPGSandboxStore(f.pool).ForkNomadPausedSandbox(f.ctx, requests[i]) })
	}
	wg.Wait()
	admitted, rejected, winner := 0, 0, -1
	for i, err := range errs {
		switch {
		case err == nil:
			admitted++
			winner = i
		case errors.Is(err, ErrPausedSandboxQuotaExceeded):
			rejected++
		default:
			t.Fatalf("unexpected fork error: %v", err)
		}
	}
	require.Equal(t, 4, admitted, "the paused parent already consumes one slot")
	require.Equal(t, attempts-4, rejected)
	current, err := f.store.CountPausedSandboxes(f.ctx, source.TeamID)
	require.NoError(t, err)
	require.Equal(t, int64(5), current)
	_, err = f.store.ForkNomadPausedSandbox(f.ctx, requests[winner])
	require.NoError(t, err, "completed fork retry works at the limit")
	// Use the durable terminal projection; physical GC may finish later.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='deleted', deleted_at=NOW() WHERE sandbox_id=$1`, requests[winner].Target.ID)
	require.NoError(t, err)
	current, err = f.store.CountPausedSandboxes(f.ctx, source.TeamID)
	require.NoError(t, err)
	require.Equal(t, int64(4), current)
	_, err = f.store.ForkNomadPausedSandbox(f.ctx, &NomadSandboxForkRequest{OperationID: "replacement", SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: nomadRunningForkTargetRecord(source, "replacement")})
	require.NoError(t, err)
}

func TestPausedQuotaNeverBlocksPauseOrResumeIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "paused-quota-stop")
	setPausedQuota(t, f.store, "team-slot", 0)
	pause, err := f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
	require.NoError(t, err)
	f.publishPlannedPause(t, pause.OperationID)
	terminalizeNomadPauseSlot(t, f, pause)
	current, err := f.store.CountPausedSandboxes(f.ctx, "team-slot")
	require.NoError(t, err)
	require.Equal(t, int64(1), current, "stopping compute may exceed the soft retention limit")
	_, err = f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.NoError(t, err, "paused quota does not prevent using existing state")
}

func TestPausedQuotaRejectsRunningForkBeforeSourceMutationIntegration(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(fmt.Sprintf("memory=%t", memory), func(t *testing.T) {
			f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "quota-running-reject", "")
			request := memoryForkRequest(t, f, f.sandboxID, "quota-running-child", "quota-running-fork")
			request.Memory = memory
			setPausedQuota(t, f.store, request.ExpectedTeamID, 0)
			if memory {
				_, err := f.store.RequestNomadSandboxRunningMemoryFork(f.ctx, request)
				require.ErrorIs(t, err, ErrPausedSandboxQuotaExceeded)
			} else {
				_, err := f.store.RequestNomadSandboxRunningFork(f.ctx, request)
				require.ErrorIs(t, err, ErrPausedSandboxQuotaExceeded)
			}
			active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Nil(t, active, "quota rejection must not pause or checkpoint the parent")
			child, err := f.store.GetSandbox(f.ctx, request.Target.ID)
			require.NoError(t, err)
			require.Nil(t, child)
		})
	}
}

func TestPausedQuotaMemoryForkReservationSurvivesPolicyChangeIntegration(t *testing.T) {
	n, request, intent := runningMemoryForkFixture(t, "quota-memory-handoff", nil)
	f := n.f
	current, err := f.store.CountPausedSandboxes(f.ctx, request.ExpectedTeamID)
	require.NoError(t, err)
	require.Equal(t, int64(1), current, "capture reserves its future paused child")
	setPausedQuota(t, f.store, request.ExpectedTeamID, 0)
	_, err = f.store.RequestNomadSandboxRunningMemoryFork(f.ctx, request)
	require.NoError(t, err, "retry honors the admitted reservation")
	finishRunningMemoryForkCapture(t, n, intent)
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, intent.CaptureOperationID)
	require.NoError(t, err, "child publication cannot strand its parent's restore")
	intent, err = f.store.GetNomadSandboxRunningMemoryFork(f.ctx, intent.OperationID)
	require.NoError(t, err)
	require.NotEmpty(t, intent.ParentResumeOperationID)
	child, err := f.store.GetSandbox(f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, child.DesiredState)
}
