package sandboxstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNomadRunningForkExplicitTTLDatabaseRoundTripIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork-explicit-ttl")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-running-fork-explicit-ttl")
	startedAt := time.Now().UTC().Truncate(time.Second).Add(123456789 * time.Nanosecond)
	ttl, hardTTL := int32(300), int32(1800)
	target.Config.TTL, target.Config.HardTTL = &ttl, &hardTTL
	target.ClaimedAt, target.CreatedAt = startedAt, startedAt
	target.ExpiresAt = startedAt.Add(5 * time.Minute)
	target.HardExpiresAt = startedAt.Add(30 * time.Minute)
	request := &NomadSandboxForkRequest{
		OperationID: "nomad-running-fork-explicit-ttl-operation", SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	}
	candidate, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, request)
	require.NoError(t, err)
	stored, err := fixture.store.GetSandbox(fixture.ctx, target.ID)
	require.NoError(t, err)
	t.Logf("expiry before persistence: %s; after persistence: %s",
		target.ExpiresAt.Format(time.RFC3339Nano), stored.ExpiresAt.Format(time.RFC3339Nano))

	// The API checks again with its original request after dispatching the node RPC.
	retry, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, candidate.TargetGenerationID, retry.TargetGenerationID)
	require.Equal(t, candidate.SourceWriterGrantID, retry.SourceWriterGrantID)

	// A controller restart reads the target from the database instead.
	recoveredRequest := *request
	recoveredRequest.Target = stored
	recovered, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, &recoveredRequest)
	require.NoError(t, err)
	require.Equal(t, candidate.TargetGenerationID, recovered.TargetGenerationID)

	forkRequest, _ := nomadRunningForkCheckpointRequest(
		t, fixture, source, target, candidate, request.OperationID,
	)
	_, err = fixture.store.ForkRunningRootFSFilesystem(fixture.ctx, forkRequest)
	require.NoError(t, err)
	completed, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, request)
	require.NoError(t, err)
	require.True(t, completed.Completed)
	completed, err = fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, &recoveredRequest)
	require.NoError(t, err)
	require.True(t, completed.Completed)
}
