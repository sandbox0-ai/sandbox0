package sandboxstore

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type destinationTestRestorer struct {
	t        *testing.T
	response protocol.MigrationRestoreObservation
	policy   string
	calls    int
	lost     bool
	invalid  string
}

func (r *destinationTestRestorer) RestoreMigration(_ context.Context, image protocol.MigrationImagePrepareRequest, policy string) (*protocol.MigrationRestoreObservation, error) {
	r.calls++
	require.Equal(r.t, r.response.Request.Image, image)
	require.Equal(r.t, r.policy, policy)
	if r.lost {
		r.lost = false
		return nil, errors.New("lost restore response")
	}
	copy := r.response
	switch r.invalid {
	case "missing":
		return nil, nil
	case "uncertain":
		copy.State = protocol.MigrationRestoreUncertain
	case "digest":
		copy.RequestDigest = "wrong"
	}
	return &copy, nil
}

type destinationFailureStore struct {
	*PGSandboxStore
	before, after bool
}

func (s *destinationFailureStore) AuthorizeNomadSandboxMigrationHandover(ctx context.Context, r protocol.MigrationRestoreObservation) (*procdapi.RuntimeMigrationRequest, error) {
	if s.before {
		s.before = false
		return nil, errors.New("before handover commit")
	}
	result, err := s.PGSandboxStore.AuthorizeNomadSandboxMigrationHandover(ctx, r)
	if err != nil {
		return nil, err
	}
	if s.after {
		s.after = false
		return nil, errors.New("lost handover commit response")
	}
	return result, nil
}

func TestNomadMigrationDestinationWorkerRecoversRestoreAndCommitResponseLossIntegration(t *testing.T) {
	f, restored, _ := migrationHandoverStoreFixture(t, "destination-worker")
	id := restored.Request.Image.Publication.Assignment.OperationID
	policy := migrationSourcePolicy(f.sandboxID, restored.Request.Image.Publication.Assignment.Target.TeamID)
	restorer := &destinationTestRestorer{t: t, response: restored, policy: policy, lost: true}
	for _, failure := range []string{"restore", "before", "after"} {
		store := &destinationFailureStore{PGSandboxStore: NewPGSandboxStore(f.pool), before: failure == "before", after: failure == "after"}
		worker, err := nomadmigration.NewDestination(store, restorer)
		require.NoError(t, err)
		result, err := worker.RunOnce(f.ctx)
		require.Error(t, err)
		require.Equal(t, 1, result.Failed)
		require.Zero(t, result.Advanced)
		if failure != "after" {
			state, err := f.store.GetNomadMigrationDestination(f.ctx, id)
			require.NoError(t, err)
			require.NotNil(t, state)
		}
	}
	require.Equal(t, 3, restorer.calls)
	worker, err := nomadmigration.NewDestination(NewPGSandboxStore(f.pool), restorer)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Zero(t, result.Candidates)
	require.Equal(t, 3, restorer.calls)
	command, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.Equal(t, procdapi.MigrationRestore, command.Action)
	var pending bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT restore_receipt IS NOT NULL AND procd_handover_request IS NOT NULL AND procd_handover_receipt IS NULL AND generation_committed_at IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).Scan(&pending))
	require.True(t, pending, "completed restore is not procd handover or routing readiness")
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, command.Assignment.SourceGeneration, sandbox.RuntimeGeneration)
	var leases int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Equal(t, 2, leases)
}

func TestNomadMigrationDestinationWorkerRejectsUncertainRestoreIntegration(t *testing.T) {
	f, restored, _ := migrationHandoverStoreFixture(t, "destination-uncertain")
	id := restored.Request.Image.Publication.Assignment.OperationID
	restorer := &destinationTestRestorer{t: t, response: restored, policy: migrationSourcePolicy(f.sandboxID, restored.Request.Image.Publication.Assignment.Target.TeamID)}
	worker, err := nomadmigration.NewDestination(f.store, restorer)
	require.NoError(t, err)
	for _, failure := range []string{"missing", "uncertain", "digest"} {
		restorer.invalid = failure
		result, err := worker.RunOnce(f.ctx)
		require.Error(t, err)
		require.Equal(t, 1, result.Failed)
		var absent bool
		require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT restore_receipt IS NULL AND procd_handover_request IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).Scan(&absent))
		require.True(t, absent)
	}
	restorer.invalid = ""
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
}

func TestNomadMigrationDestinationScanRequiresCommittedPhysicalFenceIntegration(t *testing.T) {
	f, _, request, proof := migrationFenceStoreFixture(t, "destination-scan")
	id := request.PublicationRequest.Assignment.OperationID
	ids, err := f.store.ListNomadMigrationDestinations(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	ids, err = f.store.ListNomadMigrationDestinations(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids, "regional writer retirement intent is not a physical fence")
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof))
	ids, err = f.store.ListNomadMigrationDestinations(f.ctx, "", 8)
	require.NoError(t, err)
	require.Equal(t, []string{id}, ids)
	state, err := f.store.GetNomadMigrationDestination(f.ctx, id)
	require.NoError(t, err)
	require.NoError(t, state.Validate())
	require.Equal(t, migrationSourcePolicy(f.sandboxID, request.PublicationRequest.Assignment.Target.TeamID), state.Policy)
	ids, err = f.store.ListNomadMigrationDestinations(f.ctx, id, 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}
