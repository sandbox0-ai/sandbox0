package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationFailureSourceCleanupRetainsDestinationIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "failure-source-cleanup")
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID)
	require.NoError(t, err)
	failure, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, ready.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationNotReady, "failure intent does not prove target execution stopped")
	digest, err := failure.Digest()
	require.NoError(t, err)
	stopped := protocol.MigrationFailureStopProof{RequestDigest: digest, ContainerID: protocol.NomadRunscContainerID(ready.SlotID), ContainerAbsent: true}
	require.NoError(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, *failure, stopped))
	command, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, ready.OperationID)
	require.NoError(t, err)
	require.NoError(t, command.Validate())
	require.NotNil(t, command.Failure)
	require.Equal(t, *failure, command.Failure.Request)
	require.Equal(t, protocol.MigrationAdoptionReceipt{}, command.Adoption)
	require.Equal(t, restored.Request.Image.Target, command.Destination())
	bad := *command
	bad.Adoption.Request.OperationID = ready.OperationID
	require.Error(t, bad.Validate(), "a failed target is never represented as adopted")
	retry, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, ready.OperationID)
	require.NoError(t, err)
	require.Equal(t, command, retry)
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, ready.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	proof := migrationFinalizationStoreProof(t, *command)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *command, proof))
	release, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, ready.OperationID)
	require.NoError(t, err)
	require.NotNil(t, release)
	require.True(t, release.IsSource())
	releaseDigest, err := release.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *release, protocol.MigrationStagingReleased{RequestDigest: releaseDigest}))
	release, err = f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, ready.OperationID)
	require.NoError(t, err)
	require.Nil(t, release, "failed target staging remains in custody")
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, ready.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "physical source cleanup still needs allocation absence")
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID,
		NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x8a}, 32)})
	require.NoError(t, err)
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	for range 2 {
		terminal, err := NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationSource(f.ctx, ready.OperationID)
		require.NoError(t, err)
		require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
		require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
	}
	target, err := f.store.GetRuntimeSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, target.State)
	require.Equal(t, RuntimeResourceLeaseActive, target.ResourceLeaseState)
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, ready.OperationID).Scan(&phase))
	require.Equal(t, SandboxLifecyclePhaseCommitting, phase, "source release must not complete failed target cleanup")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='committed' WHERE txn_id=$1`, ready.OperationID)
	require.Error(t, err)
	after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
}

type migrationFailureStopNode func(context.Context, protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error)

func (f migrationFailureStopNode) StopFailedMigrationDestination(ctx context.Context, r protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error) {
	return f(ctx, r)
}

func TestNomadMigrationFailureStopLostReplyIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "failure-stop-retry")
	request := protocol.MigrationFailureRequest{Restore: restored.Request, Reason: protocol.MigrationFailureDestinationUnavailable}
	digest, err := request.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationFailureStopProof{RequestDigest: digest, ContainerID: protocol.NomadRunscContainerID(ready.SlotID), ContainerAbsent: true}
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, request, proof), ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID)
	require.NoError(t, err)
	command, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
	require.NoError(t, err)
	require.Equal(t, request, *command)
	calls := 0
	node := migrationFailureStopNode(func(ctx context.Context, got protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error) {
		stored, err := NewPGSandboxStore(f.pool).GetNomadMigrationFailure(ctx, ready.OperationID)
		require.NoError(t, err)
		require.Equal(t, *stored, got, "destructive work requires previously committed authority")
		calls++
		if calls == 1 {
			return nil, errors.New("injected lost stop reply")
		}
		return &proof, nil
	})
	worker, err := nomadmigration.NewFailureStop(f.store, node)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.ErrorContains(t, err, "injected lost stop reply")
	ids, err := f.store.ListNomadMigrationFailureStops(f.ctx, "", 8)
	require.NoError(t, err)
	require.Equal(t, []string{ready.OperationID}, ids)
	worker, err = nomadmigration.NewFailureStop(NewPGSandboxStore(f.pool), node)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.Equal(t, 2, calls)
	require.NoError(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, request, proof))
	bad := proof
	bad.ContainerAbsent = false
	require.Error(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, request, bad))
	ids, err = f.store.ListNomadMigrationFailureStops(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET failure_stop_receipt=NULL WHERE operation_id=$1`, ready.OperationID)
	require.Error(t, err, "stop evidence is immutable")
	var leases int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Equal(t, 2, leases, "stopped execution is not complete physical cleanup")
	require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
}

func TestNomadMigrationFailureRetainsCustodyAndClosesExecutionIntegration(t *testing.T) {
	for _, cause := range []string{"claim_expiry", "termination", "hard_ttl", "quiescing"} {
		t.Run(cause, func(t *testing.T) {
			f, restored, ready := migrationHandoverStoreFixture(t, "failure-"+cause)
			handover, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
			require.NoError(t, err)
			healthy, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
			require.NoError(t, err)
			require.Nil(t, healthy)
			ids, err := f.store.ListNomadMigrationFailures(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
			wantReason := protocol.MigrationFailureDestinationUnavailable
			switch cause {
			case "claim_expiry":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID)
			case "termination":
				_, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete during handover")
				wantReason = protocol.MigrationFailureTermination
			case "hard_ttl":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
				wantReason = protocol.MigrationFailureTermination
			case "quiescing":
				_, err = f.store.BeginRuntimeSlotQuiesce(f.ctx, &BeginRuntimeSlotQuiesceRequest{SlotID: ready.SlotID, OperationID: ready.OperationID, ClaimID: ready.ClaimID})
			}
			require.NoError(t, err)
			before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			ids, err = f.store.ListNomadMigrationFailures(f.ctx, "", 8)
			require.NoError(t, err)
			require.Equal(t, []string{ready.OperationID}, ids)
			var wg sync.WaitGroup
			commands := make([]*protocol.MigrationFailureRequest, 4)
			errs := make([]error, len(commands))
			for i := range commands {
				wg.Go(func() {
					commands[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
				})
			}
			wg.Wait()
			for i := range commands {
				require.NoError(t, errs[i])
				require.NotNil(t, commands[i])
				require.Equal(t, wantReason, commands[i].Reason)
				require.Equal(t, restored.Request, commands[i].Restore)
				require.Equal(t, commands[0], commands[i])
			}
			ids, err = f.store.ListNomadMigrationFailures(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids, "decided failures leave intent scheduling, not physical custody")
			target, err := f.store.GetRuntimeSlot(f.ctx, ready.SlotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateQuiescing, target.State)
			var leases int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
			require.Equal(t, 2, leases, "failure intent is not physical absence")
			after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, before, after, "failure intent must not fabricate a routed generation or erase data")
			_, err = f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, restored.Request.Image.Publication.Assignment, restored.Request.Stage)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			require.ErrorIs(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *handover, migrationHandoverResponse(t, *handover)), ErrNomadSandboxMigrationConflict)
			_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
			renew := &RenewRootFSWriterGrantRequest{GrantID: target.WriterGrantID, WriterEpoch: restored.Request.Stage.Identity.WriterEpoch,
				BindingVersion: RootFSWriterBindingVersion, BindingDigest: target.RootFSBindingDigest, ConsumerNodeUID: target.NodeUID}
			policy := RootFSWriterLeaseRenewalPolicy{LeaseTTL: time.Minute, GracePeriod: time.Minute}
			_, err = f.store.RenewRootFSWriterGrant(f.ctx, renew, policy)
			require.Error(t, err)
			results, err := f.store.RenewRootFSWriterGrants(f.ctx, []*RenewRootFSWriterGrantRequest{renew}, policy)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Error(t, results[0].Err)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET state='starting' WHERE slot_id=$1`, ready.SlotID)
			require.Error(t, err, "database guards reject old callers reviving the target")
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET failure_request=NULL,failure_digest=NULL WHERE operation_id=$1`, ready.OperationID)
			require.Error(t, err)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET procd_handover_receipt='{}'::jsonb WHERE operation_id=$1`, ready.OperationID)
			require.Error(t, err)
		})
	}
}

func TestNomadMigrationFailureWorkerRetriesAtomicIntentIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "failure-rollback")
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_failure_slot() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.state='quiescing' THEN RAISE EXCEPTION 'injected failure fence error'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_failure_slot BEFORE UPDATE ON manager.runtime_slots FOR EACH ROW EXECUTE FUNCTION manager.reject_test_failure_slot()`)
	require.NoError(t, err)
	worker, err := nomadmigration.NewFailure(f.store)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.ErrorContains(t, err, "injected failure fence error")
	var absent bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT failure_request IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, ready.OperationID).Scan(&absent))
	require.True(t, absent, "target fence failure must roll back the immutable command")
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_failure_slot ON manager.runtime_slots; DROP FUNCTION manager.reject_test_failure_slot()`)
	require.NoError(t, err)
	worker, err = nomadmigration.NewFailure(NewPGSandboxStore(f.pool))
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	command, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
	require.NoError(t, err)
	require.Equal(t, restored.Request, command.Restore)
	invalid := *command
	invalid.Reason = "retry_restore"
	_, err = invalid.Digest()
	require.Error(t, err)
}

func TestNomadMigrationFailureCannotReplaceCommittedGenerationIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "failure-committed")
	handover, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *handover, migrationHandoverResponse(t, *handover)))
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.NoError(t, err)
	_, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete committed target")
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	ids, err := f.store.ListNomadMigrationFailures(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}
