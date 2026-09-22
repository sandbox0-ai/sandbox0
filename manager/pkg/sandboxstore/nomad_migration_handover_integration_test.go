package sandboxstore

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationHandoverStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, protocol.MigrationRestoreObservation, *MarkRuntimeSlotCommandReadyRequest) {
	t.Helper()
	f, assignment, stage, issue, start := migrationRestoreStoreFixture(t, suffix)
	restore, err := f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, assignment, stage)
	require.NoError(t, err)
	start.MigrationRestoreDigest, err = restore.Digest()
	require.NoError(t, err)
	_, err = f.store.ConsumeRootFSWriterGrant(f.ctx, &ConsumeRootFSWriterGrantRequest{GrantID: issue.GrantID, WriterEpoch: stage.Identity.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: issue.BindingVersion, BindingDigest: issue.BindingDigest, ConsumerNodeUID: issue.NodeUID, ConsumerAgentUID: "target-ctld", LeaseTTL: time.Minute})
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.NoError(t, err)
	address, err := protocol.NomadProcdAddress(stage.ExpectedPolicyToken.SourceIP)
	require.NoError(t, err)
	restored := protocol.MigrationRestoreObservation{Request: *restore, RequestDigest: start.MigrationRestoreDigest, State: protocol.MigrationRestoreComplete}
	ready := &MarkRuntimeSlotCommandReadyRequest{MigrationRestoreDigest: restored.RequestDigest, SlotID: start.SlotID, AllocationID: start.AllocationID,
		NodeUID: start.NodeUID, NodeBootID: start.NodeBootID, OperationID: start.OperationID, ClaimID: start.ClaimID,
		ProcdInstanceID: restore.Image.Publication.Capture.Request.ProcdInstanceID, ProcdAddress: address, CommandReadyDigest: bytes.Repeat([]byte{0x81}, 32)}
	return f, restored, ready
}

func migrationHandoverResponse(t *testing.T, request procdapi.RuntimeMigrationRequest) procdapi.RuntimeMigrationResponse {
	t.Helper()
	digest, err := request.Digest()
	require.NoError(t, err)
	return procdapi.RuntimeMigrationResponse{InstanceID: request.InstanceID, RequestDigest: digest, RuntimeGeneration: request.Assignment.Target.RuntimeGeneration, State: "ready"}
}

func TestNomadMigrationHandoverCommitsOneRoutedGenerationAfterProcdProofIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "handover-commit")
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	uncertain := restored
	uncertain.State = protocol.MigrationRestoreUncertain
	_, err = f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, uncertain)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	request, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.Equal(t, procdapi.MigrationRestore, request.Action)
	require.Equal(t, ready.ProcdInstanceID, request.InstanceID)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "intent cannot substitute for procd's response")
	response := migrationHandoverResponse(t, *request)
	invalid := response
	invalid.InstanceID = "replacement-procd"
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *request, invalid), ErrNomadSandboxMigrationConflict)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *request, response))
	visible, err := f.store.GetRuntimeSlotBySandboxID(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.slotID, visible.ID, "procd acknowledgement alone cannot publish routing")
	for _, mutate := range []func(*MarkRuntimeSlotCommandReadyRequest){
		func(r *MarkRuntimeSlotCommandReadyRequest) { r.ProcdInstanceID = "replacement-procd" },
		func(r *MarkRuntimeSlotCommandReadyRequest) { r.ProcdAddress = "http://192.0.2.99:49983" },
		func(r *MarkRuntimeSlotCommandReadyRequest) { r.NodeBootID = "rebooted" },
		func(r *MarkRuntimeSlotCommandReadyRequest) {
			r.MigrationRestoreDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		},
	} {
		changed := *ready
		mutate(&changed)
		_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, &changed)
		require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	}
	const workers = 8
	results := make([]*RuntimeSlot, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() { results[i], errs[i] = NewPGSandboxStore(f.pool).MarkRuntimeSlotCommandReady(f.ctx, ready) })
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, RuntimeSlotStateActive, results[i].State)
		require.Equal(t, results[0].CommandReadyAt, results[i].CommandReadyAt)
		require.NotNil(t, results[i].MigrationAdoption)
		require.Equal(t, results[0].MigrationAdoption, results[i].MigrationAdoption)
		require.NoError(t, results[i].MigrationAdoption.ValidateFor(restored))
	}
	visible, err = f.store.GetRuntimeSlotBySandboxID(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, ready.SlotID, visible.ID)
	after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, request.Assignment.Target.RuntimeGeneration, after.RuntimeGeneration)
	require.Equal(t, ready.AllocationID, after.RuntimeID)
	require.Equal(t, before.ExpiresAt, after.ExpiresAt)
	require.Equal(t, before.HardExpiresAt, after.HardExpiresAt)
	require.Equal(t, before.ResourceMillicpu, after.ResourceMillicpu)
	require.Equal(t, before.ResourceMemoryMiB, after.ResourceMemoryMiB)
	require.Equal(t, before.CreatedAt, after.CreatedAt)
	lifecycle, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseCommitting, lifecycle.Phase, "source cleanup still owns the transaction")
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&count))
	require.Equal(t, 2, count, "routing commit must not release physical source custody")
	replay, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.Equal(t, *request, *replay)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *request, response))
	_, err = f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, request.Assignment, restored.Request.Stage)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "source-generation restore authority cannot revive after handover")
	changed := *ready
	changed.CommandReadyDigest = bytes.Repeat([]byte{0x82}, 32)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, &changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET generation_committed_at=NULL WHERE operation_id=$1`, ready.OperationID)
	require.Error(t, err)
}

func TestNomadMigrationHandoverAtomicRollbackAndExpiredAuthorityIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "handover-rollback")
	request, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *request, migrationHandoverResponse(t, *request)))
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_migration_generation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.runtime_generation <> OLD.runtime_generation THEN RAISE EXCEPTION 'injected generation failure'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_migration_generation BEFORE UPDATE ON manager.sandboxes FOR EACH ROW EXECUTE FUNCTION manager.reject_test_migration_generation()`)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorContains(t, err, "injected generation failure")
	slot, err := f.store.GetRuntimeSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateStarting, slot.State, "slot readiness and routing must roll back together")
	require.True(t, slot.CommandReadyAt.IsZero())
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_migration_generation ON manager.sandboxes; DROP FUNCTION manager.reject_test_migration_generation()`)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE grant_id=$1`, restored.Request.Stage.Identity.WriterGrantID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	visible, err := f.store.GetRuntimeSlotBySandboxID(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.slotID, visible.ID)
}

func TestNomadMigrationHandoverCannotPublishAfterHardTTLOrTerminationIntegration(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "termination"
		if expired {
			name = "hard-ttl"
		}
		t.Run(name, func(t *testing.T) {
			f, restored, ready := migrationHandoverStoreFixture(t, "handover-"+name)
			request, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *request, migrationHandoverResponse(t, *request)))
			if expired {
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			} else {
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating' WHERE sandbox_id=$1`, f.sandboxID)
			}
			require.NoError(t, err)
			_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			record, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, request.Assignment.SourceGeneration, record.RuntimeGeneration)
			slot, err := f.store.GetRuntimeSlot(f.ctx, ready.SlotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateStarting, slot.State)
		})
	}
}
