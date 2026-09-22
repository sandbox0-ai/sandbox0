package sandboxstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationFailureCleanupStoreProof(t *testing.T, r protocol.MigrationFailureCleanupRequest) protocol.MigrationFailureCleanupProof {
	t.Helper()
	p := migrationNodeCleanupStoreProof(t, r.Cleanup)
	digest, err := r.Digest()
	require.NoError(t, err)
	result := protocol.MigrationFailureCleanupProof{RequestDigest: digest, Cleanup: p}
	require.NoError(t, result.ValidateFor(r))
	return result
}

func migrationNodeCleanupStoreProof(t *testing.T, c protocol.NodeCleanupControlRequest) protocol.NodeCleanupControlProof {
	t.Helper()
	p := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID, WriterRetireKind: c.WriterRetireKind,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, NetNSIdentity: c.NetNSIdentity,
		RunscContainerID: c.RunscContainerID, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest, RootFSOperationID: c.WriterOperationID, RootFSProofDigest: strings.Repeat("b", 64),
		Resources: c.Resources, ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest, RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	var err error
	p.ProofDigest, err = p.Digest()
	require.NoError(t, err)
	require.NoError(t, p.Validate())
	return p
}

func TestNomadMigrationFailureCleanupFencesWriterAndRetainsCapacityIntegration(t *testing.T) {
	for _, consumed := range []bool{false, true} {
		t.Run(map[bool]string{false: "issued", true: "consumed"}[consumed], func(t *testing.T) {
			f, a, stage, issue, start := migrationRestoreStoreFixture(t, "failed-target-"+map[bool]string{false: "issued", true: "consumed"}[consumed])
			restore, err := f.store.AuthorizeNomadSandboxMigrationRestore(f.ctx, a, stage)
			require.NoError(t, err)
			if consumed {
				_, err = f.store.ConsumeRootFSWriterGrant(f.ctx, &ConsumeRootFSWriterGrantRequest{GrantID: issue.GrantID, WriterEpoch: stage.Identity.WriterEpoch, RawToken: issue.RawToken,
					BindingVersion: issue.BindingVersion, BindingDigest: issue.BindingDigest, ConsumerNodeUID: issue.NodeUID, ConsumerAgentUID: "target-ctld", LeaseTTL: time.Minute})
				require.NoError(t, err)
			}
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, start.SlotID)
			require.NoError(t, err)
			failure, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, a.OperationID)
			require.NoError(t, err)
			_, err = f.store.AuthorizeNomadSandboxMigrationFailureCleanup(f.ctx, a.OperationID)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			digest, err := failure.Digest()
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, *failure, protocol.MigrationFailureStopProof{RequestDigest: digest, ContainerID: protocol.NomadRunscContainerID(start.SlotID), ContainerAbsent: true}))
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_failure_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.failure_cleanup_request IS NOT NULL THEN RAISE EXCEPTION 'injected cleanup intent error'; END IF; RETURN NEW; END; $$; CREATE TRIGGER reject_test_failure_cleanup BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_failure_cleanup()`)
			require.NoError(t, err)
			_, err = f.store.AuthorizeNomadSandboxMigrationFailureCleanup(f.ctx, a.OperationID)
			require.ErrorContains(t, err, "injected cleanup intent error")
			grant, err := f.store.GetRootFSWriterGrant(f.ctx, issue.GrantID)
			require.NoError(t, err)
			require.Equal(t, map[bool]string{false: RootFSWriterGrantStateIssued, true: RootFSWriterGrantStateConsumed}[consumed], grant.State, "intent failure rolls back writer fence")
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_failure_cleanup ON manager.sandbox_runtime_migrations; DROP FUNCTION manager.reject_test_failure_cleanup()`)
			require.NoError(t, err)
			var wg sync.WaitGroup
			commands := make([]*protocol.MigrationFailureCleanupRequest, 4)
			errs := make([]error, len(commands))
			for i := range commands {
				wg.Go(func() {
					commands[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationFailureCleanup(f.ctx, a.OperationID)
				})
			}
			wg.Wait()
			for i := range commands {
				require.NoError(t, errs[i])
				require.NotNil(t, commands[i])
				require.Equal(t, commands[0], commands[i])
			}
			command := commands[0]
			require.Equal(t, *restore, command.Failure.Request.Restore)
			grant, err = f.store.GetRootFSWriterGrant(f.ctx, issue.GrantID)
			require.NoError(t, err)
			require.Equal(t, map[bool]string{false: RootFSWriterGrantStateCanceled, true: RootFSWriterGrantStateRetiring}[consumed], grant.State)
			retry, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationFailureCleanup(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.Equal(t, command, retry)
			proof := migrationFailureCleanupStoreProof(t, *command)
			_, err = f.store.AuthorizeNomadSandboxMigrationFailureFinalization(f.ctx, a.OperationID)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "node cleanup must be regionally committed before artifact reclamation")
			bad := proof
			bad.Cleanup.RootFSWriterAbsent = false
			require.Error(t, f.store.CommitNomadSandboxMigrationFailureCleanup(f.ctx, *command, bad))
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_cleanup_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.failure_cleanup_receipt IS NOT NULL THEN RAISE EXCEPTION 'injected cleanup receipt error'; END IF; RETURN NEW; END; $$; CREATE TRIGGER reject_test_cleanup_receipt BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_cleanup_receipt()`)
			require.NoError(t, err)
			require.ErrorContains(t, f.store.CommitNomadSandboxMigrationFailureCleanup(f.ctx, *command, proof), "injected cleanup receipt error")
			grant, err = f.store.GetRootFSWriterGrant(f.ctx, issue.GrantID)
			require.NoError(t, err)
			require.Equal(t, map[bool]string{false: RootFSWriterGrantStateCanceled, true: RootFSWriterGrantStateRetiring}[consumed], grant.State)
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_cleanup_receipt ON manager.sandbox_runtime_migrations; DROP FUNCTION manager.reject_test_cleanup_receipt()`)
			require.NoError(t, err)
			before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			for range 2 {
				require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationFailureCleanup(f.ctx, *command, proof))
			}
			grant, err = f.store.GetRootFSWriterGrant(f.ctx, issue.GrantID)
			require.NoError(t, err)
			require.Equal(t, map[bool]string{false: RootFSWriterGrantStateCanceled, true: RootFSWriterGrantStateRetired}[consumed], grant.State)
			var head, phase string
			var leases int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT head_generation_id FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, grant.FilesystemID).Scan(&head))
			require.Equal(t, stage.InitialGeneration, head)
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, a.OperationID).Scan(&phase))
			require.Equal(t, SandboxLifecyclePhaseCommitting, phase)
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
			require.Equal(t, 2, leases)
			finalization, err := f.store.AuthorizeNomadSandboxMigrationFailureFinalization(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.Equal(t, *command, finalization.Request)
			require.Equal(t, proof, finalization.Proof)
			want, err := finalization.Digest()
			require.NoError(t, err)
			finalized := protocol.MigrationFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}
			invalid := finalized
			invalid.ImageAbsent = false
			require.Error(t, f.store.CommitNomadSandboxMigrationFailureFinalization(f.ctx, *finalization, invalid))
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_finalization_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.failure_finalization_receipt IS NOT NULL THEN RAISE EXCEPTION 'injected finalization receipt error'; END IF; RETURN NEW; END; $$; CREATE TRIGGER reject_test_finalization_receipt BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_finalization_receipt()`)
			require.NoError(t, err)
			require.ErrorContains(t, f.store.CommitNomadSandboxMigrationFailureFinalization(f.ctx, *finalization, finalized), "injected finalization receipt error")
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_finalization_receipt ON manager.sandbox_runtime_migrations; DROP FUNCTION manager.reject_test_finalization_receipt()`)
			require.NoError(t, err)
			calls := 0
			node := migrationFailureFinalizeNode(func(_ context.Context, got protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error) {
				require.Equal(t, *finalization, got)
				calls++
				if calls == 1 {
					return nil, errors.New("injected lost finalization response")
				}
				return &finalized, nil
			})
			worker, err := nomadmigration.NewFailureFinalization(f.store, node)
			require.NoError(t, err)
			_, err = worker.RunOnce(f.ctx)
			require.ErrorContains(t, err, "injected lost finalization response")
			worker, err = nomadmigration.NewFailureFinalization(NewPGSandboxStore(f.pool), node)
			require.NoError(t, err)
			result, err := worker.RunOnce(f.ctx)
			require.NoError(t, err)
			require.Equal(t, 1, result.Advanced)
			require.Equal(t, 2, calls)
			require.NoError(t, f.store.CommitNomadSandboxMigrationFailureFinalization(f.ctx, *finalization, finalized))
			ids, err := f.store.ListNomadMigrationFailureFinalizations(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET failure_finalization_receipt=NULL WHERE operation_id=$1`, a.OperationID)
			require.Error(t, err)
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
			require.Equal(t, 2, leases, "artifact reclamation is not allocation absence")
			release, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.NotNil(t, release)
			require.False(t, release.IsSource(), "only the finalized target may release staging")
			releaseDigest, err := release.Digest()
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *release, protocol.MigrationStagingReleased{RequestDigest: releaseDigest}))
			after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
		})
	}
}

type migrationFailureFinalizeNode func(context.Context, protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error)

func (f migrationFailureFinalizeNode) FinalizeFailedMigrationDestination(ctx context.Context, r protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error) {
	return f(ctx, r)
}

type migrationFailureCleanupNode func(context.Context, protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error)

func (f migrationFailureCleanupNode) CleanupFailedMigrationDestination(ctx context.Context, r protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error) {
	return f(ctx, r)
}

func TestNomadMigrationFailureCleanupLostReplyIntegration(t *testing.T) {
	f, _, ready := migrationHandoverStoreFixture(t, "failure-cleanup-retry")
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID)
	require.NoError(t, err)
	failure, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
	require.NoError(t, err)
	digest, err := failure.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, *failure, protocol.MigrationFailureStopProof{RequestDigest: digest, ContainerID: protocol.NomadRunscContainerID(ready.SlotID), ContainerAbsent: true}))
	var first *protocol.MigrationFailureCleanupRequest
	calls := 0
	node := migrationFailureCleanupNode(func(ctx context.Context, got protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error) {
		stored, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationFailureCleanup(ctx, ready.OperationID)
		require.NoError(t, err)
		require.Equal(t, *stored, got, "node cleanup requires a committed writer fence")
		grant, err := f.store.GetRootFSWriterGrant(ctx, got.Cleanup.WriterGrantID)
		require.NoError(t, err)
		require.Equal(t, RootFSWriterGrantStateRetiring, grant.State)
		calls++
		if calls == 1 {
			first = stored
			return nil, errors.New("injected lost cleanup reply")
		}
		require.Equal(t, *first, got, "recreated worker must retry the same physical cleanup")
		proof := migrationFailureCleanupStoreProof(t, got)
		return &proof, nil
	})
	worker, err := nomadmigration.NewFailureCleanup(f.store, node)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.ErrorContains(t, err, "injected lost cleanup reply")
	ids, err := f.store.ListNomadMigrationFailureCleanups(f.ctx, "", 8)
	require.NoError(t, err)
	require.Equal(t, []string{ready.OperationID}, ids)
	worker, err = nomadmigration.NewFailureCleanup(NewPGSandboxStore(f.pool), node)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.Equal(t, 2, calls)
	ids, err = f.store.ListNomadMigrationFailureCleanups(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	var leases int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Equal(t, 2, leases, "writer retirement must not release allocation or image custody")
}
