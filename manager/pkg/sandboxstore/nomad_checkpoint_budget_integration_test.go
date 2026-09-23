package sandboxstore

import (
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadCheckpointClaimBudgetSurvivesSlowPreparationWithoutRenewalIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "memory-budget")
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.NoError(t, err)
	target := acquireCheckpointRestoreTarget(t, f, candidate, "memory-budget", true)
	require.Equal(t, MemoryRuntimeSlotClaimTTL, target.ClaimTTL)
	require.Equal(t, MemoryRuntimeSlotClaimTTL, target.ClaimLeaseExpiresAt.Sub(target.ClaimedAt))
	a := *candidate.Checkpoint
	cpu, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, *cpu, a, migrationCPUStoreResult(t, f, *cpu)))
	image, err := f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, a, target.ID)
	require.NoError(t, err)
	// Only the isolated test database changes the clock history. A download
	// started two minutes ago must still fit the original six-minute lease.
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots DISABLE TRIGGER runtime_checkpoint_claim_budget_guard`)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE manager.runtime_slots SET claimed_at=claimed_at-INTERVAL '2 minutes',
            claim_lease_expires_at=claim_lease_expires_at-INTERVAL '2 minutes' WHERE slot_id=$1`, target.ID)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots ENABLE TRIGGER runtime_checkpoint_claim_budget_guard`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
	before, err := f.store.GetRuntimeSlot(f.ctx, target.ID)
	require.NoError(t, err)
	require.Greater(t, before.AuthorityObservedAt.Sub(before.ClaimedAt), time.Minute)
	d, err := image.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreImage(f.ctx, *image, protocol.MigrationImagePrepared{
		RequestDigest: d, ManifestDigest: image.Receipt.Reference.ManifestDigest, TotalBytes: 1024}))
	revision, err := a.Assignment.Target.Revision()
	require.NoError(t, err)
	request := &AcquireRuntimeSlotRequest{OperationID: candidate.OperationID, ClaimID: target.ClaimID, SandboxID: candidate.SandboxID,
		FilesystemID: target.FilesystemID, SourceGenerationID: target.SourceGenerationID, CompatibilityDigest: target.CompatibilityDigest,
		ClusterID: target.ClaimClusterFilter, RuntimeAssignmentRevision: revision, NetworkPolicyDigest: target.ClaimNetworkPolicyDigest,
		ClaimTTL: MemoryRuntimeSlotClaimTTL, MemoryRestore: true, Resources: runtimeSlotTestResources()}
	retry, err := NewPGSandboxStore(f.pool).AcquireRuntimeSlot(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, before.ClaimLeaseExpiresAt, retry.ClaimLeaseExpiresAt)
	require.Equal(t, before.ClaimedAt, retry.ClaimedAt)
	for _, sql := range []string{
		`UPDATE manager.runtime_slots SET claim_lease_expires_at=claim_lease_expires_at+INTERVAL '1 minute' WHERE slot_id=$1`,
		`UPDATE manager.runtime_slots SET claimed_at=NOW(),claim_lease_expires_at=NOW()+INTERVAL '6 minutes' WHERE slot_id=$1`,
		`UPDATE manager.runtime_slots SET claim_ttl_milliseconds=60000 WHERE slot_id=$1`,
	} {
		_, err = f.pool.Exec(f.ctx, sql, target.ID)
		require.Error(t, err, "SQL cannot renew or downgrade extended authority")
	}
	request.RuntimeAssignmentRevision = cpu.Source.AssignmentRevision
	_, err = f.store.AcquireRuntimeSlot(f.ctx, request)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	// Expiration still enters ordinary physical cleanup and never releases the
	// resource lease merely because the larger startup budget elapsed.
	tx, err = f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots DISABLE TRIGGER runtime_checkpoint_claim_budget_guard`)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE manager.runtime_slots SET claimed_at=claimed_at-INTERVAL '10 minutes',
		claim_lease_expires_at=claim_lease_expires_at-INTERVAL '10 minutes' WHERE slot_id=$1`, target.ID)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots ENABLE TRIGGER runtime_checkpoint_claim_budget_guard`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
	_, err = f.store.GetNomadCheckpointRestorePreparation(f.ctx, a, target.ID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	expired, err := f.store.GetRuntimeSlot(f.ctx, target.ID)
	require.NoError(t, err)
	fenced, err := f.store.FenceRuntimeSlotForReconcile(f.ctx, &FenceRuntimeSlotForReconcileRequest{SlotID: target.ID, ExpectedRevision: expired.Revision})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)
	require.Equal(t, RuntimeResourceLeaseActive, fenced.ResourceLeaseState)
}

func TestNomadCheckpointClaimBudgetCannotExtendOrdinaryClaimIntegration(t *testing.T) {
	f, _, _, _ := checkpointStoreFixture(t, "ordinary-budget")
	target, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	request := &AcquireRuntimeSlotRequest{OperationID: target.ClaimOperationID, ClaimID: target.ClaimID, SandboxID: target.SandboxID,
		FilesystemID: target.FilesystemID, SourceGenerationID: target.SourceGenerationID, CompatibilityDigest: target.CompatibilityDigest,
		ClusterID: target.ClaimClusterFilter, RuntimeAssignmentRevision: target.ClaimRuntimeAssignmentRevision,
		NetworkPolicyDigest: target.ClaimNetworkPolicyDigest, Resources: runtimeSlotTestResources(), ClaimTTL: MemoryRuntimeSlotClaimTTL}
	_, err = f.store.AcquireRuntimeSlot(f.ctx, request)
	require.ErrorContains(t, err, "claim_ttl")
	request.MemoryRestore = true
	_, err = f.store.AcquireRuntimeSlot(f.ctx, request)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_ttl_milliseconds=360000,
        claim_lease_expires_at=claimed_at+INTERVAL '6 minutes' WHERE slot_id=$1`, target.ID)
	require.Error(t, err)
	unchanged, err := f.store.GetRuntimeSlot(f.ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, target.ClaimTTL, unchanged.ClaimTTL)
	require.Equal(t, target.ClaimLeaseExpiresAt, unchanged.ClaimLeaseExpiresAt)
	request.ClaimTTL = MemoryRuntimeSlotClaimTTL + time.Millisecond
	_, err = f.store.AcquireRuntimeSlot(f.ctx, request)
	require.ErrorContains(t, err, "claim_ttl")
}
