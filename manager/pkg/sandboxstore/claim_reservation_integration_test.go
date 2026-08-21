package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReserveSandboxClaimSerializesConcurrentTeamQuotaAdmission(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	const (
		attempts = 32
		limit    = int64(5)
	)

	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			record := rootFSTestSandboxRecord(fmt.Sprintf("sandbox-%02d", index), "team-1")
			record.RuntimeBackend = SandboxRuntimeBackendNomad
			_, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
				Record: record, OperationID: fmt.Sprintf("operation-%02d", index), LeaseTTL: 15 * time.Second,
				ActiveSandboxLimit: int64Pointer(limit),
			})
			results <- err
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var admitted, rejected int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrActiveSandboxQuotaExceeded):
			rejected++
		default:
			t.Fatalf("unexpected reservation error: %v", err)
		}
	}
	require.Equal(t, int(limit), admitted)
	require.Equal(t, attempts-int(limit), rejected)
	current, err := store.CountActiveSandboxes(ctx, "team-1")
	require.NoError(t, err)
	require.Equal(t, limit, current)
}

func TestReserveSandboxClaimAllowsRetryWithoutAnotherQuotaSlot(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	record := rootFSTestSandboxRecord("sandbox-retry", "team-1")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	one := int64(1)

	created, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, OperationID: "operation-retry", LeaseTTL: 15 * time.Second,
		ActiveSandboxLimit: &one,
	})
	require.NoError(t, err)
	require.Equal(t, record.ID, created.ID)

	zero := int64(0)
	retried, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, OperationID: "operation-retry", LeaseTTL: 15 * time.Second,
		ActiveSandboxLimit: &zero,
	})
	require.NoError(t, err)
	require.Equal(t, created.ID, retried.ID)
	current, err := store.CountActiveSandboxes(ctx, "team-1")
	require.NoError(t, err)
	require.Equal(t, int64(1), current)
}

func TestCompleteSandboxClaimRequiresExactActiveRuntimeSlot(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "complete")

	_, err := store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: record.ID, OperationID: "operation-complete", SlotID: claimed.ID,
		AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
	})
	require.ErrorIs(t, err, ErrSandboxClaimReservationConflict)
	_, err = pool.Exec(ctx, `UPDATE manager.runtime_slots SET state = $2 WHERE slot_id = $1`, claimed.ID, RuntimeSlotStateActive)
	require.NoError(t, err)
	completed, err := store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: record.ID, OperationID: "operation-complete", SlotID: claimed.ID,
		AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
	})
	require.NoError(t, err)
	require.Equal(t, claimed.AllocationID, completed.CurrentPodName)
	require.Equal(t, claimed.AllocationNamespace, completed.CurrentPodNamespace)
	retried, err := store.AcquireRuntimeSlot(ctx, &AcquireRuntimeSlotRequest{
		OperationID: claimed.ClaimOperationID, ClaimID: claimed.ClaimID, SandboxID: claimed.SandboxID,
		FilesystemID: claimed.FilesystemID, SourceGenerationID: claimed.SourceGenerationID,
		CompatibilityDigest: claimed.CompatibilityDigest, ClusterID: claimed.ClaimClusterFilter,
		RuntimeAssignmentRevision: claimed.ClaimRuntimeAssignmentRevision,
		NetworkPolicyDigest:       claimed.ClaimNetworkPolicyDigest, ClaimTTL: claimed.ClaimTTL,
	})
	require.NoError(t, err)
	require.Equal(t, claimed.ID, retried.ID)
	var phase string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&phase))
	require.Equal(t, SandboxRuntimeClaimPhaseReady, phase)
}

func TestFenceExpiredSandboxClaimForcesActiveSlotIntoTerminalReconcile(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "fence")
	_, err := pool.Exec(ctx, `UPDATE manager.runtime_slots SET state = $2 WHERE slot_id = $1`, claimed.ID, RuntimeSlotStateActive)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE sandbox_id = $1
	`, record.ID)
	require.NoError(t, err)
	candidate, err := store.FenceSandboxRuntimeClaimForCleanup(
		ctx, record.ID, "operation-fence", "claim lease expired before commit",
	)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, candidate.SlotID)
	require.Equal(t, RuntimeSlotStateQuiescing, candidate.SlotState)
	fenced, err := store.GetRuntimeSlot(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)
	require.False(t, fenced.HeartbeatExpiresAt.After(fenced.AuthorityObservedAt))
	candidates, err := store.ListRuntimeSlotsForReconcile(ctx, 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	_, _, err = store.RetrySandboxClaim(ctx, &RetrySandboxClaimRequest{
		Record: record, OperationID: "operation-fence", LeaseTTL: time.Minute,
	})
	require.ErrorIs(t, err, ErrSandboxClaimCleanupPending)
}

func TestFenceExpiredSandboxClaimWithoutSlotCanBeCleaned(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-abandoned", "team-1")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	_, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, OperationID: "operation-abandoned", LeaseTTL: 15 * time.Second,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE sandbox_id = $1
	`, record.ID)
	require.NoError(t, err)
	claims, err := store.ListSandboxRuntimeClaimsForCleanup(ctx, 10)
	require.NoError(t, err)
	require.Len(t, claims, 1)
	candidate, err := store.FenceSandboxRuntimeClaimForCleanup(
		ctx, record.ID, "operation-abandoned", "claim lease expired before commit",
	)
	require.NoError(t, err)
	require.Empty(t, candidate.SlotID)
	require.NoError(t, store.MarkSandboxDeleted(ctx, record.ID, time.Now().UTC()))
	require.NoError(t, store.MarkSandboxRuntimeClaimCleaned(ctx, record.ID, "operation-abandoned"))
	var phase string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&phase))
	require.Equal(t, SandboxRuntimeClaimPhaseCleaned, phase)
}

func TestRequestSandboxRuntimeClaimCleanupFencesReadyAllocationAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "delete-ready")
	_, err := pool.Exec(ctx, `UPDATE manager.runtime_slots SET state = $2 WHERE slot_id = $1`, claimed.ID, RuntimeSlotStateActive)
	require.NoError(t, err)
	_, err = store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: record.ID, OperationID: "operation-delete-ready", SlotID: claimed.ID,
		AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
	})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, record.ID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		locked *SandboxRecord,
	) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "manual-pause-delete-ready", SandboxID: record.ID,
			Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing,
			Source: SandboxLifecycleSourceManual, Cancelable: true,
			FromGeneration:   locked.RuntimeGeneration,
			FromPodNamespace: locked.CurrentPodNamespace, FromPodName: locked.CurrentPodName,
		})
	}))

	candidate, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
	require.NoError(t, err)
	require.Equal(t, &SandboxClaimCleanupCandidate{
		SandboxID: record.ID, OperationID: "operation-delete-ready",
		SlotID: claimed.ID, SlotState: RuntimeSlotStateQuiescing, PhysicalStateRequired: true,
	}, candidate)
	loaded, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateTerminating, loaded.DesiredState)
	var phase string
	var leaseIsNull bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase, lease_expires_at IS NULL
		FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&phase, &leaseIsNull))
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, phase)
	require.True(t, leaseIsNull)
	var lifecyclePhase, lifecycleError string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase, error FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
	`, "manual-pause-delete-ready").Scan(&lifecyclePhase, &lifecycleError))
	require.Equal(t, SandboxLifecyclePhaseAborted, lifecyclePhase)
	require.Equal(t, "sandbox termination requested", lifecycleError)
	fenced, err := store.GetRuntimeSlot(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)
	require.False(t, fenced.HeartbeatExpiresAt.After(fenced.AuthorityObservedAt))
	claims, err := store.ListSandboxRuntimeClaimsForCleanup(ctx, 10)
	require.NoError(t, err)
	require.Len(t, claims, 1)
	slots, err := store.ListRuntimeSlotsForReconcile(ctx, 10)
	require.NoError(t, err)
	require.Len(t, slots, 1)

	retried, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
	require.NoError(t, err)
	require.Equal(t, candidate, retried)
	_, err = store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: record.ID, OperationID: "operation-delete-ready", SlotID: claimed.ID,
		AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
	})
	require.ErrorIs(t, err, ErrSandboxClaimCleanupPending)
}

func TestRequestHardExpiredSandboxRuntimeClaimCleanupRechecksDeadlineUnderLockIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "hard-expiry-recheck")
	_, err := pool.Exec(ctx, `
		UPDATE manager.sandboxes
		SET hard_expires_at = NOW() + INTERVAL '1 hour'
		WHERE sandbox_id = $1
	`, record.ID)
	require.NoError(t, err)

	_, err = store.RequestHardExpiredSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox hard TTL expired")
	require.ErrorIs(t, err, ErrNomadSandboxHardTTLNotExpired)
	loaded, getErr := store.GetSandbox(ctx, record.ID)
	require.NoError(t, getErr)
	require.Equal(t, SandboxDesiredStateActive, loaded.DesiredState)

	_, err = pool.Exec(ctx, `
		UPDATE manager.sandboxes
		SET hard_expires_at = NOW() - INTERVAL '1 millisecond'
		WHERE sandbox_id = $1
	`, record.ID)
	require.NoError(t, err)
	candidate, err := store.RequestHardExpiredSandboxRuntimeClaimCleanup(
		ctx, record.ID, "sandbox hard TTL expired",
	)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, candidate.SlotID)
	loaded, getErr = store.GetSandbox(ctx, record.ID)
	require.NoError(t, getErr)
	require.Equal(t, SandboxDesiredStateTerminating, loaded.DesiredState)
}

func TestRequestSandboxRuntimeClaimCleanupWinsBeforeFreshClaimLeaseExpires(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "delete-claiming")

	candidate, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
	require.NoError(t, err)
	require.Equal(t, claimed.ID, candidate.SlotID)
	require.Equal(t, RuntimeSlotStateQuiescing, candidate.SlotState)
	require.False(t, candidate.PhysicalStateRequired)
	var phase, desiredState string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT claim.phase, sandbox.desired_state
		FROM manager.sandbox_runtime_claims AS claim
		JOIN manager.sandboxes AS sandbox USING (sandbox_id)
		WHERE claim.sandbox_id = $1
	`, record.ID).Scan(&phase, &desiredState))
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, phase)
	require.Equal(t, SandboxDesiredStateTerminating, desiredState)
}

func TestRequestSandboxRuntimeClaimCleanupPreservesMatchingCrashLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "delete-crash-lifecycle")
	_, err := pool.Exec(ctx, `UPDATE manager.runtime_slots SET state = $2 WHERE slot_id = $1`, claimed.ID, RuntimeSlotStateActive)
	require.NoError(t, err)
	_, err = store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: record.ID, OperationID: "operation-delete-crash-lifecycle", SlotID: claimed.ID,
		AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
	})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, record.ID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		locked *SandboxRecord,
	) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "crash-delete-ready", SandboxID: record.ID,
			Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing,
			Source: SandboxLifecycleSourceCrash, Cancelable: false,
			FromGeneration:   locked.RuntimeGeneration,
			FromPodNamespace: locked.CurrentPodNamespace, FromPodName: locked.CurrentPodName,
		})
	}))

	_, err = store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
	require.NoError(t, err)
	var phase string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
	`, "crash-delete-ready").Scan(&phase))
	require.Equal(t, SandboxLifecyclePhasePublishing, phase)
}

func TestRequestSandboxRuntimeClaimCleanupSerializesWithClaimCompletion(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "delete-race")
	_, err := pool.Exec(ctx, `UPDATE manager.runtime_slots SET state = $2 WHERE slot_id = $1`, claimed.ID, RuntimeSlotStateActive)
	require.NoError(t, err)

	start := make(chan struct{})
	completeErr := make(chan error, 1)
	deleteErr := make(chan error, 1)
	go func() {
		<-start
		_, err := store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
			SandboxID: record.ID, OperationID: "operation-delete-race", SlotID: claimed.ID,
			AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
		})
		completeErr <- err
	}()
	go func() {
		<-start
		_, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
		deleteErr <- err
	}()
	close(start)
	require.NoError(t, <-deleteErr)
	if err := <-completeErr; err != nil {
		require.ErrorIs(t, err, ErrSandboxClaimCleanupPending)
	}

	loaded, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateTerminating, loaded.DesiredState)
	var phase string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&phase))
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, phase)
	fenced, err := store.GetRuntimeSlot(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)
}

func TestRequestSandboxRuntimeClaimCleanupPreventsLateSlotAcquisition(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, filesystem, generation, registration := sandboxRuntimeClaimReadySlotFixture(t, store, "delete-before-acquire")

	candidate, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
	require.NoError(t, err)
	require.Empty(t, candidate.SlotID)
	_, err = store.AcquireRuntimeSlot(ctx, sandboxRuntimeSlotAcquireRequest(record, filesystem, generation, registration, "delete-before-acquire"))
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	slot, err := store.GetRuntimeSlot(ctx, registration.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateFastpathReady, slot.State)
}

func TestRequestSandboxRuntimeClaimCleanupSerializesWithSlotAcquisition(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, filesystem, generation, registration := sandboxRuntimeClaimReadySlotFixture(t, store, "delete-acquire-race")
	request := sandboxRuntimeSlotAcquireRequest(record, filesystem, generation, registration, "delete-acquire-race")

	start := make(chan struct{})
	acquired := make(chan *RuntimeSlot, 1)
	acquireErr := make(chan error, 1)
	deleted := make(chan *SandboxClaimCleanupCandidate, 1)
	deleteErr := make(chan error, 1)
	go func() {
		<-start
		slot, err := store.AcquireRuntimeSlot(ctx, request)
		acquired <- slot
		acquireErr <- err
	}()
	go func() {
		<-start
		candidate, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
		deleted <- candidate
		deleteErr <- err
	}()
	close(start)
	claimed := <-acquired
	claimErr := <-acquireErr
	candidate := <-deleted
	require.NoError(t, <-deleteErr)
	if claimErr != nil {
		require.ErrorIs(t, claimErr, ErrRuntimeSlotConflict)
		require.Nil(t, claimed)
		require.Empty(t, candidate.SlotID)
	} else {
		require.NotNil(t, claimed)
		require.Equal(t, claimed.ID, candidate.SlotID)
		require.Equal(t, RuntimeSlotStateQuiescing, candidate.SlotState)
	}
	var phase string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT phase FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, record.ID).Scan(&phase))
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, phase)
}

func TestRequestSandboxRuntimeClaimCleanupRequiresReadyPhysicalRecord(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record, claimed := sandboxRuntimeClaimSlotFixture(t, store, "delete-missing-slot")
	_, err := pool.Exec(ctx, `UPDATE manager.runtime_slots SET state = $2 WHERE slot_id = $1`, claimed.ID, RuntimeSlotStateActive)
	require.NoError(t, err)
	_, err = store.CompleteSandboxClaim(ctx, &CompleteSandboxClaimRequest{
		SandboxID: record.ID, OperationID: "operation-delete-missing-slot", SlotID: claimed.ID,
		AllocationID: claimed.AllocationID, AllocationNamespace: claimed.AllocationNamespace,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `DELETE FROM manager.runtime_slots WHERE slot_id = $1`, claimed.ID)
	require.NoError(t, err)

	candidate, err := store.RequestSandboxRuntimeClaimCleanup(ctx, record.ID, "sandbox deletion requested")
	require.NoError(t, err)
	require.True(t, candidate.PhysicalStateRequired)
	require.Empty(t, candidate.SlotID)
	loaded, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateTerminating, loaded.DesiredState)
	claims, err := store.ListSandboxRuntimeClaimsForCleanup(ctx, 10)
	require.NoError(t, err)
	require.Len(t, claims, 1)
	fenced, err := store.FenceSandboxRuntimeClaimForCleanup(
		ctx, record.ID, "operation-delete-missing-slot", "claim lease expired before commit",
	)
	require.NoError(t, err)
	require.True(t, fenced.PhysicalStateRequired)
	require.Empty(t, fenced.SlotID)
}

func sandboxRuntimeClaimSlotFixture(t *testing.T, store *PGSandboxStore, suffix string) (*SandboxRecord, *RuntimeSlot) {
	t.Helper()
	ctx := context.Background()
	record, filesystem, generation, registration := sandboxRuntimeClaimReadySlotFixture(t, store, suffix)
	claimed, err := store.AcquireRuntimeSlot(ctx, sandboxRuntimeSlotAcquireRequest(
		record, filesystem, generation, registration, suffix,
	))
	require.NoError(t, err)
	return record, claimed
}

func sandboxRuntimeClaimReadySlotFixture(
	t *testing.T,
	store *PGSandboxStore,
	suffix string,
) (*SandboxRecord, *RootFSFilesystem, *RootFSGeneration, *RegisterRuntimeSlotRequest) {
	t.Helper()
	ctx := context.Background()
	sandboxID := "sandbox-" + suffix
	operationID := "operation-" + suffix
	record := rootFSTestSandboxRecord(sandboxID, "team-slot")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	record.ClusterID = "cluster-a"
	_, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: record, OperationID: operationID, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, generation, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: record.ID, TeamID: record.TeamID,
		SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest,
		BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	registration := runtimeSlotTestRegistration("slot-"+suffix, "allocation-"+suffix)
	_, err = store.RegisterRuntimeSlot(ctx, registration)
	require.NoError(t, err)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: bytes.Repeat([]byte{0x11}, 32),
		NetworkReadyDigest: bytes.Repeat([]byte{0x12}, 32),
		StorageReadyDigest: bytes.Repeat([]byte{0x13}, 32), HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	return record, filesystem, generation, registration
}

func sandboxRuntimeSlotAcquireRequest(
	record *SandboxRecord,
	filesystem *RootFSFilesystem,
	generation *RootFSGeneration,
	registration *RegisterRuntimeSlotRequest,
	suffix string,
) *AcquireRuntimeSlotRequest {
	return &AcquireRuntimeSlotRequest{
		OperationID: "operation-" + suffix, ClaimID: "claim-" + suffix, SandboxID: record.ID,
		FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}
