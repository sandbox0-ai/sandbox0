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

func sandboxRuntimeClaimSlotFixture(t *testing.T, store *PGSandboxStore, suffix string) (*SandboxRecord, *RuntimeSlot) {
	t.Helper()
	ctx := context.Background()
	sandboxID := "sandbox-" + suffix
	operationID := "operation-" + suffix
	record := rootFSTestSandboxRecord(sandboxID, "team-slot")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
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
	claimed, err := store.AcquireRuntimeSlot(ctx, &AcquireRuntimeSlotRequest{
		OperationID: operationID, ClaimID: "claim-" + suffix, SandboxID: record.ID,
		FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
	})
	require.NoError(t, err)
	return record, claimed
}

func int64Pointer(value int64) *int64 {
	return &value
}
