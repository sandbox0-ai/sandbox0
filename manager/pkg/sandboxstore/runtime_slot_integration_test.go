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

	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/stretchr/testify/require"
)

func TestRuntimeResourceLeaseMigrationDownAndUpIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPoolThrough(t, "00045")
	assertRuntimeResourceSchema := func(want bool) {
		t.Helper()
		var capacityTable, leaseTable, slotColumn bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT to_regclass('manager.runtime_node_capacities') IS NOT NULL,
				to_regclass('manager.runtime_resource_leases') IS NOT NULL,
				EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = 'manager' AND table_name = 'runtime_slots'
						AND column_name = 'resource_lease_id'
				)
		`).Scan(&capacityTable, &leaseTable, &slotColumn))
		require.Equal(t, want, capacityTable)
		require.Equal(t, want, leaseTable)
		require.Equal(t, want, slotColumn)
	}
	assertRuntimeResourceSchema(true)
	require.NoError(t, migrate.Down(ctx, pool, ".",
		migrate.WithBaseFS(sandboxStoreMigrationFilesThrough(t, "00045")),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}),
		migrate.WithSchema(sandboxStoreSchemaName),
	))
	assertRuntimeResourceSchema(false)
	require.NoError(t, migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(sandboxStoreMigrationFilesThrough(t, "00045")),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}),
		migrate.WithSchema(sandboxStoreSchemaName),
	))
	assertRuntimeResourceSchema(true)
}

func TestRuntimeSlotClaimSurvivesAllocationPurgeIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-slot")
	registration := runtimeSlotTestRegistration("slot-a", "allocation-a")

	registered, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateRegistered, registered.State)
	retried, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	require.Equal(t, registered.ID, retried.ID)
	changed := *registration
	changed.NodeBootID = "different-boot"
	_, err = registerRuntimeSlotWithTestCapacity(t, ctx, store, &changed)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)

	proof := bytes.Repeat([]byte{0x31}, 32)
	ready, err := store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: proof, NetworkReadyDigest: bytes.Repeat([]byte{0x32}, 32),
		StorageReadyDigest: bytes.Repeat([]byte{0x33}, 32), HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateFastpathReady, ready.State)

	acquire := &AcquireRuntimeSlotRequest{
		OperationID: "claim-operation-a", ClaimID: "claim-a", SandboxID: "sandbox-slot",
		FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32),
		ClaimTTL:                  time.Minute,
		Resources:                 runtimeSlotTestResources(),
	}
	claimed, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateClaiming, claimed.State)
	require.Equal(t, registration.AllocationID, claimed.AllocationID)
	claimRetry, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, claimRetry.ID)
	require.Equal(t, claimed.ClaimedAt, claimRetry.ClaimedAt)
	changedClusterFilter := *acquire
	changedClusterFilter.ClusterID = ""
	_, err = store.AcquireRuntimeSlot(ctx, &changedClusterFilter)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	changedClaimTTL := *acquire
	changedClaimTTL.ClaimTTL = 30 * time.Second
	_, err = store.AcquireRuntimeSlot(ctx, &changedClaimTTL)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	changedRuntime := *acquire
	changedRuntime.RuntimeAssignmentRevision = strings.Repeat("ef", 32)
	_, err = store.AcquireRuntimeSlot(ctx, &changedRuntime)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	changedPolicy := *acquire
	changedPolicy.NetworkPolicyDigest = "sha256:" + strings.Repeat("12", 32)
	_, err = store.AcquireRuntimeSlot(ctx, &changedPolicy)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)

	binding := bytes.Repeat([]byte{0x44}, 32)
	issue := rootFSWriterGrantTestIssueRequest("sandbox-slot", "grant-slot", acquire.ClaimID, claimed.ID, binding)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = generation.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	bound, err := store.BindRuntimeSlotWriterGrant(ctx, &BindRuntimeSlotWriterGrantRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: acquire.ClaimID, GrantID: issued.Grant.ID,
	})
	require.NoError(t, err)
	require.Equal(t, issued.Grant.ID, bound.WriterGrantID)

	consumed, err := store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding,
		ConsumerNodeUID: registration.NodeUID, ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, consumed.State)
	wrongNodeStart := &StartRuntimeSlotRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: "other-node", NodeBootID: registration.NodeBootID,
		OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		LaunchAttempt: "launch-a", RunscContainerID: "runsc-a",
		RootFSBindingDigest: binding, ClaimNetworkDigest: bytes.Repeat([]byte{0x55}, 32),
		ResourceLeaseID: claimed.ResourceLease.LeaseID, ResourceLeaseDigest: claimed.ResourceLeaseDigest,
	}
	_, err = store.StartRuntimeSlot(ctx, wrongNodeStart)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	started, err := store.StartRuntimeSlot(ctx, &StartRuntimeSlotRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		LaunchAttempt: "launch-a", RunscContainerID: "runsc-a",
		RootFSBindingDigest: binding, ClaimNetworkDigest: bytes.Repeat([]byte{0x55}, 32),
		ResourceLeaseID: claimed.ResourceLease.LeaseID, ResourceLeaseDigest: claimed.ResourceLeaseDigest,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateStarting, started.State)
	commandReady := &MarkRuntimeSlotCommandReadyRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		ProcdInstanceID: "procd-a", ProcdAddress: "http://192.0.2.2:49983",
		CommandReadyDigest: bytes.Repeat([]byte{0x66}, 32),
	}
	active, err := store.MarkRuntimeSlotCommandReady(ctx, commandReady)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateActive, active.State)
	require.Equal(t, "http://192.0.2.2:49983", active.ProcdAddress)
	projected, err := store.GetRuntimeSlotBySandboxID(ctx, acquire.SandboxID)
	require.NoError(t, err)
	require.Equal(t, active.ID, projected.ID)
	require.Equal(t, active.ProcdAddress, projected.ProcdAddress)
	changedAddress := *commandReady
	changedAddress.ProcdAddress = "http://192.0.2.3:49983"
	_, err = store.MarkRuntimeSlotCommandReady(ctx, &changedAddress)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	quiescing, err := store.BeginRuntimeSlotQuiesce(ctx, &BeginRuntimeSlotQuiesceRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, quiescing.State)
	candidates, err := store.ListRuntimeSlotsForReconcile(ctx, 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, claimed.ID, candidates[0].ID, "planned quiesce must be terminally reconcilable before heartbeat expiry")

	observation := bytes.Repeat([]byte{0x77}, 32)
	orphaned, err := store.MarkRuntimeSlotAllocationMissing(ctx, &MarkRuntimeSlotAllocationMissingRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		ObservationDigest: observation,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateOrphaned, orphaned.State)
	require.Equal(t, issued.Grant.ID, orphaned.WriterGrantID)
	require.Equal(t, "runsc-a", orphaned.RunscContainerID)
	require.Equal(t, observation, orphaned.OrphanObservationDigest)

	candidates, err = store.ListRuntimeSlotsForReconcile(ctx, 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, claimed.ID, candidates[0].ID)
	_, err = store.FinalizeRuntimeSlot(ctx, &FinalizeRuntimeSlotRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		Reason: "crash_abandon", ProofDigest: bytes.Repeat([]byte{0x78}, 32),
		ResourceLeaseID:     claimed.ResourceLease.LeaseID,
		ResourceLeaseDigest: claimed.ResourceLeaseDigest, ResourceCgroupAbsent: true,
	})
	require.ErrorIs(t, err, ErrRuntimeSlotInvalid, "the slot must retain the purge orphan until its writer is terminal")
}

func TestRuntimeSlotConcurrentAcquireUsesDistinctReadySlotsIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	compatibility := runtimeSlotTestRegistration("unused", "unused").CompatibilityDigest
	for index := 0; index < 2; index++ {
		registration := runtimeSlotTestRegistration(fmt.Sprintf("slot-%d", index), fmt.Sprintf("allocation-%d", index))
		_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
		require.NoError(t, err)
		_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
			SlotID: registration.SlotID, AllocationID: registration.AllocationID,
			NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
			RuntimeReadyDigest: bytes.Repeat([]byte{byte(0x10 + index)}, 32),
			NetworkReadyDigest: bytes.Repeat([]byte{byte(0x20 + index)}, 32),
			StorageReadyDigest: bytes.Repeat([]byte{byte(0x30 + index)}, 32),
			HeartbeatTTL:       time.Minute,
		})
		require.NoError(t, err)
	}
	type fixture struct {
		sandboxID  string
		filesystem *RootFSFilesystem
		generation *RootFSGeneration
	}
	fixtures := make([]fixture, 2)
	for index := range fixtures {
		fixtures[index].sandboxID = fmt.Sprintf("sandbox-concurrent-%d", index)
		fixtures[index].filesystem, fixtures[index].generation =
			runtimeSlotTestGeneration(t, store, fixtures[index].sandboxID)
	}

	results := make([]*RuntimeSlot, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for index := range fixtures {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index], errs[index] = store.AcquireRuntimeSlot(ctx, &AcquireRuntimeSlotRequest{
				OperationID: fmt.Sprintf("operation-%d", index), ClaimID: fmt.Sprintf("claim-%d", index),
				SandboxID: fixtures[index].sandboxID, FilesystemID: fixtures[index].filesystem.ID,
				SourceGenerationID:  fixtures[index].generation.ID,
				CompatibilityDigest: compatibility, ClusterID: "cluster-a",
				RuntimeAssignmentRevision: strings.Repeat("ab", 32),
				NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
				Resources: runtimeSlotTestResources(),
			})
		}(index)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NotEqual(t, results[0].ID, results[1].ID)
}

func TestRuntimeSlotConcurrentAcquireSameOperationIsIdempotentIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	compatibility := runtimeSlotTestRegistration("unused", "unused").CompatibilityDigest
	for index := 0; index < 2; index++ {
		registration := runtimeSlotTestRegistration(
			fmt.Sprintf("slot-idempotent-%d", index),
			fmt.Sprintf("allocation-idempotent-%d", index),
		)
		_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
		require.NoError(t, err)
		_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
			SlotID: registration.SlotID, AllocationID: registration.AllocationID,
			NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
			RuntimeReadyDigest: bytes.Repeat([]byte{byte(0x40 + index)}, 32),
			NetworkReadyDigest: bytes.Repeat([]byte{byte(0x50 + index)}, 32),
			StorageReadyDigest: bytes.Repeat([]byte{byte(0x60 + index)}, 32),
			HeartbeatTTL:       time.Minute,
		})
		require.NoError(t, err)
	}
	filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-idempotent")
	request := &AcquireRuntimeSlotRequest{
		OperationID: "operation-idempotent", ClaimID: "claim-idempotent",
		SandboxID: "sandbox-idempotent", FilesystemID: filesystem.ID,
		SourceGenerationID:  generation.ID,
		CompatibilityDigest: compatibility, ClusterID: "cluster-a",
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
		Resources: runtimeSlotTestResources(),
	}

	results := make([]*RuntimeSlot, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errs[index] = store.AcquireRuntimeSlot(ctx, request)
		}(index)
	}
	close(start)
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, results[0].ID, results[1].ID)
	require.Equal(t, results[0].ClaimedAt, results[1].ClaimedAt)

	var claimedCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.runtime_slots
		WHERE claim_operation_id = $1
	`, request.OperationID).Scan(&claimedCount))
	require.Equal(t, 1, claimedCount)
}

func TestRuntimeSlotNodeCapacityPreventsOversubscriptionAndReleasesAfterCleanupProofIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	registration := runtimeSlotTestRegistration("unused", "unused")
	_, err := store.RegisterRuntimeNodeCapacity(ctx, &RegisterRuntimeNodeCapacityRequest{
		ClusterID: registration.ClusterID, NodeID: registration.NodeID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		CPUMillicores: 2_000, MemoryBytes: 2 << 30,
		CPUSetCPUs: "0-1", CPUSetMems: "0", TTL: time.Minute,
	})
	require.NoError(t, err)

	type fixture struct {
		request *AcquireRuntimeSlotRequest
		slot    *RuntimeSlot
		err     error
	}
	fixtures := make([]fixture, 3)
	for index := range fixtures {
		suffix := fmt.Sprintf("capacity-%d", index)
		slotRegistration := runtimeSlotTestRegistration("slot-"+suffix, "allocation-"+suffix)
		_, err = store.RegisterRuntimeSlot(ctx, slotRegistration)
		require.NoError(t, err)
		proof := bytes.Repeat([]byte{byte(0x90 + index)}, 32)
		_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
			SlotID: slotRegistration.SlotID, AllocationID: slotRegistration.AllocationID,
			NodeUID: slotRegistration.NodeUID, NodeBootID: slotRegistration.NodeBootID,
			RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
			HeartbeatTTL: time.Minute,
		})
		require.NoError(t, err)
		filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-"+suffix)
		fixtures[index].request = &AcquireRuntimeSlotRequest{
			OperationID: "operation-" + suffix, ClaimID: "claim-" + suffix,
			SandboxID: "sandbox-" + suffix, FilesystemID: filesystem.ID,
			SourceGenerationID: generation.ID, CompatibilityDigest: slotRegistration.CompatibilityDigest,
			ClusterID:                 slotRegistration.ClusterID,
			RuntimeAssignmentRevision: strings.Repeat("ab", 32),
			NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
			Resources: runtimeSlotTestResources(),
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := range fixtures {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			fixtures[index].slot, fixtures[index].err = store.AcquireRuntimeSlot(ctx, fixtures[index].request)
		}(index)
	}
	close(start)
	wg.Wait()

	var successful, unavailable []int
	for index := range fixtures {
		switch {
		case fixtures[index].err == nil:
			successful = append(successful, index)
		case errors.Is(fixtures[index].err, ErrRuntimeSlotUnavailable):
			unavailable = append(unavailable, index)
		default:
			t.Fatalf("claim %d failed unexpectedly: %v", index, fixtures[index].err)
		}
	}
	require.Len(t, successful, 2)
	require.Len(t, unavailable, 1)

	first := &fixtures[successful[0]]
	retried, err := store.AcquireRuntimeSlot(ctx, first.request)
	require.NoError(t, err)
	require.Equal(t, first.slot.ResourceLease, retried.ResourceLease)
	require.Equal(t, first.slot.ResourceLeaseDigest, retried.ResourceLeaseDigest)
	changed := *first.request
	changed.Resources.CPUMillicores++
	_, err = store.AcquireRuntimeSlot(ctx, &changed)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)

	_, err = store.FinalizeRuntimeSlot(ctx, &FinalizeRuntimeSlotRequest{
		SlotID: first.slot.ID, OperationID: first.request.OperationID, ClaimID: first.request.ClaimID,
		Reason: "prelaunch_abort", ProofDigest: bytes.Repeat([]byte{0xa5}, 32),
		ResourceLeaseID:     first.slot.ResourceLease.LeaseID,
		ResourceLeaseDigest: first.slot.ResourceLeaseDigest,
	})
	require.Error(t, err)
	terminal, err := store.FinalizeRuntimeSlot(ctx, &FinalizeRuntimeSlotRequest{
		SlotID: first.slot.ID, OperationID: first.request.OperationID, ClaimID: first.request.ClaimID,
		Reason: "prelaunch_abort", ProofDigest: bytes.Repeat([]byte{0xa5}, 32),
		ResourceLeaseID:     first.slot.ResourceLease.LeaseID,
		ResourceLeaseDigest: first.slot.ResourceLeaseDigest, ResourceCgroupAbsent: true,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
	require.False(t, terminal.ResourceLeaseReleasedAt.IsZero())

	last := &fixtures[unavailable[0]]
	last.slot, err = store.AcquireRuntimeSlot(ctx, last.request)
	require.NoError(t, err)
	require.NotNil(t, last.slot)
	var activeCPU, activeMemory int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(cpu_millicores), 0), COALESCE(SUM(memory_bytes), 0)
		FROM manager.runtime_resource_leases WHERE lease_state = $1
	`, RuntimeResourceLeaseActive).Scan(&activeCPU, &activeMemory))
	require.Equal(t, int64(2_000), activeCPU)
	require.Equal(t, int64(2<<30), activeMemory)
}

func TestRuntimeSlotUnclaimedPurgeBecomesTerminalIntegration(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	registration := runtimeSlotTestRegistration("slot-unclaimed", "allocation-unclaimed")
	_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x7a}, 32)
	terminal, err := store.MarkRuntimeSlotAllocationMissing(ctx, &MarkRuntimeSlotAllocationMissingRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		ObservationDigest: proof,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	require.Equal(t, "allocation_missing", terminal.TerminalReason)
	require.Equal(t, proof, terminal.TerminalProofDigest)
}

func TestRuntimeSlotReconcileFenceRechecksExpiryIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-reconcile-fence")
	registration := runtimeSlotTestRegistration("slot-reconcile-fence", "allocation-reconcile-fence")
	_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x7b}, 32)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
		HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	claimed, err := store.AcquireRuntimeSlot(ctx, &AcquireRuntimeSlotRequest{
		OperationID: "operation-reconcile-fence", ClaimID: "claim-reconcile-fence",
		SandboxID: "sandbox-reconcile-fence", FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: registration.CompatibilityDigest, RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
		Resources: runtimeSlotTestResources(),
	})
	require.NoError(t, err)

	request := &FenceRuntimeSlotForReconcileRequest{SlotID: claimed.ID, ExpectedRevision: claimed.Revision}
	_, err = store.FenceRuntimeSlotForReconcile(ctx, request)
	require.ErrorIs(t, err, ErrRuntimeSlotNotDue)

	_, err = pool.Exec(ctx, `
		UPDATE manager.runtime_slots
		SET claim_lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE slot_id = $1
	`, claimed.ID)
	require.NoError(t, err)
	fenced, err := store.FenceRuntimeSlotForReconcile(ctx, request)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)
	require.Equal(t, claimed.Revision+1, fenced.Revision)
	require.False(t, fenced.HeartbeatExpiresAt.After(fenced.AuthorityObservedAt))

	_, err = store.HeartbeatRuntimeSlot(ctx, &HeartbeatRuntimeSlotRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID, TTL: time.Minute,
	})
	require.ErrorIs(t, err, ErrRuntimeSlotInvalid)
	retried, err := store.FenceRuntimeSlotForReconcile(ctx, request)
	require.NoError(t, err)
	require.Equal(t, fenced.Revision, retried.Revision)

	candidates, err := store.ListRuntimeSlotsForReconcile(ctx, 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, fenced.ID, candidates[0].ID)
}

func TestRuntimeSlotReconcileFenceWaitsForConsumedWriterMaturityIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-reconcile-writer")
	registration := runtimeSlotTestRegistration("slot-reconcile-writer", "allocation-reconcile-writer")
	_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x7c}, 32)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
		HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	acquire := &AcquireRuntimeSlotRequest{
		OperationID: "operation-reconcile-writer", ClaimID: "claim-reconcile-writer",
		SandboxID: "sandbox-reconcile-writer", FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: registration.CompatibilityDigest, RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
		Resources: runtimeSlotTestResources(),
	}
	claimed, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	binding := bytes.Repeat([]byte{0x7d}, 32)
	issue := rootFSWriterGrantTestIssueRequest(
		"sandbox-reconcile-writer", "grant-reconcile-writer", acquire.ClaimID, claimed.ID, binding,
	)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = generation.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.BindRuntimeSlotWriterGrant(ctx, &BindRuntimeSlotWriterGrantRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: acquire.ClaimID, GrantID: issued.Grant.ID,
	})
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issued.Grant.ID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding,
		ConsumerNodeUID: registration.NodeUID, ConsumerAgentUID: "ctld-reconcile-writer", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.runtime_slots
		SET claim_lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE slot_id = $1
	`, claimed.ID)
	require.NoError(t, err)

	current, err := store.GetRuntimeSlot(ctx, claimed.ID)
	require.NoError(t, err)
	request := &FenceRuntimeSlotForReconcileRequest{SlotID: claimed.ID, ExpectedRevision: current.Revision}
	_, err = store.FenceRuntimeSlotForReconcile(ctx, request)
	require.ErrorIs(t, err, ErrRuntimeSlotNotDue)
	stillClaimed, err := store.GetRuntimeSlot(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateClaiming, stillClaimed.State)
	require.Equal(t, current.Revision, stillClaimed.Revision)

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET lease_expires_at = NOW() - ($2::bigint * INTERVAL '1 millisecond')
		WHERE grant_id = $1
	`, issued.Grant.ID, RootFSWriterCrashAbandonGrace.Milliseconds()+1000)
	require.NoError(t, err)
	fenced, err := store.FenceRuntimeSlotForReconcile(ctx, request)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)
}

func TestRuntimeSlotPrelaunchAbortRetainsClaimBindingIntegration(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-abort")
	_, unrelatedGeneration := runtimeSlotTestGeneration(t, store, "sandbox-unrelated")
	registration := runtimeSlotTestRegistration("slot-abort", "allocation-abort")
	_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: bytes.Repeat([]byte{0x81}, 32),
		NetworkReadyDigest: bytes.Repeat([]byte{0x82}, 32),
		StorageReadyDigest: bytes.Repeat([]byte{0x83}, 32),
		HeartbeatTTL:       time.Minute,
	})
	require.NoError(t, err)
	acquire := &AcquireRuntimeSlotRequest{
		OperationID: "operation-abort", ClaimID: "claim-abort", SandboxID: "sandbox-abort",
		FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest:       registration.CompatibilityDigest,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: 20 * time.Second,
		Resources: runtimeSlotTestResources(),
	}
	invalidSource := *acquire
	invalidSource.OperationID = "operation-invalid-source"
	invalidSource.ClaimID = "claim-invalid-source"
	invalidSource.SourceGenerationID = unrelatedGeneration.ID
	_, err = store.AcquireRuntimeSlot(ctx, &invalidSource)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	claimed, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x84}, 32)
	terminal, err := store.FinalizeRuntimeSlot(ctx, &FinalizeRuntimeSlotRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		Reason: "prelaunch_abort", ProofDigest: proof,
		ResourceLeaseID:     claimed.ResourceLease.LeaseID,
		ResourceLeaseDigest: claimed.ResourceLeaseDigest, ResourceCgroupAbsent: true,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	require.Equal(t, acquire.OperationID, terminal.ClaimOperationID)
	require.Equal(t, acquire.ClaimID, terminal.ClaimID)
	require.Equal(t, acquire.FilesystemID, terminal.FilesystemID)
	require.Equal(t, 20*time.Second, terminal.ClaimTTL)

	retried, err := store.AcquireRuntimeSlot(ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, terminal.ID, retried.ID)
	require.Equal(t, RuntimeSlotStateTerminal, retried.State)
}

func runtimeSlotTestGeneration(
	t *testing.T,
	store *PGSandboxStore,
	sandboxID string,
) (*RootFSFilesystem, *RootFSGeneration) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(sandboxID, "team-slot")))
	artifactRequest := readyRootFSBaseArtifactTestRequest()
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, artifactRequest)
	require.NoError(t, err)
	filesystem, generation, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: sandboxID, TeamID: "team-slot",
		SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest,
		BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	return filesystem, generation
}
