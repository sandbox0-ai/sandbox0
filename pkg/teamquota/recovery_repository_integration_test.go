package teamquota

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestListRecoveryAllocationsStableKeysetAndFilters(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	runtime := RuntimeRef{
		Namespace:  "sandbox",
		Name:       "pod-active",
		UID:        "uid-active",
		Generation: 7,
	}
	activeOwner := Owner{
		TeamID:    "team-a",
		Kind:      "sandbox",
		ID:        "sandbox-active",
		ClusterID: "cluster-a",
	}
	activeCommitted := Values{
		KeySandboxIdentityCount: 1,
		KeySandboxRuntimeCount:  1,
		KeySandboxMemoryBytes:   64,
	}
	if err := repo.ReconcileTarget(ctx, activeOwner, activeCommitted, runtime); err != nil {
		t.Fatalf("reconcile active owner: %v", err)
	}
	release := Operation{ID: "delete-active", Kind: "delete", Generation: 7}
	if _, err := repo.BeginRelease(ctx, ReleaseRequest{
		Owner:     activeOwner,
		Operation: release,
		Target: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  0,
			KeySandboxMemoryBytes:   0,
		},
		Runtime: runtime,
	}); err != nil {
		t.Fatalf("begin active owner release: %v", err)
	}

	fixtures := []struct {
		owner  Owner
		target Values
	}{
		{
			owner: Owner{
				TeamID:    "team-a",
				Kind:      "warm_pool",
				ID:        "template-a",
				ClusterID: "cluster-a",
			},
			target: Values{KeySandboxRuntimeCount: 1},
		},
		{
			owner: Owner{
				TeamID:    "team-b",
				Kind:      "sandbox",
				ID:        "sandbox-b",
				ClusterID: "cluster-a",
			},
			target: Values{KeySandboxIdentityCount: 1},
		},
		{
			owner: Owner{
				TeamID:    "team-a",
				Kind:      "sandbox",
				ID:        "sandbox-other-cluster",
				ClusterID: "cluster-b",
			},
			target: Values{KeySandboxIdentityCount: 1},
		},
		{
			owner: Owner{
				TeamID:    "team-a",
				Kind:      "sandbox",
				ID:        "sandbox-released",
				ClusterID: "cluster-a",
			},
			target: Values{KeySandboxIdentityCount: 0},
		},
	}
	for _, fixture := range fixtures {
		if err := repo.ReconcileTarget(ctx, fixture.owner, fixture.target, RuntimeRef{}); err != nil {
			t.Fatalf("reconcile %s/%s: %v", fixture.owner.Kind, fixture.owner.ID, err)
		}
	}

	filtered, err := repo.ListRecoveryAllocations(ctx, RecoveryAllocationFilter{
		ClusterID: "cluster-a",
		TeamID:    "team-a",
		OwnerKind: "sandbox",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListRecoveryAllocations(filtered) error = %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("filtered allocations = %+v, want one", filtered)
	}
	got := filtered[0]
	if got.Owner != activeOwner {
		t.Fatalf("filtered owner = %+v, want %+v", got.Owner, activeOwner)
	}
	if got.State != "releasing" || got.Operation == nil || *got.Operation != release {
		t.Fatalf("active operation = state %q operation %+v", got.State, got.Operation)
	}
	if got.OperationBaseState != "active" {
		t.Fatalf("operation base state = %q, want active", got.OperationBaseState)
	}
	if got.Runtime != runtime {
		t.Fatalf("runtime = %+v, want %+v", got.Runtime, runtime)
	}
	assertRecoveryValuesEqual(t, got.Committed, activeCommitted)
	assertRecoveryValuesEqual(t, got.Pending, Values{
		KeySandboxIdentityCount: 1,
		KeySandboxRuntimeCount:  0,
		KeySandboxMemoryBytes:   0,
	})
	if got.ReconcileAfter == nil {
		t.Fatal("reconcile_after is nil for active release")
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("updated_at is zero")
	}
	if got.Revision <= 0 {
		t.Fatalf("allocation revision = %d, want positive", got.Revision)
	}

	due, err := repo.ListRecoveryAllocations(ctx, RecoveryAllocationFilter{
		ClusterID: "cluster-a",
		TeamID:    "team-a",
		OwnerKind: "sandbox",
		OnlyDue:   true,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListRecoveryAllocations(not yet due) error = %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("not-yet-due allocations = %+v, want none", due)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE quota.allocations
		SET reconcile_after = NOW() - INTERVAL '1 second'
		WHERE allocation_id = $1
	`, got.AllocationID); err != nil {
		t.Fatalf("make recovery allocation due: %v", err)
	}
	due, err = repo.ListRecoveryAllocations(ctx, RecoveryAllocationFilter{
		ClusterID: "cluster-a",
		TeamID:    "team-a",
		OwnerKind: "sandbox",
		OnlyDue:   true,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListRecoveryAllocations(due) error = %v", err)
	}
	if len(due) != 1 || due[0].AllocationID != got.AllocationID {
		t.Fatalf("due allocations = %+v, want allocation %s", due, got.AllocationID)
	}

	byOwner, err := repo.GetRecoveryAllocation(ctx, Owner{
		TeamID:    activeOwner.TeamID,
		Kind:      activeOwner.Kind,
		ID:        activeOwner.ID,
		ClusterID: "cluster-b",
	})
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(cross-cluster) error = %v", err)
	}
	if byOwner == nil || byOwner.Owner != activeOwner {
		t.Fatalf("owner-specific recovery allocation = %+v, want owner %+v", byOwner, activeOwner)
	}
	if byOwner.Operation == nil || *byOwner.Operation != release {
		t.Fatalf("owner-specific recovery operation = %+v, want %+v", byOwner.Operation, release)
	}
	missing, err := repo.GetRecoveryAllocation(ctx, Owner{
		TeamID: "team-a",
		Kind:   "sandbox",
		ID:     "missing",
	})
	if err != nil || missing != nil {
		t.Fatalf("GetRecoveryAllocation(missing) = %+v, %v, want nil, nil", missing, err)
	}

	firstPage, err := repo.ListRecoveryAllocations(ctx, RecoveryAllocationFilter{
		ClusterID: "cluster-a",
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListRecoveryAllocations(first page) error = %v", err)
	}
	if len(firstPage) != 2 {
		t.Fatalf("first page length = %d, want 2", len(firstPage))
	}
	secondPage, err := repo.ListRecoveryAllocations(ctx, RecoveryAllocationFilter{
		ClusterID:         "cluster-a",
		AfterAllocationID: firstPage[len(firstPage)-1].AllocationID,
		Limit:             2,
	})
	if err != nil {
		t.Fatalf("ListRecoveryAllocations(second page) error = %v", err)
	}
	all := append(append([]RecoveryAllocation(nil), firstPage...), secondPage...)
	if len(all) != 3 {
		t.Fatalf("cluster-a allocations = %d, want 3; %+v", len(all), all)
	}
	if !sort.SliceIsSorted(all, func(i, j int) bool {
		return all[i].AllocationID < all[j].AllocationID
	}) {
		t.Fatalf("allocation keyset order is unstable: %+v", all)
	}
	for _, allocation := range all {
		if allocation.Owner.ID == "sandbox-released" {
			t.Fatal("fully released allocation was returned")
		}
	}

	warmPool, err := repo.ListRecoveryAllocations(ctx, RecoveryAllocationFilter{
		ClusterID: "cluster-a",
		OwnerKind: "warm_pool",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListRecoveryAllocations(warm pool) error = %v", err)
	}
	if len(warmPool) != 1 || warmPool[0].Owner.ID != "template-a" {
		t.Fatalf("warm-pool filter = %+v", warmPool)
	}
}

func TestReconcileTargetIfRevisionFencesStaleInventory(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-revision",
		Kind:      "sandbox",
		ID:        "sandbox-a",
		ClusterID: "cluster-a",
	}
	if err := repo.ReconcileTarget(
		ctx,
		owner,
		Values{KeySandboxRuntimeCount: 1},
		RuntimeRef{},
	); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	initial, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(initial) error = %v", err)
	}
	if initial == nil || initial.Revision <= 0 {
		t.Fatalf("initial allocation = %+v, want positive revision", initial)
	}

	operation := Operation{ID: "resize-a", Kind: "resize", Generation: 1}
	if _, err := repo.ReserveTarget(ctx, ReserveRequest{
		Owner:     owner,
		Operation: operation,
		Target:    Values{KeySandboxRuntimeCount: 2},
	}); err != nil {
		t.Fatalf("ReserveTarget() error = %v", err)
	}
	if err := repo.Commit(ctx, Ref(owner, operation)); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	current, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(current) error = %v", err)
	}
	if current == nil || current.Revision <= initial.Revision {
		t.Fatalf("current revision = %+v, want greater than %d", current, initial.Revision)
	}

	applied, err := repo.ReconcileTargetIfRevision(
		ctx,
		owner,
		Values{KeySandboxRuntimeCount: 0},
		RuntimeRef{},
		initial.Revision,
	)
	if err != nil {
		t.Fatalf("ReconcileTargetIfRevision(stale) error = %v", err)
	}
	if applied {
		t.Fatal("stale inventory observation was applied")
	}
	unchanged, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(unchanged) error = %v", err)
	}
	assertRecoveryValuesEqual(t, unchanged.Committed, Values{KeySandboxRuntimeCount: 2})

	applied, err = repo.ReconcileTargetIfRevision(
		ctx,
		owner,
		Values{KeySandboxRuntimeCount: 1},
		RuntimeRef{},
		current.Revision,
	)
	if err != nil {
		t.Fatalf("ReconcileTargetIfRevision(current) error = %v", err)
	}
	if !applied {
		t.Fatal("current inventory observation was not applied")
	}
	reconciled, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(reconciled) error = %v", err)
	}
	if reconciled.Revision <= current.Revision {
		t.Fatalf("reconciled revision = %d, want greater than %d", reconciled.Revision, current.Revision)
	}
	assertRecoveryValuesEqual(t, reconciled.Committed, Values{KeySandboxRuntimeCount: 1})

	absentOwner := Owner{
		TeamID:    owner.TeamID,
		Kind:      "sandbox",
		ID:        "sandbox-b",
		ClusterID: owner.ClusterID,
	}
	applied, err = repo.ReconcileTargetIfRevision(
		ctx,
		absentOwner,
		Values{KeySandboxRuntimeCount: 1},
		RuntimeRef{},
		0,
	)
	if err != nil {
		t.Fatalf("ReconcileTargetIfRevision(absent) error = %v", err)
	}
	if !applied {
		t.Fatal("absent allocation observation was not applied")
	}
	applied, err = repo.ReconcileTargetIfRevision(
		ctx,
		absentOwner,
		Values{KeySandboxRuntimeCount: 0},
		RuntimeRef{},
		0,
	)
	if err != nil {
		t.Fatalf("ReconcileTargetIfRevision(stale absent) error = %v", err)
	}
	if applied {
		t.Fatal("stale absent observation overwrote a newly created allocation")
	}
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestListRecoveryTransfersReturnsPreparedOnlyInStableOrder(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}

	preparedZ := prepareRecoveryTransfer(t, repo, "cluster-a", "source-z", "sandbox-z", "op-z")
	preparedA := prepareRecoveryTransfer(t, repo, "cluster-a", "source-a", "sandbox-a", "op-a")
	committed := prepareRecoveryTransfer(t, repo, "cluster-a", "source-m", "sandbox-m", "op-m")
	if err := repo.CommitTransfer(ctx, Ref(committed.Destination, committed.Operation)); err != nil {
		t.Fatalf("commit transfer: %v", err)
	}
	otherCluster := prepareRecoveryTransfer(t, repo, "cluster-b", "source-b", "sandbox-b", "op-b")

	first, err := repo.ListRecoveryTransfers(ctx, "cluster-a", 0, 1)
	if err != nil {
		t.Fatalf("ListRecoveryTransfers(first) error = %v", err)
	}
	if len(first) != 1 || first[0].Operation.ID != preparedZ.Operation.ID {
		t.Fatalf("first transfer = %+v, want oldest %s", first, preparedZ.Operation.ID)
	}
	all, err := repo.ListRecoveryTransfers(ctx, "cluster-a", 0, 10)
	if err != nil {
		t.Fatalf("ListRecoveryTransfers(all) error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("prepared transfers = %+v, want two", all)
	}
	if all[0].Operation.ID != preparedZ.Operation.ID ||
		all[1].Operation.ID != preparedA.Operation.ID {
		t.Fatalf("prepared transfer order = %q, %q", all[0].Operation.ID, all[1].Operation.ID)
	}
	if all[1].Source != preparedA.Source || all[1].Destination != preparedA.Destination {
		t.Fatalf("prepared transfer owners = (%+v, %+v)", all[0].Source, all[0].Destination)
	}
	if all[1].Runtime != preparedA.Runtime {
		t.Fatalf("prepared transfer runtime = %+v, want %+v", all[1].Runtime, preparedA.Runtime)
	}
	assertRecoveryValuesEqual(t, all[1].SourceDecrease, preparedA.SourceDecrease)
	assertRecoveryValuesEqual(t, all[1].DestinationTarget, preparedA.DestinationTarget)
	if all[0].CreatedAt.IsZero() {
		t.Fatal("prepared transfer created_at is zero")
	}
	fresh, err := repo.ListRecoveryTransfers(ctx, "cluster-a", time.Hour, 10)
	if err != nil {
		t.Fatalf("ListRecoveryTransfers(fresh) error = %v", err)
	}
	if len(fresh) != 0 {
		t.Fatalf("fresh transfers = %+v, want none before database stale threshold", fresh)
	}

	clusterB, err := repo.ListRecoveryTransfers(ctx, "cluster-b", 0, 10)
	if err != nil {
		t.Fatalf("ListRecoveryTransfers(cluster-b) error = %v", err)
	}
	if len(clusterB) != 1 || clusterB[0].Operation.ID != otherCluster.Operation.ID {
		t.Fatalf("cluster-b transfers = %+v", clusterB)
	}
}

func prepareRecoveryTransfer(
	t *testing.T,
	repo *Repository,
	clusterID string,
	sourceID string,
	destinationID string,
	operationID string,
) TransferRequest {
	t.Helper()
	ctx := context.Background()
	source := Owner{
		TeamID:    "team-transfer",
		Kind:      "warm_pool",
		ID:        sourceID,
		ClusterID: clusterID,
	}
	if err := repo.ReconcileTarget(
		ctx,
		source,
		Values{KeySandboxRuntimeCount: 1},
		RuntimeRef{},
	); err != nil {
		t.Fatalf("reconcile transfer source %s: %v", sourceID, err)
	}
	request := TransferRequest{
		Source: source,
		Destination: Owner{
			TeamID:    source.TeamID,
			Kind:      "sandbox",
			ID:        destinationID,
			ClusterID: clusterID,
		},
		Operation: Operation{
			ID:         operationID,
			Kind:       "claim",
			Generation: 1,
		},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
		},
		DestinationTarget: Values{
			KeySandboxRuntimeCount: 1,
		},
		Runtime: RuntimeRef{
			Namespace:  "sandbox",
			Name:       "pod-" + destinationID,
			UID:        "uid-" + destinationID,
			Generation: 1,
		},
	}
	if _, err := repo.PrepareTransfer(ctx, request); err != nil {
		t.Fatalf("prepare transfer %s: %v", operationID, err)
	}
	return request
}

func assertRecoveryValuesEqual(t *testing.T, got Values, want Values) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values = %+v, want %+v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("values[%s] = %d, want %d; all values %+v", key, got[key], value, got)
		}
	}
}
