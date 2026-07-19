package teamquota

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestEffectivePolicyRevisionAndFallback(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	defaults := completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 10},
		Policy{Key: KeyAPIRequests, Kind: KindRate, Tokens: 100, IntervalMillis: 1000, Burst: 200},
	)
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, defaults); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	assertTeamQuotaStatusSource(
		t,
		repo,
		ctx,
		"team-1",
		KeySandboxRuntimeCount,
		PolicySourceDefault,
	)
	initial, err := repo.EffectivePolicy(ctx, "team-1", KeySandboxRuntimeCount)
	if err != nil || initial == nil || initial.Limit != 10 || initial.Revision <= 0 {
		t.Fatalf("EffectivePolicy() = (%+v, %v)", initial, err)
	}
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, defaults); err != nil {
		t.Fatalf("repeat ReplaceDefaultPolicies() error = %v", err)
	}
	unchanged, err := repo.EffectivePolicy(ctx, "team-1", KeySandboxRuntimeCount)
	if err != nil || unchanged == nil || unchanged.Revision != initial.Revision {
		t.Fatalf("unchanged policy = (%+v, %v), initial revision %d", unchanged, err, initial.Revision)
	}
	if err := repo.UnsafePutTeamPolicyForTest(ctx, "team-1", Policy{
		Key:   KeySandboxRuntimeCount,
		Kind:  KindCapacity,
		Limit: 3,
	}); err != nil {
		t.Fatalf("PutTeamPolicy() error = %v", err)
	}
	override, err := repo.EffectivePolicy(ctx, "team-1", KeySandboxRuntimeCount)
	if err != nil || override == nil || override.Limit != 3 || override.Revision == initial.Revision {
		t.Fatalf("override policy = (%+v, %v)", override, err)
	}
	assertTeamQuotaStatusSource(
		t,
		repo,
		ctx,
		"team-1",
		KeySandboxRuntimeCount,
		PolicySourceOverride,
	)
	if err := repo.UnsafeDeleteTeamPolicyForTest(ctx, "team-1", KeySandboxRuntimeCount); err != nil {
		t.Fatalf("DeleteTeamPolicy() error = %v", err)
	}
	fallback, err := repo.EffectivePolicy(ctx, "team-1", KeySandboxRuntimeCount)
	if err != nil || fallback == nil || fallback.Limit != 10 || fallback.Revision != initial.Revision {
		t.Fatalf("fallback policy = (%+v, %v)", fallback, err)
	}
	statuses, err := repo.ListStatus(ctx, "team-1")
	if err != nil || len(statuses) != len(Keys()) {
		t.Fatalf("ListStatus() = (%+v, %v)", statuses, err)
	}
	assertTeamQuotaStatusSource(
		t,
		repo,
		ctx,
		"team-1",
		KeySandboxRuntimeCount,
		PolicySourceDefault,
	)
}

func assertTeamQuotaStatusSource(
	t *testing.T,
	repo *Repository,
	ctx context.Context,
	teamID string,
	key Key,
	want PolicySource,
) {
	t.Helper()
	statuses, err := repo.ListStatus(ctx, teamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	for _, status := range statuses {
		if status.Key == key {
			if status.Source != want {
				t.Fatalf("status source for %q = %q, want %q", key, status.Source, want)
			}
			return
		}
	}
	t.Fatalf("ListStatus() missing key %q", key)
}

func TestReserveTargetBundleRollbackAndIdempotency(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 2},
		Policy{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: 1024},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}

	rejected := ReserveRequest{
		Owner:     Owner{TeamID: "team-1", Kind: "sandbox", ID: "rejected"},
		Operation: Operation{ID: "claim-rejected", Kind: "claim", Generation: 1},
		Target: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  2048,
		},
	}
	if _, err := repo.ReserveTarget(ctx, rejected); !IsExceeded(err) {
		t.Fatalf("ReserveTarget() error = %v, want ExceededError", err)
	}
	assertTeamQuotaCount(t, repo.pool, `SELECT COUNT(*) FROM quota.allocations`, 0)

	request := ReserveRequest{
		Owner:     Owner{TeamID: "team-1", Kind: "sandbox", ID: "sandbox-1"},
		Operation: Operation{ID: "claim-1", Kind: "claim", Generation: 1},
		Target: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
	}
	first, err := repo.ReserveTarget(ctx, request)
	if err != nil {
		t.Fatalf("ReserveTarget() error = %v", err)
	}
	second, err := repo.ReserveTarget(ctx, request)
	if err != nil {
		t.Fatalf("idempotent ReserveTarget() error = %v", err)
	}
	if first.AllocationID != second.AllocationID {
		t.Fatalf("allocation IDs differ: %q != %q", first.AllocationID, second.AllocationID)
	}
	if err := repo.Commit(ctx, Ref(request.Owner, request.Operation)); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := repo.Commit(ctx, Ref(request.Owner, request.Operation)); err != nil {
		t.Fatalf("idempotent Commit() error = %v", err)
	}
	retried, err := repo.ReserveTarget(ctx, request)
	if err != nil || retried.Operation.ID != request.Operation.ID {
		t.Fatalf("post-commit ReserveTarget() = (%+v, %v)", retried, err)
	}
	if err := repo.ValidateUsageInvariant(ctx, "team-1"); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestAllocationOperationHistoryRejectsChangedAndStaleReplays(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 4},
		Policy{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: 4096},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-operation-history",
		Kind:      "sandbox",
		ID:        "sandbox-1",
		ClusterID: "cluster-1",
	}
	first := ReserveRequest{
		Owner: owner,
		Operation: Operation{
			ID:         "resize-1",
			Kind:       "resize",
			Generation: 2,
		},
		Target: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
	}
	if _, err := repo.ReserveTarget(ctx, first); err != nil {
		t.Fatalf("ReserveTarget(first) error = %v", err)
	}

	changedTarget := first
	changedTarget.Target = first.Target.Clone()
	changedTarget.Target[KeySandboxMemoryBytes] = 1024
	if _, err := repo.ReserveTarget(ctx, changedTarget); err == nil {
		t.Fatal("ReserveTarget(changed target) error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ReserveTarget(changed target) error = %v, want OperationConflictError", err)
		}
	}
	changedKind := first
	changedKind.Operation.Kind = "claim"
	if _, err := repo.ReserveTarget(ctx, changedKind); err == nil {
		t.Fatal("ReserveTarget(changed kind) error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ReserveTarget(changed kind) error = %v, want OperationConflictError", err)
		}
	}
	changedGeneration := first
	changedGeneration.Operation.Generation = 3
	if _, err := repo.ReserveTarget(ctx, changedGeneration); err == nil {
		t.Fatal("ReserveTarget(changed generation) error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ReserveTarget(changed generation) error = %v, want OperationConflictError", err)
		}
	}

	if err := repo.Commit(ctx, Ref(owner, first.Operation)); err != nil {
		t.Fatalf("Commit(first) error = %v", err)
	}
	if _, err := repo.ReserveTarget(ctx, first); err != nil {
		t.Fatalf("ReserveTarget(immediate committed replay) error = %v", err)
	}
	var historyState, historyFingerprint string
	if err := repo.pool.QueryRow(ctx, `
		SELECT state, request_fingerprint
		FROM quota.allocation_operations
		WHERE allocation_id = $1 AND operation_id = $2
	`, sourceAllocationID(t, repo.pool, owner), first.Operation.ID).Scan(
		&historyState,
		&historyFingerprint,
	); err != nil {
		t.Fatalf("query allocation operation history: %v", err)
	}
	if historyState != "committed" || historyFingerprint == "" {
		t.Fatalf("operation history = state %q fingerprint %q", historyState, historyFingerprint)
	}

	second := ReserveRequest{
		Owner: owner,
		Operation: Operation{
			ID:         "resize-2",
			Kind:       "resize",
			Generation: 2,
		},
		Target: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  768,
		},
	}
	if _, err := repo.ReserveTarget(ctx, second); err != nil {
		t.Fatalf("ReserveTarget(second) error = %v", err)
	}
	if err := repo.Commit(ctx, Ref(owner, second.Operation)); err != nil {
		t.Fatalf("Commit(second) error = %v", err)
	}
	if _, err := repo.ReserveTarget(ctx, first); err == nil {
		t.Fatal("ReserveTarget(stale committed replay) error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ReserveTarget(stale committed replay) error = %v, want OperationConflictError", err)
		}
	}
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestAllocationOperationGenerationFenceRejectsOlderMutation(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 4},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-operation-fence",
		Kind:      "sandbox",
		ID:        "sandbox-1",
		ClusterID: "cluster-1",
	}
	runtime := RuntimeRef{
		Namespace:  "sandboxes",
		Name:       "sandbox-1",
		UID:        "uid-5",
		Generation: 5,
	}
	if err := repo.ReconcileTarget(
		ctx,
		owner,
		Values{KeySandboxRuntimeCount: 1},
		runtime,
	); err != nil {
		t.Fatalf("ReconcileTarget() error = %v", err)
	}
	stale := ReserveRequest{
		Owner: owner,
		Operation: Operation{
			ID:         "resize-stale",
			Kind:       "resize",
			Generation: 4,
		},
		Target: Values{KeySandboxRuntimeCount: 2},
	}
	if _, err := repo.ReserveTarget(ctx, stale); err == nil {
		t.Fatal("ReserveTarget(stale generation) error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ReserveTarget(stale generation) error = %v, want OperationConflictError", err)
		}
	}
	current := stale
	current.Operation.ID = "resize-current"
	current.Operation.Generation = runtime.Generation
	if _, err := repo.ReserveTarget(ctx, current); err != nil {
		t.Fatalf("ReserveTarget(current generation) error = %v", err)
	}
	if err := repo.Abort(ctx, Ref(owner, current.Operation), "test"); err != nil {
		t.Fatalf("Abort(current generation) error = %v", err)
	}
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestBeginReleaseRequiresAndPreservesCommittedRuntime(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxIdentityCount, Kind: KindCapacity, Limit: 2},
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 2},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-release-runtime",
		Kind:      "sandbox",
		ID:        "sandbox-1",
		ClusterID: "cluster-1",
	}
	runtime := RuntimeRef{
		Namespace:  "sandboxes",
		Name:       "sandbox-1",
		UID:        "uid-current",
		Generation: 7,
	}
	if err := repo.ReconcileTarget(ctx, owner, Values{
		KeySandboxIdentityCount: 1,
		KeySandboxRuntimeCount:  1,
	}, runtime); err != nil {
		t.Fatalf("ReconcileTarget() error = %v", err)
	}
	release := ReleaseRequest{
		Owner: owner,
		Operation: Operation{
			ID:         "delete-current",
			Kind:       "delete",
			Generation: runtime.Generation,
		},
		Target: Values{
			KeySandboxIdentityCount: 0,
			KeySandboxRuntimeCount:  0,
		},
		Runtime: runtime,
	}
	staleUID := release
	staleUID.Operation.ID = "delete-stale-uid"
	staleUID.Runtime.UID = "uid-stale"
	if _, err := repo.BeginRelease(ctx, staleUID); err == nil {
		t.Fatal("BeginRelease(stale UID) error = nil")
	}
	staleGeneration := release
	staleGeneration.Operation.ID = "delete-stale-generation"
	staleGeneration.Operation.Generation--
	staleGeneration.Runtime.Generation--
	if _, err := repo.BeginRelease(ctx, staleGeneration); err == nil {
		t.Fatal("BeginRelease(stale generation) error = nil")
	}
	allocation, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(after stale release) error = %v", err)
	}
	if allocation == nil ||
		allocation.Operation != nil ||
		allocation.State != "active" ||
		allocation.Runtime != runtime {
		t.Fatalf("allocation after stale release = %+v", allocation)
	}

	reservation, err := repo.BeginRelease(ctx, release)
	if err != nil {
		t.Fatalf("BeginRelease(exact runtime) error = %v", err)
	}
	pending, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(pending release) error = %v", err)
	}
	if pending == nil || pending.Runtime != runtime {
		t.Fatalf("pending release runtime = %+v, want %+v", pending, runtime)
	}
	if err := repo.ConfirmRelease(
		ctx,
		Ref(reservation.Owner, reservation.Operation),
		runtime,
	); err != nil {
		t.Fatalf("ConfirmRelease() error = %v", err)
	}
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
	statuses, err := repo.ListStatus(ctx, owner.TeamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 0 || got.Reserved != 0 {
		t.Fatalf("runtime status after release = %+v", got)
	}
}

func TestReserveDeltaAbortAndConfirmedRelease(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeyVolumeStorageBytes, Kind: KindCapacity, Limit: 100},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{TeamID: "team-1", Kind: "volume", ID: "volume-1"}
	create := Operation{ID: "create", Kind: "create"}
	if _, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner: owner, Operation: create, Delta: Values{KeyVolumeStorageBytes: 40},
	}); err != nil {
		t.Fatalf("ReserveDelta() error = %v", err)
	}
	if err := repo.Commit(ctx, Ref(owner, create)); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	grow := Operation{ID: "grow", Kind: "write"}
	if _, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner: owner, Operation: grow, Delta: Values{KeyVolumeStorageBytes: 20},
	}); err != nil {
		t.Fatalf("second ReserveDelta() error = %v", err)
	}
	if err := repo.Abort(ctx, Ref(owner, grow), "write failed"); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := repo.Abort(ctx, Ref(owner, grow), "write failed"); err != nil {
		t.Fatalf("idempotent Abort() error = %v", err)
	}

	release := Operation{ID: "delete", Kind: "delete", Generation: 1}
	runtime := RuntimeRef{Namespace: "storage", Name: "volume-1", UID: "uid-1", Generation: 1}
	if _, err := repo.BeginRelease(ctx, ReleaseRequest{
		Owner: owner, Operation: release,
		Target:  Values{KeyVolumeStorageBytes: 0},
		Runtime: runtime,
	}); err != nil {
		t.Fatalf("BeginRelease() error = %v", err)
	}
	if err := repo.ConfirmRelease(ctx, Ref(owner, release), RuntimeRef{
		Namespace: "storage", Name: "volume-1", UID: "stale", Generation: 1,
	}); err == nil {
		t.Fatal("ConfirmRelease() stale runtime error = nil")
	}
	if err := repo.ConfirmRelease(ctx, Ref(owner, release), runtime); err != nil {
		t.Fatalf("ConfirmRelease() error = %v", err)
	}
	if err := repo.ConfirmRelease(ctx, Ref(owner, release), runtime); err != nil {
		t.Fatalf("idempotent ConfirmRelease() error = %v", err)
	}
	if err := repo.ValidateUsageInvariant(ctx, "team-1"); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestReconcileTargetAdoptsUsageAboveLimit(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 1},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	if err := repo.ReconcileTarget(ctx,
		Owner{TeamID: "team-1", Kind: "warm_pool", ID: "template-1"},
		Values{KeySandboxRuntimeCount: 2},
		RuntimeRef{},
	); err != nil {
		t.Fatalf("ReconcileTarget() error = %v", err)
	}
	if _, err := repo.ReserveTarget(ctx, ReserveRequest{
		Owner:     Owner{TeamID: "team-1", Kind: "sandbox", ID: "sandbox-1"},
		Operation: Operation{ID: "claim", Kind: "claim"},
		Target:    Values{KeySandboxRuntimeCount: 1},
	}); !IsExceeded(err) {
		t.Fatalf("ReserveTarget() error = %v, want ExceededError", err)
	}
	if err := repo.ValidateUsageInvariant(ctx, "team-1"); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestReconcileTargetRequiresEffectiveCapacityPolicy(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		repo := newTeamQuotaTestRepository(t)
		err := repo.ReconcileTarget(
			context.Background(),
			Owner{TeamID: "team-missing-policy", Kind: "sandbox", ID: "sandbox-1"},
			Values{KeySandboxRuntimeCount: 1},
			RuntimeRef{},
		)
		if !IsUnavailable(err) {
			t.Fatalf("ReconcileTarget() error = %v, want UnavailableError", err)
		}
		assertTeamQuotaCount(t, repo.pool, `SELECT COUNT(*) FROM quota.allocations`, 0)
	})

	t.Run("wrong kind", func(t *testing.T) {
		repo := newTeamQuotaTestRepository(t)
		ctx := context.Background()
		if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
			t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
		}
		if _, err := repo.pool.Exec(ctx, `
			UPDATE quota.region_default_policies
			SET kind = 'rate',
				limit_value = NULL,
				rate_tokens = 1,
				rate_interval_ms = 1000,
				rate_burst = 1
			WHERE quota_key = $1
		`, string(KeySandboxRuntimeCount)); err != nil {
			t.Fatalf("corrupt effective policy kind: %v", err)
		}
		err := repo.ReconcileTarget(
			ctx,
			Owner{TeamID: "team-wrong-policy", Kind: "sandbox", ID: "sandbox-1"},
			Values{KeySandboxRuntimeCount: 1},
			RuntimeRef{},
		)
		if !IsUnavailable(err) {
			t.Fatalf("ReconcileTarget() error = %v, want UnavailableError", err)
		}
		assertTeamQuotaCount(t, repo.pool, `SELECT COUNT(*) FROM quota.allocations`, 0)
	})
}

func TestEmptyOwnerClusterDoesNotClearExistingAllocationCluster(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-cluster",
		Kind:      "sandbox",
		ID:        "sandbox-1",
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
	ownerWithoutCluster := owner
	ownerWithoutCluster.ClusterID = ""
	reserve := Operation{ID: "resize-1", Kind: "resize"}
	if _, err := repo.ReserveTarget(ctx, ReserveRequest{
		Owner:     ownerWithoutCluster,
		Operation: reserve,
		Target:    Values{KeySandboxRuntimeCount: 2},
	}); err != nil {
		t.Fatalf("ReserveTarget(empty cluster) error = %v", err)
	}
	assertAllocationCluster(t, repo.pool, owner, "cluster-a")
	if err := repo.Abort(ctx, Ref(ownerWithoutCluster, reserve), "test"); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := repo.ReconcileTarget(
		ctx,
		ownerWithoutCluster,
		Values{KeySandboxRuntimeCount: 1},
		RuntimeRef{},
	); err != nil {
		t.Fatalf("ReconcileTarget(empty cluster) error = %v", err)
	}
	assertAllocationCluster(t, repo.pool, owner, "cluster-a")
}

func TestActiveAllocationRejectsClusterOwnershipRewrite(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-cluster-fence",
		Kind:      "sandbox",
		ID:        "sandbox-1",
		ClusterID: "cluster-a",
	}
	runtime := RuntimeRef{
		Namespace:  "sandbox",
		Name:       "sandbox-1",
		UID:        "pod-a",
		Generation: 1,
	}
	if err := repo.ReconcileTarget(
		ctx,
		owner,
		Values{KeySandboxRuntimeCount: 1},
		runtime,
	); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}

	otherCluster := owner
	otherCluster.ClusterID = "cluster-b"
	_, err := repo.ReserveTarget(ctx, ReserveRequest{
		Owner:     otherCluster,
		Operation: Operation{ID: "resize-b", Kind: "resize", Generation: 2},
		Target:    Values{KeySandboxRuntimeCount: 2},
	})
	var conflict *OperationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("ReserveTarget(cross-cluster) error = %v, want OperationConflictError", err)
	}
	err = repo.ReconcileTarget(
		ctx,
		otherCluster,
		Values{KeySandboxRuntimeCount: 1},
		runtime,
	)
	if !errors.As(err, &conflict) {
		t.Fatalf("ReconcileTarget(cross-cluster) error = %v, want OperationConflictError", err)
	}
	assertAllocationCluster(t, repo.pool, owner, "cluster-a")
}

func TestPausedAllocationAllowsFencedClusterMigration(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	owner := Owner{
		TeamID:    "team-cluster-migration",
		Kind:      "sandbox",
		ID:        "sandbox-1",
		ClusterID: "cluster-a",
	}
	runtime := RuntimeRef{
		Namespace:  "sandbox",
		Name:       "sandbox-1",
		UID:        "pod-a",
		Generation: 1,
	}
	if err := repo.ReconcileTarget(
		ctx,
		owner,
		Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  1,
		},
		runtime,
	); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	pause := Operation{ID: "pause-a", Kind: "pause_runtime", Generation: 1}
	if _, err := repo.BeginRelease(ctx, ReleaseRequest{
		Owner:     owner,
		Operation: pause,
		Target: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  0,
		},
		Runtime: runtime,
	}); err != nil {
		t.Fatalf("BeginRelease(pause) error = %v", err)
	}
	if err := repo.ConfirmRelease(ctx, Ref(owner, pause), runtime); err != nil {
		t.Fatalf("ConfirmRelease(pause) error = %v", err)
	}

	migrated := owner
	migrated.ClusterID = "cluster-b"
	if _, err := repo.ReserveTarget(ctx, ReserveRequest{
		Owner:     migrated,
		Operation: Operation{ID: "resume-b", Kind: "resume", Generation: 2},
		Target: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  1,
		},
	}); err != nil {
		t.Fatalf("ReserveTarget(migrated resume) error = %v", err)
	}
	assertAllocationCluster(t, repo.pool, owner, "cluster-b")
}

func TestTransferTargetAdmitsNetIncreaseAndIsIdempotent(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxIdentityCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: 768},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}

	source := Owner{
		TeamID:    "team-transfer",
		Kind:      "warm_pool",
		ID:        "template-1",
		ClusterID: "cluster-1",
	}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount:  1,
		KeySandboxMemoryBytes:   512,
		KeySandboxCPUMillicores: 100,
	}, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(source) error = %v", err)
	}
	request := TransferRequest{
		Source: source,
		Destination: Owner{
			TeamID:    source.TeamID,
			Kind:      "sandbox",
			ID:        "sandbox-1",
			ClusterID: source.ClusterID,
		},
		Operation: Operation{ID: "hot-claim-1", Kind: "hot_claim", Generation: 1},
		SourceDecrease: Values{
			KeySandboxRuntimeCount:  1,
			KeySandboxMemoryBytes:   512,
			KeySandboxCPUMillicores: 100,
		},
		DestinationTarget: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  1,
			KeySandboxMemoryBytes:   768,
			KeySandboxCPUMillicores: 100,
		},
		Runtime: RuntimeRef{
			Namespace:  "sandboxes",
			Name:       "idle-1",
			UID:        "runtime-uid-1",
			Generation: 3,
		},
	}

	tx, err := repo.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transfer transaction: %v", err)
	}
	first, err := repo.TransferTargetTx(ctx, tx, request)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("TransferTargetTx() error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transfer transaction: %v", err)
	}
	if first.State != "active" ||
		first.Committed[KeySandboxIdentityCount] != 1 ||
		first.Committed[KeySandboxMemoryBytes] != 768 ||
		len(first.Reserved) != 0 {
		t.Fatalf("TransferTargetTx() reservation = %+v", first)
	}

	retried, err := repo.TransferTarget(ctx, request)
	if err != nil {
		t.Fatalf("idempotent TransferTarget() error = %v", err)
	}
	if retried.AllocationID != first.AllocationID {
		t.Fatalf("idempotent allocation IDs differ: %q != %q", retried.AllocationID, first.AllocationID)
	}
	conflicting := request
	conflicting.DestinationTarget = request.DestinationTarget.Clone()
	conflicting.DestinationTarget[KeySandboxMemoryBytes]++
	if _, err := repo.TransferTarget(ctx, conflicting); err == nil {
		t.Fatal("conflicting TransferTarget() error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("conflicting TransferTarget() error = %v, want OperationConflictError", err)
		}
	}

	var sourceRuntime, sourceMemory, destinationRuntime, destinationMemory int64
	if err := repo.pool.QueryRow(ctx, `
		SELECT
			(SELECT committed_value FROM quota.allocation_items
				WHERE allocation_id = $1 AND quota_key = $3),
			(SELECT committed_value FROM quota.allocation_items
				WHERE allocation_id = $1 AND quota_key = $4),
			(SELECT committed_value FROM quota.allocation_items
				WHERE allocation_id = $2 AND quota_key = $3),
			(SELECT committed_value FROM quota.allocation_items
				WHERE allocation_id = $2 AND quota_key = $4)
	`, sourceAllocationID(t, repo.pool, source), first.AllocationID,
		string(KeySandboxRuntimeCount), string(KeySandboxMemoryBytes)).Scan(
		&sourceRuntime,
		&sourceMemory,
		&destinationRuntime,
		&destinationMemory,
	); err != nil {
		t.Fatalf("query transferred allocation values: %v", err)
	}
	if sourceRuntime != 0 || sourceMemory != 0 ||
		destinationRuntime != 1 || destinationMemory != 768 {
		t.Fatalf(
			"transferred values source=(%d,%d) destination=(%d,%d)",
			sourceRuntime,
			sourceMemory,
			destinationRuntime,
			destinationMemory,
		)
	}
	statuses, err := repo.ListStatus(ctx, source.TeamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 1 || got.Reserved != 0 {
		t.Fatalf("runtime status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxMemoryBytes); got.Committed != 768 || got.Reserved != 0 {
		t.Fatalf("memory status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxIdentityCount); got.Committed != 1 || got.Reserved != 0 {
		t.Fatalf("identity status = %+v", got)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*) FROM quota.transfer_operations
		WHERE team_id = 'team-transfer'
	`, 1)
}

func TestPrepareAndCommitTransferSaga(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxIdentityCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: 768},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	source := Owner{
		TeamID:    "team-saga",
		Kind:      "warm_pool",
		ID:        "template-1",
		ClusterID: "cluster-1",
	}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount: 1,
		KeySandboxMemoryBytes:  512,
	}, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(source) error = %v", err)
	}
	request := TransferRequest{
		Source: source,
		Destination: Owner{
			TeamID:    source.TeamID,
			Kind:      "sandbox",
			ID:        "sandbox-1",
			ClusterID: source.ClusterID,
		},
		Operation: Operation{ID: "prepare-claim-1", Kind: "hot_claim", Generation: 2},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
		DestinationTarget: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  1,
			KeySandboxMemoryBytes:   768,
		},
		Runtime: RuntimeRef{
			Namespace:  "sandboxes",
			Name:       "idle-1",
			UID:        "runtime-uid-1",
			Generation: 4,
		},
	}

	prepared, err := repo.PrepareTransfer(ctx, request)
	if err != nil {
		t.Fatalf("PrepareTransfer() error = %v", err)
	}
	if prepared.State != "reserved" ||
		prepared.Committed[KeySandboxIdentityCount] != 0 ||
		prepared.Reserved[KeySandboxIdentityCount] != 1 ||
		prepared.Reserved[KeySandboxMemoryBytes] != 256 ||
		prepared.Reserved[KeySandboxRuntimeCount] != 0 {
		t.Fatalf("prepared reservation = %+v", prepared)
	}
	retried, err := repo.PrepareTransfer(ctx, request)
	if err != nil || retried.AllocationID != prepared.AllocationID {
		t.Fatalf("idempotent PrepareTransfer() = (%+v, %v)", retried, err)
	}
	conflicting := request
	conflicting.DestinationTarget = request.DestinationTarget.Clone()
	conflicting.DestinationTarget[KeySandboxMemoryBytes]++
	if _, err := repo.PrepareTransfer(ctx, conflicting); err == nil {
		t.Fatal("conflicting PrepareTransfer() error = nil")
	} else {
		var conflict *OperationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("conflicting PrepareTransfer() error = %v, want OperationConflictError", err)
		}
	}

	statuses, err := repo.ListStatus(ctx, source.TeamID)
	if err != nil {
		t.Fatalf("ListStatus(prepared) error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 1 || got.Reserved != 0 {
		t.Fatalf("prepared runtime status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxMemoryBytes); got.Committed != 512 || got.Reserved != 256 {
		t.Fatalf("prepared memory status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxIdentityCount); got.Committed != 0 || got.Reserved != 1 {
		t.Fatalf("prepared identity status = %+v", got)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant(prepared) error = %v", err)
	}
	if err := repo.Commit(ctx, Ref(prepared.Owner, prepared.Operation)); err == nil {
		t.Fatal("generic Commit(prepared transfer) error = nil")
	}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount: 0,
		KeySandboxMemoryBytes:  0,
	}, RuntimeRef{}); err == nil {
		t.Fatal("ReconcileTarget(source with prepared transfer) error = nil")
	}

	if err := repo.CommitTransfer(ctx, Ref(prepared.Owner, prepared.Operation)); err != nil {
		t.Fatalf("CommitTransfer() error = %v", err)
	}
	if err := repo.CommitTransfer(ctx, Ref(prepared.Owner, prepared.Operation)); err != nil {
		t.Fatalf("idempotent CommitTransfer() error = %v", err)
	}
	committed, err := repo.PrepareTransfer(ctx, request)
	if err != nil {
		t.Fatalf("post-commit PrepareTransfer() error = %v", err)
	}
	if committed.State != "active" ||
		committed.Committed[KeySandboxIdentityCount] != 1 ||
		committed.Committed[KeySandboxMemoryBytes] != 768 ||
		len(committed.Reserved) != 0 {
		t.Fatalf("post-commit reservation = %+v", committed)
	}
	if err := repo.AbortTransfer(ctx, Ref(prepared.Owner, prepared.Operation), "too late"); err == nil {
		t.Fatal("AbortTransfer(committed) error = nil")
	}
	sourceScale := Operation{ID: "scale-after-transfer", Kind: "scale_warm_pool"}
	if _, err := repo.ReserveTarget(ctx, ReserveRequest{
		Owner:     source,
		Operation: sourceScale,
		Target: Values{
			KeySandboxRuntimeCount: 0,
			KeySandboxMemoryBytes:  0,
		},
	}); err != nil {
		t.Fatalf("ReserveTarget(source after transfer) error = %v", err)
	}
	if err := repo.Commit(ctx, Ref(source, sourceScale)); err != nil {
		t.Fatalf("Commit(source after transfer) error = %v", err)
	}

	statuses, err = repo.ListStatus(ctx, source.TeamID)
	if err != nil {
		t.Fatalf("ListStatus(committed) error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 1 || got.Reserved != 0 {
		t.Fatalf("committed runtime status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxMemoryBytes); got.Committed != 768 || got.Reserved != 0 {
		t.Fatalf("committed memory status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxIdentityCount); got.Committed != 1 || got.Reserved != 0 {
		t.Fatalf("committed identity status = %+v", got)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant(committed) error = %v", err)
	}
}

func TestAbortPreparedTransferLeavesSourceCommitted(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxIdentityCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 1},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	source := Owner{TeamID: "team-abort-transfer", Kind: "warm_pool", ID: "template-1"}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount: 1,
	}, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(source) error = %v", err)
	}
	request := TransferRequest{
		Source:      source,
		Destination: Owner{TeamID: source.TeamID, Kind: "sandbox", ID: "sandbox-1"},
		Operation:   Operation{ID: "abort-claim-1", Kind: "hot_claim", Generation: 1},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
		},
		DestinationTarget: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  1,
		},
		Runtime: RuntimeRef{
			Namespace: "sandboxes",
			Name:      "idle-1",
			UID:       "runtime-uid-1",
		},
	}
	prepared, err := repo.PrepareTransfer(ctx, request)
	if err != nil {
		t.Fatalf("PrepareTransfer() error = %v", err)
	}
	ref := Ref(prepared.Owner, prepared.Operation)
	if err := repo.AbortTransfer(ctx, ref, "pod update failed"); err != nil {
		t.Fatalf("AbortTransfer() error = %v", err)
	}
	if err := repo.AbortTransfer(ctx, ref, "pod update failed"); err != nil {
		t.Fatalf("idempotent AbortTransfer() error = %v", err)
	}
	if _, err := repo.PrepareTransfer(ctx, request); err == nil {
		t.Fatal("post-abort PrepareTransfer() error = nil")
	} else {
		var aborted *OperationAbortedError
		if !errors.As(err, &aborted) {
			t.Fatalf("post-abort PrepareTransfer() error = %v, want OperationAbortedError", err)
		}
	}
	if err := repo.CommitTransfer(ctx, ref); err == nil {
		t.Fatal("CommitTransfer(aborted) error = nil")
	} else {
		var aborted *OperationAbortedError
		if !errors.As(err, &aborted) {
			t.Fatalf("CommitTransfer(aborted) error = %v, want OperationAbortedError", err)
		}
	}
	statuses, err := repo.ListStatus(ctx, source.TeamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 1 || got.Reserved != 0 {
		t.Fatalf("runtime status after abort = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxIdentityCount); got.Committed != 0 || got.Reserved != 0 {
		t.Fatalf("identity status after abort = %+v", got)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
	var state, result string
	if err := repo.pool.QueryRow(ctx, `
		SELECT state, last_operation_result
		FROM quota.allocations
		WHERE allocation_id = $1
	`, prepared.AllocationID).Scan(&state, &result); err != nil {
		t.Fatalf("query aborted destination: %v", err)
	}
	if state != "released" || result != "aborted" {
		t.Fatalf("aborted destination state/result = %q/%q", state, result)
	}
}

func TestConcurrentPrepareTransferRespectsNetQuota(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxIdentityCount, Kind: KindCapacity, Limit: 10},
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 100},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	source := Owner{TeamID: "team-concurrent-transfer", Kind: "warm_pool", ID: "template-1"}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount: 100,
	}, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(source) error = %v", err)
	}

	var allowed atomic.Int64
	var exceeded atomic.Int64
	var unexpected atomic.Value
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := repo.PrepareTransfer(ctx, TransferRequest{
				Source: source,
				Destination: Owner{
					TeamID: source.TeamID,
					Kind:   "sandbox",
					ID:     fmt.Sprintf("sandbox-%d", index),
				},
				Operation: Operation{
					ID:   fmt.Sprintf("prepare-claim-%d", index),
					Kind: "hot_claim",
				},
				SourceDecrease: Values{KeySandboxRuntimeCount: 1},
				DestinationTarget: Values{
					KeySandboxIdentityCount: 1,
					KeySandboxRuntimeCount:  1,
				},
				Runtime: RuntimeRef{
					Namespace: "sandboxes",
					Name:      fmt.Sprintf("idle-%d", index),
					UID:       fmt.Sprintf("runtime-uid-%d", index),
				},
			})
			switch {
			case err == nil:
				allowed.Add(1)
			case IsExceeded(err):
				exceeded.Add(1)
			default:
				unexpected.Store(err)
			}
		}(i)
	}
	wg.Wait()
	if err, _ := unexpected.Load().(error); err != nil {
		t.Fatalf("unexpected prepare error = %v", err)
	}
	if allowed.Load() != 10 || exceeded.Load() != 90 {
		t.Fatalf("prepare results = allowed %d, exceeded %d; want 10 and 90", allowed.Load(), exceeded.Load())
	}
	statuses, err := repo.ListStatus(ctx, source.TeamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 100 || got.Reserved != 0 {
		t.Fatalf("prepared runtime status = %+v", got)
	}
	if got := statusForKey(t, statuses, KeySandboxIdentityCount); got.Committed != 0 || got.Reserved != 10 {
		t.Fatalf("prepared identity status = %+v", got)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*) FROM quota.transfer_operations
		WHERE team_id = 'team-concurrent-transfer' AND state = 'prepared'
	`, 10)
}

func TestTransferTargetOverLimitRollsBackSource(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxIdentityCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 1},
		Policy{Key: KeySandboxMemoryBytes, Kind: KindCapacity, Limit: 700},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	source := Owner{TeamID: "team-over-limit", Kind: "warm_pool", ID: "template-1"}
	runtime := RuntimeRef{
		Namespace:  "sandboxes",
		Name:       "idle-1",
		UID:        "runtime-uid-1",
		Generation: 1,
	}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount: 1,
		KeySandboxMemoryBytes:  512,
	}, runtime); err != nil {
		t.Fatalf("ReconcileTarget(source) error = %v", err)
	}
	_, err := repo.PrepareTransfer(ctx, TransferRequest{
		Source:      source,
		Destination: Owner{TeamID: source.TeamID, Kind: "sandbox", ID: "sandbox-1"},
		Operation: Operation{
			ID:         "hot-claim-over-limit",
			Kind:       "hot_claim",
			Generation: runtime.Generation,
		},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
		DestinationTarget: Values{
			KeySandboxIdentityCount: 1,
			KeySandboxRuntimeCount:  1,
			KeySandboxMemoryBytes:   768,
		},
		Runtime: runtime,
	})
	if !IsExceeded(err) {
		t.Fatalf("PrepareTransfer() error = %v, want ExceededError", err)
	}
	var exceeded *ExceededError
	if !errors.As(err, &exceeded) || exceeded.Key != KeySandboxMemoryBytes || exceeded.Requested != 256 {
		t.Fatalf("ExceededError = %+v", exceeded)
	}
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*) FROM quota.allocations
		WHERE team_id = 'team-over-limit'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*) FROM quota.transfer_operations
		WHERE team_id = 'team-over-limit'
	`, 0)
	sourceID := sourceAllocationID(t, repo.pool, source)
	var runtimeCount, memoryBytes int64
	if err := repo.pool.QueryRow(ctx, `
		SELECT
			(SELECT committed_value FROM quota.allocation_items
				WHERE allocation_id = $1 AND quota_key = $2),
			(SELECT committed_value FROM quota.allocation_items
				WHERE allocation_id = $1 AND quota_key = $3)
	`, sourceID, string(KeySandboxRuntimeCount), string(KeySandboxMemoryBytes)).Scan(
		&runtimeCount,
		&memoryBytes,
	); err != nil {
		t.Fatalf("query source after rejected transfer: %v", err)
	}
	if runtimeCount != 1 || memoryBytes != 512 {
		t.Fatalf("source after rejected transfer = runtime %d memory %d", runtimeCount, memoryBytes)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestConcurrentReservationsRespectTeamLimit(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 10},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}

	var allowed atomic.Int64
	var exceeded atomic.Int64
	var unexpected atomic.Value
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := repo.ReserveTarget(ctx, ReserveRequest{
				Owner: Owner{
					TeamID: "team-1",
					Kind:   "sandbox",
					ID:     fmt.Sprintf("sandbox-%d", index),
				},
				Operation: Operation{
					ID:   fmt.Sprintf("claim-%d", index),
					Kind: "claim",
				},
				Target: Values{KeySandboxRuntimeCount: 1},
			})
			switch {
			case err == nil:
				allowed.Add(1)
			case IsExceeded(err):
				exceeded.Add(1)
			default:
				unexpected.Store(err)
			}
		}(i)
	}
	wg.Wait()
	if err, _ := unexpected.Load().(error); err != nil {
		t.Fatalf("unexpected reservation error = %v", err)
	}
	if allowed.Load() != 10 || exceeded.Load() != 90 {
		t.Fatalf("results = allowed %d, exceeded %d; want 10 and 90", allowed.Load(), exceeded.Load())
	}
	if err := repo.ValidateUsageInvariant(ctx, "team-1"); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
	statuses, err := repo.ListStatus(ctx, "team-1")
	if err != nil || len(statuses) != len(Keys()) {
		t.Fatalf("ListStatus() = (%+v, %v)", statuses, err)
	}
	status := statusForKey(t, statuses, KeySandboxRuntimeCount)
	if status.Committed != 0 || status.Reserved != 10 || status.Remaining == nil || *status.Remaining != 0 {
		t.Fatalf("status = %+v", status)
	}
}

func TestRunMigrationsDropsLegacyQuotaStateWithoutTranslation(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	ctx := context.Background()
	installLegacyQuotaSchema(t, pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO quota.team_quota_limits (team_id, dimension, limit_value)
		VALUES ('legacy-team', 'active_sandboxes', 3)
	`); err != nil {
		t.Fatalf("insert legacy quota: %v", err)
	}

	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}

	var translated int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM quota.team_policies
		WHERE team_id = 'legacy-team'
	`).Scan(&translated); err != nil {
		t.Fatalf("count translated team policies: %v", err)
	}
	if translated != 0 {
		t.Fatalf("translated team policies = %d, want 0", translated)
	}

	var legacyTableExists bool
	if err := pool.QueryRow(ctx, `
		SELECT to_regclass('quota.team_quota_limits') IS NOT NULL
	`).Scan(&legacyTableExists); err != nil {
		t.Fatalf("inspect legacy quota table: %v", err)
	}
	if legacyTableExists {
		t.Fatal("legacy quota table still exists after migration")
	}
}

func TestRunMigrationsSerializesConcurrentRegionalConsumers(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	ctx := context.Background()

	const consumers = 8
	var wg sync.WaitGroup
	errs := make(chan error, consumers)
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- RunMigrations(ctx, pool, nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent RunMigrations() error = %v", err)
		}
	}
	assertTeamQuotaTableExists(t, pool, "allocations")
}

func completeDefaultPolicies(overrides ...Policy) []Policy {
	byKey := make(map[Key]Policy, len(Keys()))
	for _, key := range Keys() {
		kind, _ := KindForKey(key)
		policy := Policy{Key: key, Kind: kind}
		switch kind {
		case KindCapacity:
			policy.Limit = 1 << 60
		case KindConcurrency:
			policy.Limit = 1 << 52
		default:
			policy.Tokens = 1000
			policy.IntervalMillis = 1000
			policy.Burst = 2000
		}
		byKey[key] = policy
	}
	for _, policy := range overrides {
		byKey[policy.Key] = policy
	}
	policies := make([]Policy, 0, len(byKey))
	for _, key := range Keys() {
		policies = append(policies, byKey[key])
	}
	return policies
}

func statusForKey(t *testing.T, statuses []Status, key Key) Status {
	t.Helper()
	for _, status := range statuses {
		if status.Key == key {
			return status
		}
	}
	t.Fatalf("status for %s was not returned", key)
	return Status{}
}

func sourceAllocationID(t *testing.T, pool *pgxpool.Pool, owner Owner) string {
	t.Helper()
	var allocationID string
	if err := pool.QueryRow(context.Background(), `
		SELECT allocation_id
		FROM quota.allocations
		WHERE team_id = $1 AND owner_kind = $2 AND owner_id = $3
	`, owner.TeamID, owner.Kind, owner.ID).Scan(&allocationID); err != nil {
		t.Fatalf("query source allocation ID: %v", err)
	}
	return allocationID
}

func assertAllocationCluster(t *testing.T, pool *pgxpool.Pool, owner Owner, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(context.Background(), `
		SELECT cluster_id
		FROM quota.allocations
		WHERE team_id = $1 AND owner_kind = $2 AND owner_id = $3
	`, owner.TeamID, owner.Kind, owner.ID).Scan(&got); err != nil {
		t.Fatalf("query allocation cluster: %v", err)
	}
	if got != want {
		t.Fatalf("allocation cluster = %q, want %q", got, want)
	}
}

func newTeamQuotaTestRepository(t *testing.T) *Repository {
	t.Helper()
	pool := newTeamQuotaTestDatabase(t)
	if err := RunMigrations(context.Background(), pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	return NewRepository(pool)
}

func installLegacyQuotaSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
	CREATE SCHEMA quota;
	CREATE TABLE quota.team_quota_limits (
    team_id TEXT NOT NULL,
    dimension TEXT NOT NULL,
    limit_value BIGINT NOT NULL CHECK (limit_value >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, dimension)
	);
	CREATE TABLE quota.goose_db_version (
		id SERIAL PRIMARY KEY,
		version_id BIGINT NOT NULL,
		is_applied BOOLEAN NOT NULL,
		tstamp TIMESTAMP NOT NULL DEFAULT NOW()
	);
	INSERT INTO quota.goose_db_version(version_id, is_applied)
	VALUES (1, TRUE);
	`); err != nil {
		t.Fatalf("install legacy quota schema: %v", err)
	}
}

func newTeamQuotaTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("connect test database admin: %v", err)
	}
	databaseName := "team_quota_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedName := `"` + strings.ReplaceAll(databaseName, `"`, `""`) + `"`
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedName); err != nil {
		admin.Close()
		t.Fatalf("create test database: %v", err)
	}
	testConfig := adminConfig.Copy()
	testConfig.ConnConfig.Database = databaseName
	testConfig.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP DATABASE "+quotedName)
		admin.Close()
		t.Fatalf("connect isolated test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+quotedName); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		admin.Close()
	})
	return pool
}

func assertTeamQuotaTableExists(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'quota' AND table_name = $1
		)
	`, table).Scan(&exists); err != nil {
		t.Fatalf("query table %s: %v", table, err)
	}
	if !exists {
		t.Fatalf("quota.%s does not exist", table)
	}
}

func assertTeamQuotaCount(t *testing.T, pool *pgxpool.Pool, query string, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
}
