package teamquota

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestCommitExactAtomicallyUsesMeasuredTargetAndReleasesReservation(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	installCommitExactPolicies(t, repo)

	owner := Owner{
		TeamID:    "team-commit-exact",
		Kind:      "volume",
		ID:        "volume-1",
		ClusterID: "region-storage:region-1",
	}
	initial := Values{
		KeyVolumeStorageBytes: 100,
		KeyStorageObjectCount: 5,
	}
	if err := repo.ReconcileTarget(ctx, owner, initial, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	operation := Operation{ID: "write-1", Kind: "volume_write", Generation: 1}
	reservation, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner:     owner,
		Operation: operation,
		Delta: Values{
			KeyVolumeStorageBytes: 40,
			KeyStorageObjectCount: 4,
		},
		Observed: initial,
	})
	if err != nil {
		t.Fatalf("ReserveDelta() error = %v", err)
	}
	if got := reservation.Target[KeyVolumeStorageBytes]; got != 140 {
		t.Fatalf("admitted volume bytes = %d, want 140", got)
	}

	exact := Values{
		KeyVolumeStorageBytes: 125,
		KeyStorageObjectCount: 8,
	}
	ref := Ref(owner, operation)
	if err := repo.CommitExact(ctx, ref, exact); err != nil {
		t.Fatalf("CommitExact() error = %v", err)
	}
	assertCommitExactStatus(t, repo, owner.TeamID, KeyVolumeStorageBytes, 125, 0)
	assertCommitExactStatus(t, repo, owner.TeamID, KeyStorageObjectCount, 8, 0)
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}

	recovery, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation() error = %v", err)
	}
	if recovery == nil || recovery.Operation != nil ||
		recovery.Committed[KeyVolumeStorageBytes] != 125 ||
		len(recovery.Pending) != 0 {
		t.Fatalf("recovery allocation after exact commit = %+v", recovery)
	}
	var historyState string
	if err := repo.Pool().QueryRow(ctx, `
		SELECT state
		FROM quota.allocation_operations
		WHERE allocation_id = $1 AND operation_id = $2
	`, recovery.AllocationID, operation.ID).Scan(&historyState); err != nil {
		t.Fatalf("query operation history: %v", err)
	}
	if historyState != "committed" {
		t.Fatalf("operation history state = %q, want committed", historyState)
	}

	if err := repo.CommitExact(ctx, ref, exact); err != nil {
		t.Fatalf("idempotent CommitExact() error = %v", err)
	}
	err = repo.CommitExact(ctx, ref, Values{
		KeyVolumeStorageBytes: 124,
		KeyStorageObjectCount: 8,
	})
	var conflict *OperationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("changed CommitExact retry error = %v, want OperationConflictError", err)
	}
}

func TestCommitExactRejectsTargetAbovePendingWithoutPartialWrites(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	installCommitExactPolicies(t, repo)

	owner := Owner{
		TeamID:    "team-commit-exact-bound",
		Kind:      "volume",
		ID:        "volume-1",
		ClusterID: "region-storage:region-1",
	}
	initial := Values{
		KeyVolumeStorageBytes: 100,
		KeyStorageObjectCount: 5,
	}
	if err := repo.ReconcileTarget(ctx, owner, initial, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	operation := Operation{ID: "write-1", Kind: "volume_write", Generation: 1}
	if _, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner:     owner,
		Operation: operation,
		Delta: Values{
			KeyVolumeStorageBytes: 40,
			KeyStorageObjectCount: 4,
		},
		Observed: initial,
	}); err != nil {
		t.Fatalf("ReserveDelta() error = %v", err)
	}

	err := repo.CommitExact(ctx, Ref(owner, operation), Values{
		KeyVolumeStorageBytes: 141,
		KeyStorageObjectCount: 8,
	})
	if err == nil {
		t.Fatal("CommitExact(over admitted target) error = nil")
	}
	recovery, recoveryErr := repo.GetRecoveryAllocation(ctx, owner)
	if recoveryErr != nil {
		t.Fatalf("GetRecoveryAllocation() error = %v", recoveryErr)
	}
	if recovery == nil || recovery.Operation == nil ||
		recovery.Operation.ID != operation.ID ||
		recovery.Committed[KeyVolumeStorageBytes] != 100 ||
		recovery.Pending[KeyVolumeStorageBytes] != 140 {
		t.Fatalf("allocation changed after rejected exact commit: %+v", recovery)
	}
	assertCommitExactStatus(t, repo, owner.TeamID, KeyVolumeStorageBytes, 100, 40)
	assertCommitExactStatus(t, repo, owner.TeamID, KeyStorageObjectCount, 5, 4)
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() after rejection error = %v", err)
	}
}

func TestCommitObservedExactAdoptsTargetAbovePending(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	installCommitExactPolicies(t, repo)

	owner := Owner{
		TeamID:    "team-commit-observed-exact",
		Kind:      "volume",
		ID:        "volume-1",
		ClusterID: "region-storage:region-1",
	}
	initial := Values{
		KeyVolumeStorageBytes: 100,
		KeyStorageObjectCount: 5,
	}
	if err := repo.ReconcileTarget(ctx, owner, initial, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	operation := Operation{ID: "write-1", Kind: "volume_write", Generation: 1}
	if _, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner:     owner,
		Operation: operation,
		Delta: Values{
			KeyVolumeStorageBytes: 40,
			KeyStorageObjectCount: 4,
		},
		Observed: initial,
	}); err != nil {
		t.Fatalf("ReserveDelta() error = %v", err)
	}

	exact := Values{
		KeyVolumeStorageBytes: 151,
		KeyStorageObjectCount: 10,
	}
	ref := Ref(owner, operation)
	if err := repo.CommitObservedExact(ctx, ref, exact); err != nil {
		t.Fatalf("CommitObservedExact() error = %v", err)
	}
	assertCommitExactStatus(t, repo, owner.TeamID, KeyVolumeStorageBytes, 151, 0)
	assertCommitExactStatus(t, repo, owner.TeamID, KeyStorageObjectCount, 10, 0)
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
	if err := repo.CommitObservedExact(ctx, ref, exact); err != nil {
		t.Fatalf("idempotent CommitObservedExact() error = %v", err)
	}
	err := repo.CommitObservedExact(ctx, ref, Values{
		KeyVolumeStorageBytes: 152,
		KeyStorageObjectCount: 10,
	})
	var conflict *OperationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("changed CommitObservedExact retry error = %v, want OperationConflictError", err)
	}
}

func TestCommitExactTxKeepsNextReservationBehindExactFinalize(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	installCommitExactPolicies(t, repo)

	owner := Owner{
		TeamID:    "team-commit-exact-fence",
		Kind:      "volume",
		ID:        "volume-1",
		ClusterID: "region-storage:region-1",
	}
	initial := Values{
		KeyVolumeStorageBytes: 100,
		KeyStorageObjectCount: 1,
	}
	if err := repo.ReconcileTarget(ctx, owner, initial, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	first := Operation{ID: "write-1", Kind: "volume_write", Generation: 1}
	if _, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner:     owner,
		Operation: first,
		Delta: Values{
			KeyVolumeStorageBytes: 40,
			KeyStorageObjectCount: 0,
		},
		Observed: initial,
	}); err != nil {
		t.Fatalf("ReserveDelta(first) error = %v", err)
	}

	finalizeTx, err := repo.Pool().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin exact finalize transaction: %v", err)
	}
	defer func() { _ = finalizeTx.Rollback(ctx) }()
	exact := Values{
		KeyVolumeStorageBytes: 125,
		KeyStorageObjectCount: 1,
	}
	if err := repo.CommitExactTx(ctx, finalizeTx, Ref(owner, first), exact); err != nil {
		t.Fatalf("CommitExactTx() error = %v", err)
	}

	blockedTx, err := repo.Pool().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin blocked reservation transaction: %v", err)
	}
	if _, err := blockedTx.Exec(ctx, `SET LOCAL lock_timeout = '100ms'`); err != nil {
		_ = blockedTx.Rollback(ctx)
		t.Fatalf("set reservation lock timeout: %v", err)
	}
	second := Operation{ID: "write-2", Kind: "volume_write", Generation: 2}
	_, blockedErr := repo.ReserveDeltaTx(ctx, blockedTx, DeltaRequest{
		Owner:     owner,
		Operation: second,
		Delta: Values{
			KeyVolumeStorageBytes: 10,
			KeyStorageObjectCount: 0,
		},
		Observed: exact,
	})
	_ = blockedTx.Rollback(ctx)
	if blockedErr == nil {
		t.Fatal("next reservation passed before exact finalize committed")
	}
	if !IsUnavailable(blockedErr) {
		t.Fatalf("blocked reservation error = %v, want UnavailableError", blockedErr)
	}
	if err := finalizeTx.Commit(ctx); err != nil {
		t.Fatalf("commit exact finalize transaction: %v", err)
	}

	reservation, err := repo.ReserveDelta(ctx, DeltaRequest{
		Owner:     owner,
		Operation: second,
		Delta: Values{
			KeyVolumeStorageBytes: 10,
			KeyStorageObjectCount: 0,
		},
		Observed: exact,
	})
	if err != nil {
		t.Fatalf("ReserveDelta(second) error = %v", err)
	}
	if got := reservation.Committed[KeyVolumeStorageBytes]; got != 125 {
		t.Fatalf("second reservation committed baseline = %d, want exact 125", got)
	}
	if got := reservation.Target[KeyVolumeStorageBytes]; got != 135 {
		t.Fatalf("second reservation target = %d, want 135", got)
	}
	if err := repo.CommitExact(ctx, Ref(owner, second), Values{
		KeyVolumeStorageBytes: 135,
		KeyStorageObjectCount: 1,
	}); err != nil {
		t.Fatalf("CommitExact(second) error = %v", err)
	}
	assertCommitExactStatus(t, repo, owner.TeamID, KeyVolumeStorageBytes, 135, 0)
	if err := repo.ValidateUsageInvariant(ctx, owner.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func installCommitExactPolicies(t *testing.T, repo *Repository) {
	t.Helper()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(
		context.Background(),
		completeDefaultPolicies(
			Policy{
				Key:   KeyVolumeStorageBytes,
				Kind:  KindCapacity,
				Limit: 1000,
			},
			Policy{
				Key:   KeyStorageObjectCount,
				Kind:  KindCapacity,
				Limit: 100,
			},
		),
	); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
}

func assertCommitExactStatus(
	t *testing.T,
	repo *Repository,
	teamID string,
	key Key,
	committed int64,
	reserved int64,
) {
	t.Helper()
	statuses, err := repo.ListStatus(context.Background(), teamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	for _, status := range statuses {
		if status.Key != key {
			continue
		}
		if status.Committed != committed || status.Reserved != reserved {
			t.Fatalf(
				"status for %s = committed %d reserved %d, want %d/%d",
				key,
				status.Committed,
				status.Reserved,
				committed,
				reserved,
			)
		}
		return
	}
	t.Fatalf("status for %s is missing", key)
}
