package teamquota

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTeamDeletionLifecycleRetainsPermanentAdmissionTombstone(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-delete"
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies()); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	if err := repo.UnsafePutTeamPolicyForTest(ctx, teamID, Policy{
		Key:   KeySandboxRuntimeCount,
		Kind:  KindCapacity,
		Limit: 1,
	}); err != nil {
		t.Fatalf("PutTeamPolicy() error = %v", err)
	}

	if err := repo.DisableTeamAdmission(ctx, teamID); err != nil {
		t.Fatalf("DisableTeamAdmission() error = %v", err)
	}
	if err := repo.DisableTeamAdmission(ctx, teamID); err != nil {
		t.Fatalf("idempotent DisableTeamAdmission() error = %v", err)
	}
	disabled, err := repo.TeamAdmissionDisabled(ctx, teamID)
	if err != nil || !disabled {
		t.Fatalf("TeamAdmissionDisabled() = (%v, %v), want true", disabled, err)
	}
	if _, err := repo.EffectivePolicy(ctx, teamID, KeyAPIRequests); !IsUnavailable(err) || !IsTeamAdmissionDisabled(err) {
		t.Fatalf("EffectivePolicy() error = %v, want admission-disabled unavailable", err)
	}
	if err := repo.UnsafePutTeamPolicyForTest(ctx, teamID, Policy{
		Key:   KeySandboxRuntimeCount,
		Kind:  KindCapacity,
		Limit: 2,
	}); !IsUnavailable(err) || !IsTeamAdmissionDisabled(err) {
		t.Fatalf("PutTeamPolicy() error = %v, want admission-disabled unavailable", err)
	}

	if err := repo.FinalizeTeamDeletion(ctx, teamID); err != nil {
		t.Fatalf("FinalizeTeamDeletion() error = %v", err)
	}
	if err := repo.FinalizeTeamDeletion(ctx, teamID); err != nil {
		t.Fatalf("idempotent FinalizeTeamDeletion() error = %v", err)
	}
	var (
		admissionDisabled bool
		deleted           bool
		policyCount       int64
	)
	if err := repo.pool.QueryRow(ctx, `
		SELECT admission_disabled, deleted_at IS NOT NULL
		FROM quota.team_states
		WHERE team_id = $1
	`, teamID).Scan(&admissionDisabled, &deleted); err != nil {
		t.Fatalf("query team tombstone: %v", err)
	}
	if !admissionDisabled || !deleted {
		t.Fatalf("tombstone = disabled:%v deleted:%v, want true/true", admissionDisabled, deleted)
	}
	if err := repo.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM quota.team_policies WHERE team_id = $1
	`, teamID).Scan(&policyCount); err != nil {
		t.Fatalf("query team policies: %v", err)
	}
	if policyCount != 0 {
		t.Fatalf("team policies = %d, want 0 after finalization", policyCount)
	}
}

func TestDisableTeamAdmissionConflictsWithLiveAllocation(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-live"
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 10},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	request := ReserveRequest{
		Owner:     Owner{TeamID: teamID, Kind: "sandbox", ID: "sandbox-1"},
		Operation: Operation{ID: "claim-1", Kind: "claim"},
		Target:    Values{KeySandboxRuntimeCount: 1},
	}
	if _, err := repo.ReserveTarget(ctx, request); err != nil {
		t.Fatalf("ReserveTarget() error = %v", err)
	}

	err := repo.DisableTeamAdmission(ctx, teamID)
	if !IsDeletionConflict(err) {
		t.Fatalf("DisableTeamAdmission() error = %v, want deletion conflict", err)
	}
	disabled, stateErr := repo.TeamAdmissionDisabled(ctx, teamID)
	if stateErr != nil || disabled {
		t.Fatalf("TeamAdmissionDisabled() = (%v, %v), want false after rollback", disabled, stateErr)
	}
}

func TestDisabledTeamRejectsConcurrentCapacityRecreation(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-stale-jwt"
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 100},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}
	if err := repo.DisableTeamAdmission(ctx, teamID); err != nil {
		t.Fatalf("DisableTeamAdmission() error = %v", err)
	}
	if err := repo.FinalizeTeamDeletion(ctx, teamID); err != nil {
		t.Fatalf("FinalizeTeamDeletion() error = %v", err)
	}

	const workers = 16
	errorsByWorker := make(chan error, workers)
	for index := 0; index < workers; index++ {
		go func(index int) {
			_, err := repo.ReserveTarget(ctx, ReserveRequest{
				Owner:     Owner{TeamID: teamID, Kind: "sandbox", ID: "sandbox-" + string(rune('a'+index))},
				Operation: Operation{ID: "claim-" + string(rune('a'+index)), Kind: "claim"},
				Target:    Values{KeySandboxRuntimeCount: 1},
			})
			errorsByWorker <- err
		}(index)
	}
	for index := 0; index < workers; index++ {
		err := <-errorsByWorker
		if !IsUnavailable(err) || !IsTeamAdmissionDisabled(err) {
			t.Fatalf("concurrent ReserveTarget() error = %v, want admission-disabled unavailable", err)
		}
	}
	var stateCount int64
	if err := repo.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM quota.team_states WHERE team_id = $1
	`, teamID).Scan(&stateCount); err != nil {
		t.Fatalf("query team tombstone count: %v", err)
	}
	if stateCount != 1 {
		t.Fatalf("team tombstones = %d, want exactly 1", stateCount)
	}
}

func TestDisableTeamAdmissionDrainsSharedBusinessMutationFence(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-mutation-drain"

	tx, err := repo.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin business mutation: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := AdmitTeamMutationTx(ctx, tx, teamID); err != nil {
		t.Fatalf("AdmitTeamMutationTx() error = %v", err)
	}

	finalCheckCalled := make(chan struct{}, 1)
	deleted := make(chan error, 1)
	go func() {
		deleted <- repo.DisableTeamAdmissionWithFinalCheck(ctx, teamID, func(context.Context) error {
			finalCheckCalled <- struct{}{}
			return nil
		})
	}()

	select {
	case err := <-deleted:
		t.Fatalf("DisableTeamAdmissionWithFinalCheck() returned before mutation committed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-finalCheckCalled:
		t.Fatal("final deletion inventory ran before shared mutation fence drained")
	default:
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit business mutation: %v", err)
	}
	if err := <-deleted; err != nil {
		t.Fatalf("DisableTeamAdmissionWithFinalCheck() error = %v", err)
	}
	select {
	case <-finalCheckCalled:
	default:
		t.Fatal("final deletion inventory was not run")
	}
}

func TestDisableTeamAdmissionFinalCheckRollsBackTombstone(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-final-check"
	wantErr := errors.New("business resource appeared")

	err := repo.DisableTeamAdmissionWithFinalCheck(ctx, teamID, func(context.Context) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("DisableTeamAdmissionWithFinalCheck() error = %v, want %v", err, wantErr)
	}
	disabled, stateErr := repo.TeamAdmissionDisabled(ctx, teamID)
	if stateErr != nil || disabled {
		t.Fatalf("TeamAdmissionDisabled() = (%v, %v), want active after rollback", disabled, stateErr)
	}

	tx, err := repo.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin retry mutation: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := AdmitTeamMutationTx(ctx, tx, teamID); err != nil {
		t.Fatalf("AdmitTeamMutationTx() after rollback error = %v", err)
	}
}
