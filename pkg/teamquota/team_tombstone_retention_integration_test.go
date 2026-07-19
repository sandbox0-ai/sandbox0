package teamquota

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeletedTeamTombstoneRetentionIsAgeBoundedAndConcurrentSafe(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-24 * time.Hour)

	oldTeamID := "deleted-old"
	if err := repo.DisableTeamAdmission(ctx, oldTeamID); err != nil {
		t.Fatalf("DisableTeamAdmission(old) error = %v", err)
	}
	if err := repo.FinalizeTeamDeletion(ctx, oldTeamID); err != nil {
		t.Fatalf("FinalizeTeamDeletion(old) error = %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE quota.team_states
		SET deleted_at = $2
		WHERE team_id = $1
	`, oldTeamID, cutoff.Add(-time.Hour)); err != nil {
		t.Fatalf("age old tombstone: %v", err)
	}

	newTeamID := "deleted-new"
	if err := repo.DisableTeamAdmission(ctx, newTeamID); err != nil {
		t.Fatalf("DisableTeamAdmission(new) error = %v", err)
	}
	if err := repo.FinalizeTeamDeletion(ctx, newTeamID); err != nil {
		t.Fatalf("FinalizeTeamDeletion(new) error = %v", err)
	}

	tombstones, err := repo.ListDeletedTeamTombstones(ctx, cutoff, nil, 10)
	if err != nil {
		t.Fatalf("ListDeletedTeamTombstones() error = %v", err)
	}
	if len(tombstones) != 1 || tombstones[0].TeamID != oldTeamID {
		t.Fatalf("eligible tombstones = %v, want [%s]", tombstones, oldTeamID)
	}
	if pruned, err := repo.PruneDeletedTeamTombstone(ctx, newTeamID, cutoff); err != nil || pruned {
		t.Fatalf("PruneDeletedTeamTombstone(new) = (%v, %v), want false", pruned, err)
	}

	const workers = 16
	var prunedCount atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pruned, pruneErr := repo.PruneDeletedTeamTombstone(ctx, oldTeamID, cutoff)
			if pruneErr != nil {
				errs <- pruneErr
				return
			}
			if pruned {
				prunedCount.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for pruneErr := range errs {
		t.Errorf("concurrent PruneDeletedTeamTombstone() error = %v", pruneErr)
	}
	if got := prunedCount.Load(); got != 1 {
		t.Fatalf("successful prunes = %d, want exactly 1", got)
	}
	disabled, err := repo.TeamAdmissionDisabled(ctx, oldTeamID)
	if err != nil || disabled {
		t.Fatalf("TeamAdmissionDisabled(pruned) = (%v, %v), want absent", disabled, err)
	}
	disabled, err = repo.TeamAdmissionDisabled(ctx, newTeamID)
	if err != nil || !disabled {
		t.Fatalf("TeamAdmissionDisabled(new) = (%v, %v), want retained", disabled, err)
	}
}

func TestDeletedTeamTombstoneRetentionRejectsRemainingReferences(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "deleted-referenced"
	cutoff := time.Now().UTC().Add(-24 * time.Hour)

	if err := repo.DisableTeamAdmission(ctx, teamID); err != nil {
		t.Fatalf("DisableTeamAdmission() error = %v", err)
	}
	if err := repo.FinalizeTeamDeletion(ctx, teamID); err != nil {
		t.Fatalf("FinalizeTeamDeletion() error = %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE quota.team_states
		SET deleted_at = $2
		WHERE team_id = $1
	`, teamID, cutoff.Add(-time.Hour)); err != nil {
		t.Fatalf("age tombstone: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.team_policies (
			team_id, quota_key, kind, limit_value
		)
		VALUES ($1, $2, 'capacity', 1)
	`, teamID, string(KeySandboxRuntimeCount)); err != nil {
		t.Fatalf("insert lingering policy: %v", err)
	}

	if pruned, err := repo.PruneDeletedTeamTombstone(ctx, teamID, cutoff); !IsUnavailable(err) || pruned {
		t.Fatalf("PruneDeletedTeamTombstone() = (%v, %v), want unavailable reference fence", pruned, err)
	}
	disabled, err := repo.TeamAdmissionDisabled(ctx, teamID)
	if err != nil || !disabled {
		t.Fatalf("TeamAdmissionDisabled() = (%v, %v), want retained", disabled, err)
	}
}
