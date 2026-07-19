package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestOwnedSandboxVolumeRepositoryScopesOwnerIdentityByTeam(t *testing.T) {
	repo := newCoordinationRetentionTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for _, item := range []struct {
		volumeID string
		teamID   string
	}{
		{volumeID: "team-1-volume", teamID: "team-1"},
		{volumeID: "team-2-volume", teamID: "team-2"},
	} {
		if _, err := repo.Pool().Exec(ctx, `
			INSERT INTO sandbox_volumes (id, team_id, user_id)
			VALUES ($1, $2, $3)
		`, item.volumeID, item.teamID, item.teamID+"-user"); err != nil {
			t.Fatalf("create %s: %v", item.volumeID, err)
		}
		err := repo.WithTx(ctx, func(tx pgx.Tx) error {
			return repo.CreateSandboxVolumeOwnerTx(ctx, tx, &SandboxVolumeOwner{
				VolumeID:       item.volumeID,
				TeamID:         item.teamID,
				OwnerKind:      SandboxVolumeOwnerKindSandbox,
				OwnerSandboxID: "sandbox-a",
				OwnerClusterID: "cluster-a",
				Purpose:        "webhook-state",
				CreatedAt:      now,
				UpdatedAt:      now,
			})
		})
		if err != nil {
			t.Fatalf("create %s owner: %v", item.volumeID, err)
		}
	}

	for _, item := range []struct {
		teamID       string
		wantVolumeID string
	}{
		{teamID: "team-1", wantVolumeID: "team-1-volume"},
		{teamID: "team-2", wantVolumeID: "team-2-volume"},
	} {
		owned, err := repo.GetOwnedSandboxVolumeByOwner(
			ctx,
			item.teamID,
			"cluster-a",
			"sandbox-a",
			"webhook-state",
		)
		if err != nil {
			t.Fatalf("GetOwnedSandboxVolumeByOwner(%s) error = %v", item.teamID, err)
		}
		if owned.Volume.ID != item.wantVolumeID || owned.Owner.TeamID != item.teamID {
			t.Fatalf("GetOwnedSandboxVolumeByOwner(%s) = %#v", item.teamID, owned)
		}
	}
	if _, err := repo.GetOwnedSandboxVolumeByOwner(
		ctx,
		"team-3",
		"cluster-a",
		"sandbox-a",
		"webhook-state",
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetOwnedSandboxVolumeByOwner(team-3) error = %v, want ErrNotFound", err)
	}

	marked, err := repo.MarkTeamOwnedSandboxVolumesForCleanup(
		ctx,
		"team-1",
		"cluster-a",
		"sandbox-a",
		"test",
	)
	if err != nil {
		t.Fatalf("MarkTeamOwnedSandboxVolumesForCleanup() error = %v", err)
	}
	if marked != 1 {
		t.Fatalf("MarkTeamOwnedSandboxVolumesForCleanup() marked = %d, want 1", marked)
	}
	team1Owner, err := repo.GetSandboxVolumeOwner(ctx, "team-1-volume")
	if err != nil {
		t.Fatalf("GetSandboxVolumeOwner(team-1) error = %v", err)
	}
	team2Owner, err := repo.GetSandboxVolumeOwner(ctx, "team-2-volume")
	if err != nil {
		t.Fatalf("GetSandboxVolumeOwner(team-2) error = %v", err)
	}
	if team1Owner.CleanupRequestedAt == nil || team2Owner.CleanupRequestedAt != nil {
		t.Fatalf("cleanup scope mismatch: team-1=%#v team-2=%#v", team1Owner, team2Owner)
	}

	marked, err = repo.MarkOwnedSandboxVolumesForCleanup(ctx, "cluster-a", "sandbox-a", "system")
	if err != nil {
		t.Fatalf("MarkOwnedSandboxVolumesForCleanup() error = %v", err)
	}
	if marked != 1 {
		t.Fatalf("MarkOwnedSandboxVolumesForCleanup() marked = %d, want 1", marked)
	}
}

func TestSandboxVolumeOwnerTeamMustMatchVolumeTeam(t *testing.T) {
	repo := newCoordinationRetentionTestRepository(t)
	ctx := context.Background()
	if _, err := repo.Pool().Exec(ctx, `
		INSERT INTO sandbox_volumes (id, team_id, user_id)
		VALUES ('team-1-volume', 'team-1', 'user-1')
	`); err != nil {
		t.Fatalf("create volume: %v", err)
	}

	err := repo.WithTx(ctx, func(tx pgx.Tx) error {
		now := time.Now().UTC()
		return repo.CreateSandboxVolumeOwnerTx(ctx, tx, &SandboxVolumeOwner{
			VolumeID:       "team-1-volume",
			TeamID:         "team-2",
			OwnerKind:      SandboxVolumeOwnerKindSandbox,
			OwnerSandboxID: "sandbox-a",
			OwnerClusterID: "cluster-a",
			Purpose:        "webhook-state",
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	})
	if err == nil {
		t.Fatal("CreateSandboxVolumeOwnerTx() error = nil, want team foreign-key violation")
	}
}
