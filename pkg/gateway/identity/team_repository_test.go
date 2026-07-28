package identity

import (
	"context"
	"testing"

	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

func TestTeamRepositoryAllowsDuplicateNamesAndSlugs(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	ownerA := &User{Email: "team-owner-a@example.com", Name: "Owner A"}
	ownerB := &User{Email: "team-owner-b@example.com", Name: "Owner B"}
	if err := repo.CreateUser(ctx, ownerA); err != nil {
		t.Fatalf("create owner A: %v", err)
	}
	if err := repo.CreateUser(ctx, ownerB); err != nil {
		t.Fatalf("create owner B: %v", err)
	}

	ownerAID := ownerA.ID
	ownerBID := ownerB.ID
	teamA := &Team{Name: "GCP US East 4", Slug: "gcp-us-east-4", OwnerID: &ownerAID}
	if err := repo.CreateTeam(ctx, teamA); err != nil {
		t.Fatalf("create team A: %v", err)
	}

	teamB := &Team{Name: "GCP US East 4", Slug: "gcp-us-east-4", OwnerID: &ownerBID}
	if err := repo.CreateTeam(ctx, teamB); err != nil {
		t.Fatalf("create team B with same slug for another owner: %v", err)
	}

	teamC := &Team{Name: "GCP US East 4", Slug: "gcp-us-east-4", OwnerID: &ownerAID}
	if err := repo.CreateTeam(ctx, teamC); err != nil {
		t.Fatalf("create team C with same name and slug for same owner: %v", err)
	}

	teamIDs := map[string]struct{}{
		teamA.ID: {},
		teamB.ID: {},
		teamC.ID: {},
	}
	if len(teamIDs) != 3 {
		t.Fatalf("team IDs are not unique: A=%s B=%s C=%s", teamA.ID, teamB.ID, teamC.ID)
	}
}

func TestGatewayMigration14RepairsProductionSlugConstraint(t *testing.T) {
	pool, schema := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		DELETE FROM goose_db_version WHERE version_id = 14;
		INSERT INTO goose_db_version (version_id, is_applied)
		SELECT version_id, true
		FROM generate_series(9, 13) AS versions(version_id);
		ALTER TABLE teams ADD CONSTRAINT teams_slug_key UNIQUE (slug);
	`); err != nil {
		t.Fatalf("prepare production migration state: %v", err)
	}

	if err := migrate.Up(
		ctx,
		pool,
		".",
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithSchema(schema),
	); err != nil {
		t.Fatalf("apply migration 14 from production version: %v", err)
	}

	var hasGlobalConstraint bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_constraint
			WHERE conrelid = 'teams'::regclass
			  AND conname = 'teams_slug_key'
		)
	`).Scan(&hasGlobalConstraint); err != nil {
		t.Fatalf("query global slug constraint: %v", err)
	}
	if hasGlobalConstraint {
		t.Fatal("global team slug constraint still exists after migration 14")
	}
}

func TestTeamRepositorySearchTeamMembers(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	owner := &User{Email: "owner@example.com", Name: "Owner User"}
	developer := &User{Email: "developer@example.com", Name: "Build Runner"}
	viewer := &User{Email: "viewer@example.com", Name: "Viewer User"}
	for _, user := range []*User{owner, developer, viewer} {
		if err := repo.CreateUser(ctx, user); err != nil {
			t.Fatalf("create user %s: %v", user.Email, err)
		}
	}
	ownerID := owner.ID
	team := &Team{Name: "Team Search", Slug: "team-search", OwnerID: &ownerID}
	if err := repo.CreateTeam(ctx, team); err != nil {
		t.Fatalf("create team: %v", err)
	}
	for _, member := range []*TeamMember{
		{TeamID: team.ID, UserID: owner.ID, Role: "admin"},
		{TeamID: team.ID, UserID: developer.ID, Role: "builder"},
		{TeamID: team.ID, UserID: viewer.ID, Role: "viewer"},
	} {
		if err := repo.AddTeamMember(ctx, member); err != nil {
			t.Fatalf("add member %s: %v", member.UserID, err)
		}
	}

	members, err := repo.SearchTeamMembers(ctx, team.ID, "build")
	if err != nil {
		t.Fatalf("search members: %v", err)
	}
	if len(members) != 1 || members[0].UserID != developer.ID {
		t.Fatalf("search by name returned %#v, want developer", members)
	}

	members, err = repo.SearchTeamMembers(ctx, team.ID, "VIEWER@EXAMPLE.COM")
	if err != nil {
		t.Fatalf("search members by email: %v", err)
	}
	if len(members) != 1 || members[0].UserID != viewer.ID {
		t.Fatalf("search by email returned %#v, want viewer", members)
	}
}

func TestTeamRepositoryTransferTeamOwnerPromotesMember(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	owner := &User{Email: "transfer-owner@example.com", Name: "Owner User"}
	nextOwner := &User{Email: "transfer-next@example.com", Name: "Next Owner"}
	for _, user := range []*User{owner, nextOwner} {
		if err := repo.CreateUser(ctx, user); err != nil {
			t.Fatalf("create user %s: %v", user.Email, err)
		}
	}
	ownerID := owner.ID
	team := &Team{Name: "Transfer Team", Slug: "transfer-team", OwnerID: &ownerID}
	if err := repo.CreateTeam(ctx, team); err != nil {
		t.Fatalf("create team: %v", err)
	}
	nextOwnerID := nextOwner.ID
	existingTeam := &Team{Name: "Existing Team", Slug: "transfer-team", OwnerID: &nextOwnerID}
	if err := repo.CreateTeam(ctx, existingTeam); err != nil {
		t.Fatalf("create next owner's team with same slug: %v", err)
	}
	for _, member := range []*TeamMember{
		{TeamID: team.ID, UserID: owner.ID, Role: "admin"},
		{TeamID: team.ID, UserID: nextOwner.ID, Role: "viewer"},
	} {
		if err := repo.AddTeamMember(ctx, member); err != nil {
			t.Fatalf("add member %s: %v", member.UserID, err)
		}
	}

	updated, err := repo.TransferTeamOwner(ctx, team.ID, nextOwner.ID)
	if err != nil {
		t.Fatalf("transfer owner: %v", err)
	}
	if updated.OwnerID == nil || *updated.OwnerID != nextOwner.ID {
		t.Fatalf("owner id = %#v, want %s", updated.OwnerID, nextOwner.ID)
	}
	member, err := repo.GetTeamMember(ctx, team.ID, nextOwner.ID)
	if err != nil {
		t.Fatalf("get next owner member: %v", err)
	}
	if member.Role != "admin" {
		t.Fatalf("next owner role = %q, want admin", member.Role)
	}
}
