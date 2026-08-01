package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

type testTeamCreationHook struct {
	before func(context.Context, pgx.Tx, *Team) error
	after  func(context.Context, *Team)
}

func (h testTeamCreationHook) BeforeTeamCreateCommit(
	ctx context.Context,
	tx pgx.Tx,
	team *Team,
) error {
	return h.before(ctx, tx, team)
}

func (h testTeamCreationHook) AfterTeamCreateCommit(ctx context.Context, team *Team) {
	h.after(ctx, team)
}

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

func TestTeamRepositoryCreateTeamWithMemberIsAtomic(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	owner := &User{Email: "atomic-team-owner@example.com", Name: "Atomic Owner"}
	if err := repo.CreateUser(ctx, owner); err != nil {
		t.Fatalf("create owner: %v", err)
	}

	ownerID := owner.ID
	team := &Team{Name: "Atomic Team", OwnerID: &ownerID}
	member := &TeamMember{UserID: owner.ID, Role: "admin"}
	if err := repo.CreateTeamWithMember(ctx, team, member); err != nil {
		t.Fatalf("create team with member: %v", err)
	}
	if team.ID == "" || member.ID == "" || member.TeamID != team.ID {
		t.Fatalf("created team=%#v member=%#v", team, member)
	}
	storedMember, err := repo.GetTeamMember(ctx, team.ID, owner.ID)
	if err != nil {
		t.Fatalf("get owner membership: %v", err)
	}
	if storedMember.Role != "admin" {
		t.Fatalf("owner membership role = %q, want admin", storedMember.Role)
	}

	teams, err := repo.GetTeamsByUserID(ctx, owner.ID)
	if err != nil {
		t.Fatalf("list owner teams: %v", err)
	}
	if len(teams) != 1 || teams[0].ID != team.ID {
		t.Fatalf("owner teams = %#v, want team %s", teams, team.ID)
	}

	rolledBackTeam := &Team{Name: "Rolled Back Team", OwnerID: &ownerID}
	rolledBackMember := &TeamMember{
		UserID: "00000000-0000-0000-0000-000000000000",
		Role:   "admin",
	}
	if err := repo.CreateTeamWithMember(ctx, rolledBackTeam, rolledBackMember); err == nil {
		t.Fatal("create team with missing member user error = nil")
	}
	if rolledBackTeam.ID != "" || rolledBackMember.TeamID != "" {
		t.Fatalf("failed creation mutated inputs: team=%#v member=%#v", rolledBackTeam, rolledBackMember)
	}

	var rolledBackCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM teams WHERE name = $1`,
		rolledBackTeam.Name,
	).Scan(&rolledBackCount); err != nil {
		t.Fatalf("count rolled back teams: %v", err)
	}
	if rolledBackCount != 0 {
		t.Fatalf("rolled back team count = %d, want 0", rolledBackCount)
	}
}

func TestTeamCreationHookCoversRepositoryCreationPaths(t *testing.T) {
	pool, _ := newGatewayIdentityTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	var beforeIDs []string
	var afterIDs []string
	wantFailure := errors.New("provisioning failed")
	hook := testTeamCreationHook{
		before: func(ctx context.Context, tx pgx.Tx, team *Team) error {
			if team.Name == "Rejected Team" {
				return wantFailure
			}
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM teams WHERE id = $1)`, team.ID).
				Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return errors.New("team is not visible inside its creation transaction")
			}
			beforeIDs = append(beforeIDs, team.ID)
			return nil
		},
		after: func(ctx context.Context, team *Team) {
			var exists bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM teams WHERE id = $1)`, team.ID).
				Scan(&exists); err != nil || !exists {
				t.Errorf("team is not committed before post-commit hook: exists=%t err=%v", exists, err)
			}
			afterIDs = append(afterIDs, team.ID)
		},
	}
	repo := NewRepository(pool, WithTeamCreationHook(hook))

	owner := &User{Email: "team-hook-owner@example.com", Name: "Hook Owner"}
	if err := repo.CreateUser(ctx, owner); err != nil {
		t.Fatalf("create owner: %v", err)
	}
	ownerID := owner.ID
	direct := &Team{Name: "Direct Team", OwnerID: &ownerID}
	if err := repo.CreateTeam(ctx, direct); err != nil {
		t.Fatalf("create direct team: %v", err)
	}

	withMember := &Team{Name: "Member Team", OwnerID: &ownerID}
	member := &TeamMember{UserID: owner.ID, Role: "admin"}
	if err := repo.CreateTeamWithMember(ctx, withMember, member); err != nil {
		t.Fatalf("create team with member: %v", err)
	}

	registered := &User{Email: "team-hook-register@example.com", Name: "Registered"}
	if _, _, err := repo.CreateUserWithInitialTeam(ctx, registered, "Initial Team", nil); err != nil {
		t.Fatalf("create user with initial team: %v", err)
	}

	if len(beforeIDs) != 3 || len(afterIDs) != 3 {
		t.Fatalf("team hook calls = before %d, after %d, want 3 each", len(beforeIDs), len(afterIDs))
	}

	rejected := &Team{Name: "Rejected Team", OwnerID: &ownerID}
	if err := repo.CreateTeam(ctx, rejected); !errors.Is(err, wantFailure) {
		t.Fatalf("create rejected team error = %v, want %v", err, wantFailure)
	}
	var rejectedCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM teams WHERE name = 'Rejected Team'`).
		Scan(&rejectedCount); err != nil {
		t.Fatalf("count rejected teams: %v", err)
	}
	if rejectedCount != 0 || len(afterIDs) != 3 {
		t.Fatalf("rejected team state = rows %d, after calls %d", rejectedCount, len(afterIDs))
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
