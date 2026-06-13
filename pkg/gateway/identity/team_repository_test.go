package identity

import (
	"context"
	"errors"
	"testing"
)

func TestTeamRepositoryScopesSlugUniquenessToOwner(t *testing.T) {
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

	duplicate := &Team{Name: "Duplicate", Slug: "gcp-us-east-4", OwnerID: &ownerAID}
	if err := repo.CreateTeam(ctx, duplicate); !errors.Is(err, ErrTeamAlreadyExists) {
		t.Fatalf("duplicate owner slug error = %v, want %v", err, ErrTeamAlreadyExists)
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
