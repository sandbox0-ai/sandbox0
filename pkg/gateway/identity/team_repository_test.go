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
