package tenantdir

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
)

type mockIdentityStore struct {
	teams   map[string]*identity.Team
	members map[string]*identity.TeamMember
}

func (m *mockIdentityStore) GetTeamByID(_ context.Context, id string) (*identity.Team, error) {
	team, ok := m.teams[id]
	if !ok {
		return nil, identity.ErrTeamNotFound
	}
	return team, nil
}

func (m *mockIdentityStore) GetTeamMember(_ context.Context, teamID, userID string) (*identity.TeamMember, error) {
	member, ok := m.members[teamID+":"+userID]
	if !ok {
		return nil, identity.ErrMemberNotFound
	}
	return member, nil
}

func TestDirectoryResolveTeamAccessUsesExplicitTeam(t *testing.T) {
	teamID := "team-1"
	homeRegionID := "aws-us-east-1"
	store := &mockIdentityStore{
		teams: map[string]*identity.Team{
			teamID: {
				ID:           teamID,
				HomeRegionID: &homeRegionID,
				UpdatedAt:    time.Unix(100, 0),
			},
		},
		members: map[string]*identity.TeamMember{
			teamID + ":user-1": {
				TeamID: teamID,
				UserID: "user-1",
				Role:   "admin",
			},
		},
	}
	regions := NewStaticRegions([]Region{{
		ID:                 homeRegionID,
		RegionalGatewayURL: "https://use1.example.com",
		Enabled:            true,
	}})

	resolver := NewResolver(store, regions)
	active, err := resolver.ResolveTeamAccess(context.Background(), "user-1", teamID)
	if err != nil {
		t.Fatalf("resolve team access: %v", err)
	}
	if active.TeamID != teamID {
		t.Fatalf("expected team %q, got %q", teamID, active.TeamID)
	}
	if active.HomeRegionID != homeRegionID {
		t.Fatalf("expected region %q, got %q", homeRegionID, active.HomeRegionID)
	}
	if active.RegionalGatewayURL != "https://use1.example.com" {
		t.Fatalf("expected regional gateway URL to be propagated, got %q", active.RegionalGatewayURL)
	}
}

func TestDirectoryResolveTeamAccessRequiresTeamID(t *testing.T) {
	resolver := NewResolver(&mockIdentityStore{}, nil)

	_, err := resolver.ResolveTeamAccess(context.Background(), "user-1", "")
	if !errors.Is(err, ErrTeamRequired) {
		t.Fatalf("expected ErrTeamRequired, got %v", err)
	}
}

func TestDirectoryGetRegionReturnsNotFoundWithoutDirectory(t *testing.T) {
	resolver := NewResolver(&mockIdentityStore{}, nil)

	_, err := resolver.GetRegion(context.Background(), "aws-us-east-1")
	if !errors.Is(err, ErrRegionNotFound) {
		t.Fatalf("expected ErrRegionNotFound, got %v", err)
	}
}
