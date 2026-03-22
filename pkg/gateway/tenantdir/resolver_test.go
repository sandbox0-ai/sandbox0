package tenantdir

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
)

type mockIdentityStore struct {
	users   map[string]*identity.User
	teams   map[string]*identity.Team
	members map[string]*identity.TeamMember
}

func (m *mockIdentityStore) GetUserByID(_ context.Context, id string) (*identity.User, error) {
	user, ok := m.users[id]
	if !ok {
		return nil, identity.ErrUserNotFound
	}
	return user, nil
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

func TestDirectoryResolveActiveTeamUsesDefaultTeam(t *testing.T) {
	defaultTeamID := "team-1"
	homeRegionID := "aws/us-east-1"
	store := &mockIdentityStore{
		users: map[string]*identity.User{
			"user-1": {
				ID:            "user-1",
				DefaultTeamID: &defaultTeamID,
			},
		},
		teams: map[string]*identity.Team{
			defaultTeamID: {
				ID:           defaultTeamID,
				HomeRegionID: &homeRegionID,
				UpdatedAt:    time.Unix(100, 0),
			},
		},
		members: map[string]*identity.TeamMember{
			defaultTeamID + ":user-1": {
				TeamID: defaultTeamID,
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
	active, err := resolver.ResolveActiveTeam(context.Background(), "user-1", "")
	if err != nil {
		t.Fatalf("resolve active team: %v", err)
	}
	if !active.DefaultTeam {
		t.Fatal("expected default team resolution")
	}
	if active.TeamID != defaultTeamID {
		t.Fatalf("expected team %q, got %q", defaultTeamID, active.TeamID)
	}
	if active.HomeRegionID != homeRegionID {
		t.Fatalf("expected region %q, got %q", homeRegionID, active.HomeRegionID)
	}
	if active.RegionalGatewayURL != "https://use1.example.com" {
		t.Fatalf("expected regional gateway URL to be propagated, got %q", active.RegionalGatewayURL)
	}
}

func TestDirectoryResolveActiveTeamRequiresSelectedOrDefaultTeam(t *testing.T) {
	resolver := NewResolver(&mockIdentityStore{
		users: map[string]*identity.User{
			"user-1": {ID: "user-1"},
		},
	}, nil)

	_, err := resolver.ResolveActiveTeam(context.Background(), "user-1", "")
	if !errors.Is(err, ErrNoActiveTeam) {
		t.Fatalf("expected ErrNoActiveTeam, got %v", err)
	}
}

func TestDirectoryGetRegionReturnsNotFoundWithoutDirectory(t *testing.T) {
	resolver := NewResolver(&mockIdentityStore{}, nil)

	_, err := resolver.GetRegion(context.Background(), "aws/us-east-1")
	if !errors.Is(err, ErrRegionNotFound) {
		t.Fatalf("expected ErrRegionNotFound, got %v", err)
	}
}
