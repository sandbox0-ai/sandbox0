package tenantdir

import (
	"context"
	"errors"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
)

var (
	ErrTeamRequired   = errors.New("team_id is required")
	ErrRegionNotFound = errors.New("region not found")
)

type identityStore interface {
	GetTeamByID(ctx context.Context, id string) (*identity.Team, error)
	GetTeamMember(ctx context.Context, teamID, userID string) (*identity.TeamMember, error)
}

type regionLookup interface {
	GetRegion(ctx context.Context, regionID string) (*Region, error)
}

// Directory resolves team ownership and optional region metadata.
type Directory struct {
	identities identityStore
	regions    regionLookup
}

// NewResolver creates a tenant directory resolver backed by identity data.
func NewResolver(identities identityStore, regions regionLookup) *Directory {
	return &Directory{
		identities: identities,
		regions:    regions,
	}
}

// ResolveTeamAccess resolves explicit team access and routing information.
func (d *Directory) ResolveTeamAccess(ctx context.Context, userID, teamID string) (*TeamAccess, error) {
	resolvedTeamID := strings.TrimSpace(teamID)
	if resolvedTeamID == "" {
		return nil, ErrTeamRequired
	}

	member, err := d.identities.GetTeamMember(ctx, resolvedTeamID, userID)
	if err != nil {
		return nil, err
	}

	teamHomeRegion, err := d.GetTeamHomeRegion(ctx, resolvedTeamID)
	if err != nil {
		return nil, err
	}

	access := &TeamAccess{
		UserID:       userID,
		TeamID:       resolvedTeamID,
		TeamRole:     member.Role,
		HomeRegionID: teamHomeRegion.HomeRegionID,
	}
	if access.HomeRegionID == "" {
		return access, nil
	}

	region, err := d.GetRegion(ctx, access.HomeRegionID)
	if err != nil {
		if errors.Is(err, ErrRegionNotFound) {
			return access, nil
		}
		return nil, err
	}
	access.RegionalGatewayURL = region.RegionalGatewayURL
	return access, nil
}

// GetTeamHomeRegion returns the canonical team-to-region binding.
func (d *Directory) GetTeamHomeRegion(ctx context.Context, teamID string) (*TeamHomeRegion, error) {
	team, err := d.identities.GetTeamByID(ctx, teamID)
	if err != nil {
		return nil, err
	}

	homeRegionID := ""
	if team.HomeRegionID != nil {
		homeRegionID = strings.TrimSpace(*team.HomeRegionID)
	}

	return &TeamHomeRegion{
		TeamID:       team.ID,
		HomeRegionID: homeRegionID,
		UpdatedAt:    team.UpdatedAt,
	}, nil
}

// GetRegion returns a region entry from the optional directory.
func (d *Directory) GetRegion(ctx context.Context, regionID string) (*Region, error) {
	resolvedRegionID := strings.TrimSpace(regionID)
	if resolvedRegionID == "" || d.regions == nil {
		return nil, ErrRegionNotFound
	}
	region, err := d.regions.GetRegion(ctx, resolvedRegionID)
	if err != nil {
		return nil, err
	}
	return region, nil
}

// StaticRegions is an in-memory region directory useful for tests and bootstrap flows.
type StaticRegions struct {
	entries map[string]Region
}

// NewStaticRegions creates a static region lookup.
func NewStaticRegions(regions []Region) *StaticRegions {
	entries := make(map[string]Region, len(regions))
	for _, region := range regions {
		region.ID = strings.TrimSpace(region.ID)
		entries[region.ID] = region
	}
	return &StaticRegions{entries: entries}
}

// GetRegion returns a region by ID.
func (s *StaticRegions) GetRegion(_ context.Context, regionID string) (*Region, error) {
	region, ok := s.entries[regionID]
	if !ok {
		return nil, ErrRegionNotFound
	}
	copied := region
	return &copied, nil
}
