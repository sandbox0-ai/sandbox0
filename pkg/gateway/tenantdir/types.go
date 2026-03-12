package tenantdir

import (
	"context"
	"time"
)

// TeamHomeRegion is the canonical team-to-region binding.
type TeamHomeRegion struct {
	TeamID       string    `json:"team_id"`
	HomeRegionID string    `json:"home_region_id"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

// Region describes a routable region entry.
type Region struct {
	ID                string `json:"id"`
	DisplayName       string `json:"display_name,omitempty"`
	EdgeGatewayURL    string `json:"edge_gateway_url,omitempty"`
	MeteringExportURL string `json:"metering_export_url,omitempty"`
	Enabled           bool   `json:"enabled"`
}

// ActiveTeam captures the resolved team and its regional ownership.
type ActiveTeam struct {
	UserID         string `json:"user_id"`
	TeamID         string `json:"team_id"`
	TeamRole       string `json:"team_role,omitempty"`
	HomeRegionID   string `json:"home_region_id"`
	DefaultTeam    bool   `json:"default_team,omitempty"`
	EdgeGatewayURL string `json:"edge_gateway_url,omitempty"`
}

// RoutingToken represents a short-lived routing grant from a global directory.
type RoutingToken struct {
	RegionID string    `json:"region_id"`
	TeamID   string    `json:"team_id"`
	UserID   string    `json:"user_id,omitempty"`
	Expiry   time.Time `json:"expiry"`
}

// Resolver resolves active team and region ownership.
type Resolver interface {
	ResolveActiveTeam(ctx context.Context, userID, teamID string) (*ActiveTeam, error)
	GetTeamHomeRegion(ctx context.Context, teamID string) (*TeamHomeRegion, error)
	GetRegion(ctx context.Context, regionID string) (*Region, error)
}
