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
	ID                 string `json:"id"`
	DisplayName        string `json:"display_name,omitempty"`
	RegionalGatewayURL string `json:"regional_gateway_url,omitempty"`
	MeteringExportURL  string `json:"metering_export_url,omitempty"`
	Enabled            bool   `json:"enabled"`
}

// TeamAccess captures explicit team access and routing information.
type TeamAccess struct {
	UserID             string `json:"user_id"`
	TeamID             string `json:"team_id"`
	TeamRole           string `json:"team_role,omitempty"`
	HomeRegionID       string `json:"home_region_id"`
	RegionalGatewayURL string `json:"regional_gateway_url,omitempty"`
}

// RoutingToken represents a short-lived routing grant from a global gateway.
type RoutingToken struct {
	RegionID string    `json:"region_id"`
	TeamID   string    `json:"team_id"`
	UserID   string    `json:"user_id,omitempty"`
	Expiry   time.Time `json:"expiry"`
}

// Resolver resolves explicit team access and region ownership.
type Resolver interface {
	ResolveTeamAccess(ctx context.Context, userID, teamID string) (*TeamAccess, error)
	GetTeamHomeRegion(ctx context.Context, teamID string) (*TeamHomeRegion, error)
	GetRegion(ctx context.Context, regionID string) (*Region, error)
}
