package identity

import (
	"encoding/json"
	"time"
)

// User represents a user in the system.
type User struct {
	ID            string    `json:"id"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	AvatarURL     string    `json:"avatar_url,omitempty"`
	PasswordHash  string    `json:"-"`
	EmailVerified bool      `json:"email_verified"`
	IsAdmin       bool      `json:"is_admin"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Team represents a team/organization.
type Team struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Slug         string    `json:"slug"`
	OwnerID      *string   `json:"owner_id,omitempty"`
	HomeRegionID *string   `json:"home_region_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// TeamMember represents a team membership.
type TeamMember struct {
	ID       string    `json:"id"`
	TeamID   string    `json:"team_id"`
	UserID   string    `json:"user_id"`
	Role     string    `json:"role"`
	JoinedAt time.Time `json:"joined_at"`
}

// TeamMemberWithUser combines membership and user details.
type TeamMemberWithUser struct {
	ID            string    `json:"id"`
	TeamID        string    `json:"team_id"`
	UserID        string    `json:"user_id"`
	Role          string    `json:"role"`
	JoinedAt      time.Time `json:"joined_at"`
	UserID2       string    `json:"user_id2"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	AvatarURL     string    `json:"avatar_url"`
	EmailVerified bool      `json:"email_verified"`
	IsAdmin       bool      `json:"is_admin"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// TeamGrantRecord stores the data needed to issue JWT team grants.
type TeamGrantRecord struct {
	TeamID       string  `json:"team_id"`
	TeamRole     string  `json:"team_role"`
	HomeRegionID *string `json:"home_region_id,omitempty"`
}

// UserIdentity represents an OIDC identity mapping.
type UserIdentity struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	Provider  string          `json:"provider"`
	Subject   string          `json:"subject"`
	RawClaims json.RawMessage `json:"raw_claims,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// RefreshToken represents a refresh token for session management.
type RefreshToken struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	TokenHash string    `json:"-"`
	ExpiresAt time.Time `json:"expires_at"`
	Revoked   bool      `json:"revoked"`
	CreatedAt time.Time `json:"created_at"`
}

// DeviceAuthSession stores a pending device authorization login.
type DeviceAuthSession struct {
	ID                      string     `json:"id"`
	Provider                string     `json:"provider"`
	DeviceCode              string     `json:"-"`
	UserCode                string     `json:"user_code"`
	VerificationURI         string     `json:"verification_uri"`
	VerificationURIComplete string     `json:"verification_uri_complete,omitempty"`
	IntervalSeconds         int        `json:"interval_seconds"`
	ExpiresAt               time.Time  `json:"expires_at"`
	ConsumedAt              *time.Time `json:"consumed_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

// WebLoginCode stores a one-time browser login handoff code.
type WebLoginCode struct {
	ID         string     `json:"id"`
	CodeHash   string     `json:"-"`
	UserID     string     `json:"user_id"`
	ReturnURL  string     `json:"return_url"`
	ExpiresAt  time.Time  `json:"expires_at"`
	ConsumedAt *time.Time `json:"consumed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}
