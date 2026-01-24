package db

import (
	"encoding/json"
	"time"
)

// User represents a user in the system
type User struct {
	ID            string    `json:"id"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	AvatarURL     string    `json:"avatar_url,omitempty"`
	PasswordHash  string    `json:"-"` // Never serialize password hash
	DefaultTeamID *string   `json:"default_team_id,omitempty"`
	EmailVerified bool      `json:"email_verified"`
	IsAdmin       bool      `json:"is_admin"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Team represents a team/organization
type Team struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	OwnerID   *string   `json:"owner_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TeamMember represents a team membership
type TeamMember struct {
	ID       string    `json:"id"`
	TeamID   string    `json:"team_id"`
	UserID   string    `json:"user_id"`
	Role     string    `json:"role"`
	JoinedAt time.Time `json:"joined_at"`
}

// UserIdentity represents an OIDC identity mapping
type UserIdentity struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	Provider  string          `json:"provider"`
	Subject   string          `json:"subject"`
	RawClaims json.RawMessage `json:"raw_claims,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// RefreshToken represents a refresh token for session management
type RefreshToken struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	TokenHash string    `json:"-"` // Never serialize
	ExpiresAt time.Time `json:"expires_at"`
	Revoked   bool      `json:"revoked"`
	CreatedAt time.Time `json:"created_at"`
}

// APIKey represents an API key stored in the database
type APIKey struct {
	ID         string    `json:"id"`
	KeyValue   string    `json:"key_value"`
	TeamID     string    `json:"team_id"`
	UserID     *string   `json:"user_id,omitempty"`
	CreatedBy  string    `json:"created_by"`
	Name       string    `json:"name"`
	Type       string    `json:"type"` // 'user', 'service', 'internal'
	Roles      []string  `json:"roles"`
	IsActive   bool      `json:"is_active"`
	ExpiresAt  time.Time `json:"expires_at"`
	LastUsed   time.Time `json:"last_used_at"`
	UsageCount int64     `json:"usage_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
