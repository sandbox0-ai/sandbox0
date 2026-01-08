package db

import (
	"encoding/json"
	"time"
)

// AuthMethod defines the authentication method
type AuthMethod string

const (
	AuthMethodAPIKey   AuthMethod = "api_key"
	AuthMethodJWT      AuthMethod = "jwt"
	AuthMethodInternal AuthMethod = "internal"
)

// APIKey represents an API key stored in the database
type APIKey struct {
	ID         string    `json:"id"`
	KeyValue   string    `json:"key_value"`
	TeamID     string    `json:"team_id"`
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

// Team represents a team in the system
type Team struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Quota     json.RawMessage `json:"quota"`
	IsActive  bool            `json:"is_active"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// User represents a user in the system
type User struct {
	ID            string    `json:"id"`
	ExternalID    string    `json:"external_id"`
	Provider      string    `json:"provider"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	PrimaryTeamID string    `json:"primary_team_id"`
	Roles         []string  `json:"roles"`
	Permissions   []string  `json:"permissions"`
	IsActive      bool      `json:"is_active"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}
