package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidKey    = errors.New("invalid api key")
	ErrExpiredKey    = errors.New("api key expired")
	ErrInactiveKey   = errors.New("api key inactive")
	ErrQuotaExceeded = errors.New("quota exceeded")
)

// Repository provides database access for internal-gateway
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new database repository
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Pool returns the underlying connection pool
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

// ValidateAPIKey validates an API key and returns the associated auth context
func (r *Repository) ValidateAPIKey(ctx context.Context, keyValue string) (*APIKey, error) {
	// API key format: sb0_<team_id>_<random_secret>
	if !strings.HasPrefix(keyValue, "sb0_") {
		return nil, ErrInvalidKey
	}

	var key APIKey
	var rolesJSON []byte

	err := r.pool.QueryRow(ctx, `
		SELECT id, key_value, team_id, created_by, name, type, roles, 
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE key_value = $1
	`, keyValue).Scan(
		&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy,
		&key.Name, &key.Type, &rolesJSON, &key.IsActive,
		&key.ExpiresAt, &key.LastUsed, &key.UsageCount,
		&key.CreatedAt, &key.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvalidKey
		}
		return nil, fmt.Errorf("query api key: %w", err)
	}

	// Parse roles
	if len(rolesJSON) > 0 {
		if err := json.Unmarshal(rolesJSON, &key.Roles); err != nil {
			return nil, fmt.Errorf("parse roles: %w", err)
		}
	}

	// Check if key is active
	if !key.IsActive {
		return nil, ErrInactiveKey
	}

	// Check if key is expired
	if time.Now().After(key.ExpiresAt) {
		return nil, ErrExpiredKey
	}

	// Update usage statistics (fire and forget)
	go func() {
		_, _ = r.pool.Exec(context.Background(), `
			UPDATE api_keys 
			SET last_used_at = NOW(), usage_count = usage_count + 1
			WHERE id = $1
		`, key.ID)
	}()

	return &key, nil
}

// GetTeam retrieves a team by ID
func (r *Repository) GetTeam(ctx context.Context, teamID string) (*Team, error) {
	var team Team

	err := r.pool.QueryRow(ctx, `
		SELECT id, name, quota, is_active, created_at, updated_at
		FROM teams
		WHERE id = $1
	`, teamID).Scan(
		&team.ID, &team.Name, &team.Quota, &team.IsActive,
		&team.CreatedAt, &team.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query team: %w", err)
	}

	return &team, nil
}

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
