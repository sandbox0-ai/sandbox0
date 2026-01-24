package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CreateAPIKey creates a new API key
func (r *Repository) CreateAPIKey(ctx context.Context, teamID, userID, name, keyType string, roles []string, expiresAt time.Time) (*APIKey, string, error) {
	// Generate API key: s0_<team_id_prefix>_<random>
	teamPrefix := teamID
	if len(teamPrefix) > 8 {
		teamPrefix = teamPrefix[:8]
	}

	randomBytes := make([]byte, 24)
	if _, err := rand.Read(randomBytes); err != nil {
		return nil, "", fmt.Errorf("generate random: %w", err)
	}
	keyValue := fmt.Sprintf("s0_%s_%s", teamPrefix, hex.EncodeToString(randomBytes))

	id := uuid.New().String()
	rolesJSON, err := json.Marshal(roles)
	if err != nil {
		return nil, "", fmt.Errorf("marshal roles: %w", err)
	}

	var key APIKey
	err = r.pool.QueryRow(ctx, `
		INSERT INTO api_keys (id, key_value, team_id, created_by, name, type, roles, is_active, expires_at, user_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8, $9)
		RETURNING id, key_value, team_id, created_by, name, type, roles, is_active, expires_at, last_used_at, usage_count, created_at, updated_at
	`, id, keyValue, teamID, userID, name, keyType, rolesJSON, expiresAt, userID,
	).Scan(
		&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
		&key.Type, &rolesJSON, &key.IsActive, &key.ExpiresAt,
		&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
	)
	if err != nil {
		return nil, "", fmt.Errorf("insert api key: %w", err)
	}

	if err := json.Unmarshal(rolesJSON, &key.Roles); err != nil {
		return nil, "", fmt.Errorf("unmarshal roles: %w", err)
	}

	return &key, keyValue, nil
}

// GetAPIKeysByTeamID retrieves all API keys for a team
func (r *Repository) GetAPIKeysByTeamID(ctx context.Context, teamID string) ([]*APIKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, key_value, team_id, created_by, name, type, roles, 
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE team_id = $1
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("query api keys: %w", err)
	}
	defer rows.Close()

	var keys []*APIKey
	for rows.Next() {
		var key APIKey
		var rolesJSON []byte
		if err := rows.Scan(
			&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
			&key.Type, &rolesJSON, &key.IsActive, &key.ExpiresAt,
			&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		if len(rolesJSON) > 0 {
			_ = json.Unmarshal(rolesJSON, &key.Roles)
		}
		// Mask the key value for security
		key.KeyValue = maskAPIKey(key.KeyValue)
		keys = append(keys, &key)
	}

	return keys, nil
}

// GetAPIKeysByUserID retrieves all API keys created by a user
func (r *Repository) GetAPIKeysByUserID(ctx context.Context, userID string) ([]*APIKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, key_value, team_id, created_by, name, type, roles, 
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("query api keys: %w", err)
	}
	defer rows.Close()

	var keys []*APIKey
	for rows.Next() {
		var key APIKey
		var rolesJSON []byte
		if err := rows.Scan(
			&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
			&key.Type, &rolesJSON, &key.IsActive, &key.ExpiresAt,
			&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		if len(rolesJSON) > 0 {
			_ = json.Unmarshal(rolesJSON, &key.Roles)
		}
		// Mask the key value for security
		key.KeyValue = maskAPIKey(key.KeyValue)
		keys = append(keys, &key)
	}

	return keys, nil
}

// DeleteAPIKey deletes an API key
func (r *Repository) DeleteAPIKey(ctx context.Context, id string) error {
	result, err := r.pool.Exec(ctx, `DELETE FROM api_keys WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete api key: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// DeactivateAPIKey deactivates an API key
func (r *Repository) DeactivateAPIKey(ctx context.Context, id string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE api_keys SET is_active = false WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("deactivate api key: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// GetAPIKeyByID retrieves an API key by ID (for ownership checks)
func (r *Repository) GetAPIKeyByID(ctx context.Context, id string) (*APIKey, error) {
	var key APIKey
	var rolesJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT id, key_value, team_id, created_by, name, type, roles, 
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE id = $1
	`, id).Scan(
		&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
		&key.Type, &rolesJSON, &key.IsActive, &key.ExpiresAt,
		&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("query api key: %w", err)
	}

	if len(rolesJSON) > 0 {
		_ = json.Unmarshal(rolesJSON, &key.Roles)
	}
	key.KeyValue = maskAPIKey(key.KeyValue)

	return &key, nil
}

// ValidateAPIKey validates an API key and returns the associated auth context
func (r *Repository) ValidateAPIKey(ctx context.Context, keyValue string) (*APIKey, error) {
	// API key format: s0_<team_id>_<random_secret>
	if !strings.HasPrefix(keyValue, "s0_") {
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

// maskAPIKey masks an API key for display
func maskAPIKey(key string) string {
	if len(key) <= 12 {
		return "***"
	}
	return key[:12] + "***"
}
