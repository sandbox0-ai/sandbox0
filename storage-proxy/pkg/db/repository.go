package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("not found")
)

// Repository provides database access for storage-proxy
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

// CreateSandboxVolume creates a new sandbox volume record
func (r *Repository) CreateSandboxVolume(ctx context.Context, volume *SandboxVolume) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO sandbox_volumes (
			id, team_id, user_id,
			cache_size, prefetch, buffer_size, writeback, read_only,
			created_at, updated_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6, $7, $8,
			$9, $10
		)
	`,
		volume.ID, volume.TeamID, volume.UserID,
		volume.CacheSize, volume.Prefetch, volume.BufferSize, volume.Writeback, volume.ReadOnly,
		volume.CreatedAt, volume.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("create sandbox volume: %w", err)
	}

	return nil
}

// GetSandboxVolume retrieves a sandbox volume by ID
func (r *Repository) GetSandboxVolume(ctx context.Context, id string) (*SandboxVolume, error) {
	var v SandboxVolume

	err := r.pool.QueryRow(ctx, `
		SELECT
			id, team_id, user_id,
			cache_size, prefetch, buffer_size, writeback, read_only,
			created_at, updated_at
		FROM sandbox_volumes
		WHERE id = $1
	`, id).Scan(
		&v.ID, &v.TeamID, &v.UserID,
		&v.CacheSize, &v.Prefetch, &v.BufferSize, &v.Writeback, &v.ReadOnly,
		&v.CreatedAt, &v.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query sandbox volume: %w", err)
	}

	return &v, nil
}

// UpdateSandboxVolume updates an existing sandbox volume
func (r *Repository) UpdateSandboxVolume(ctx context.Context, volume *SandboxVolume) error {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE sandbox_volumes SET
			cache_size = $2,
			prefetch = $3,
			buffer_size = $4,
			writeback = $5,
			read_only = $6,
			updated_at = NOW()
		WHERE id = $1
	`,
		volume.ID,
		volume.CacheSize, volume.Prefetch, volume.BufferSize, volume.Writeback, volume.ReadOnly,
	)

	if err != nil {
		return fmt.Errorf("update sandbox volume: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// ListSandboxVolumesByTeam retrieves all volumes for a team
func (r *Repository) ListSandboxVolumesByTeam(ctx context.Context, teamID string) ([]*SandboxVolume, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			id, team_id, user_id,
			cache_size, prefetch, buffer_size, writeback, read_only,
			created_at, updated_at
		FROM sandbox_volumes
		WHERE team_id = $1
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("query sandbox volumes: %w", err)
	}
	defer rows.Close()

	var volumes []*SandboxVolume
	for rows.Next() {
		var v SandboxVolume
		err := rows.Scan(
			&v.ID, &v.TeamID, &v.UserID,
			&v.CacheSize, &v.Prefetch, &v.BufferSize, &v.Writeback, &v.ReadOnly,
			&v.CreatedAt, &v.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan sandbox volume: %w", err)
		}
		volumes = append(volumes, &v)
	}

	return volumes, nil
}
