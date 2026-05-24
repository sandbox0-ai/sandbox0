package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateArtifactTx creates an artifact metadata record within a transaction.
func (r *Repository) CreateArtifactTx(ctx context.Context, tx pgx.Tx, artifact *Artifact) error {
	return r.createArtifact(ctx, tx, artifact)
}

func (r *Repository) createArtifact(ctx context.Context, db DB, artifact *Artifact) error {
	if artifact.Metadata == nil {
		artifact.Metadata = json.RawMessage(`{}`)
	}
	_, err := db.Exec(ctx, `
		INSERT INTO artifacts (
			id, team_id, user_id, name, kind, media_type, digest,
			source_volume_id, snapshot_id, size_bytes, metadata,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11,
			$12, $13
		)
	`,
		artifact.ID, artifact.TeamID, artifact.UserID, artifact.Name, artifact.Kind, artifact.MediaType, artifact.Digest,
		artifact.SourceVolumeID, artifact.SnapshotID, artifact.SizeBytes, artifact.Metadata,
		artifact.CreatedAt, artifact.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create artifact: %w", err)
	}
	return nil
}

// ListArtifactsByTeam returns artifacts owned by one team.
func (r *Repository) ListArtifactsByTeam(ctx context.Context, teamID string) ([]*Artifact, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			id, team_id, user_id, name, kind, media_type, COALESCE(digest, ''),
			source_volume_id, snapshot_id, size_bytes, metadata,
			created_at, updated_at
		FROM artifacts
		WHERE team_id = $1
		ORDER BY created_at DESC, id DESC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("list artifacts: %w", err)
	}
	defer rows.Close()

	var artifacts []*Artifact
	for rows.Next() {
		artifact, err := scanArtifact(rows)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list artifacts rows: %w", err)
	}
	return artifacts, nil
}

// GetArtifact retrieves an artifact by ID.
func (r *Repository) GetArtifact(ctx context.Context, id string) (*Artifact, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT
			id, team_id, user_id, name, kind, media_type, COALESCE(digest, ''),
			source_volume_id, snapshot_id, size_bytes, metadata,
			created_at, updated_at
		FROM artifacts
		WHERE id = $1
	`, id)
	artifact, err := scanArtifact(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return artifact, nil
}

// DeleteArtifactTx deletes an artifact metadata record within a transaction.
func (r *Repository) DeleteArtifactTx(ctx context.Context, tx pgx.Tx, id string) error {
	cmd, err := tx.Exec(ctx, `DELETE FROM artifacts WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete artifact: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

type artifactScanner interface {
	Scan(dest ...any) error
}

func scanArtifact(row artifactScanner) (*Artifact, error) {
	var artifact Artifact
	var metadata []byte
	if err := row.Scan(
		&artifact.ID, &artifact.TeamID, &artifact.UserID, &artifact.Name, &artifact.Kind, &artifact.MediaType, &artifact.Digest,
		&artifact.SourceVolumeID, &artifact.SnapshotID, &artifact.SizeBytes, &metadata,
		&artifact.CreatedAt, &artifact.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan artifact: %w", err)
	}
	if len(metadata) == 0 {
		metadata = []byte(`{}`)
	}
	artifact.Metadata = append(json.RawMessage(nil), metadata...)
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = time.Now().UTC()
	}
	if artifact.UpdatedAt.IsZero() {
		artifact.UpdatedAt = artifact.CreatedAt
	}
	return &artifact, nil
}
