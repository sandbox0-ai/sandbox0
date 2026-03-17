package egressauth

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BindingStore is the shared manager/broker contract for effective sandbox bindings.
type BindingStore interface {
	GetBindings(ctx context.Context, clusterID, sandboxID string) (*BindingRecord, error)
	UpsertBindings(ctx context.Context, record *BindingRecord) error
	DeleteBindings(ctx context.Context, clusterID, sandboxID string) error
}

// Repository persists effective credential bindings in PostgreSQL.
type Repository struct {
	db   DB
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		db:   pool,
		pool: pool,
	}
}

func (r *Repository) Pool() *pgxpool.Pool {
	if r == nil {
		return nil
	}
	return r.pool
}

func (r *Repository) GetBindings(ctx context.Context, clusterID, sandboxID string) (*BindingRecord, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if clusterID == "" {
		return nil, fmt.Errorf("cluster_id is required")
	}
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}

	var (
		record       BindingRecord
		bindingsJSON []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT cluster_id, sandbox_id, team_id, bindings, updated_at
		FROM sandbox_egress_credential_bindings
		WHERE cluster_id = $1 AND sandbox_id = $2
	`, clusterID, sandboxID).Scan(
		&record.ClusterID,
		&record.SandboxID,
		&record.TeamID,
		&bindingsJSON,
		&record.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get bindings: %w", err)
	}
	if len(bindingsJSON) > 0 {
		if err := json.Unmarshal(bindingsJSON, &record.Bindings); err != nil {
			return nil, fmt.Errorf("unmarshal bindings: %w", err)
		}
	}
	return &record, nil
}

func (r *Repository) UpsertBindings(ctx context.Context, record *BindingRecord) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if record == nil {
		return fmt.Errorf("binding record is nil")
	}
	if record.ClusterID == "" {
		return fmt.Errorf("cluster_id is required")
	}
	if record.SandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}

	bindingsJSON, err := json.Marshal(record.Bindings)
	if err != nil {
		return fmt.Errorf("marshal bindings: %w", err)
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now().UTC()
	}

	_, err = r.db.Exec(ctx, `
		INSERT INTO sandbox_egress_credential_bindings (
			cluster_id, sandbox_id, team_id, bindings, updated_at
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (cluster_id, sandbox_id)
		DO UPDATE SET
			team_id = EXCLUDED.team_id,
			bindings = EXCLUDED.bindings,
			updated_at = EXCLUDED.updated_at
	`, record.ClusterID, record.SandboxID, record.TeamID, bindingsJSON, record.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert bindings: %w", err)
	}
	return nil
}

func (r *Repository) DeleteBindings(ctx context.Context, clusterID, sandboxID string) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if clusterID == "" {
		return fmt.Errorf("cluster_id is required")
	}
	if sandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}

	if _, err := r.db.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE cluster_id = $1 AND sandbox_id = $2
	`, clusterID, sandboxID); err != nil {
		return fmt.Errorf("delete bindings: %w", err)
	}
	return nil
}
