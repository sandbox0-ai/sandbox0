package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

const bootstrapBatchSize = 500
const clickHouseBootstrapSource = "clickhouse_projection_state_v1"

// ProjectionStateSource exposes only the current ClickHouse producer state
// needed to move an existing installation onto the PostgreSQL outbox.
type ProjectionStateSource interface {
	ListActiveSandboxProjectionStates(context.Context) ([]*metering.SandboxProjectionState, error)
	ListActiveStorageProjectionStates(context.Context) ([]*metering.StorageProjectionState, error)
}

type BootstrapResult struct {
	SandboxStates int64
	StorageStates int64
}

// BootstrapProjectionStates copies current producer state, not historical
// usage, from ClickHouse. PostgreSQL rows that are equally new or newer win.
func (r *Repository) BootstrapProjectionStates(ctx context.Context, source ProjectionStateSource) (*BootstrapResult, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("metering outbox pool is not configured")
	}
	if source == nil {
		return nil, fmt.Errorf("metering projection state source is not configured")
	}
	sandboxStates, err := source.ListActiveSandboxProjectionStates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list active sandbox projection states: %w", err)
	}
	storageStates, err := source.ListActiveStorageProjectionStates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list active storage projection states: %w", err)
	}
	result := &BootstrapResult{}
	for start := 0; start < len(sandboxStates); start += bootstrapBatchSize {
		end := min(start+bootstrapBatchSize, len(sandboxStates))
		if err := r.InTx(ctx, func(tx pgx.Tx) error {
			for _, state := range sandboxStates[start:end] {
				if state == nil || state.SandboxID == "" || state.Namespace == "" || state.LastObservedAt.IsZero() {
					return fmt.Errorf("invalid active sandbox projection state")
				}
				tag, err := tx.Exec(ctx, bootstrapSandboxStateSQL,
					state.SandboxID, state.Namespace, state.TeamID, state.UserID, state.TemplateID, state.ClusterID,
					state.OwnerKind, state.ResourceMillicpu, state.ResourceMemoryMiB,
					state.ClaimedAt, state.ActiveSince, state.Paused, state.PausedAt, state.TerminatedAt,
					state.LastObservedAt.UTC(), state.LastResourceVer,
				)
				if err != nil {
					return fmt.Errorf("bootstrap sandbox projection state %q: %w", state.SandboxID, err)
				}
				result.SandboxStates += tag.RowsAffected()
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	for start := 0; start < len(storageStates); start += bootstrapBatchSize {
		end := min(start+bootstrapBatchSize, len(storageStates))
		if err := r.InTx(ctx, func(tx pgx.Tx) error {
			for _, state := range storageStates[start:end] {
				if state == nil || state.SubjectType == "" || state.SubjectID == "" || state.ObservedAt.IsZero() {
					return fmt.Errorf("invalid active storage projection state")
				}
				tag, err := tx.Exec(ctx, bootstrapStorageStateSQL,
					state.SubjectType, state.SubjectID, state.Product, state.OwnerKind,
					state.TeamID, state.UserID, nullableString(state.SandboxID), nullableString(state.VolumeID), nullableString(state.SnapshotID),
					nullableString(state.ClusterID), state.RegionID, state.SizeBytes, state.ObservedAt.UTC(), state.UnbilledByteNanoseconds,
				)
				if err != nil {
					return fmt.Errorf("bootstrap storage projection state %q/%q: %w", state.SubjectType, state.SubjectID, err)
				}
				result.StorageStates += tag.RowsAffected()
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if _, err := r.pool.Exec(ctx, `
		INSERT INTO metering.projection_bootstrap (source, completed_at)
		VALUES ($1, NOW())
		ON CONFLICT (source) DO UPDATE SET completed_at = EXCLUDED.completed_at
	`, clickHouseBootstrapSource); err != nil {
		return nil, fmt.Errorf("mark ClickHouse projection state bootstrap complete: %w", err)
	}
	return result, nil
}

func (r *Repository) ProjectionBootstrapCompleted(ctx context.Context) (bool, error) {
	if r == nil || r.pool == nil {
		return false, fmt.Errorf("metering outbox pool is not configured")
	}
	var completed bool
	if err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM metering.projection_bootstrap
			WHERE source = $1
		)
	`, clickHouseBootstrapSource).Scan(&completed); err != nil {
		return false, fmt.Errorf("query ClickHouse projection state bootstrap: %w", err)
	}
	return completed, nil
}

const bootstrapSandboxStateSQL = `
	INSERT INTO metering.manager_sandbox_projection_state (
		sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
		owner_kind, resource_millicpu, resource_memory_mib,
		claimed_at, active_since, paused, paused_at, terminated_at,
		last_observed_at, last_resource_version
	) VALUES (
		$1, $2, $3, $4, $5, $6,
		$7, $8, $9,
		$10, $11, $12, $13, $14,
		$15, $16
	)
	ON CONFLICT (sandbox_id) DO UPDATE
	SET namespace = EXCLUDED.namespace,
		team_id = EXCLUDED.team_id,
		user_id = EXCLUDED.user_id,
		template_id = EXCLUDED.template_id,
		cluster_id = EXCLUDED.cluster_id,
		owner_kind = EXCLUDED.owner_kind,
		resource_millicpu = EXCLUDED.resource_millicpu,
		resource_memory_mib = EXCLUDED.resource_memory_mib,
		claimed_at = EXCLUDED.claimed_at,
		active_since = EXCLUDED.active_since,
		paused = EXCLUDED.paused,
		paused_at = EXCLUDED.paused_at,
		terminated_at = EXCLUDED.terminated_at,
		last_observed_at = EXCLUDED.last_observed_at,
		last_resource_version = EXCLUDED.last_resource_version
	WHERE metering.manager_sandbox_projection_state.last_observed_at < EXCLUDED.last_observed_at
`

const bootstrapStorageStateSQL = `
	INSERT INTO metering.storage_projection_state (
		subject_type, subject_id, product, owner_kind,
		team_id, user_id, sandbox_id, volume_id, snapshot_id,
		cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds
	) VALUES (
		$1, $2, $3, $4,
		$5, $6, $7, $8, $9,
		$10, $11, $12, $13, $14
	)
	ON CONFLICT (subject_type, subject_id) DO UPDATE
	SET product = EXCLUDED.product,
		owner_kind = EXCLUDED.owner_kind,
		team_id = EXCLUDED.team_id,
		user_id = EXCLUDED.user_id,
		sandbox_id = EXCLUDED.sandbox_id,
		volume_id = EXCLUDED.volume_id,
		snapshot_id = EXCLUDED.snapshot_id,
		cluster_id = EXCLUDED.cluster_id,
		region_id = EXCLUDED.region_id,
		size_bytes = EXCLUDED.size_bytes,
		observed_at = EXCLUDED.observed_at,
		unbilled_byte_nanoseconds = EXCLUDED.unbilled_byte_nanoseconds,
		updated_at = NOW()
	WHERE metering.storage_projection_state.observed_at < EXCLUDED.observed_at
`
