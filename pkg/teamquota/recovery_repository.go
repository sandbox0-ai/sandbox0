package teamquota

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultRecoveryPageLimit = 100
	maxRecoveryPageLimit     = 1000
)

var _ RecoveryStore = (*Repository)(nil)

// GetRecoveryAllocation returns one allocation by its region-unique logical
// owner. The caller's cluster_id is intentionally ignored: this read lets a
// shared regional catalog inspect an interrupted operation without stealing
// recovery ownership from the cluster recorded on the allocation.
func (r *Repository) GetRecoveryAllocation(
	ctx context.Context,
	owner Owner,
) (*RecoveryAllocation, error) {
	if r == nil || r.pool == nil {
		return nil, &UnavailableError{
			Operation: "get recovery allocation",
			Err:       fmt.Errorf("database pool is not configured"),
		}
	}
	owner = normalizeOwner(owner)
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	row := r.pool.QueryRow(ctx, `
		SELECT a.allocation_id, a.revision, a.team_id, a.owner_kind, a.owner_id,
			a.cluster_id, a.state, a.operation_id, a.operation_kind,
			a.operation_generation, a.operation_base_state,
			a.pod_namespace, a.pod_name, a.pod_uid,
			a.runtime_generation, a.reconcile_after,
			COALESCE(a.reconcile_after <= NOW(), FALSE), a.updated_at,
			COALESCE(
				jsonb_object_agg(i.quota_key, i.committed_value)
					FILTER (WHERE i.quota_key IS NOT NULL),
				'{}'::jsonb
			),
			COALESCE(
				jsonb_object_agg(i.quota_key, i.pending_value)
					FILTER (WHERE i.quota_key IS NOT NULL
						AND i.pending_value IS NOT NULL),
				'{}'::jsonb
			)
		FROM quota.allocations a
		LEFT JOIN quota.allocation_items i
			ON i.allocation_id = a.allocation_id
		WHERE a.team_id = $1
			AND a.owner_kind = $2
			AND a.owner_id = $3
		GROUP BY a.allocation_id, a.revision, a.team_id, a.owner_kind, a.owner_id,
			a.cluster_id, a.state, a.operation_id, a.operation_kind,
			a.operation_generation, a.operation_base_state,
			a.pod_namespace, a.pod_name, a.pod_uid,
			a.runtime_generation, a.reconcile_after, a.updated_at
	`, owner.TeamID, owner.Kind, owner.ID)
	allocation, err := scanRecoveryAllocation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{Operation: "get recovery allocation", Err: err}
	}
	return &allocation, nil
}

// ListRecoveryAllocations returns a stable keyset page of live or interrupted
// allocations for one cluster. Fully released allocations without an active
// operation are not recovery work and are omitted.
func (r *Repository) ListRecoveryAllocations(
	ctx context.Context,
	filter RecoveryAllocationFilter,
) ([]RecoveryAllocation, error) {
	if r == nil || r.pool == nil {
		return nil, &UnavailableError{
			Operation: "list recovery allocations",
			Err:       fmt.Errorf("database pool is not configured"),
		}
	}
	filter.ClusterID = strings.TrimSpace(filter.ClusterID)
	filter.TeamID = strings.TrimSpace(filter.TeamID)
	filter.OwnerKind = strings.TrimSpace(filter.OwnerKind)
	filter.AfterAllocationID = strings.TrimSpace(filter.AfterAllocationID)
	if filter.ClusterID == "" {
		return nil, fmt.Errorf("recovery allocation cluster_id is required")
	}
	limit := normalizeRecoveryPageLimit(filter.Limit)
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT allocation_id, revision, team_id, owner_kind, owner_id, cluster_id,
				state, operation_id, operation_kind, operation_generation,
				operation_base_state, pod_namespace, pod_name, pod_uid,
				runtime_generation, reconcile_after,
				COALESCE(reconcile_after <= NOW(), FALSE) AS reconcile_due,
				updated_at
			FROM quota.allocations
			WHERE cluster_id = $1
				AND ($2 = '' OR team_id = $2)
				AND ($3 = '' OR owner_kind = $3)
				AND allocation_id > $4
				AND (
					NOT $5
					OR (
						operation_id IS NOT NULL
						AND reconcile_after <= NOW()
					)
				)
				AND (state <> 'released' OR operation_id IS NOT NULL)
			ORDER BY allocation_id
			LIMIT $6
		)
		SELECT c.allocation_id, c.revision, c.team_id, c.owner_kind, c.owner_id,
			c.cluster_id, c.state, c.operation_id, c.operation_kind,
			c.operation_generation, c.operation_base_state,
			c.pod_namespace, c.pod_name, c.pod_uid,
			c.runtime_generation, c.reconcile_after, c.reconcile_due,
			c.updated_at,
			COALESCE(
				jsonb_object_agg(i.quota_key, i.committed_value)
					FILTER (WHERE i.quota_key IS NOT NULL),
				'{}'::jsonb
			),
			COALESCE(
				jsonb_object_agg(i.quota_key, i.pending_value)
					FILTER (WHERE i.quota_key IS NOT NULL
						AND i.pending_value IS NOT NULL),
				'{}'::jsonb
			)
		FROM candidates c
		LEFT JOIN quota.allocation_items i
			ON i.allocation_id = c.allocation_id
		GROUP BY c.allocation_id, c.revision, c.team_id, c.owner_kind, c.owner_id,
			c.cluster_id, c.state, c.operation_id, c.operation_kind,
			c.operation_generation, c.operation_base_state,
			c.pod_namespace, c.pod_name, c.pod_uid,
			c.runtime_generation, c.reconcile_after, c.reconcile_due,
			c.updated_at
		ORDER BY c.allocation_id
	`, filter.ClusterID, filter.TeamID, filter.OwnerKind,
		filter.AfterAllocationID, filter.OnlyDue, limit)
	if err != nil {
		return nil, &UnavailableError{Operation: "list recovery allocations", Err: err}
	}
	defer rows.Close()

	allocations := make([]RecoveryAllocation, 0, limit)
	for rows.Next() {
		allocation, err := scanRecoveryAllocation(rows)
		if err != nil {
			return nil, &UnavailableError{Operation: "scan recovery allocation", Err: err}
		}
		allocations = append(allocations, allocation)
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "iterate recovery allocations", Err: err}
	}
	return allocations, nil
}

type recoveryAllocationScanner interface {
	Scan(dest ...any) error
}

func scanRecoveryAllocation(scanner recoveryAllocationScanner) (RecoveryAllocation, error) {
	var allocation RecoveryAllocation
	var operationID sql.NullString
	var operationKind string
	var operationGeneration int64
	var reconcileAfter sql.NullTime
	var committedJSON []byte
	var pendingJSON []byte
	if err := scanner.Scan(
		&allocation.AllocationID,
		&allocation.Revision,
		&allocation.Owner.TeamID,
		&allocation.Owner.Kind,
		&allocation.Owner.ID,
		&allocation.Owner.ClusterID,
		&allocation.State,
		&operationID,
		&operationKind,
		&operationGeneration,
		&allocation.OperationBaseState,
		&allocation.Runtime.Namespace,
		&allocation.Runtime.Name,
		&allocation.Runtime.UID,
		&allocation.Runtime.Generation,
		&reconcileAfter,
		&allocation.ReconcileDue,
		&allocation.UpdatedAt,
		&committedJSON,
		&pendingJSON,
	); err != nil {
		return RecoveryAllocation{}, err
	}
	if operationID.Valid {
		allocation.Operation = &Operation{
			ID:         operationID.String,
			Kind:       operationKind,
			Generation: operationGeneration,
		}
	}
	if reconcileAfter.Valid {
		value := reconcileAfter.Time
		allocation.ReconcileAfter = &value
	}
	if err := decodeRecoveryValues(committedJSON, &allocation.Committed); err != nil {
		return RecoveryAllocation{}, fmt.Errorf("decode committed values: %w", err)
	}
	if err := decodeRecoveryValues(pendingJSON, &allocation.Pending); err != nil {
		return RecoveryAllocation{}, fmt.Errorf("decode pending values: %w", err)
	}
	return allocation, nil
}

// ListRecoveryTransfers returns only prepared transfers old enough for
// recovery in one cluster. PostgreSQL decides age from the same clock that
// wrote created_at. The stable order lets a reconciler process a bounded batch
// and repeat until the query is empty.
func (r *Repository) ListRecoveryTransfers(
	ctx context.Context,
	clusterID string,
	staleAfter time.Duration,
	limit int,
) ([]RecoveryTransfer, error) {
	if r == nil || r.pool == nil {
		return nil, &UnavailableError{
			Operation: "list recovery transfers",
			Err:       fmt.Errorf("database pool is not configured"),
		}
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return nil, fmt.Errorf("recovery transfer cluster_id is required")
	}
	if staleAfter < 0 {
		return nil, fmt.Errorf("recovery transfer stale_after must be non-negative")
	}
	limit = normalizeRecoveryPageLimit(limit)
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT o.team_id, o.operation_id, o.operation_kind,
				o.operation_generation, o.pod_namespace, o.pod_name,
				o.pod_uid, o.runtime_generation, o.created_at,
				source.team_id AS source_team_id,
				source.owner_kind AS source_owner_kind,
				source.owner_id AS source_owner_id,
				source.cluster_id AS source_cluster_id,
				destination.team_id AS destination_team_id,
				destination.owner_kind AS destination_owner_kind,
				destination.owner_id AS destination_owner_id,
				destination.cluster_id AS destination_cluster_id
			FROM quota.transfer_operations o
			JOIN quota.allocations source
				ON source.allocation_id = o.source_allocation_id
			JOIN quota.allocations destination
				ON destination.allocation_id = o.destination_allocation_id
			WHERE o.state = 'prepared'
				AND (source.cluster_id = $1 OR destination.cluster_id = $1)
				AND o.created_at
					+ ($2::bigint * INTERVAL '1 microsecond') <= NOW()
			ORDER BY o.created_at, o.team_id, o.operation_id
			LIMIT $3
		)
		SELECT c.operation_id, c.operation_kind, c.operation_generation,
			c.source_team_id, c.source_owner_kind, c.source_owner_id,
			c.source_cluster_id, c.destination_team_id,
			c.destination_owner_kind, c.destination_owner_id,
			c.destination_cluster_id, c.pod_namespace, c.pod_name,
			c.pod_uid, c.runtime_generation, c.created_at,
			COALESCE(jsonb_object_agg(i.quota_key, i.source_decrease), '{}'::jsonb),
			COALESCE(jsonb_object_agg(i.quota_key, i.destination_committed), '{}'::jsonb),
			COALESCE(jsonb_object_agg(i.quota_key, i.destination_target), '{}'::jsonb),
			COALESCE(jsonb_object_agg(i.quota_key, i.reserved_value), '{}'::jsonb)
		FROM candidates c
		JOIN quota.transfer_items i
			ON i.team_id = c.team_id AND i.operation_id = c.operation_id
		GROUP BY c.team_id, c.operation_id, c.operation_kind,
			c.operation_generation, c.source_team_id, c.source_owner_kind,
			c.source_owner_id, c.source_cluster_id,
			c.destination_team_id, c.destination_owner_kind,
			c.destination_owner_id, c.destination_cluster_id,
			c.pod_namespace, c.pod_name, c.pod_uid,
			c.runtime_generation, c.created_at
		ORDER BY c.created_at, c.team_id, c.operation_id
	`, clusterID, staleAfter.Microseconds(), limit)
	if err != nil {
		return nil, &UnavailableError{Operation: "list recovery transfers", Err: err}
	}
	defer rows.Close()

	transfers := make([]RecoveryTransfer, 0, limit)
	for rows.Next() {
		var transfer RecoveryTransfer
		var sourceDecreaseJSON []byte
		var destinationCommittedJSON []byte
		var destinationTargetJSON []byte
		var reservedJSON []byte
		if err := rows.Scan(
			&transfer.Operation.ID,
			&transfer.Operation.Kind,
			&transfer.Operation.Generation,
			&transfer.Source.TeamID,
			&transfer.Source.Kind,
			&transfer.Source.ID,
			&transfer.Source.ClusterID,
			&transfer.Destination.TeamID,
			&transfer.Destination.Kind,
			&transfer.Destination.ID,
			&transfer.Destination.ClusterID,
			&transfer.Runtime.Namespace,
			&transfer.Runtime.Name,
			&transfer.Runtime.UID,
			&transfer.Runtime.Generation,
			&transfer.CreatedAt,
			&sourceDecreaseJSON,
			&destinationCommittedJSON,
			&destinationTargetJSON,
			&reservedJSON,
		); err != nil {
			return nil, &UnavailableError{Operation: "scan recovery transfer", Err: err}
		}
		if err := decodeRecoveryValues(sourceDecreaseJSON, &transfer.SourceDecrease); err != nil {
			return nil, &UnavailableError{Operation: "decode recovery transfer source values", Err: err}
		}
		if err := decodeRecoveryValues(destinationCommittedJSON, &transfer.DestinationCommitted); err != nil {
			return nil, &UnavailableError{Operation: "decode recovery transfer committed values", Err: err}
		}
		if err := decodeRecoveryValues(destinationTargetJSON, &transfer.DestinationTarget); err != nil {
			return nil, &UnavailableError{Operation: "decode recovery transfer target values", Err: err}
		}
		if err := decodeRecoveryValues(reservedJSON, &transfer.Reserved); err != nil {
			return nil, &UnavailableError{Operation: "decode recovery transfer reserved values", Err: err}
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "iterate recovery transfers", Err: err}
	}
	return transfers, nil
}

func normalizeRecoveryPageLimit(limit int) int {
	if limit <= 0 {
		return defaultRecoveryPageLimit
	}
	if limit > maxRecoveryPageLimit {
		return maxRecoveryPageLimit
	}
	return limit
}

func decodeRecoveryValues(raw []byte, destination *Values) error {
	if len(raw) == 0 {
		*destination = make(Values)
		return nil
	}
	return json.Unmarshal(raw, destination)
}
