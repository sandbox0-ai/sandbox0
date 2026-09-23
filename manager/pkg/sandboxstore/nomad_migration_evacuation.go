package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

var _ nomadmigration.EvacuationStore = (*PGSandboxStore)(nil)

// Keep both nodes reserved until execution and staging custody have converged.
// Recent aborts add a five-minute node-wide retry delay, including operations
// which failed before a staging receipt. Neither rule changes capacity leases.
const nomadMigrationOccupiedNodesSQL = `SELECT DISTINCT source.cluster_id, participant.node_uid
 FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
 JOIN manager.runtime_slots source ON source.slot_id=m.source_slot_id
 JOIN manager.runtime_slots target ON target.slot_id=m.target_slot_id
 CROSS JOIN LATERAL (VALUES(source.node_uid),(target.node_uid)) participant(node_uid)
 WHERE l.phase IN ('preparing','barriered','publishing','committing')
  OR (l.phase='aborted' AND COALESCE(l.aborted_at,l.updated_at)+INTERVAL '5 minutes'>clock_timestamp())
  OR (m.staging_request IS NOT NULL AND (m.staging_source_release_receipt IS NULL OR m.staging_destination_release_receipt IS NULL))
  OR (m.capture_request IS NOT NULL AND m.capture_failure_completed_at IS NULL AND (m.source_finalization_receipt IS NULL OR (m.adoption_receipt IS NULL AND m.failure_completed_at IS NULL)))`

const nomadMigrationEvacuationSourceSQL = ` FROM manager.sandboxes s JOIN manager.runtime_slots r
 ON r.sandbox_id=s.sandbox_id AND r.cluster_id=s.cluster_id AND r.allocation_id=s.runtime_id AND r.allocation_namespace=s.runtime_namespace
 JOIN manager.runtime_node_fences f ON f.cluster_id=r.cluster_id AND f.node_id=r.node_id AND f.node_uid=r.node_uid AND f.state='draining'
 WHERE f.reason NOT LIKE 'audited-runtime-rollout:%'
 AND s.desired_state='active' AND s.deleted_at IS NULL AND s.runtime_generation>0 AND s.runtime_generation<9223372036854775807
 AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
 AND r.state='active' AND NOT r.carrier_retired AND r.heartbeat_expires_at>clock_timestamp()
 AND r.claim_runtime_assignment IS NOT NULL AND r.claim_network_policy IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM manager.sandbox_lifecycle_txns l WHERE l.sandbox_id=s.sandbox_id
  AND l.phase IN ('preparing','barriered','publishing','committing'))
 AND NOT EXISTS(SELECT 1 FROM occupied o WHERE o.cluster_id=r.cluster_id AND o.node_uid=r.node_uid)`

func (s *PGSandboxStore) ListNomadMigrationEvacuations(ctx context.Context, after string, limit int) ([]string, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 256 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	rows, err := s.pool.Query(ctx, `WITH occupied AS MATERIALIZED (`+nomadMigrationOccupiedNodesSQL+`) SELECT s.sandbox_id`+
		nomadMigrationEvacuationSourceSQL+` AND s.sandbox_id COLLATE "C">$1 COLLATE "C" ORDER BY s.sandbox_id COLLATE "C" LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ReserveNomadMigrationEvacuation starts only from current retained claim input.
// The subsequent reservation transaction rechecks the drain fence, exact writer,
// generation, assignment and node concurrency before leasing target capacity.
func (s *PGSandboxStore) ReserveNomadMigrationEvacuation(ctx context.Context, sandbox string) (bool, error) {
	var slot string
	var generation int64
	err := s.pool.QueryRow(ctx, `WITH occupied AS MATERIALIZED (`+nomadMigrationOccupiedNodesSQL+`) SELECT r.slot_id,s.runtime_generation`+
		nomadMigrationEvacuationSourceSQL+` AND s.sandbox_id=$1`, sandbox).Scan(&slot, &generation)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	inputs, err := s.GetRuntimeSlotClaimInputs(ctx, slot)
	if err != nil || inputs == nil {
		return false, err
	}
	if generation <= 0 || generation == math.MaxInt64 || inputs.Runtime.RuntimeGeneration != generation || inputs.Runtime.SandboxID != sandbox {
		return false, ErrNomadSandboxMigrationConflict
	}
	revision, err := inputs.Runtime.Revision()
	if err != nil {
		return false, err
	}
	target := inputs.Runtime
	target.RuntimeGeneration++
	assignment := runtimecontrol.MigrationAssignment{OperationID: "evacuate-" + uuid.NewString(), SourceGeneration: generation, SourceRevision: revision, Target: target}
	_, err = s.reserveNomadSandboxMigration(ctx, assignment, true)
	if errors.Is(err, ErrNomadSandboxMigrationConflict) || errors.Is(err, ErrRuntimeSlotUnavailable) || errors.Is(err, ErrNomadSandboxForkNotReady) {
		return false, nil
	}
	return err == nil, err
}

// Cluster-local admission serializes automatic source/destination pairing.
// It is a transaction lock, not a second lifecycle or a persistent leader.
// Try-locking avoids waiting while holding a source sandbox/writer lock.
func lockNomadMigrationEvacuation(ctx context.Context, tx pgx.Tx, source *RuntimeSlot, a runtimecontrol.MigrationAssignment) ([]string, bool, error) {
	var acquired bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "sandbox0-migration-evacuation/"+source.ClusterID).Scan(&acquired); err != nil {
		return nil, false, err
	}
	if !acquired {
		return nil, false, ErrNomadSandboxMigrationConflict
	}
	var state, reason string
	err := tx.QueryRow(ctx, `SELECT state,reason FROM manager.runtime_node_fences WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 FOR SHARE`, source.ClusterID, source.NodeID, source.NodeUID).Scan(&state, &reason)
	// Audited runtime upgrades own an explicit filesystem pause inventory.
	// Starting a competing migration would change its retained generation.
	if err == pgx.ErrNoRows || err == nil && (state != "draining" || strings.HasPrefix(reason, "audited-runtime-rollout:")) {
		return nil, false, ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return nil, false, err
	}
	var payload, policy *string
	if err := tx.QueryRow(ctx, `SELECT claim_runtime_assignment,claim_network_policy FROM manager.runtime_slots WHERE slot_id=$1`, source.ID).Scan(&payload, &policy); err != nil {
		return nil, false, err
	}
	if payload == nil || policy == nil {
		return nil, false, ErrNomadSandboxMigrationConflict
	}
	var assignment runtimecontrol.Assignment
	if json.Unmarshal([]byte(*payload), &assignment) != nil {
		return nil, false, ErrNomadSandboxMigrationConflict
	}
	revision, err := assignment.Revision()
	if err != nil || revision != a.SourceRevision || assignment.RuntimeGeneration != a.SourceGeneration || validateMigrationSourcePolicy(a, *policy) != nil {
		return nil, false, ErrNomadSandboxMigrationConflict
	}
	rows, err := tx.Query(ctx, `SELECT node_uid FROM (`+nomadMigrationOccupiedNodesSQL+`) occupied WHERE cluster_id=$1`, source.ClusterID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	excluded := []string{}
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, false, err
		}
		excluded = append(excluded, uid)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if slices.Contains(excluded, source.NodeUID) {
		return nil, false, ErrNomadSandboxMigrationConflict
	}
	return excluded, reason == RuntimeNodeConsolidationReason, nil
}
