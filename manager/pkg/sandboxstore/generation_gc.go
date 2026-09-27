package sandboxstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Ancestry protects incomplete inventories, but is not an eternal root after
// all live descendants have registered their actual mapping dependencies.
const rootFSGenerationRetainedSQL = `
	EXISTS (SELECT 1 FROM manager.rootfs_filesystems filesystem WHERE filesystem.head_generation_id=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.rootfs_snapshots snapshot WHERE snapshot.head_generation_id=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.rootfs_head_rollbacks rollback WHERE rollback.old_generation_id=generation.generation_id OR rollback.new_generation_id=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.runtime_slots slot WHERE slot.source_generation_ref=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.rootfs_writer_grants writer WHERE writer.initial_generation_id=generation.generation_id AND writer.state IN ('issued','consumed','retiring'))
	OR EXISTS (SELECT 1 FROM manager.sandbox_lifecycle_txns lifecycle WHERE lifecycle.phase NOT IN ('committed','aborted') AND
		(lifecycle.expected_generation_id=generation.generation_id OR lifecycle.prepared_generation_id=generation.generation_id OR lifecycle.target_generation_id=generation.generation_id))
	OR EXISTS (SELECT 1 FROM manager.rootfs_running_forks fork WHERE fork.source_generation_id=generation.generation_id OR fork.checkpoint_generation_id=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.rootfs_running_template_captures capture WHERE capture.source_generation_id=generation.generation_id OR capture.checkpoint_generation_id=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs checkpoint WHERE checkpoint.generation_id=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_forks fork WHERE fork.generation_ref=generation.generation_id)
	OR EXISTS (SELECT 1 FROM manager.rootfs_materialization_members member WHERE member.generation_id=generation.generation_id)
`

// DeleteUnreferencedRootFSGenerations retires a bounded number of obsolete
// versions even while their filesystem remains bound to a sandbox. Real
// descriptor dependencies are already inventoried on retained descendants.
func (s *PGSandboxStore) DeleteUnreferencedRootFSGenerations(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	limit = normalizeRootFSObjectLimit(limit)
	total := 0
	for total < limit {
		deleted, err := s.deleteUnreferencedRootFSGenerationLeaves(ctx, strings.TrimSpace(teamID), limit-total)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted == 0 {
			break
		}
	}
	return total, nil
}

// Release only historical fork storage; its immutable generation ID and mode
// remain available for exact retries. Current heads and other image owners have
// independent FKs. The row lock and insertion guards serialize release/reuse.
func releaseRootFSCheckpointForkStorage(ctx context.Context, tx pgx.Tx, sandboxID, teamID string, limit int) error {
	_, err := tx.Exec(ctx, `WITH candidates AS (
		SELECT fork.operation_id FROM manager.sandbox_runtime_checkpoint_forks fork
		JOIN manager.sandbox_lifecycle_txns lifecycle ON lifecycle.txn_id=fork.operation_id
		JOIN manager.sandboxes target ON target.sandbox_id=fork.target_sandbox_id
		WHERE fork.generation_ref IS NOT NULL AND lifecycle.phase='committed'
		AND ($1='' OR fork.target_sandbox_id=$1) AND ($2='' OR target.team_id=$2)
		AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs reference
			WHERE reference.sandbox_id=fork.target_sandbox_id AND reference.checkpoint_id=fork.checkpoint_id)
		ORDER BY fork.created_at,fork.operation_id LIMIT $3 FOR UPDATE OF fork SKIP LOCKED
	) UPDATE manager.sandbox_runtime_checkpoint_forks fork SET generation_ref=NULL,storage_released_at=clock_timestamp()
	FROM candidates WHERE candidates.operation_id=fork.operation_id`, sandboxID, teamID, limit)
	return err
}

// Old binaries could leave a deleted origin's head behind when a fork retained
// its filesystem row. Require durable owner deletion evidence, rather than
// treating every temporarily unbound filesystem as abandoned.
func (s *PGSandboxStore) releaseRootFSHistoricalCustody(ctx context.Context, teamID string, limit int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := releaseRootFSCheckpointForkStorage(ctx, tx, "", teamID, limit); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `WITH candidates AS (
		SELECT slot.slot_id FROM manager.runtime_slots slot
		JOIN manager.rootfs_generations generation ON generation.generation_id=slot.source_generation_ref
		JOIN manager.rootfs_filesystems filesystem ON filesystem.filesystem_id=generation.filesystem_id
		WHERE slot.state='terminal' AND octet_length(slot.terminal_proof_digest)=32
		AND ($1='' OR filesystem.team_id=$1)
		AND (slot.resource_lease_id IS NULL OR EXISTS (SELECT 1 FROM manager.runtime_resource_leases lease
			WHERE lease.lease_id=slot.resource_lease_id AND lease.lease_state='released' AND lease.released_at IS NOT NULL))
		AND (slot.writer_grant_id IS NULL OR EXISTS (SELECT 1 FROM manager.rootfs_writer_grants writer
			WHERE writer.grant_id=slot.writer_grant_id AND writer.state IN ('retired','canceled')))
		ORDER BY slot.terminal_at,slot.slot_id LIMIT $2 FOR UPDATE OF slot SKIP LOCKED
	) UPDATE manager.runtime_slots slot SET source_generation_ref=NULL,updated_at=NOW()
	FROM candidates WHERE candidates.slot_id=slot.slot_id`, teamID, limit); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `WITH candidates AS (
		SELECT filesystem.filesystem_id FROM manager.rootfs_filesystems filesystem
		WHERE filesystem.head_generation_id IS NOT NULL AND ($1='' OR filesystem.team_id=$1)
		AND (EXISTS (SELECT 1 FROM manager.sandboxes owner WHERE owner.sandbox_id=filesystem.filesystem_id AND owner.desired_state='deleted')
			OR EXISTS (SELECT 1 FROM manager.rootfs_writer_grants writer JOIN manager.sandboxes owner ON owner.sandbox_id=writer.sandbox_id
				WHERE writer.filesystem_id=filesystem.filesystem_id AND owner.desired_state='deleted'))
		AND NOT EXISTS (SELECT 1 FROM manager.sandbox_rootfs_bindings binding WHERE binding.filesystem_id=filesystem.filesystem_id)
		AND NOT EXISTS (SELECT 1 FROM manager.rootfs_writer_grants writer WHERE writer.filesystem_id=filesystem.filesystem_id AND writer.state IN ('issued','consumed','retiring'))
		AND NOT EXISTS (SELECT 1 FROM manager.rootfs_running_template_captures capture WHERE capture.source_filesystem_id=filesystem.filesystem_id OR capture.target_filesystem_id=filesystem.filesystem_id)
		AND NOT EXISTS (SELECT 1 FROM manager.rootfs_running_forks fork WHERE fork.source_filesystem_id=filesystem.filesystem_id OR fork.target_filesystem_id=filesystem.filesystem_id)
		ORDER BY filesystem.updated_at,filesystem.filesystem_id LIMIT $2 FOR UPDATE OF filesystem SKIP LOCKED
	) UPDATE manager.rootfs_filesystems filesystem SET head_generation_id=NULL,updated_at=NOW()
	FROM candidates WHERE candidates.filesystem_id=filesystem.filesystem_id`, teamID, limit)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGSandboxStore) deleteUnreferencedRootFSGenerationLeaves(ctx context.Context, teamID string, limit int) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT generation.generation_id FROM manager.rootfs_generations generation
		JOIN manager.rootfs_filesystems filesystem USING (filesystem_id)
		WHERE ($1='' OR filesystem.team_id=$1)
		AND NOT generation.storage_inventory_required
		AND NOT (`+rootFSGenerationRetainedSQL+`)
		AND NOT EXISTS (SELECT 1 FROM manager.rootfs_generation_dependencies dependency
			WHERE dependency.parent_generation_id=generation.generation_id)
		ORDER BY generation.created_at,generation.generation_id LIMIT $2
		FOR UPDATE OF generation SKIP LOCKED`, teamID, limit)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	objectRows, err := tx.Query(ctx, `SELECT DISTINCT object_key FROM manager.rootfs_generation_materialization_objects
		WHERE generation_id=ANY($1::TEXT[]) ORDER BY object_key`, ids)
	if err != nil {
		return 0, err
	}
	var keys []string
	for objectRows.Next() {
		var key string
		if err := objectRows.Scan(&key); err != nil {
			objectRows.Close()
			return 0, err
		}
		keys = append(keys, key)
	}
	objectRows.Close()
	if err := objectRows.Err(); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM manager.rootfs_generations WHERE generation_id=ANY($1::TEXT[])`, ids); err != nil {
		return 0, fmt.Errorf("delete obsolete rootfs generations: %w", err)
	}
	for _, key := range keys {
		if _, err := releaseUnreferencedRootFSMaterializationObject(ctx, tx, key, teamID); err != nil {
			return 0, err
		}
	}
	return len(ids), tx.Commit(ctx)
}
