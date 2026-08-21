package sandboxstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DeleteReleasedNomadTemplateCaptures removes unbound capture filesystems only
// after their template snapshot and every derived sandbox filesystem are gone.
// This keeps template deletion asynchronous without leaking generation or
// materialization metadata.
func (s *PGSandboxStore) DeleteReleasedNomadTemplateCaptures(
	ctx context.Context,
	teamID string,
	limit int,
) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = defaultRootFSObjectDeleteLimit
	}
	if limit > MaxRootFSObjectDeleteLimit {
		limit = MaxRootFSObjectDeleteLimit
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin released template capture GC tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT c.operation_id, c.target_filesystem_id, c.team_id
		FROM manager.rootfs_running_template_captures c
		JOIN manager.rootfs_filesystems f ON f.filesystem_id = c.target_filesystem_id
		JOIN manager.rootfs_generations generation
		  ON generation.generation_id = f.head_generation_id
		 AND generation.filesystem_id = f.filesystem_id
		WHERE c.state = 'published'
		  AND c.cancel_reason <> ''
		  AND ($1 = '' OR c.team_id = $1)
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_snapshots s WHERE s.snapshot_id = c.snapshot_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.sandbox_rootfs_bindings b WHERE b.filesystem_id = c.target_filesystem_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_filesystems child
			WHERE child.source_filesystem_id = c.target_filesystem_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.runtime_slots slot WHERE slot.filesystem_id = c.target_filesystem_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_writer_grants writer_grant
			WHERE writer_grant.filesystem_id = c.target_filesystem_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_generations other
			WHERE other.filesystem_id = c.target_filesystem_id
			  AND other.generation_id <> generation.generation_id
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM manager.rootfs_generations child_generation
			WHERE child_generation.parent_generation_id = generation.generation_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_head_rollbacks rollback
			WHERE rollback.filesystem_id = c.target_filesystem_id
			   OR rollback.old_generation_id = generation.generation_id
			   OR rollback.new_generation_id = generation.generation_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_running_forks fork
			WHERE fork.source_filesystem_id = c.target_filesystem_id
			   OR fork.target_filesystem_id = c.target_filesystem_id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM manager.rootfs_materialization_members member
			WHERE member.generation_id = generation.generation_id
			  AND member.state = 'uploading'
		  )
		ORDER BY c.updated_at ASC
		LIMIT $2
		FOR UPDATE OF c, f, generation SKIP LOCKED
	`, strings.TrimSpace(teamID), limit)
	if err != nil {
		return 0, fmt.Errorf("list released template captures: %w", err)
	}
	type candidate struct{ operationID, filesystemID, teamID string }
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.operationID, &item.filesystemID, &item.teamID); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	for _, item := range candidates {
		objectRows, err := tx.Query(ctx, `
			SELECT DISTINCT object_key
			FROM manager.rootfs_generation_materialization_objects objects
			JOIN manager.rootfs_generations generation USING (generation_id)
			WHERE generation.filesystem_id = $1
			ORDER BY object_key
		`, item.filesystemID)
		if err != nil {
			return 0, fmt.Errorf("list released template capture objects: %w", err)
		}
		var objectKeys []string
		for objectRows.Next() {
			var objectKey string
			if err := objectRows.Scan(&objectKey); err != nil {
				objectRows.Close()
				return 0, err
			}
			objectKeys = append(objectKeys, objectKey)
		}
		if err := objectRows.Err(); err != nil {
			objectRows.Close()
			return 0, err
		}
		objectRows.Close()
		if _, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_filesystems SET head_generation_id = NULL, updated_at = NOW()
			WHERE filesystem_id = $1
		`, item.filesystemID); err != nil {
			return 0, fmt.Errorf("clear released template capture head: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_generations WHERE filesystem_id = $1
		`, item.filesystemID); err != nil {
			return 0, fmt.Errorf("delete released template capture generations: %w", err)
		}
		for _, objectKey := range objectKeys {
			if _, err := releaseUnreferencedRootFSMaterializationObject(ctx, tx, objectKey, item.teamID); err != nil {
				return 0, err
			}
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_running_template_captures WHERE operation_id = $1
		`, item.operationID); err != nil {
			return 0, fmt.Errorf("delete released template capture audit: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_filesystems WHERE filesystem_id = $1
		`, item.filesystemID); err != nil {
			return 0, fmt.Errorf("delete released template capture filesystem: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit released template capture GC: %w", err)
	}
	return len(candidates), nil
}
