package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// AcquireSandboxFilesystemMount records the active ctld owner for a writable
// SandboxFilesystem branch. A filesystem is always treated as RWO.
func (r *Repository) AcquireSandboxFilesystemMount(ctx context.Context, mount *SandboxFilesystemMount, heartbeatTimeout int) error {
	if mount == nil {
		return fmt.Errorf("sandbox filesystem mount is required")
	}
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 15
	}
	mount.FilesystemID = strings.TrimSpace(mount.FilesystemID)
	mount.ClusterID = strings.TrimSpace(mount.ClusterID)
	mount.PodID = strings.TrimSpace(mount.PodID)
	if mount.FilesystemID == "" || mount.ClusterID == "" || mount.PodID == "" {
		return fmt.Errorf("filesystem_id, cluster_id and pod_id are required")
	}

	return r.WithTx(ctx, func(tx pgx.Tx) error {
		var state string
		if err := tx.QueryRow(ctx, `
			SELECT state
			FROM sandbox_filesystems
			WHERE id = $1
				AND deleted_at IS NULL
			FOR UPDATE
		`, mount.FilesystemID).Scan(&state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("lock sandbox filesystem: %w", err)
		}
		if state == SandboxFilesystemStateDeleted {
			return ErrNotFound
		}

		activeMounts, err := r.getActiveSandboxFilesystemMounts(ctx, tx, mount.FilesystemID, heartbeatTimeout)
		if err != nil {
			return err
		}
		for _, active := range activeMounts {
			if active.ClusterID == mount.ClusterID && active.PodID == mount.PodID {
				continue
			}
			return fmt.Errorf("%w: filesystem %s already mounted on another instance", ErrConflict, mount.FilesystemID)
		}
		if err := r.createSandboxFilesystemMount(ctx, tx, mount); err != nil {
			return err
		}
		return r.setSandboxFilesystemStateTx(ctx, tx, mount.FilesystemID, SandboxFilesystemStateBound)
	})
}

func (r *Repository) createSandboxFilesystemMount(ctx context.Context, db DB, mount *SandboxFilesystemMount) error {
	_, err := db.Exec(ctx, `
		INSERT INTO sandbox_filesystem_mounts (
			id, filesystem_id, cluster_id, pod_id,
			last_heartbeat, mounted_at, mount_options
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7
		)
		ON CONFLICT (filesystem_id, cluster_id, pod_id)
		DO UPDATE SET last_heartbeat = $5,
			mount_options = $7
	`,
		mount.ID, mount.FilesystemID, mount.ClusterID, mount.PodID,
		mount.LastHeartbeat, mount.MountedAt, mount.MountOptions,
	)
	if err != nil {
		return fmt.Errorf("create sandbox filesystem mount: %w", err)
	}
	return nil
}

// UpdateSandboxFilesystemMountHeartbeat refreshes an active filesystem owner lease.
func (r *Repository) UpdateSandboxFilesystemMountHeartbeat(ctx context.Context, filesystemID, clusterID, podID string) error {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE sandbox_filesystem_mounts
		SET last_heartbeat = NOW()
		WHERE filesystem_id = $1
			AND cluster_id = $2
			AND pod_id = $3
	`, filesystemID, clusterID, podID)
	if err != nil {
		return fmt.Errorf("update sandbox filesystem mount heartbeat: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteSandboxFilesystemMount releases a filesystem owner lease.
func (r *Repository) DeleteSandboxFilesystemMount(ctx context.Context, filesystemID, clusterID, podID string) error {
	return r.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			DELETE FROM sandbox_filesystem_mounts
			WHERE filesystem_id = $1
				AND cluster_id = $2
				AND pod_id = $3
		`, filesystemID, clusterID, podID); err != nil {
			return fmt.Errorf("delete sandbox filesystem mount: %w", err)
		}
		return r.setSandboxFilesystemAvailableIfUnmountedTx(ctx, tx, filesystemID)
	})
}

// GetActiveSandboxFilesystemMounts retrieves active mounts for a filesystem.
func (r *Repository) GetActiveSandboxFilesystemMounts(ctx context.Context, filesystemID string, heartbeatTimeout int) ([]*SandboxFilesystemMount, error) {
	return r.getActiveSandboxFilesystemMounts(ctx, r.pool, filesystemID, heartbeatTimeout)
}

func (r *Repository) getActiveSandboxFilesystemMounts(ctx context.Context, db DB, filesystemID string, heartbeatTimeout int) ([]*SandboxFilesystemMount, error) {
	rows, err := db.Query(ctx, `
		SELECT
			id, filesystem_id, cluster_id, pod_id,
			last_heartbeat, mounted_at, mount_options
		FROM sandbox_filesystem_mounts
		WHERE filesystem_id = $1
			AND last_heartbeat > NOW() - INTERVAL '1 second' * $2
		ORDER BY mounted_at DESC
	`, filesystemID, heartbeatTimeout)
	if err != nil {
		return nil, fmt.Errorf("query active sandbox filesystem mounts: %w", err)
	}
	defer rows.Close()

	var mounts []*SandboxFilesystemMount
	for rows.Next() {
		mount, err := scanSandboxFilesystemMount(rows)
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, mount)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active sandbox filesystem mounts: %w", err)
	}
	return mounts, nil
}

func scanSandboxFilesystemMount(rows pgx.Rows) (*SandboxFilesystemMount, error) {
	var mount SandboxFilesystemMount
	err := rows.Scan(
		&mount.ID,
		&mount.FilesystemID,
		&mount.ClusterID,
		&mount.PodID,
		&mount.LastHeartbeat,
		&mount.MountedAt,
		&mount.MountOptions,
	)
	if err != nil {
		return nil, fmt.Errorf("scan sandbox filesystem mount: %w", err)
	}
	return &mount, nil
}

// DeleteStaleSandboxFilesystemMounts removes expired filesystem owner leases.
func (r *Repository) DeleteStaleSandboxFilesystemMounts(ctx context.Context, heartbeatTimeout int) (int64, error) {
	cmdTag, err := r.pool.Exec(ctx, `
		DELETE FROM sandbox_filesystem_mounts
		WHERE last_heartbeat < NOW() - INTERVAL '1 second' * $1
	`, heartbeatTimeout)
	if err != nil {
		return 0, fmt.Errorf("delete stale sandbox filesystem mounts: %w", err)
	}
	return cmdTag.RowsAffected(), nil
}

func (r *Repository) setSandboxFilesystemAvailableIfUnmountedTx(ctx context.Context, tx pgx.Tx, filesystemID string) error {
	cmdTag, err := tx.Exec(ctx, `
		UPDATE sandbox_filesystems
		SET state = $2,
			updated_at = NOW()
		WHERE id = $1
			AND deleted_at IS NULL
			AND NOT EXISTS (
				SELECT 1
				FROM sandbox_filesystem_mounts
				WHERE filesystem_id = $1
			)
	`, filesystemID, SandboxFilesystemStateAvailable)
	if err != nil {
		return fmt.Errorf("mark sandbox filesystem available: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return nil
	}
	return nil
}

func (r *Repository) setSandboxFilesystemStateTx(ctx context.Context, tx pgx.Tx, filesystemID, state string) error {
	cmdTag, err := tx.Exec(ctx, `
		UPDATE sandbox_filesystems
		SET state = $2,
			updated_at = NOW()
		WHERE id = $1
			AND deleted_at IS NULL
	`, filesystemID, state)
	if err != nil {
		return fmt.Errorf("set sandbox filesystem state: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
