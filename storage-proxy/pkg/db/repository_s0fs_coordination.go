package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

func (r *Repository) BeginS0FSCommit(ctx context.Context, volumeID, commitID string, expected *S0FSCommittedHead, expiresAt time.Time) error {
	if strings.TrimSpace(volumeID) == "" || strings.TrimSpace(commitID) == "" || !expiresAt.After(time.Now()) {
		return fmt.Errorf("begin s0fs commit: invalid volume, commit, or expiry")
	}
	return r.WithTx(ctx, func(tx pgx.Tx) error {
		if err := r.lockS0FSVolume(ctx, tx, volumeID); err != nil {
			return err
		}
		if err := r.requireS0FSHeadIdentity(ctx, tx, volumeID, expected); err != nil {
			return err
		}
		if err := cleanupExpiredS0FSCoordination(ctx, tx, volumeID); err != nil {
			return err
		}
		var leased bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM sandbox_volume_s0fs_gc_leases WHERE volume_id = $1 AND expires_at > NOW()
		)`, volumeID).Scan(&leased); err != nil {
			return err
		}
		if leased {
			return ErrConflict
		}
		baseGeneration := uint64(0)
		if expected != nil {
			baseGeneration = expected.Generation
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO sandbox_volume_s0fs_commit_intents (volume_id, commit_id, base_generation, expires_at)
			VALUES ($1, $2, $3, $4)
		`, volumeID, commitID, int64(baseGeneration), expiresAt.UTC())
		if err != nil {
			return fmt.Errorf("insert s0fs commit intent: %w", err)
		}
		return nil
	})
}

func (r *Repository) RenewS0FSCommit(ctx context.Context, volumeID, commitID string, expiresAt time.Time) error {
	command, err := r.pool.Exec(ctx, `
		UPDATE sandbox_volume_s0fs_commit_intents SET expires_at = $3
		WHERE volume_id = $1 AND commit_id = $2 AND expires_at > NOW()
	`, volumeID, commitID, expiresAt.UTC())
	if err != nil {
		return fmt.Errorf("renew s0fs commit intent: %w", err)
	}
	if command.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (r *Repository) AbortS0FSCommit(ctx context.Context, volumeID, commitID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM sandbox_volume_s0fs_commit_intents WHERE volume_id = $1 AND commit_id = $2`, volumeID, commitID)
	return err
}

func (r *Repository) AcquireS0FSGarbageCollection(ctx context.Context, volumeID, token string, expected *S0FSCommittedHead, expiresAt time.Time) error {
	if expected == nil || strings.TrimSpace(token) == "" || !expiresAt.After(time.Now()) {
		return ErrConflict
	}
	return r.WithTx(ctx, func(tx pgx.Tx) error {
		if err := r.lockS0FSVolume(ctx, tx, volumeID); err != nil {
			return err
		}
		if err := r.requireS0FSHeadIdentity(ctx, tx, volumeID, expected); err != nil {
			return err
		}
		if err := cleanupExpiredS0FSCoordination(ctx, tx, volumeID); err != nil {
			return err
		}
		var committing bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM sandbox_volume_s0fs_commit_intents WHERE volume_id = $1 AND expires_at > NOW()
		)`, volumeID).Scan(&committing); err != nil {
			return err
		}
		if committing {
			return ErrConflict
		}
		command, err := tx.Exec(ctx, `
			INSERT INTO sandbox_volume_s0fs_gc_leases (volume_id, token, head_generation, expires_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (volume_id) DO UPDATE
			SET token = EXCLUDED.token, head_generation = EXCLUDED.head_generation,
			    expires_at = EXCLUDED.expires_at, created_at = NOW()
			WHERE sandbox_volume_s0fs_gc_leases.expires_at <= NOW()
		`, volumeID, token, int64(expected.Generation), expiresAt.UTC())
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *Repository) ReleaseS0FSGarbageCollection(ctx context.Context, volumeID, token string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM sandbox_volume_s0fs_gc_leases WHERE volume_id = $1 AND token = $2`, volumeID, token)
	return err
}

func (r *Repository) ValidateS0FSGarbageCollection(ctx context.Context, volumeID, token string, expected *S0FSCommittedHead) error {
	if expected == nil {
		return ErrConflict
	}
	var valid bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1
		FROM sandbox_volume_s0fs_gc_leases lease
		JOIN sandbox_volume_s0fs_heads head ON head.volume_id = lease.volume_id
		WHERE lease.volume_id = $1 AND lease.token = $2 AND lease.expires_at > NOW()
		  AND head.manifest_seq = $3 AND head.checkpoint_seq = $4
		  AND head.manifest_key = $5 AND head.manifest_digest = $6
		  AND head.commit_id = $7 AND head.generation = $8
	)`, volumeID, token, int64(expected.ManifestSeq), int64(expected.CheckpointSeq),
		expected.ManifestKey, expected.ManifestDigest, expected.CommitID, int64(expected.Generation)).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return ErrConflict
	}
	return nil
}

func (r *Repository) StageS0FSGarbageCollection(ctx context.Context, volumeID, token string, expected *S0FSCommittedHead, candidates []string, deleteAfter time.Time) ([]string, error) {
	var due []string
	err := r.WithTx(ctx, func(tx pgx.Tx) error {
		if err := r.lockS0FSVolume(ctx, tx, volumeID); err != nil {
			return err
		}
		if err := r.requireS0FSHeadIdentity(ctx, tx, volumeID, expected); err != nil {
			return err
		}
		var leaseValid bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM sandbox_volume_s0fs_gc_leases
			WHERE volume_id = $1 AND token = $2 AND head_generation = $3 AND expires_at > NOW()
		)`, volumeID, token, int64(expected.Generation)).Scan(&leaseValid); err != nil {
			return err
		}
		if !leaseValid {
			return ErrConflict
		}
		if len(candidates) == 0 {
			_, err := tx.Exec(ctx, `DELETE FROM sandbox_volume_s0fs_gc_tombstones WHERE volume_id = $1`, volumeID)
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM sandbox_volume_s0fs_gc_tombstones
			WHERE volume_id = $1 AND NOT (object_key = ANY($2::text[]))
		`, volumeID, candidates); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sandbox_volume_s0fs_gc_tombstones (volume_id, object_key, delete_after)
			SELECT $1, candidate, $3 FROM unnest($2::text[]) AS candidate
			ON CONFLICT (volume_id, object_key) DO NOTHING
		`, volumeID, candidates, deleteAfter.UTC()); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT object_key FROM sandbox_volume_s0fs_gc_tombstones
			WHERE volume_id = $1 AND object_key = ANY($2::text[]) AND delete_after <= NOW()
			ORDER BY object_key
		`, volumeID, candidates)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				return err
			}
			due = append(due, key)
		}
		return rows.Err()
	})
	return due, err
}

func (r *Repository) lockS0FSVolume(ctx context.Context, tx pgx.Tx, volumeID string) error {
	var locked string
	err := tx.QueryRow(ctx, `SELECT id FROM sandbox_volumes WHERE id = $1 FOR UPDATE`, volumeID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func (r *Repository) requireS0FSHeadIdentity(ctx context.Context, tx pgx.Tx, volumeID string, expected *S0FSCommittedHead) error {
	current, err := r.getS0FSCommittedHead(ctx, tx, volumeID, true)
	if errors.Is(err, ErrNotFound) && expected == nil {
		return nil
	}
	if err != nil || !sameS0FSCommittedHeadIdentity(current, expected) {
		return ErrConflict
	}
	return nil
}

func cleanupExpiredS0FSCoordination(ctx context.Context, tx pgx.Tx, volumeID string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM sandbox_volume_s0fs_commit_intents WHERE volume_id = $1 AND expires_at <= NOW()`, volumeID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `DELETE FROM sandbox_volume_s0fs_gc_leases WHERE volume_id = $1 AND expires_at <= NOW()`, volumeID)
	return err
}

func s0fsHeadToDB(head *s0fs.CommittedHead) *S0FSCommittedHead {
	if head == nil {
		return nil
	}
	return &S0FSCommittedHead{
		VolumeID: head.VolumeID, ManifestSeq: head.ManifestSeq, CheckpointSeq: head.CheckpointSeq,
		ManifestKey: head.ManifestKey, ManifestDigest: head.ManifestDigest,
		CommitID: head.CommitID, Generation: head.Generation, UpdatedAt: head.UpdatedAt,
	}
}
