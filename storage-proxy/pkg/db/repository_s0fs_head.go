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

// GetS0FSCommittedHead returns the committed immutable manifest pointer for a volume.
func (r *Repository) GetS0FSCommittedHead(ctx context.Context, volumeID string) (*S0FSCommittedHead, error) {
	return r.getS0FSCommittedHead(ctx, r.pool, volumeID, false)
}

func (r *Repository) getS0FSCommittedHead(ctx context.Context, db DB, volumeID string, forUpdate bool) (*S0FSCommittedHead, error) {
	query := `
		SELECT volume_id, manifest_seq, checkpoint_seq, manifest_key, updated_at
		FROM sandbox_volume_s0fs_heads
		WHERE volume_id = $1
	`
	if forUpdate {
		query += " FOR UPDATE"
	}

	var head S0FSCommittedHead
	err := db.QueryRow(ctx, query, volumeID).Scan(
		&head.VolumeID,
		&head.ManifestSeq,
		&head.CheckpointSeq,
		&head.ManifestKey,
		&head.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get s0fs committed head: %w", err)
	}
	return &head, nil
}

// CompareAndSwapS0FSCommittedHead advances the committed manifest pointer only
// when the current manifest sequence matches expectedManifestSeq. expected 0
// inserts a new head when none exists.
func (r *Repository) CompareAndSwapS0FSCommittedHead(ctx context.Context, volumeID string, expectedManifestSeq uint64, head *S0FSCommittedHead) error {
	if head == nil {
		return fmt.Errorf("compare and swap s0fs committed head: head is required")
	}
	if strings.TrimSpace(volumeID) == "" {
		return fmt.Errorf("compare and swap s0fs committed head: volume id is required")
	}
	if head.VolumeID == "" {
		head.VolumeID = volumeID
	}
	if head.VolumeID != volumeID {
		return fmt.Errorf("compare and swap s0fs committed head: volume id mismatch")
	}
	if head.ManifestSeq == 0 {
		return fmt.Errorf("compare and swap s0fs committed head: manifest sequence must be non-zero")
	}
	if head.ManifestKey == "" {
		return fmt.Errorf("compare and swap s0fs committed head: manifest key is required")
	}

	return r.WithTx(ctx, func(tx pgx.Tx) error {
		existing, err := r.getS0FSCommittedHead(ctx, tx, volumeID, true)
		switch {
		case errors.Is(err, ErrNotFound):
			if expectedManifestSeq != 0 {
				return ErrConflict
			}
			updatedAt := head.UpdatedAt
			if updatedAt.IsZero() {
				updatedAt = time.Now().UTC()
			}
			_, err := tx.Exec(ctx, `
				INSERT INTO sandbox_volume_s0fs_heads (
					volume_id, manifest_seq, checkpoint_seq, manifest_key, updated_at
				) VALUES ($1, $2, $3, $4, $5)
			`, volumeID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey, updatedAt)
			if err != nil {
				return fmt.Errorf("insert s0fs committed head: %w", err)
			}
			return nil
		case err != nil:
			return err
		}

		if existing.ManifestSeq != expectedManifestSeq {
			return ErrConflict
		}
		if head.ManifestSeq <= existing.ManifestSeq {
			return ErrConflict
		}

		updatedAt := head.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = time.Now().UTC()
		}
		_, err = tx.Exec(ctx, `
			UPDATE sandbox_volume_s0fs_heads
			SET manifest_seq = $2,
				checkpoint_seq = $3,
				manifest_key = $4,
				updated_at = $5
			WHERE volume_id = $1
		`, volumeID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey, updatedAt)
		if err != nil {
			return fmt.Errorf("update s0fs committed head: %w", err)
		}
		return nil
	})
}

type S0FSHeadStore struct {
	repo *Repository
}

func NewS0FSHeadStore(repo *Repository) *S0FSHeadStore {
	if repo == nil {
		return nil
	}
	return &S0FSHeadStore{repo: repo}
}

func (s *S0FSHeadStore) LoadCommittedHead(ctx context.Context, volumeID string) (*s0fs.CommittedHead, error) {
	if s == nil || s.repo == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	head, err := s.repo.GetS0FSCommittedHead(ctx, volumeID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, s0fs.ErrCommittedHeadNotFound
		}
		return nil, err
	}
	return &s0fs.CommittedHead{
		VolumeID:      head.VolumeID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     head.UpdatedAt,
	}, nil
}

func (s *S0FSHeadStore) CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expectedManifestSeq uint64, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	err := s.repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, expectedManifestSeq, &S0FSCommittedHead{
		VolumeID:      head.VolumeID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     head.UpdatedAt,
	})
	if errors.Is(err, ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}
