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
		SELECT volume_id, manifest_seq, checkpoint_seq, manifest_key,
		       manifest_digest, commit_id, generation, updated_at
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
		&head.ManifestDigest,
		&head.CommitID,
		&head.Generation,
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
// when the complete current identity matches expected. A nil expected head
// inserts generation one when no head exists.
func (r *Repository) CompareAndSwapS0FSCommittedHead(ctx context.Context, volumeID string, expected, head *S0FSCommittedHead) error {
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
	if head.Generation == 0 {
		return fmt.Errorf("compare and swap s0fs committed head: generation must be non-zero")
	}

	return r.WithTx(ctx, func(tx pgx.Tx) error {
		// Serialize the empty-head insert path with commit/GC coordination. A
		// SELECT FOR UPDATE on the head alone cannot lock a row that does not yet
		// exist, so concurrent first commits would otherwise surface a raw unique
		// constraint error instead of an exact-CAS conflict.
		if err := r.lockS0FSVolume(ctx, tx, volumeID); err != nil {
			return err
		}
		existing, err := r.getS0FSCommittedHead(ctx, tx, volumeID, true)
		switch {
		case errors.Is(err, ErrNotFound):
			if expected != nil || head.Generation != 1 {
				return ErrConflict
			}
			if err := requireS0FSCommitIntent(ctx, tx, volumeID, head.CommitID, 0); err != nil {
				return err
			}
			updatedAt := head.UpdatedAt
			if updatedAt.IsZero() {
				updatedAt = time.Now().UTC()
			}
			_, err := tx.Exec(ctx, `
				INSERT INTO sandbox_volume_s0fs_heads (
					volume_id, manifest_seq, checkpoint_seq, manifest_key,
					manifest_digest, commit_id, generation, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, volumeID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey,
				head.ManifestDigest, head.CommitID, int64(head.Generation), updatedAt)
			if err != nil {
				return fmt.Errorf("insert s0fs committed head: %w", err)
			}
			if err := deleteS0FSCommitIntent(ctx, tx, volumeID, head.CommitID); err != nil {
				return err
			}
			return nil
		case err != nil:
			return err
		}

		if !sameS0FSCommittedHeadIdentity(existing, expected) {
			return ErrConflict
		}
		if head.ManifestSeq <= existing.ManifestSeq || head.Generation != existing.Generation+1 {
			return ErrConflict
		}
		if err := requireS0FSCommitIntent(ctx, tx, volumeID, head.CommitID, existing.Generation); err != nil {
			return err
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
				manifest_digest = $5,
				commit_id = $6,
				generation = $7,
				updated_at = $8
			WHERE volume_id = $1
		`, volumeID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey,
			head.ManifestDigest, head.CommitID, int64(head.Generation), updatedAt)
		if err != nil {
			return fmt.Errorf("update s0fs committed head: %w", err)
		}
		if err := deleteS0FSCommitIntent(ctx, tx, volumeID, head.CommitID); err != nil {
			return err
		}
		return nil
	})
}

func requireS0FSCommitIntent(ctx context.Context, tx pgx.Tx, volumeID, commitID string, baseGeneration uint64) error {
	if commitID == "" {
		return nil
	}
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM sandbox_volume_s0fs_commit_intents
		WHERE volume_id = $1 AND commit_id = $2 AND base_generation = $3 AND expires_at > NOW()
	) AND NOT EXISTS (
		SELECT 1 FROM sandbox_volume_s0fs_gc_leases WHERE volume_id = $1 AND expires_at > NOW()
	)`, volumeID, commitID, int64(baseGeneration)).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return ErrConflict
	}
	return nil
}

func deleteS0FSCommitIntent(ctx context.Context, tx pgx.Tx, volumeID, commitID string) error {
	if commitID == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM sandbox_volume_s0fs_commit_intents WHERE volume_id = $1 AND commit_id = $2`, volumeID, commitID)
	return err
}

func sameS0FSCommittedHeadIdentity(left, right *S0FSCommittedHead) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.VolumeID == right.VolumeID &&
		left.ManifestSeq == right.ManifestSeq &&
		left.CheckpointSeq == right.CheckpointSeq &&
		left.ManifestKey == right.ManifestKey &&
		left.ManifestDigest == right.ManifestDigest &&
		left.CommitID == right.CommitID &&
		left.Generation == right.Generation
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
		VolumeID:       head.VolumeID,
		ManifestSeq:    head.ManifestSeq,
		CheckpointSeq:  head.CheckpointSeq,
		ManifestKey:    head.ManifestKey,
		ManifestDigest: head.ManifestDigest,
		CommitID:       head.CommitID,
		Generation:     head.Generation,
		UpdatedAt:      head.UpdatedAt,
	}, nil
}

func (s *S0FSHeadStore) CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expected, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	var expectedDB *S0FSCommittedHead
	if expected != nil {
		expectedDB = &S0FSCommittedHead{
			VolumeID: expected.VolumeID, ManifestSeq: expected.ManifestSeq, CheckpointSeq: expected.CheckpointSeq,
			ManifestKey: expected.ManifestKey, ManifestDigest: expected.ManifestDigest, CommitID: expected.CommitID, Generation: expected.Generation,
		}
	}
	err := s.repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, expectedDB, &S0FSCommittedHead{
		VolumeID:       head.VolumeID,
		ManifestSeq:    head.ManifestSeq,
		CheckpointSeq:  head.CheckpointSeq,
		ManifestKey:    head.ManifestKey,
		ManifestDigest: head.ManifestDigest,
		CommitID:       head.CommitID,
		Generation:     head.Generation,
		UpdatedAt:      head.UpdatedAt,
	})
	if errors.Is(err, ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

func (s *S0FSHeadStore) BeginCommit(ctx context.Context, volumeID, commitID string, expected *s0fs.CommittedHead, expiresAt time.Time) error {
	err := s.repo.BeginS0FSCommit(ctx, volumeID, commitID, s0fsHeadToDB(expected), expiresAt)
	if errors.Is(err, ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

func (s *S0FSHeadStore) RenewCommit(ctx context.Context, volumeID, commitID string, expiresAt time.Time) error {
	err := s.repo.RenewS0FSCommit(ctx, volumeID, commitID, expiresAt)
	if errors.Is(err, ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

func (s *S0FSHeadStore) AbortCommit(ctx context.Context, volumeID, commitID string) error {
	return s.repo.AbortS0FSCommit(ctx, volumeID, commitID)
}

func (s *S0FSHeadStore) AcquireGarbageCollection(ctx context.Context, volumeID, token string, expected *s0fs.CommittedHead, expiresAt time.Time) error {
	err := s.repo.AcquireS0FSGarbageCollection(ctx, volumeID, token, s0fsHeadToDB(expected), expiresAt)
	if errors.Is(err, ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

func (s *S0FSHeadStore) ReleaseGarbageCollection(ctx context.Context, volumeID, token string) error {
	return s.repo.ReleaseS0FSGarbageCollection(ctx, volumeID, token)
}

func (s *S0FSHeadStore) ValidateGarbageCollection(ctx context.Context, volumeID, token string, expected *s0fs.CommittedHead) error {
	err := s.repo.ValidateS0FSGarbageCollection(ctx, volumeID, token, s0fsHeadToDB(expected))
	if errors.Is(err, ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

func (s *S0FSHeadStore) StageGarbageCollection(ctx context.Context, volumeID, token string, expected *s0fs.CommittedHead, candidates []string, deleteAfter time.Time) ([]string, error) {
	due, err := s.repo.StageS0FSGarbageCollection(ctx, volumeID, token, s0fsHeadToDB(expected), candidates, deleteAfter)
	if errors.Is(err, ErrConflict) {
		return nil, s0fs.ErrCommittedHeadConflict
	}
	return due, err
}
