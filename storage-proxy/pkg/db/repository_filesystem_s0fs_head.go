package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

// GetSandboxFilesystemS0FSCommittedHead returns the committed immutable
// manifest pointer for a sandbox filesystem.
func (r *Repository) GetSandboxFilesystemS0FSCommittedHead(ctx context.Context, filesystemID string) (*SandboxFilesystemS0FSCommittedHead, error) {
	return r.getSandboxFilesystemS0FSCommittedHead(ctx, r.pool, filesystemID, false)
}

func (r *Repository) getSandboxFilesystemS0FSCommittedHead(ctx context.Context, db DB, filesystemID string, forUpdate bool) (*SandboxFilesystemS0FSCommittedHead, error) {
	query := `
		SELECT filesystem_id, manifest_seq, checkpoint_seq, manifest_key, updated_at
		FROM sandbox_filesystem_s0fs_heads
		WHERE filesystem_id = $1
	`
	if forUpdate {
		query += " FOR UPDATE"
	}

	var head SandboxFilesystemS0FSCommittedHead
	err := db.QueryRow(ctx, query, filesystemID).Scan(
		&head.FilesystemID,
		&head.ManifestSeq,
		&head.CheckpointSeq,
		&head.ManifestKey,
		&head.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sandbox filesystem s0fs committed head: %w", err)
	}
	return &head, nil
}

// CompareAndSwapSandboxFilesystemS0FSCommittedHead advances the committed
// manifest pointer only when the current manifest sequence matches
// expectedManifestSeq. expected 0 inserts a new head when none exists.
func (r *Repository) CompareAndSwapSandboxFilesystemS0FSCommittedHead(ctx context.Context, filesystemID string, expectedManifestSeq uint64, head *SandboxFilesystemS0FSCommittedHead) error {
	if head == nil {
		return fmt.Errorf("compare and swap sandbox filesystem s0fs committed head: head is required")
	}
	if strings.TrimSpace(filesystemID) == "" {
		return fmt.Errorf("compare and swap sandbox filesystem s0fs committed head: filesystem id is required")
	}
	if head.FilesystemID == "" {
		head.FilesystemID = filesystemID
	}
	if head.FilesystemID != filesystemID {
		return fmt.Errorf("compare and swap sandbox filesystem s0fs committed head: filesystem id mismatch")
	}
	if head.ManifestSeq == 0 {
		return fmt.Errorf("compare and swap sandbox filesystem s0fs committed head: manifest sequence must be non-zero")
	}
	if head.ManifestKey == "" {
		return fmt.Errorf("compare and swap sandbox filesystem s0fs committed head: manifest key is required")
	}

	return r.WithTx(ctx, func(tx pgx.Tx) error {
		existing, err := r.getSandboxFilesystemS0FSCommittedHead(ctx, tx, filesystemID, true)
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
				INSERT INTO sandbox_filesystem_s0fs_heads (
					filesystem_id, manifest_seq, checkpoint_seq, manifest_key, updated_at
				) VALUES ($1, $2, $3, $4, $5)
			`, filesystemID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey, updatedAt)
			if err != nil {
				return fmt.Errorf("insert sandbox filesystem s0fs committed head: %w", err)
			}
			return r.updateSandboxFilesystemS0FSHeadTx(ctx, tx, filesystemID, head.ManifestKey)
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
			UPDATE sandbox_filesystem_s0fs_heads
			SET manifest_seq = $2,
				checkpoint_seq = $3,
				manifest_key = $4,
				updated_at = $5
			WHERE filesystem_id = $1
		`, filesystemID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey, updatedAt)
		if err != nil {
			return fmt.Errorf("update sandbox filesystem s0fs committed head: %w", err)
		}
		return r.updateSandboxFilesystemS0FSHeadTx(ctx, tx, filesystemID, head.ManifestKey)
	})
}

// SetSandboxFilesystemS0FSCommittedHead replaces the committed filesystem head.
// Snapshot restore uses this because restore may move the branch back to an
// older manifest sequence.
func (r *Repository) SetSandboxFilesystemS0FSCommittedHead(ctx context.Context, filesystemID string, head *SandboxFilesystemS0FSCommittedHead) error {
	if head == nil {
		return fmt.Errorf("set sandbox filesystem s0fs committed head: head is required")
	}
	if strings.TrimSpace(filesystemID) == "" {
		return fmt.Errorf("set sandbox filesystem s0fs committed head: filesystem id is required")
	}
	if head.FilesystemID == "" {
		head.FilesystemID = filesystemID
	}
	if head.FilesystemID != filesystemID {
		return fmt.Errorf("set sandbox filesystem s0fs committed head: filesystem id mismatch")
	}
	if head.ManifestSeq == 0 {
		return fmt.Errorf("set sandbox filesystem s0fs committed head: manifest sequence must be non-zero")
	}
	if head.ManifestKey == "" {
		return fmt.Errorf("set sandbox filesystem s0fs committed head: manifest key is required")
	}
	updatedAt := head.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	return r.WithTx(ctx, func(tx pgx.Tx) error {
		head.UpdatedAt = updatedAt
		return r.setSandboxFilesystemS0FSCommittedHeadTx(ctx, tx, filesystemID, head)
	})
}

func (r *Repository) setSandboxFilesystemS0FSCommittedHeadTx(ctx context.Context, tx pgx.Tx, filesystemID string, head *SandboxFilesystemS0FSCommittedHead) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO sandbox_filesystem_s0fs_heads (
			filesystem_id, manifest_seq, checkpoint_seq, manifest_key, updated_at
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (filesystem_id) DO UPDATE
		SET manifest_seq = EXCLUDED.manifest_seq,
			checkpoint_seq = EXCLUDED.checkpoint_seq,
			manifest_key = EXCLUDED.manifest_key,
			updated_at = EXCLUDED.updated_at
	`, filesystemID, int64(head.ManifestSeq), int64(head.CheckpointSeq), head.ManifestKey, head.UpdatedAt)
	if err != nil {
		return fmt.Errorf("set sandbox filesystem s0fs committed head: %w", err)
	}
	return r.updateSandboxFilesystemS0FSHeadTx(ctx, tx, filesystemID, head.ManifestKey)
}

func (r *Repository) clearSandboxFilesystemS0FSCommittedHeadTx(ctx context.Context, tx pgx.Tx, filesystemID string) error {
	_, err := tx.Exec(ctx, `
		DELETE FROM sandbox_filesystem_s0fs_heads
		WHERE filesystem_id = $1
	`, filesystemID)
	if err != nil {
		return fmt.Errorf("clear sandbox filesystem s0fs committed head: %w", err)
	}
	return nil
}

func (r *Repository) updateSandboxFilesystemS0FSHeadTx(ctx context.Context, tx pgx.Tx, filesystemID, manifestKey string) error {
	cmdTag, err := tx.Exec(ctx, `
		UPDATE sandbox_filesystems
		SET s0fs_head = $2,
			updated_at = NOW()
		WHERE id = $1
			AND deleted_at IS NULL
	`, filesystemID, manifestKey)
	if err != nil {
		return fmt.Errorf("update sandbox filesystem s0fs head: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func sandboxFilesystemS0FSHeadFromManifestKey(filesystemID, manifestKey string, updatedAt time.Time) (*SandboxFilesystemS0FSCommittedHead, error) {
	manifestKey = strings.TrimSpace(manifestKey)
	if manifestKey == "" {
		return nil, nil
	}
	seqText := strings.TrimPrefix(manifestKey, "manifests/")
	seqText = strings.TrimSuffix(seqText, ".json")
	manifestSeq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil || manifestSeq == 0 {
		return nil, fmt.Errorf("invalid sandbox filesystem s0fs head %q", manifestKey)
	}
	return &SandboxFilesystemS0FSCommittedHead{
		FilesystemID:  filesystemID,
		ManifestSeq:   manifestSeq,
		CheckpointSeq: 0,
		ManifestKey:   manifestKey,
		UpdatedAt:     updatedAt,
	}, nil
}

type SandboxFilesystemS0FSHeadStore struct {
	repo *Repository
}

func NewSandboxFilesystemS0FSHeadStore(repo *Repository) *SandboxFilesystemS0FSHeadStore {
	if repo == nil {
		return nil
	}
	return &SandboxFilesystemS0FSHeadStore{repo: repo}
}

func (s *SandboxFilesystemS0FSHeadStore) LoadCommittedHead(ctx context.Context, filesystemID string) (*s0fs.CommittedHead, error) {
	if s == nil || s.repo == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	head, err := s.repo.GetSandboxFilesystemS0FSCommittedHead(ctx, filesystemID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, s0fs.ErrCommittedHeadNotFound
		}
		return nil, err
	}
	return &s0fs.CommittedHead{
		VolumeID:      head.FilesystemID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     head.UpdatedAt,
	}, nil
}

func (s *SandboxFilesystemS0FSHeadStore) CompareAndSwapCommittedHead(ctx context.Context, filesystemID string, expectedManifestSeq uint64, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	err := s.repo.CompareAndSwapSandboxFilesystemS0FSCommittedHead(ctx, filesystemID, expectedManifestSeq, &SandboxFilesystemS0FSCommittedHead{
		FilesystemID:  head.VolumeID,
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
