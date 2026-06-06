package portal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

type sandboxFilesystemHeadStore struct {
	repo *db.Repository
}

type sandboxFilesystemHeadJSON struct {
	VolumeID      string    `json:"volume_id"`
	ManifestSeq   uint64    `json:"manifest_seq"`
	CheckpointSeq uint64    `json:"checkpoint_seq"`
	ManifestKey   string    `json:"manifest_key"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func newSandboxFilesystemHeadStore(repo *db.Repository) *sandboxFilesystemHeadStore {
	if repo == nil || repo.Pool() == nil {
		return nil
	}
	return &sandboxFilesystemHeadStore{repo: repo}
}

func (s *sandboxFilesystemHeadStore) LoadCommittedHead(ctx context.Context, filesystemID string) (*s0fs.CommittedHead, error) {
	if s == nil || s.repo == nil || s.repo.Pool() == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	filesystemID = strings.TrimSpace(filesystemID)
	if filesystemID == "" {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	var raw []byte
	err := s.repo.Pool().QueryRow(ctx, `
		SELECT upperdir_head
		FROM manager.sandbox_filesystems
		WHERE filesystem_id = $1
	`, filesystemID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, s0fs.ErrCommittedHeadNotFound
		}
		return nil, fmt.Errorf("load sandbox filesystem head: %w", err)
	}
	head, err := decodeSandboxFilesystemHead(filesystemID, raw)
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	return head, nil
}

func (s *sandboxFilesystemHeadStore) CompareAndSwapCommittedHead(ctx context.Context, filesystemID string, expectedManifestSeq uint64, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil || s.repo.Pool() == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	filesystemID = strings.TrimSpace(filesystemID)
	if filesystemID == "" || head == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	if head.VolumeID == "" {
		head.VolumeID = filesystemID
	}
	if head.VolumeID != filesystemID {
		return fmt.Errorf("sandbox filesystem head volume id mismatch")
	}
	if head.ManifestSeq == 0 || strings.TrimSpace(head.ManifestKey) == "" {
		return fmt.Errorf("sandbox filesystem head is incomplete")
	}

	return s.repo.WithTx(ctx, func(tx pgx.Tx) error {
		var raw []byte
		err := tx.QueryRow(ctx, `
			SELECT upperdir_head
			FROM manager.sandbox_filesystems
			WHERE filesystem_id = $1
			FOR UPDATE
		`, filesystemID).Scan(&raw)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return s0fs.ErrCommittedHeadNotFound
			}
			return fmt.Errorf("load sandbox filesystem head for update: %w", err)
		}

		existing, err := decodeSandboxFilesystemHead(filesystemID, raw)
		if err != nil {
			return err
		}
		if !canAdvanceSandboxFilesystemHead(existing, expectedManifestSeq, head.ManifestSeq) {
			return s0fs.ErrCommittedHeadConflict
		}
		encoded, err := encodeSandboxFilesystemHead(filesystemID, head)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE manager.sandbox_filesystems
			SET upperdir_head = $2,
				updated_at = NOW()
			WHERE filesystem_id = $1
		`, filesystemID, encoded)
		if err != nil {
			return fmt.Errorf("update sandbox filesystem head: %w", err)
		}
		return nil
	})
}

func decodeSandboxFilesystemHead(filesystemID string, raw []byte) (*s0fs.CommittedHead, error) {
	if len(raw) == 0 || string(raw) == "{}" || string(raw) == "null" {
		return nil, nil
	}
	var stored sandboxFilesystemHeadJSON
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, fmt.Errorf("decode sandbox filesystem head: %w", err)
	}
	if stored.ManifestSeq == 0 || strings.TrimSpace(stored.ManifestKey) == "" {
		return nil, nil
	}
	if stored.VolumeID == "" {
		stored.VolumeID = filesystemID
	}
	if stored.VolumeID != filesystemID {
		return nil, fmt.Errorf("sandbox filesystem head volume id mismatch")
	}
	return &s0fs.CommittedHead{
		VolumeID:      stored.VolumeID,
		ManifestSeq:   stored.ManifestSeq,
		CheckpointSeq: stored.CheckpointSeq,
		ManifestKey:   stored.ManifestKey,
		UpdatedAt:     stored.UpdatedAt,
	}, nil
}

func encodeSandboxFilesystemHead(filesystemID string, head *s0fs.CommittedHead) ([]byte, error) {
	if head == nil {
		return nil, fmt.Errorf("sandbox filesystem head is required")
	}
	updatedAt := head.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	return json.Marshal(sandboxFilesystemHeadJSON{
		VolumeID:      filesystemID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     updatedAt,
	})
}

func canAdvanceSandboxFilesystemHead(existing *s0fs.CommittedHead, expectedManifestSeq, nextManifestSeq uint64) bool {
	if nextManifestSeq == 0 {
		return false
	}
	if existing == nil {
		return expectedManifestSeq == 0
	}
	return existing.ManifestSeq == expectedManifestSeq && nextManifestSeq > existing.ManifestSeq
}
